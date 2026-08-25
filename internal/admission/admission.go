// Package admission owns durable release source admission and the shared MRF
// materialization-slot scheduler. PostgreSQL is authoritative; River only
// carries work after a source has been selected and has a slot.
package admission

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

const (
	TargetUnset   = "unset"
	TargetNumeric = "numeric"
	TargetAll     = "all"
)

type Target struct {
	Kind  string
	Count int64
}

type SetResult struct {
	PayerID          string `json:"payer_id"`
	CollectionMonth  string `json:"collection_month"`
	OldTarget        string `json:"old_target"`
	NewTarget        string `json:"new_target"`
	NewlySelected    int64  `json:"newly_selected"`
	KnownSources     int64  `json:"known_sources"`
	SelectedSources  int64  `json:"selected_sources"`
	HeldSlots        int64  `json:"held_slots"`
	ResidentCapacity int64  `json:"resident_capacity"`
}

type Runtime struct {
	ArtifactRoot     string
	ResidentCapacity int64
}

// SourceAdmitted reports whether a source is both selected for at least one
// building release and currently owns the shared resident slot. MRF workers
// use this guard so stale jobs from before admission cannot consume capacity.
func SourceAdmitted(ctx context.Context, tx pgx.Tx, sourceID int64) (bool, error) {
	if tx == nil || sourceID <= 0 {
		return false, jobs.Failure(jobs.FailureInvalidArguments)
	}
	var admitted bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM mrfpipeline.monthly_release_mrf_sources a
    WHERE a.mrf_source_id = $1
) AND EXISTS (
    SELECT 1 FROM mrfpipeline.mrf_materialization_slots s
    WHERE s.mrf_source_id = $1
)`, sourceID).Scan(&admitted); err != nil {
		return false, dbFailure(ctx, err)
	}
	return admitted, nil
}

func FormatTarget(target Target) string {
	switch target.Kind {
	case TargetAll:
		return TargetAll
	case TargetNumeric:
		return fmt.Sprintf("%d", target.Count)
	default:
		return TargetUnset
	}
}

func ParseTarget(kind string, count *int64) (Target, error) {
	switch kind {
	case TargetUnset:
		if count != nil {
			return Target{}, jobs.Failure(jobs.FailureDomainInvariant)
		}
		return Target{Kind: TargetUnset}, nil
	case TargetAll:
		if count != nil {
			return Target{}, jobs.Failure(jobs.FailureDomainInvariant)
		}
		return Target{Kind: TargetAll}, nil
	case TargetNumeric:
		if count == nil || *count <= 0 {
			return Target{}, jobs.Failure(jobs.FailureDomainInvariant)
		}
		return Target{Kind: TargetNumeric, Count: *count}, nil
	default:
		return Target{}, jobs.Failure(jobs.FailureDomainInvariant)
	}
}

// ConfigureRuntime stores the database-authoritative capacity for one
// artifact root. It is called only by the control role while it holds the
// control lease.
func ConfigureRuntime(ctx context.Context, pool *pgxpool.Pool, root string, capacity int64) (Runtime, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return Runtime{}, err
	}
	if pool == nil || root == "" || capacity <= 0 {
		return Runtime{}, jobs.Failure(jobs.FailureInvalidArguments)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return Runtime{}, dbFailure(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var storedRoot string
	var storedCapacity int64
	err = tx.QueryRow(ctx, `
SELECT artifact_root, resident_capacity
FROM mrfpipeline.pipeline_runtime
WHERE id = true
FOR UPDATE`).Scan(&storedRoot, &storedCapacity)
	if errors.Is(err, pgx.ErrNoRows) {
		var held int64
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.mrf_materialization_slots`).Scan(&held); err != nil {
			return Runtime{}, dbFailure(ctx, err)
		}
		if held > capacity {
			return Runtime{}, jobs.Failure(jobs.FailureCapacityBelowHeld)
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO mrfpipeline.pipeline_runtime (artifact_root, resident_capacity)
VALUES ($1, $2)`, root, capacity); err != nil {
			return Runtime{}, dbFailure(ctx, err)
		}
	} else if err != nil {
		return Runtime{}, dbFailure(ctx, err)
	} else {
		var held int64
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.mrf_materialization_slots`).Scan(&held); err != nil {
			return Runtime{}, dbFailure(ctx, err)
		}
		if storedRoot != root {
			return Runtime{}, jobs.Failure(jobs.FailureRuntimeRootMismatch)
		}
		if held > capacity {
			return Runtime{}, jobs.Failure(jobs.FailureCapacityBelowHeld)
		}
		if storedCapacity != capacity {
			if _, err := tx.Exec(ctx, `
UPDATE mrfpipeline.pipeline_runtime
SET resident_capacity = $1, updated_at = transaction_timestamp()
WHERE id = true`, capacity); err != nil {
				return Runtime{}, dbFailure(ctx, err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Runtime{}, dbFailure(ctx, err)
	}
	return Runtime{ArtifactRoot: root, ResidentCapacity: capacity}, nil
}

// NormalizePending makes the admission boundary explicit after upgrading from
// the pre-Story-21 schema. Sources that were never selected are blocked before
// reconciliation can recreate their old MRF jobs. Selected sources retain
// their stage state and are repaired by ScheduleWaiting.
func NormalizePending(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return dbFailure(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_sources s
SET download_status = 'blocked', download_river_job_id = NULL,
    parse_status = CASE WHEN s.parse_status = 'pending' THEN 'blocked' ELSE s.parse_status END,
    parse_river_job_id = CASE WHEN s.parse_status = 'pending' THEN NULL ELSE s.parse_river_job_id END,
    updated_at = transaction_timestamp()
WHERE s.download_status = 'pending'
  AND s.parse_status IN ('blocked', 'pending')
  AND NOT EXISTS (
      SELECT 1 FROM mrfpipeline_river.river_job j
      WHERE j.id = s.download_river_job_id
        AND j.kind = 'mrf.download'
        AND j.state = 'retryable'
  )
  AND NOT EXISTS (
      SELECT 1 FROM mrfpipeline.monthly_release_mrf_sources a
      WHERE a.mrf_source_id = s.id
  )`); err != nil {
		return dbFailure(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return dbFailure(ctx, err)
	}
	return nil
}

// BackfillSlots accounts for retained raw or parser staging artifacts that
// predate the durable slot table. It is deliberately conservative: an
// existing raw/staging entry consumes a slot, while a fully cleaned parsed
// source does not.
func BackfillSlots(ctx context.Context, pool *pgxpool.Pool, ws *artifact.Workspace) error {
	if pool == nil || ws == nil {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	rows, err := pool.Query(ctx, `
SELECT DISTINCT s.id, s.download_status, s.parse_status,
       s.download_river_job_id, s.parse_river_job_id
FROM mrfpipeline.mrf_sources s
JOIN mrfpipeline.mrf_snapshots n ON n.mrf_source_id = s.id
ORDER BY s.id`)
	if err != nil {
		return dbFailure(ctx, err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		var downloadStatus, parseStatus string
		var downloadJob, parseJob *int64
		if err := rows.Scan(&id, &downloadStatus, &parseStatus, &downloadJob, &parseJob); err != nil {
			return dbFailure(ctx, err)
		}
		raw, err := ws.InspectDownloadState(artifact.KindMRF, id)
		if err != nil {
			return jobs.Failure(jobs.FailureArtifactReconciliationFailed)
		}
		downloadStaging, err := ws.HasDownloadStaging(artifact.KindMRF, id)
		if err != nil {
			return jobs.Failure(jobs.FailureArtifactReconciliationFailed)
		}
		parserStaging, err := ws.HasMRFParserTemp(id)
		if err != nil {
			return jobs.Failure(jobs.FailureArtifactReconciliationFailed)
		}
		begun := downloadStatus == jobs.StatusRunning || parseStatus == jobs.StatusRunning ||
			(downloadStatus == jobs.StatusPending && downloadJob != nil) ||
			(parseStatus == jobs.StatusPending && parseJob != nil)
		retained := raw == artifact.DownloadComplete || raw == artifact.DownloadIncomplete || downloadStaging || parserStaging
		if begun || retained {
			ids = append(ids, id)
		}
	}
	if err := rows.Err(); err != nil {
		return dbFailure(ctx, err)
	}
	rows.Close()
	tx, err := pool.Begin(ctx)
	if err != nil {
		return dbFailure(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, id := range ids {
		if _, err := tx.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_release_mrf_sources (payer_id, collection_month, mrf_source_id)
SELECT DISTINCT payer_id, collection_month, $1::bigint
FROM mrfpipeline.mrf_snapshots
WHERE mrf_source_id = $1::bigint
ON CONFLICT DO NOTHING`, id); err != nil {
			return dbFailure(ctx, err)
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_materialization_slots (mrf_source_id)
VALUES ($1) ON CONFLICT DO NOTHING`, id); err != nil {
			return dbFailure(ctx, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return dbFailure(ctx, err)
	}
	return nil
}

// AuditSlots fails closed when physical MRF materialization is present without
// its durable slot. Scheduling in that state could exceed the shared disk
// bound; the operator must repair the database/artifact pair before work is
// resumed.
func AuditSlots(ctx context.Context, pool *pgxpool.Pool, ws *artifact.Workspace) error {
	if pool == nil || ws == nil {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	rows, err := pool.Query(ctx, `SELECT id FROM mrfpipeline.mrf_sources ORDER BY id`)
	if err != nil {
		return dbFailure(ctx, err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return dbFailure(ctx, err)
		}
		raw, err := ws.InspectDownloadState(artifact.KindMRF, id)
		if err != nil {
			return jobs.Failure(jobs.FailureArtifactReconciliationFailed)
		}
		downloadStaging, err := ws.HasDownloadStaging(artifact.KindMRF, id)
		if err != nil {
			return jobs.Failure(jobs.FailureArtifactReconciliationFailed)
		}
		parserStaging, err := ws.HasMRFParserTemp(id)
		if err != nil {
			return jobs.Failure(jobs.FailureArtifactReconciliationFailed)
		}
		if raw == artifact.DownloadAbsent && !downloadStaging && !parserStaging {
			continue
		}
		var held bool
		if err := pool.QueryRow(ctx, `
SELECT EXISTS (SELECT 1 FROM mrfpipeline.mrf_materialization_slots WHERE mrf_source_id = $1)`, id).Scan(&held); err != nil {
			return dbFailure(ctx, err)
		}
		if !held {
			return jobs.Failure(jobs.FailureArtifactReconciliationFailed)
		}
	}
	if err := rows.Err(); err != nil {
		return dbFailure(ctx, err)
	}
	return nil
}

func ReadRuntime(ctx context.Context, pool *pgxpool.Pool) (Runtime, error) {
	if pool == nil {
		return Runtime{}, jobs.Failure(jobs.FailureInvalidArguments)
	}
	var out Runtime
	if err := pool.QueryRow(ctx, `
SELECT artifact_root, resident_capacity
FROM mrfpipeline.pipeline_runtime WHERE id = true`).Scan(&out.ArtifactRoot, &out.ResidentCapacity); err != nil {
		return Runtime{}, dbFailure(ctx, err)
	}
	return out, nil
}

// ValidateRuntimeRoot performs the read-only root check used before control
// touches legacy state or artifacts. An unconfigured database is accepted;
// ConfigureRuntime will create its row after backfill validation.
func ValidateRuntimeRoot(ctx context.Context, pool *pgxpool.Pool, root string) error {
	if pool == nil || root == "" {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	var stored string
	err := pool.QueryRow(ctx, `SELECT artifact_root FROM mrfpipeline.pipeline_runtime WHERE id = true`).Scan(&stored)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return dbFailure(ctx, err)
	}
	if stored != root {
		return jobs.Failure(jobs.FailureRuntimeRootMismatch)
	}
	return nil
}

// SetTarget changes a release's cumulative admission policy and immediately
// selects the stable prefix. Work scheduling remains the control worker's job.
func SetTarget(ctx context.Context, pool *pgxpool.Pool, payer string, month time.Time, target Target) (SetResult, error) {
	if pool == nil {
		return SetResult{}, jobs.Failure(jobs.FailureInvalidArguments)
	}
	client, err := jobs.NewInsertClient(ctx, pool, nil)
	if err != nil {
		return SetResult{}, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return SetResult{}, dbFailure(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	old, newly, err := setTargetTxResult(ctx, tx, payer, month, target)
	if err != nil {
		return SetResult{}, err
	}
	if err := WakeTx(ctx, tx, client); err != nil {
		return SetResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SetResult{}, dbFailure(ctx, err)
	}
	return loadSetResult(ctx, pool, payer, month, old, target, newly)
}

func SetTargetTx(ctx context.Context, tx pgx.Tx, payer string, month time.Time, target Target) (int64, error) {
	_, newly, err := setTargetTxResult(ctx, tx, payer, month, target)
	return newly, err
}

// PrepareDiscoverTarget applies an explicit discover target, or reuses the
// release's already initialized target when discover omitted the option. An
// unset release is rejected without changing discovery state.
func PrepareDiscoverTarget(ctx context.Context, tx pgx.Tx, payer string, month time.Time, target Target) error {
	var status, kind string
	var count *int64
	if err := tx.QueryRow(ctx, `
SELECT status, mrf_source_target_kind, mrf_source_target_count
FROM mrfpipeline.monthly_releases
WHERE payer_id = $1 AND collection_month = $2
FOR UPDATE`, payer, month).Scan(&status, &kind, &count); err != nil {
		return dbFailure(ctx, err)
	}
	if status != release.Building {
		return jobs.Failure(jobs.FailureSealedReleaseInconsistent)
	}
	current, err := ParseTarget(kind, count)
	if err != nil {
		return err
	}
	if target.Kind == TargetUnset && current.Kind == TargetUnset {
		return jobs.Failure(jobs.FailureSourceTargetUnset)
	}
	if target.Kind != TargetUnset {
		if _, err := ParseTarget(target.Kind, nullableTargetCount(target)); err != nil {
			return err
		}
		if current.Kind != TargetUnset {
			if current.Kind != target.Kind || current.Count != target.Count {
				return jobs.Failure(jobs.FailureSourceTargetConflict)
			}
			_, err = selectSourcesTx(ctx, tx, payer, month, current)
			return err
		}
		if target.Kind == TargetNumeric {
			var selected int64
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.monthly_release_mrf_sources WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&selected); err != nil {
				return dbFailure(ctx, err)
			}
			if target.Count < selected {
				return jobs.Failure(jobs.FailureSourceTargetDecrease)
			}
		}
		if _, err := tx.Exec(ctx, `
UPDATE mrfpipeline.monthly_releases
SET mrf_source_target_kind = $3, mrf_source_target_count = $4,
    updated_at = transaction_timestamp()
WHERE payer_id = $1 AND collection_month = $2`, payer, month, target.Kind, nullableCount(target)); err != nil {
			return dbFailure(ctx, err)
		}
		current = target
	}
	_, err = selectSourcesTx(ctx, tx, payer, month, current)
	return err
}

func nullableTargetCount(t Target) *int64 {
	if t.Kind != TargetNumeric {
		return nil
	}
	n := t.Count
	return &n
}

func setTargetTx(ctx context.Context, tx pgx.Tx, payer string, month time.Time, target Target) (int64, error) {
	_, newly, err := setTargetTxResult(ctx, tx, payer, month, target)
	return newly, err
}

func setTargetTxResult(ctx context.Context, tx pgx.Tx, payer string, month time.Time, target Target) (Target, int64, error) {
	if tx == nil || payer == "" || month.IsZero() {
		return Target{}, 0, jobs.Failure(jobs.FailureInvalidArguments)
	}
	var status, oldKind string
	var oldCount *int64
	err := tx.QueryRow(ctx, `
SELECT status, mrf_source_target_kind, mrf_source_target_count
FROM mrfpipeline.monthly_releases
WHERE payer_id = $1 AND collection_month = $2
FOR UPDATE`, payer, month).Scan(&status, &oldKind, &oldCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return Target{}, 0, jobs.Failure(jobs.FailureReleaseNotFound)
	}
	if err != nil {
		return Target{}, 0, dbFailure(ctx, err)
	}
	if status != release.Building {
		return Target{}, 0, jobs.Failure(jobs.FailureSealedReleaseInconsistent)
	}
	old, err := ParseTarget(oldKind, oldCount)
	if err != nil {
		return Target{}, 0, err
	}
	if err := validateTargetTransition(old, target); err != nil {
		return Target{}, 0, err
	}
	if target.Kind == TargetNumeric {
		var selected int64
		if err := tx.QueryRow(ctx, `
SELECT count(*) FROM mrfpipeline.monthly_release_mrf_sources
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&selected); err != nil {
			return Target{}, 0, dbFailure(ctx, err)
		}
		if target.Count < selected {
			return Target{}, 0, jobs.Failure(jobs.FailureSourceTargetDecrease)
		}
	}
	if old.Kind != target.Kind || old.Count != target.Count {
		if _, err := tx.Exec(ctx, `
UPDATE mrfpipeline.monthly_releases
SET mrf_source_target_kind = $3,
    mrf_source_target_count = $4,
    updated_at = transaction_timestamp()
WHERE payer_id = $1 AND collection_month = $2`, payer, month, target.Kind, nullableCount(target)); err != nil {
			return Target{}, 0, dbFailure(ctx, err)
		}
	}
	newly, err := selectSourcesTx(ctx, tx, payer, month, target)
	return old, newly, err
}

func validateTargetTransition(old, next Target) error {
	if old.Kind == TargetUnset {
		return nil
	}
	if old.Kind == TargetAll {
		if next.Kind != TargetAll {
			return jobs.Failure(jobs.FailureSourceTargetDecrease)
		}
		return nil
	}
	if next.Kind == TargetNumeric && next.Count < old.Count {
		return jobs.Failure(jobs.FailureSourceTargetDecrease)
	}
	if next.Kind == TargetUnset {
		return jobs.Failure(jobs.FailureSourceTargetDecrease)
	}
	return nil
}

func nullableCount(t Target) any {
	if t.Kind == TargetNumeric {
		return t.Count
	}
	return nil
}

func selectSourcesTx(ctx context.Context, tx pgx.Tx, payer string, month time.Time, target Target) (int64, error) {
	if target.Kind == TargetUnset {
		return 0, nil
	}
	var selected int64
	if err := tx.QueryRow(ctx, `
SELECT count(*)
FROM mrfpipeline.monthly_release_mrf_sources
	WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&selected); err != nil {
		return 0, dbFailure(ctx, err)
	}
	remaining := int64(-1)
	if target.Kind == TargetNumeric {
		remaining = target.Count - selected
		if remaining <= 0 {
			return 0, nil
		}
	}
	rows, err := tx.Query(ctx, `
	SELECT s.mrf_source_id
FROM mrfpipeline.mrf_snapshots s
JOIN mrfpipeline.mrf_sources m ON m.id = s.mrf_source_id
LEFT JOIN mrfpipeline.monthly_release_mrf_sources a
  ON a.payer_id = s.payer_id AND a.collection_month = s.collection_month
 AND a.mrf_source_id = s.mrf_source_id
WHERE s.payer_id = $1 AND s.collection_month = $2
  AND a.mrf_source_id IS NULL
	ORDER BY m.source_url, s.mrf_source_id`, payer, month)
	if err != nil {
		return 0, dbFailure(ctx, err)
	}
	var sourceIDs []int64
	var added int64
	for rows.Next() {
		if remaining >= 0 && added >= remaining {
			break
		}
		var sourceID int64
		if err := rows.Scan(&sourceID); err != nil {
			return 0, dbFailure(ctx, err)
		}
		sourceIDs = append(sourceIDs, sourceID)
		added++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, dbFailure(ctx, err)
	}
	rows.Close()
	for _, sourceID := range sourceIDs {
		if _, err := tx.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_release_mrf_sources
    (payer_id, collection_month, mrf_source_id)
VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, payer, month, sourceID); err != nil {
			return 0, dbFailure(ctx, err)
		}
	}
	return added, nil
}

func SelectSourcesTx(ctx context.Context, tx pgx.Tx, payer string, month time.Time) (int64, error) {
	var kind string
	var count *int64
	if err := tx.QueryRow(ctx, `
SELECT mrf_source_target_kind, mrf_source_target_count
FROM mrfpipeline.monthly_releases
WHERE payer_id = $1 AND collection_month = $2
FOR UPDATE`, payer, month).Scan(&kind, &count); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, jobs.Failure(jobs.FailureReleaseNotFound)
		}
		return 0, dbFailure(ctx, err)
	}
	target, err := ParseTarget(kind, count)
	if err != nil {
		return 0, err
	}
	added, err := selectSourcesTx(ctx, tx, payer, month, target)
	if err != nil {
		return 0, err
	}
	return added, nil
}

// ScheduleWaiting acquires slots and schedules the current stage for selected
// sources. The control role calls it after imports, retries, and slot release.
func ScheduleWaiting(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx]) error {
	if pool == nil || client == nil {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return dbFailure(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := scheduleWaitingTx(ctx, tx, client); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return dbFailure(ctx, err)
	}
	return nil
}

// ScheduleWaitingWithLogger emits one aggregate state record after a
// successful refill pass. The database remains authoritative; the log only
// explains why a pass is waiting or how much capacity remains.
func ScheduleWaitingWithLogger(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], logger *slog.Logger) error {
	if err := ScheduleWaiting(ctx, pool, client); err != nil {
		return err
	}
	LogCapacityState(ctx, pool, logger)
	return nil
}

// LogCapacityState records safe aggregate capacity information for operators.
func LogCapacityState(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) {
	if pool == nil || logger == nil {
		return
	}
	var capacity, held, waiting int64
	err := pool.QueryRow(ctx, `
SELECT r.resident_capacity,
       (SELECT count(*) FROM mrfpipeline.mrf_materialization_slots),
       (SELECT count(*)
        FROM mrfpipeline.mrf_sources s
        WHERE s.parse_status <> 'succeeded'
          AND s.download_status <> 'failed'
          AND s.parse_status <> 'failed'
          AND NOT EXISTS (SELECT 1 FROM mrfpipeline.mrf_materialization_slots x WHERE x.mrf_source_id = s.id)
          AND EXISTS (SELECT 1 FROM mrfpipeline.monthly_release_mrf_sources a
                      JOIN mrfpipeline.monthly_releases m ON m.payer_id = a.payer_id AND m.collection_month = a.collection_month
                      WHERE a.mrf_source_id = s.id AND m.status = 'building'))
FROM mrfpipeline.pipeline_runtime r WHERE r.id = true`).Scan(&capacity, &held, &waiting)
	if err != nil {
		return
	}
	msg := "mrf_slot_refill"
	if waiting > 0 && held >= capacity {
		msg = "mrf_capacity_waiting"
	}
	logger.LogAttrs(ctx, slog.LevelInfo, msg,
		slog.Int64("resident_capacity", capacity),
		slog.Int64("resident_held", held),
		slog.Int64("waiting_sources", waiting))
}

func ScheduleWaitingTx(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx]) error {
	return scheduleWaitingTx(ctx, tx, client)
}

// ScheduleSelectedConsumers advances snapshots for newly selected sources
// whose physical parse output already exists. It lets a later release reuse
// immutable parsed work without acquiring a new MRF materialization slot.
func ScheduleSelectedConsumersTx(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx], payer string, month time.Time) error {
	if tx == nil || client == nil || payer == "" || month.IsZero() {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	rows, err := tx.Query(ctx, `
SELECT s.id
FROM mrfpipeline.mrf_snapshots s
JOIN mrfpipeline.mrf_sources m ON m.id = s.mrf_source_id
JOIN mrfpipeline.monthly_release_mrf_sources a
  ON a.payer_id = s.payer_id AND a.collection_month = s.collection_month
 AND a.mrf_source_id = s.mrf_source_id
WHERE s.payer_id = $1 AND s.collection_month = $2
  AND m.parse_status = 'succeeded'
  AND s.consume_status = 'blocked' AND s.consume_river_job_id IS NULL
ORDER BY s.id

FOR UPDATE OF s`, payer, month)
	if err != nil {
		return dbFailure(ctx, err)
	}
	return scheduleConsumerSnapshotRows(ctx, tx, client, rows)
}

// ScheduleSelectedConsumersAllTx is used by a durable control wake. It also
// covers an already-parsed source selected for a release after the normal
// import path has finished, so its consumer work cannot wait for an unrelated
// reconciliation pass.
func ScheduleSelectedConsumersAllTx(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx]) error {
	if tx == nil || client == nil {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	rows, err := tx.Query(ctx, `
SELECT s.id
FROM mrfpipeline.mrf_snapshots s
JOIN mrfpipeline.mrf_sources m ON m.id = s.mrf_source_id
WHERE m.parse_status = 'succeeded'
  AND s.consume_status = 'blocked' AND s.consume_river_job_id IS NULL
  AND EXISTS (SELECT 1 FROM mrfpipeline.monthly_release_mrf_sources a
              JOIN mrfpipeline.monthly_releases r
                ON r.payer_id = a.payer_id AND r.collection_month = a.collection_month
              WHERE a.payer_id = s.payer_id AND a.collection_month = s.collection_month
                AND a.mrf_source_id = s.mrf_source_id AND r.status = 'building')
ORDER BY s.id
FOR UPDATE OF s`)
	if err != nil {
		return dbFailure(ctx, err)
	}
	return scheduleConsumerSnapshotRows(ctx, tx, client, rows)
}

func scheduleConsumerSnapshotRows(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx], rows pgx.Rows) error {
	if rows == nil {
		return dbFailure(ctx, nil)
	}
	defer rows.Close()
	var snapshotIDs []int64
	for rows.Next() {
		var snapshotID int64
		if err := rows.Scan(&snapshotID); err != nil {
			return dbFailure(ctx, err)
		}
		snapshotIDs = append(snapshotIDs, snapshotID)
	}
	if err := rows.Err(); err != nil {
		return dbFailure(ctx, err)
	}
	for _, snapshotID := range snapshotIDs {
		if _, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_snapshots
SET consume_status = 'pending', updated_at = transaction_timestamp()
WHERE id = $1`, snapshotID); err != nil {
			return dbFailure(ctx, err)
		}
		jobID, err := jobs.InsertTx(ctx, client, tx, &jobs.ConsumerIngestArgs{MRFSnapshotID: snapshotID})
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_snapshots
SET consume_river_job_id = $2, updated_at = transaction_timestamp()
WHERE id = $1`, snapshotID, jobID); err != nil {
			return dbFailure(ctx, err)
		}
	}
	return nil
}

func scheduleWaitingTx(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx]) error {
	if tx == nil || client == nil {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	var capacity int64
	err := tx.QueryRow(ctx, `
SELECT resident_capacity FROM mrfpipeline.pipeline_runtime
WHERE id = true FOR UPDATE`).Scan(&capacity)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return dbFailure(ctx, err)
	}
	var held int64
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.mrf_materialization_slots`).Scan(&held); err != nil {
		return dbFailure(ctx, err)
	}
	rows, err := tx.Query(ctx, `
SELECT s.id
FROM mrfpipeline.mrf_sources s
WHERE s.parse_status <> 'succeeded'
  AND s.download_status <> 'failed'
  AND s.parse_status <> 'failed'
  AND (s.download_status IN ('blocked', 'pending')
       OR (s.download_status = 'succeeded' AND s.parse_status IN ('blocked', 'pending')))
  AND EXISTS (
      SELECT 1
      FROM mrfpipeline.monthly_release_mrf_sources a
      JOIN mrfpipeline.monthly_releases r
        ON r.payer_id = a.payer_id AND r.collection_month = a.collection_month
      WHERE a.mrf_source_id = s.id AND r.status = 'building'
  )
  AND NOT EXISTS (
      SELECT 1 FROM mrfpipeline.mrf_materialization_slots x
      WHERE x.mrf_source_id = s.id
  )
ORDER BY s.source_url, s.id`)
	if err != nil {
		return dbFailure(ctx, err)
	}
	var sourceIDs []int64
	for rows.Next() {
		if held >= capacity {
			break
		}
		var sourceID int64
		if err := rows.Scan(&sourceID); err != nil {
			return dbFailure(ctx, err)
		}
		sourceIDs = append(sourceIDs, sourceID)
	}
	if err := rows.Err(); err != nil {
		return dbFailure(ctx, err)
	}
	rows.Close()
	for _, sourceID := range sourceIDs {
		if held >= capacity {
			break
		}
		var downloadStatus, parseStatus string
		if err := tx.QueryRow(ctx, `
SELECT download_status, parse_status FROM mrfpipeline.mrf_sources
		WHERE id = $1 FOR UPDATE`, sourceID).Scan(&downloadStatus, &parseStatus); err != nil {
			return dbFailure(ctx, err)
		}
		if downloadStatus == jobs.StatusFailed || parseStatus == jobs.StatusFailed || parseStatus == jobs.StatusSucceeded {
			continue
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_materialization_slots (mrf_source_id)
VALUES ($1) ON CONFLICT DO NOTHING`, sourceID); err != nil {
			return dbFailure(ctx, err)
		}
		if downloadStatus != jobs.StatusSucceeded {
			if downloadStatus == jobs.StatusBlocked {
				if err := jobs.Unblock(ctx, tx, jobs.MRFDownloadStage, sourceID); err != nil {
					return err
				}
			}
			if _, err := jobs.Schedule(ctx, tx, client, jobs.MRFDownloadStage, sourceID, &jobs.MRFDownloadArgs{MRFSourceID: sourceID}); err != nil {
				return err
			}
		} else {
			if parseStatus == jobs.StatusBlocked {
				if err := jobs.Unblock(ctx, tx, jobs.MRFParseStage, sourceID); err != nil {
					return err
				}
			}
			if _, err := jobs.Schedule(ctx, tx, client, jobs.MRFParseStage, sourceID, &jobs.MRFParseArgs{MRFSourceID: sourceID}); err != nil {
				return err
			}
		}
		held++
	}
	return nil
}

func ReleaseSlotTx(ctx context.Context, tx pgx.Tx, sourceID int64) error {
	if tx == nil || sourceID <= 0 {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	if _, err := tx.Exec(ctx, `
DELETE FROM mrfpipeline.mrf_materialization_slots
WHERE mrf_source_id = $1`, sourceID); err != nil {
		return dbFailure(ctx, err)
	}
	return nil
}

// Wake publishes one coalesced control scheduler job. It is safe to call
// after admission changes even when the control process is temporarily down.
func Wake(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	client, err := jobs.NewInsertClient(ctx, pool, nil)
	if err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return dbFailure(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := WakeTx(ctx, tx, client); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return dbFailure(ctx, err)
	}
	return nil
}

// ReleaseSlotAndWake commits slot release and its durable refill event in one
// transaction. Callers should hold the source execution lock when this is a
// repair of a particular source.
func ReleaseSlotAndWake(ctx context.Context, pool *pgxpool.Pool, sourceID int64, loggers ...*slog.Logger) error {
	if pool == nil || sourceID <= 0 {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	client, err := jobs.NewInsertClient(ctx, pool, nil)
	if err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return dbFailure(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := ReleaseSlotTx(ctx, tx, sourceID); err != nil {
		return err
	}
	if err := WakeTx(ctx, tx, client); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return dbFailure(ctx, err)
	}
	if len(loggers) > 0 && loggers[0] != nil {
		loggers[0].LogAttrs(ctx, slog.LevelInfo, "mrf_slot_released", slog.Int64("source_id", sourceID))
	}
	return nil
}

func WakeTx(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx]) error {
	var eventID int64
	if err := tx.QueryRow(ctx, `INSERT INTO mrfpipeline.control_schedule_events DEFAULT VALUES RETURNING id`).Scan(&eventID); err != nil {
		return dbFailure(ctx, err)
	}
	return jobs.InsertCoalescedTx(ctx, client, tx, jobs.ControlScheduleArgs{EventID: eventID})
}

// RestoreWakes republishes durable events whose River delivery was cancelled
// or lost after the event commit. Existing available/running deliveries are
// ignored by River's ByArgs uniqueness, so this is safe on every control
// startup and reconciliation pass.
func RestoreWakes(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx]) error {
	if pool == nil || client == nil {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return dbFailure(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
SELECT id
FROM mrfpipeline.control_schedule_events
ORDER BY id
FOR UPDATE`)
	if err != nil {
		return dbFailure(ctx, err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return dbFailure(ctx, err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return dbFailure(ctx, err)
	}
	rows.Close()
	for _, id := range ids {
		if err := jobs.InsertCoalescedTx(ctx, client, tx, jobs.ControlScheduleArgs{EventID: id}); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return dbFailure(ctx, err)
	}
	return nil
}

// ConsumeWakeTx removes the event in the same transaction as scheduling so
// the small event table does not become a second unbounded job history.
func ConsumeWakeTx(ctx context.Context, tx pgx.Tx, eventID int64) error {
	if tx == nil || eventID <= 0 {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM mrfpipeline.control_schedule_events WHERE id = $1`, eventID); err != nil {
		return dbFailure(ctx, err)
	}
	return nil
}

func loadSetResult(ctx context.Context, pool *pgxpool.Pool, payer string, month time.Time, old, target Target, newly int64) (SetResult, error) {
	var known, selected, held, capacity int64
	if err := pool.QueryRow(ctx, `
SELECT count(DISTINCT s.mrf_source_id)
FROM mrfpipeline.mrf_snapshots s
WHERE s.payer_id = $1 AND s.collection_month = $2`, payer, month).Scan(&known); err != nil {
		return SetResult{}, dbFailure(ctx, err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM mrfpipeline.monthly_release_mrf_sources
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&selected); err != nil {
		return SetResult{}, dbFailure(ctx, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.mrf_materialization_slots`).Scan(&held); err != nil {
		return SetResult{}, dbFailure(ctx, err)
	}
	if err := pool.QueryRow(ctx, `SELECT resident_capacity FROM mrfpipeline.pipeline_runtime WHERE id = true`).Scan(&capacity); err != nil {
		capacity = 0
	}
	return SetResult{
		PayerID: payer, CollectionMonth: month.Format("2006-01"),
		OldTarget: FormatTarget(old), NewTarget: FormatTarget(target), NewlySelected: newly,
		KnownSources: known, SelectedSources: selected, HeldSlots: held,
		ResidentCapacity: capacity,
	}, nil
}

func dbFailure(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if err == nil {
		return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
}
