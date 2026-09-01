// Package release owns the small monthly serving control plane.
package release

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	Building = "building"
	Active   = "active"
	Inactive = "inactive"
)

type Target struct {
	PayerID         string
	CollectionMonth string
	OutputID        string
	SnapshotID      int64
}

type StatusReport struct {
	PayerID                   string   `json:"payer_id"`
	CollectionMonth           string   `json:"collection_month"`
	Status                    string   `json:"status"`
	DatabaseReady             bool     `json:"database_ready"`
	Blockers                  []string `json:"blockers"`
	DiscoveryRunCount         int64    `json:"discovery_run_count"`
	TOCCount                  int64    `json:"toc_count"`
	SnapshotCount             int64    `json:"snapshot_count"`
	MRFSourceTarget           string   `json:"mrf_source_target"`
	MRFSourcesSelected        int64    `json:"mrf_sources_selected"`
	MRFSourcesKnown           int64    `json:"mrf_sources_known"`
	MRFSourcesBlockedByLimit  int64    `json:"mrf_sources_blocked_by_limit"`
	MRFSourcesWaitingCapacity int64    `json:"mrf_sources_waiting_capacity"`
	MRFSourcesReusingOutput   int64    `json:"mrf_sources_reusing_output"`
	MRFResidentHeld           int64    `json:"mrf_resident_held"`
	MRFResidentCapacity       int64    `json:"mrf_resident_capacity"`
	MRFResidentFree           int64    `json:"mrf_resident_free"`
	MRFDownloadPending        int64    `json:"mrf_download_pending"`
	MRFDownloadRunning        int64    `json:"mrf_download_running"`
	MRFDownloadRetrying       int64    `json:"mrf_download_retrying"`
	MRFDownloadFailed         int64    `json:"mrf_download_failed"`
	MRFParsePending           int64    `json:"mrf_parse_pending"`
	MRFParseRunning           int64    `json:"mrf_parse_running"`
	MRFParseRetrying          int64    `json:"mrf_parse_retrying"`
	MRFParseFailed            int64    `json:"mrf_parse_failed"`
	MRFParsedSucceeded        int64    `json:"mrf_parsed_succeeded"`
	MRFRawCleanupFailed       int64    `json:"mrf_raw_cleanup_failed"`
	ConsumerPending           int64    `json:"consumer_pending"`
	ConsumerRunning           int64    `json:"consumer_running"`
	ConsumerRetrying          int64    `json:"consumer_retrying"`
	ConsumerFailed            int64    `json:"consumer_failed"`
	ConsumerSucceeded         int64    `json:"consumer_succeeded"`
	Partial                   bool     `json:"partial"`
	FilterCatalogReady        bool     `json:"filter_catalog_ready"`
	FilterCatalogID           *int64   `json:"filter_catalog_id"`
	FilterCatalogGeneration   *int64   `json:"filter_catalog_generation"`
	FilterCatalogStatus       *string  `json:"filter_catalog_status"`
	FilterCatalogOutputCount  *int64   `json:"filter_catalog_output_count"`
	ProspectiveCandidateValid bool     `json:"prospective_candidate_valid"`
}

type ActivationResult struct {
	PayerID                  string  `json:"payer_id"`
	CollectionMonth          string  `json:"collection_month"`
	OutputCount              int64   `json:"output_count"`
	PublicationGeneration    int64   `json:"publication_generation"`
	AddedOutputCount         int64   `json:"added_output_count"`
	SkippedOutputCount       int64   `json:"skipped_output_count"`
	TerminalFileFailureCount int64   `json:"terminal_file_failure_count"`
	PreviousCollectionMonth  *string `json:"previous_collection_month"`
	FilterCatalogID          int64   `json:"filter_catalog_id"`
}

type rowsQueryer interface {
	queryer
	Query(context.Context, string, ...any) (pgx.Rows, error)
}
type queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func EnsureBuilding(ctx context.Context, tx pgx.Tx, payer string, month time.Time) error {
	if tx == nil {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ($1, $2)
ON CONFLICT (payer_id, collection_month) DO NOTHING`, payer, month); err != nil {
		return dbFailure(ctx)
	}
	return requireStatus(ctx, tx, payer, month, Building, true)
}

func RequireBuildingForTOC(ctx context.Context, tx pgx.Tx, tocID int64) error {
	var payer string
	var month time.Time
	err := tx.QueryRow(ctx, `
SELECT payer_id, collection_month
FROM mrfpipeline.toc_files
WHERE id = $1`, tocID).Scan(&payer, &month)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return dbFailure(ctx)
	}
	if err := lockBuildingRelease(ctx, tx, payer, month); err != nil {
		return err
	}
	var lockedPayer string
	var lockedMonth time.Time
	err = tx.QueryRow(ctx, `
SELECT payer_id, collection_month
FROM mrfpipeline.toc_files
WHERE id = $1
FOR UPDATE`, tocID).Scan(&lockedPayer, &lockedMonth)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return dbFailure(ctx)
	}
	if lockedPayer != payer || !lockedMonth.Equal(month) {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	return nil
}

// RequireBuildingForSnapshot permits materialization while the release is
// building or active. Inactive releases remain frozen.
func RequireBuildingForSnapshot(ctx context.Context, tx pgx.Tx, snapshotID int64) error {
	var payer string
	var month time.Time
	err := tx.QueryRow(ctx, `
SELECT payer_id, collection_month
FROM mrfpipeline.mrf_snapshots
WHERE id = $1`, snapshotID).Scan(&payer, &month)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return dbFailure(ctx)
	}
	if err := lockMaterializingRelease(ctx, tx, payer, month); err != nil {
		return err
	}
	var lockedPayer string
	var lockedMonth time.Time
	err = tx.QueryRow(ctx, `
SELECT payer_id, collection_month
FROM mrfpipeline.mrf_snapshots
WHERE id = $1
FOR UPDATE`, snapshotID).Scan(&lockedPayer, &lockedMonth)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return dbFailure(ctx)
	}
	if lockedPayer != payer || !lockedMonth.Equal(month) {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	return nil
}

// RequireBuildingForBatch permits plan publication while the release is
// building or active. Inactive releases remain frozen.
func RequireBuildingForBatch(ctx context.Context, tx pgx.Tx, batchID int64) error {
	var snapshotID int64
	err := tx.QueryRow(ctx, `
SELECT mrf_snapshot_id
FROM mrfpipeline.plan_attachment_batches
WHERE id = $1`, batchID).Scan(&snapshotID)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return dbFailure(ctx)
	}
	var payer string
	var month time.Time
	err = tx.QueryRow(ctx, `
SELECT payer_id, collection_month
FROM mrfpipeline.mrf_snapshots
WHERE id = $1`, snapshotID).Scan(&payer, &month)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return dbFailure(ctx)
	}
	if err := lockMaterializingRelease(ctx, tx, payer, month); err != nil {
		return err
	}
	var lockedSnapshotID int64
	if err := tx.QueryRow(ctx, `
SELECT id
FROM mrfpipeline.mrf_snapshots
WHERE id = $1
FOR UPDATE`, snapshotID).Scan(&lockedSnapshotID); errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureMissingRecord)
	} else if err != nil {
		return dbFailure(ctx)
	}
	var batchSnapshotID int64
	err = tx.QueryRow(ctx, `
SELECT mrf_snapshot_id
FROM mrfpipeline.plan_attachment_batches
WHERE id = $1
FOR UPDATE`, batchID).Scan(&batchSnapshotID)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return dbFailure(ctx)
	}
	if batchSnapshotID != lockedSnapshotID {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	return nil
}

// RequireBuildingForStage keeps discovery/TOC work building-only and permits
// MRF/consumer materialization for building or active releases.
func RequireBuildingForStage(ctx context.Context, tx pgx.Tx, kind string, domainID int64) error {
	switch kind {
	case jobs.KindDiscoveryRun:
		return requireBuildingForDiscovery(ctx, tx, domainID)
	case jobs.KindTOCDownload, jobs.KindTOCParse, jobs.KindTOCImport:
		return RequireBuildingForTOC(ctx, tx, domainID)
	case jobs.KindMRFDownload, jobs.KindMRFParse:
		return requireBuildingForSource(ctx, tx, domainID)
	case jobs.KindConsumerIngest:
		return RequireBuildingForSnapshot(ctx, tx, domainID)
	case jobs.KindConsumerAttachPlans:
		return RequireBuildingForBatch(ctx, tx, domainID)
	default:
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
}

func lockBuildingRelease(ctx context.Context, tx pgx.Tx, payer string, month time.Time) error {
	var status string
	err := tx.QueryRow(ctx, `
SELECT status
FROM mrfpipeline.monthly_releases
WHERE payer_id = $1 AND collection_month = $2
FOR UPDATE`, payer, month).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return dbFailure(ctx)
	}
	if status != Building {
		return jobs.Failure(jobs.FailureSealedReleaseInconsistent)
	}
	return nil
}

func lockMaterializingRelease(ctx context.Context, tx pgx.Tx, payer string, month time.Time) error {
	var status string
	err := tx.QueryRow(ctx, `
SELECT status
FROM mrfpipeline.monthly_releases
WHERE payer_id = $1 AND collection_month = $2
FOR UPDATE`, payer, month).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return dbFailure(ctx)
	}
	if status != Building && status != Active {
		return jobs.Failure(jobs.FailureSealedReleaseInconsistent)
	}
	return nil
}

func requireBuildingForDiscovery(ctx context.Context, tx pgx.Tx, runID int64) error {
	var payer string
	var month time.Time
	err := tx.QueryRow(ctx, `
SELECT payer_id, collection_month
FROM mrfpipeline.discovery_runs
WHERE id = $1`, runID).Scan(&payer, &month)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return dbFailure(ctx)
	}
	if err := lockBuildingRelease(ctx, tx, payer, month); err != nil {
		return err
	}
	var lockedPayer string
	var lockedMonth time.Time
	err = tx.QueryRow(ctx, `
SELECT payer_id, collection_month
FROM mrfpipeline.discovery_runs
WHERE id = $1
FOR UPDATE`, runID).Scan(&lockedPayer, &lockedMonth)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return dbFailure(ctx)
	}
	if lockedPayer != payer || !lockedMonth.Equal(month) {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	return nil
}

type sourceReleaseKey struct {
	payer string
	month time.Time
}

func sourceReleaseKeys(ctx context.Context, tx pgx.Tx, sourceID int64) ([]sourceReleaseKey, error) {
	rows, err := tx.Query(ctx, `
SELECT DISTINCT n.payer_id, n.collection_month
FROM mrfpipeline.mrf_sources s
JOIN mrfpipeline.mrf_snapshots n ON n.mrf_source_id = s.id
WHERE s.id = $1
ORDER BY n.payer_id, n.collection_month`, sourceID)
	if err != nil {
		return nil, dbFailure(ctx)
	}
	defer rows.Close()
	var keys []sourceReleaseKey
	for rows.Next() {
		var key sourceReleaseKey
		if err := rows.Scan(&key.payer, &key.month); err != nil {
			return nil, dbFailure(ctx)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, dbFailure(ctx)
	}
	return keys, nil
}

func sameSourceReleaseKeys(left, right []sourceReleaseKey) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func requireBuildingForSource(ctx context.Context, tx pgx.Tx, sourceID int64) error {
	keys, err := sourceReleaseKeys(ctx, tx, sourceID)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return jobs.Failure(jobs.FailureMissingRecord)
	}
	for _, key := range keys {
		if err := lockMaterializingRelease(ctx, tx, key.payer, key.month); err != nil {
			return err
		}
	}
	if err := tx.QueryRow(ctx, `
SELECT id FROM mrfpipeline.mrf_sources
WHERE id = $1
FOR UPDATE`, sourceID).Scan(new(int64)); errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureMissingRecord)
	} else if err != nil {
		return dbFailure(ctx)
	}
	current, err := sourceReleaseKeys(ctx, tx, sourceID)
	if err != nil {
		return err
	}
	if !sameSourceReleaseKeys(keys, current) {
		for _, key := range current {
			var status string
			if err := tx.QueryRow(ctx, `
SELECT status
FROM mrfpipeline.monthly_releases
WHERE payer_id = $1 AND collection_month = $2`, key.payer, key.month).Scan(&status); err != nil {
				return dbFailure(ctx)
			}
			if status != Building && status != Active {
				return jobs.Failure(jobs.FailureSealedReleaseInconsistent)
			}
		}
		// A new building dependent release appeared after the release locks
		// were acquired. Do not lock it after the source: that would invert
		// the supported release-before-source order. Let the delivery retry
		// from a fresh transaction instead.
		return fmt.Errorf("%w: source release set changed", jobs.ErrJob)
	}
	return nil
}

func Readiness(ctx context.Context, pool *pgxpool.Pool, payer string, month time.Time) (StatusReport, error) {
	if pool == nil {
		return StatusReport{}, jobs.Failure(jobs.FailureInvalidArguments)
	}
	return readiness(ctx, pool, payer, month)
}

func readiness(ctx context.Context, q rowsQueryer, payer string, month time.Time) (StatusReport, error) {
	var out StatusReport
	var status string
	err := q.QueryRow(ctx, `
SELECT status FROM mrfpipeline.monthly_releases
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return StatusReport{}, jobs.Failure(jobs.FailureReleaseNotFound)
	}
	if err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	out.PayerID = payer
	out.CollectionMonth = formatMonth(month)
	out.Status = status
	out.Blockers = make([]string, 0)
	var targetKind string
	var targetCount *int64
	if err := q.QueryRow(ctx, `
SELECT mrf_source_target_kind, mrf_source_target_count
FROM mrfpipeline.monthly_releases
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&targetKind, &targetCount); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	if targetKind == "numeric" && targetCount != nil {
		out.MRFSourceTarget = fmt.Sprintf("%d", *targetCount)
	} else {
		out.MRFSourceTarget = targetKind
	}
	if err := q.QueryRow(ctx, `
SELECT count(*) FROM mrfpipeline.monthly_release_mrf_sources
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&out.MRFSourcesSelected); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.mrf_materialization_slots`).Scan(&out.MRFResidentHeld); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	if err := q.QueryRow(ctx, `
SELECT coalesce((SELECT resident_capacity FROM mrfpipeline.pipeline_runtime WHERE id = true), 0)`).Scan(&out.MRFResidentCapacity); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	if err := q.QueryRow(ctx, `
SELECT count(DISTINCT mrf_source_id)
FROM mrfpipeline.mrf_snapshots
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&out.MRFSourcesKnown); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	out.MRFSourcesBlockedByLimit = out.MRFSourcesKnown - out.MRFSourcesSelected
	if out.MRFSourcesBlockedByLimit < 0 {
		out.MRFSourcesBlockedByLimit = 0
	}
	out.MRFResidentFree = out.MRFResidentCapacity - out.MRFResidentHeld
	if out.MRFResidentFree < 0 {
		out.MRFResidentFree = 0
	}
	out.Partial = targetKind == "unset" || out.MRFSourcesBlockedByLimit > 0
	if out.Partial {
		addBlocker(&out.Blockers, true, "mrf_source_target_partial")
	}
	if err := q.QueryRow(ctx, `
	SELECT count(*) FILTER (WHERE s.parse_status = 'succeeded'),
	       count(*) FILTER (WHERE s.parse_status = 'succeeded' AND a.selected_at > s.updated_at),
       count(*) FILTER (WHERE s.download_status = 'pending'),
       count(*) FILTER (WHERE s.download_status = 'running'),
       count(*) FILTER (WHERE s.download_status = 'failed'),
       count(*) FILTER (WHERE s.parse_status = 'pending'),
       count(*) FILTER (WHERE s.parse_status = 'running'),
       count(*) FILTER (WHERE s.parse_status = 'failed'),
       count(*) FILTER (WHERE s.parse_status = 'failed' AND s.failure_code = 'mrf_parse_cleanup_failed'),
       count(*) FILTER (WHERE s.download_status <> 'failed' AND s.parse_status <> 'failed'
                         AND (s.download_status IN ('blocked', 'pending', 'running')
                          OR (s.download_status = 'succeeded' AND s.parse_status IN ('blocked', 'pending', 'running')))
                         AND NOT EXISTS (
           SELECT 1 FROM mrfpipeline.mrf_materialization_slots x WHERE x.mrf_source_id = s.id
       ))
FROM mrfpipeline.mrf_sources s
JOIN mrfpipeline.monthly_release_mrf_sources a ON a.mrf_source_id = s.id
WHERE a.payer_id = $1 AND a.collection_month = $2`, payer, month).Scan(
		&out.MRFParsedSucceeded, &out.MRFSourcesReusingOutput, &out.MRFDownloadPending, &out.MRFDownloadRunning,
		&out.MRFDownloadFailed, &out.MRFParsePending, &out.MRFParseRunning,
		&out.MRFParseFailed, &out.MRFRawCleanupFailed, &out.MRFSourcesWaitingCapacity); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	if err := q.QueryRow(ctx, `
SELECT
  (SELECT count(*) FROM mrfpipeline.mrf_snapshots s
   JOIN mrfpipeline.monthly_release_mrf_sources a ON a.payer_id = s.payer_id AND a.collection_month = s.collection_month AND a.mrf_source_id = s.mrf_source_id
   WHERE s.payer_id = $1 AND s.collection_month = $2 AND s.consume_status = 'pending')
  + (SELECT count(*) FROM mrfpipeline.plan_attachment_batches b JOIN mrfpipeline.mrf_snapshots s ON s.id = b.mrf_snapshot_id
     WHERE s.payer_id = $1 AND s.collection_month = $2 AND b.status = 'pending'),
  (SELECT count(*) FROM mrfpipeline.mrf_snapshots s
   JOIN mrfpipeline.monthly_release_mrf_sources a ON a.payer_id = s.payer_id AND a.collection_month = s.collection_month AND a.mrf_source_id = s.mrf_source_id
   WHERE s.payer_id = $1 AND s.collection_month = $2 AND s.consume_status = 'running')
  + (SELECT count(*) FROM mrfpipeline.plan_attachment_batches b JOIN mrfpipeline.mrf_snapshots s ON s.id = b.mrf_snapshot_id
     WHERE s.payer_id = $1 AND s.collection_month = $2 AND b.status = 'running'),
  (SELECT count(*) FROM mrfpipeline.mrf_snapshots s
   JOIN mrfpipeline.monthly_release_mrf_sources a ON a.payer_id = s.payer_id AND a.collection_month = s.collection_month AND a.mrf_source_id = s.mrf_source_id
   WHERE s.payer_id = $1 AND s.collection_month = $2 AND s.consume_status = 'failed')
  + (SELECT count(*) FROM mrfpipeline.plan_attachment_batches b JOIN mrfpipeline.mrf_snapshots s ON s.id = b.mrf_snapshot_id
     WHERE s.payer_id = $1 AND s.collection_month = $2 AND b.status = 'failed'),
  (SELECT count(*) FROM mrfpipeline.mrf_snapshots s
   JOIN mrfpipeline.monthly_release_mrf_sources a ON a.payer_id = s.payer_id AND a.collection_month = s.collection_month AND a.mrf_source_id = s.mrf_source_id
   WHERE s.payer_id = $1 AND s.collection_month = $2 AND s.consume_status = 'succeeded')
  + (SELECT count(*) FROM mrfpipeline.plan_attachment_batches b JOIN mrfpipeline.mrf_snapshots s ON s.id = b.mrf_snapshot_id
     WHERE s.payer_id = $1 AND s.collection_month = $2 AND b.status = 'succeeded')`, payer, month).Scan(
		&out.ConsumerPending, &out.ConsumerRunning, &out.ConsumerFailed, &out.ConsumerSucceeded); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	if err := q.QueryRow(ctx, `
SELECT
  (SELECT count(*) FROM mrfpipeline.mrf_snapshots s
   JOIN mrfpipeline.monthly_release_mrf_sources a ON a.payer_id = s.payer_id AND a.collection_month = s.collection_month AND a.mrf_source_id = s.mrf_source_id
   WHERE s.payer_id = $1 AND s.collection_month = $2 AND s.consume_status IN ('pending', 'running')
     AND s.consume_river_job_id IS NOT NULL
     AND EXISTS (SELECT 1 FROM mrfpipeline_river.river_job j WHERE j.id = s.consume_river_job_id AND j.state = 'retryable'))
  + (SELECT count(*) FROM mrfpipeline.plan_attachment_batches b
     JOIN mrfpipeline.mrf_snapshots s ON s.id = b.mrf_snapshot_id
     WHERE s.payer_id = $1 AND s.collection_month = $2 AND b.status IN ('pending', 'running')
       AND b.river_job_id IS NOT NULL
       AND EXISTS (SELECT 1 FROM mrfpipeline_river.river_job j WHERE j.id = b.river_job_id AND j.state = 'retryable'))`, payer, month).Scan(&out.ConsumerRetrying); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	if err := q.QueryRow(ctx, `
SELECT count(*) FROM mrfpipeline.mrf_sources s
JOIN mrfpipeline.monthly_release_mrf_sources a ON a.mrf_source_id = s.id
WHERE a.payer_id = $1 AND a.collection_month = $2
  AND s.download_status IN ('pending', 'running')
  AND s.download_river_job_id IS NOT NULL
  AND EXISTS (SELECT 1 FROM mrfpipeline_river.river_job j
              WHERE j.id = s.download_river_job_id AND j.state = 'retryable')`, payer, month).Scan(&out.MRFDownloadRetrying); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	if err := q.QueryRow(ctx, `
SELECT count(*) FROM mrfpipeline.mrf_sources s
JOIN mrfpipeline.monthly_release_mrf_sources a ON a.mrf_source_id = s.id
WHERE a.payer_id = $1 AND a.collection_month = $2
  AND s.parse_status IN ('pending', 'running')
  AND s.parse_river_job_id IS NOT NULL
  AND EXISTS (SELECT 1 FROM mrfpipeline_river.river_job j
              WHERE j.id = s.parse_river_job_id AND j.state = 'retryable')`, payer, month).Scan(&out.MRFParseRetrying); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}

	var bad int64
	if err := q.QueryRow(ctx, `
SELECT count(*), count(*) FILTER (WHERE status <> 'succeeded')
FROM mrfpipeline.discovery_runs
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&out.DiscoveryRunCount, &bad); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	addBlocker(&out.Blockers, out.DiscoveryRunCount == 0, "discovery_missing")
	addBlocker(&out.Blockers, bad != 0, "discovery_incomplete")

	var tocBad int64
	if err := q.QueryRow(ctx, `
SELECT count(*), count(*) FILTER (WHERE download_status <> 'succeeded' OR parse_status <> 'succeeded' OR import_status <> 'succeeded')
FROM mrfpipeline.toc_files
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&out.TOCCount, &tocBad); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	addBlocker(&out.Blockers, out.TOCCount == 0, "toc_missing")
	addBlocker(&out.Blockers, tocBad != 0, "toc_incomplete")

	var sourceBad int64
	if err := q.QueryRow(ctx, `
SELECT count(*), count(*) FILTER (WHERE s.download_status <> 'succeeded' OR s.parse_status <> 'succeeded')
FROM mrfpipeline.mrf_sources s
JOIN mrfpipeline.monthly_release_mrf_sources a ON a.mrf_source_id = s.id
WHERE a.payer_id = $1 AND a.collection_month = $2`, payer, month).Scan(new(int64), &sourceBad); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	addBlocker(&out.Blockers, sourceBad != 0, "source_incomplete")

	var snapshotBad int64
	if err := q.QueryRow(ctx, `
SELECT count(*), count(*) FILTER (WHERE consume_status <> 'succeeded')
FROM mrfpipeline.mrf_snapshots
WHERE payer_id = $1 AND collection_month = $2
  AND EXISTS (
      SELECT 1 FROM mrfpipeline.monthly_release_mrf_sources a
      WHERE a.payer_id = $1 AND a.collection_month = $2
        AND a.mrf_source_id = mrf_snapshots.mrf_source_id
  )`, payer, month).Scan(&out.SnapshotCount, &snapshotBad); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	addBlocker(&out.Blockers, out.SnapshotCount == 0, "snapshot_missing")
	addBlocker(&out.Blockers, snapshotBad != 0, "consume_incomplete")

	var missingAttachment, incompleteAttachment int64
	if err := q.QueryRow(ctx, `
SELECT
  count(*) FILTER (WHERE NOT EXISTS (
      SELECT 1 FROM mrfpipeline.plan_attachment_batches b
      WHERE b.mrf_snapshot_id = s.id AND b.status = 'succeeded')),
  count(*) FILTER (WHERE EXISTS (
      SELECT 1 FROM mrfpipeline.plan_attachment_batches b
      WHERE b.mrf_snapshot_id = s.id AND b.status IN ('pending', 'running', 'failed')))
FROM mrfpipeline.mrf_snapshots s
WHERE s.payer_id = $1 AND s.collection_month = $2
  AND EXISTS (SELECT 1 FROM mrfpipeline.monthly_release_mrf_sources a
              WHERE a.payer_id = $1 AND a.collection_month = $2
                AND a.mrf_source_id = s.mrf_source_id)`, payer, month).Scan(&missingAttachment, &incompleteAttachment); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	addBlocker(&out.Blockers, missingAttachment != 0, "attachment_missing")
	addBlocker(&out.Blockers, incompleteAttachment != 0, "attachment_incomplete")

	var unassigned int64
	if err := q.QueryRow(ctx, `
SELECT count(*)
FROM mrfpipeline.mrf_plans p
JOIN mrfpipeline.mrf_snapshots s ON s.id = p.mrf_snapshot_id
WHERE s.payer_id = $1 AND s.collection_month = $2
  AND EXISTS (SELECT 1 FROM mrfpipeline.monthly_release_mrf_sources a
              WHERE a.payer_id = $1 AND a.collection_month = $2
                AND a.mrf_source_id = s.mrf_source_id)
  AND NOT EXISTS (
      SELECT 1
      FROM mrfpipeline.plan_attachment_batch_items i
      JOIN mrfpipeline.plan_attachment_batches b ON b.id = i.plan_attachment_batch_id
      WHERE i.mrf_plan_id = p.id
        AND b.mrf_snapshot_id = p.mrf_snapshot_id
        AND b.status = 'succeeded')`, payer, month).Scan(&unassigned); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	addBlocker(&out.Blockers, unassigned != 0, "plan_unassigned")

	var inconsistent bool
	if err := q.QueryRow(ctx, `
WITH relevant(job_id, kind, arg_key, domain_id, nonterminal) AS (
    SELECT d.river_job_id, 'discovery.run', 'discovery_run_id', d.id,
           d.status IN ('pending', 'running')
    FROM mrfpipeline.discovery_runs d
    WHERE d.payer_id = $1 AND d.collection_month = $2
    UNION ALL
    SELECT t.download_river_job_id, 'toc.download', 'toc_file_id', t.id,
           t.download_status IN ('pending', 'running')
    FROM mrfpipeline.toc_files t
    WHERE t.payer_id = $1 AND t.collection_month = $2
    UNION ALL
    SELECT t.parse_river_job_id, 'toc.parse', 'toc_file_id', t.id,
           t.parse_status IN ('pending', 'running')
    FROM mrfpipeline.toc_files t
    WHERE t.payer_id = $1 AND t.collection_month = $2
    UNION ALL
    SELECT t.import_river_job_id, 'toc.import', 'toc_file_id', t.id,
           t.import_status IN ('pending', 'running')
    FROM mrfpipeline.toc_files t
    WHERE t.payer_id = $1 AND t.collection_month = $2
    UNION ALL
    SELECT m.download_river_job_id, 'mrf.download', 'mrf_source_id', m.id,
           m.download_status IN ('pending', 'running')
    FROM mrfpipeline.mrf_sources m
    JOIN (SELECT DISTINCT mrf_source_id FROM mrfpipeline.mrf_snapshots
          WHERE payer_id = $1 AND collection_month = $2
            AND EXISTS (SELECT 1 FROM mrfpipeline.monthly_release_mrf_sources a
                        WHERE a.payer_id = $1 AND a.collection_month = $2
                          AND a.mrf_source_id = mrf_snapshots.mrf_source_id)) x ON x.mrf_source_id = m.id
    UNION ALL
    SELECT m.parse_river_job_id, 'mrf.parse', 'mrf_source_id', m.id,
           m.parse_status IN ('pending', 'running')
    FROM mrfpipeline.mrf_sources m
    JOIN (SELECT DISTINCT mrf_source_id FROM mrfpipeline.mrf_snapshots
          WHERE payer_id = $1 AND collection_month = $2
            AND EXISTS (SELECT 1 FROM mrfpipeline.monthly_release_mrf_sources a
                        WHERE a.payer_id = $1 AND a.collection_month = $2
                          AND a.mrf_source_id = mrf_snapshots.mrf_source_id)) x ON x.mrf_source_id = m.id
    UNION ALL
    SELECT s.consume_river_job_id, 'consumer.ingest', 'mrf_snapshot_id', s.id,
           s.consume_status IN ('pending', 'running')
    FROM mrfpipeline.mrf_snapshots s
    WHERE s.payer_id = $1 AND s.collection_month = $2
    UNION ALL
    SELECT b.river_job_id, 'consumer.attach_plans', 'plan_attachment_batch_id', b.id,
           b.status IN ('pending', 'running')
    FROM mrfpipeline.plan_attachment_batches b
    JOIN mrfpipeline.mrf_snapshots s ON s.id = b.mrf_snapshot_id
    WHERE s.payer_id = $1 AND s.collection_month = $2
)
SELECT EXISTS (
    SELECT 1
    FROM relevant r
    WHERE r.nonterminal
      AND (r.job_id IS NULL OR NOT EXISTS (
          SELECT 1
          FROM mrfpipeline_river.river_job j
          WHERE j.id = r.job_id
            AND j.kind = r.kind
            AND j.state IN ('available', 'pending', 'scheduled', 'retryable', 'running')
            AND j.args = jsonb_build_object(r.arg_key, to_jsonb(r.domain_id))
      ))
)`, payer, month).Scan(&inconsistent); err != nil {
		return StatusReport{}, dbFailure(ctx)
	}
	addBlocker(&out.Blockers, inconsistent, "job_inconsistent")
	candidate, candidateErr := SelectCatalogCandidate(ctx, q, payer, month)
	if candidateErr != nil {
		if errors.Is(candidateErr, database.ErrDatabase) || jobs.IsFailure(candidateErr, jobs.FailureFilterCatalogDatabaseFailed) || ctx.Err() != nil || jobs.IsFailure(candidateErr, jobs.FailureReleaseNotFound) {
			return StatusReport{}, candidateErr
		}
	} else {
		out.ProspectiveCandidateValid = true
		catalog, catalogErr := inspectActivationCatalog(ctx, q, payer, formatMonth(month), candidate.TargetPublicationGeneration, candidate.Targets)
		if catalogErr != nil {
			if errors.Is(catalogErr, database.ErrDatabase) || jobs.IsFailure(catalogErr, jobs.FailureFilterCatalogDatabaseFailed) || ctx.Err() != nil {
				return StatusReport{}, catalogErr
			}
			if catalog.ID > 0 {
				out.FilterCatalogID = &catalog.ID
				out.FilterCatalogGeneration = &candidate.TargetPublicationGeneration
				out.FilterCatalogStatus = &catalog.Status
				out.FilterCatalogOutputCount = &catalog.OutputCount
			}
			switch {
			case jobs.IsFailure(catalogErr, jobs.FailureFilterCatalogMissing):
				addBlocker(&out.Blockers, true, "filter_catalog_missing")
			case catalog.Status == "building" || catalog.Status == "failed":
				addBlocker(&out.Blockers, true, "filter_catalog_"+catalog.Status)
			case jobs.IsFailure(catalogErr, jobs.FailureFilterCatalogStale):
				addBlocker(&out.Blockers, true, "filter_catalog_stale")
			default:
				addBlocker(&out.Blockers, true, "filter_catalog_inconsistent")
			}
		} else {
			id, status, count := catalog.ID, catalog.Status, catalog.OutputCount
			out.FilterCatalogID = &id
			out.FilterCatalogGeneration = &candidate.TargetPublicationGeneration
			out.FilterCatalogStatus = &status
			out.FilterCatalogOutputCount = &count
			if status == "ready" || (status == "published" && !candidate.HasNewOutputs) {
				out.FilterCatalogReady = true
			} else if status == "building" || status == "failed" {
				addBlocker(&out.Blockers, true, "filter_catalog_"+status)
			} else {
				addBlocker(&out.Blockers, true, "filter_catalog_not_ready")
			}
		}
	}
	sort.Strings(out.Blockers)
	if out.ProspectiveCandidateValid && !out.FilterCatalogReady {
		out.DatabaseReady = false
	} else {
		out.DatabaseReady = len(out.Blockers) == 0
	}
	return out, nil
}

func ListActiveOutputs(ctx context.Context, pool *pgxpool.Pool) ([]Target, error) {
	if pool == nil {
		return nil, jobs.Failure(jobs.FailureInvalidArguments)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, dbFailure(ctx)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var activeCount, catalogCount int64
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.monthly_releases WHERE status='active'`).Scan(&activeCount); err != nil {
		return nil, dbFailure(ctx)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_catalogs`).Scan(&catalogCount); err != nil {
		return nil, dbFailure(ctx)
	}
	if activeCount != catalogCount {
		return nil, jobs.Failure(jobs.FailureFilterCatalogInconsistent)
	}
	rows, err := tx.Query(ctx, `SELECT s.payer_id,s.collection_month,s.id FROM mrfweb.active_release_outputs c JOIN mrfpipeline.mrf_snapshots s ON c.payer_id=s.payer_id AND c.collection_month=s.collection_month AND c.output_id='mrf-'||s.id ORDER BY s.payer_id,s.collection_month,s.id`)
	if err != nil {
		return nil, dbFailure(ctx)
	}
	defer rows.Close()
	var out []Target
	for rows.Next() {
		var p string
		var m time.Time
		var id int64
		if err := rows.Scan(&p, &m, &id); err != nil {
			return nil, dbFailure(ctx)
		}
		out = append(out, Target{PayerID: p, CollectionMonth: formatMonth(m), OutputID: formatOutputID(id), SnapshotID: id})
	}
	if err := rows.Err(); err != nil {
		return nil, dbFailure(ctx)
	}
	return out, nil
}

func ListTargets(ctx context.Context, pool *pgxpool.Pool, payer string, month time.Time) ([]Target, error) {
	rows, err := pool.Query(ctx, `
SELECT payer_id, collection_month, id
FROM mrfpipeline.mrf_snapshots
WHERE payer_id = $1 AND collection_month = $2
  AND EXISTS (
      SELECT 1 FROM mrfpipeline.monthly_release_mrf_sources a
      WHERE a.payer_id = $1 AND a.collection_month = $2
        AND a.mrf_source_id = mrf_snapshots.mrf_source_id
  )
ORDER BY id`, payer, month)
	if err != nil {
		return nil, dbFailure(ctx)
	}
	defer rows.Close()
	var out []Target
	for rows.Next() {
		var p string
		var m time.Time
		var id int64
		if err := rows.Scan(&p, &m, &id); err != nil {
			return nil, dbFailure(ctx)
		}
		out = append(out, Target{PayerID: p, CollectionMonth: formatMonth(m), OutputID: formatOutputID(id), SnapshotID: id})
	}
	if err := rows.Err(); err != nil {
		return nil, dbFailure(ctx)
	}
	return out, nil
}

func ListPublishableTargets(ctx context.Context, pool *pgxpool.Pool, payer string, month time.Time) ([]Target, error) {
	if pool == nil {
		return nil, jobs.Failure(jobs.FailureInvalidArguments)
	}
	return listPublishableTargets(ctx, pool, payer, month)
}

func listPublishableTargets(ctx context.Context, q rowsQueryer, payer string, month time.Time) ([]Target, error) {
	rows, err := q.Query(ctx, `
SELECT s.payer_id, s.collection_month, s.id
FROM mrfpipeline.mrf_snapshots s
JOIN mrfpipeline.monthly_release_mrf_sources a
  ON a.payer_id = s.payer_id
 AND a.collection_month = s.collection_month
 AND a.mrf_source_id = s.mrf_source_id
WHERE s.payer_id = $1
  AND s.collection_month = $2
  AND s.consume_status = 'succeeded'
  AND EXISTS (
      SELECT 1
      FROM mrfpipeline.plan_attachment_batches b
      WHERE b.mrf_snapshot_id = s.id
        AND b.status = 'succeeded'
        AND b.added_plan_count > 0)
  AND NOT EXISTS (
      SELECT 1
      FROM mrfpipeline.plan_attachment_batches b
      WHERE b.mrf_snapshot_id = s.id AND b.status <> 'succeeded')
  AND NOT EXISTS (
      SELECT 1
      FROM mrfpipeline.mrf_plans p
      WHERE p.mrf_snapshot_id = s.id
        AND NOT EXISTS (
            SELECT 1
            FROM mrfpipeline.plan_attachment_batch_items i
            JOIN mrfpipeline.plan_attachment_batches b
              ON b.id = i.plan_attachment_batch_id
            WHERE i.mrf_plan_id = p.id
              AND b.mrf_snapshot_id = s.id
              AND b.status = 'succeeded'))
ORDER BY s.id`, payer, month)
	if err != nil {
		return nil, dbFailure(ctx)
	}
	defer rows.Close()
	var out []Target
	for rows.Next() {
		var p string
		var m time.Time
		var id int64
		if err := rows.Scan(&p, &m, &id); err != nil {
			return nil, dbFailure(ctx)
		}
		out = append(out, Target{PayerID: p, CollectionMonth: formatMonth(m), OutputID: formatOutputID(id), SnapshotID: id})
	}
	if err := rows.Err(); err != nil {
		return nil, dbFailure(ctx)
	}
	return out, nil
}

func ListPublishedTargets(ctx context.Context, pool *pgxpool.Pool, payer string, month time.Time) ([]Target, error) {
	if pool == nil {
		return nil, jobs.Failure(jobs.FailureInvalidArguments)
	}
	return listPublishedTargets(ctx, pool, payer, month)
}

func listPublishedTargets(ctx context.Context, q rowsQueryer, payer string, month time.Time) ([]Target, error) {
	rows, err := q.Query(ctx, `
SELECT s.payer_id, s.collection_month, s.id
FROM mrfpipeline.monthly_release_outputs o
JOIN mrfpipeline.mrf_snapshots s ON s.id = o.mrf_snapshot_id
WHERE o.payer_id = $1 AND o.collection_month = $2
ORDER BY s.id`, payer, month)
	if err != nil {
		return nil, dbFailure(ctx)
	}
	defer rows.Close()
	var out []Target
	for rows.Next() {
		var p string
		var m time.Time
		var id int64
		if err := rows.Scan(&p, &m, &id); err != nil {
			return nil, dbFailure(ctx)
		}
		out = append(out, Target{PayerID: p, CollectionMonth: formatMonth(m), OutputID: formatOutputID(id), SnapshotID: id})
	}
	if err := rows.Err(); err != nil {
		return nil, dbFailure(ctx)
	}
	return out, nil
}

// ListPlanBatchIDs returns the durable succeeded attachment batches whose
// positive publications are expected for one snapshot.
func ListPlanBatchIDs(ctx context.Context, pool *pgxpool.Pool, snapshotID int64) ([]int64, error) {
	if pool == nil || snapshotID <= 0 {
		return nil, jobs.Failure(jobs.FailureInvalidArguments)
	}
	rows, err := pool.Query(ctx, `
SELECT id
FROM mrfpipeline.plan_attachment_batches
WHERE mrf_snapshot_id = $1 AND status = 'succeeded' AND added_plan_count > 0
ORDER BY id`, snapshotID)
	if err != nil {
		return nil, dbFailure(ctx)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, dbFailure(ctx)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, dbFailure(ctx)
	}
	return ids, nil
}

// HasPlanRowsAfterSeal reports unsupported plan-set changes after the first
// release seal. The caller passes only a non-nil sealed timestamp.
func HasPlanRowsAfterSeal(ctx context.Context, pool *pgxpool.Pool, snapshotID int64, sealedAt time.Time) (bool, error) {
	if pool == nil || snapshotID <= 0 {
		return false, jobs.Failure(jobs.FailureInvalidArguments)
	}
	var changed bool
	if err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM mrfpipeline.mrf_plans
    WHERE mrf_snapshot_id = $1 AND created_at > $2
    UNION ALL
    SELECT 1
    FROM mrfpipeline.plan_attachment_batches
    WHERE mrf_snapshot_id = $1 AND created_at > $2
    UNION ALL
    SELECT 1
    FROM mrfpipeline.plan_attachment_batch_items i
    JOIN mrfpipeline.plan_attachment_batches b ON b.id = i.plan_attachment_batch_id
    WHERE b.mrf_snapshot_id = $1 AND i.created_at > $2
)`, snapshotID, sealedAt).Scan(&changed); err != nil {
		return false, dbFailure(ctx)
	}
	return changed, nil
}

func ReleaseSealedAt(ctx context.Context, pool *pgxpool.Pool, payer string, month time.Time) (*time.Time, error) {
	if pool == nil {
		return nil, jobs.Failure(jobs.FailureInvalidArguments)
	}
	var sealedAt *time.Time
	if err := pool.QueryRow(ctx, `
SELECT sealed_at
FROM mrfpipeline.monthly_releases
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&sealedAt); errors.Is(err, pgx.ErrNoRows) {
		return nil, jobs.Failure(jobs.FailureReleaseNotFound)
	} else if err != nil {
		return nil, dbFailure(ctx)
	}
	return sealedAt, nil
}

func OutputPublishedAt(ctx context.Context, pool *pgxpool.Pool, payer string, month time.Time, snapshotID int64) (*time.Time, error) {
	if pool == nil || snapshotID <= 0 {
		return nil, jobs.Failure(jobs.FailureInvalidArguments)
	}
	var publishedAt time.Time
	err := pool.QueryRow(ctx, `
SELECT published_at
FROM mrfpipeline.monthly_release_outputs
WHERE payer_id = $1 AND collection_month = $2 AND mrf_snapshot_id = $3`,
		payer, month, snapshotID).Scan(&publishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, dbFailure(ctx)
	}
	return &publishedAt, nil
}

type publicationCheckpoint struct {
	SnapshotCount            int64
	PublishedCount           int64
	TerminalFileFailureCount int64
}

func inspectPublicationCheckpoint(ctx context.Context, q queryer, payer string, month time.Time) (publicationCheckpoint, error) {
	var out publicationCheckpoint
	var targetKind string
	var targetCount *int64
	if err := q.QueryRow(ctx, `
SELECT mrf_source_target_kind, mrf_source_target_count
FROM mrfpipeline.monthly_releases
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&targetKind, &targetCount); errors.Is(err, pgx.ErrNoRows) {
		return out, jobs.Failure(jobs.FailureReleaseNotFound)
	} else if err != nil {
		return out, dbFailure(ctx)
	}
	if targetKind == "unset" {
		return out, jobs.Failure(jobs.FailureReleaseNotReady)
	}
	var knownSources, selectedSources int64
	if err := q.QueryRow(ctx, `
SELECT
  (SELECT count(DISTINCT mrf_source_id)
   FROM mrfpipeline.mrf_snapshots
   WHERE payer_id = $1 AND collection_month = $2),
  (SELECT count(*)
   FROM mrfpipeline.monthly_release_mrf_sources
   WHERE payer_id = $1 AND collection_month = $2)`,
		payer, month).Scan(&knownSources, &selectedSources); err != nil {
		return out, dbFailure(ctx)
	}
	requiredSelected := knownSources
	if targetKind == "numeric" && targetCount != nil && *targetCount < requiredSelected {
		requiredSelected = *targetCount
	}
	if selectedSources != requiredSelected {
		return out, jobs.Failure(jobs.FailureReleaseNotReady)
	}

	var discoveryCount, discoveryBad int64
	if err := q.QueryRow(ctx, `
SELECT count(*), count(*) FILTER (WHERE status <> 'succeeded')
FROM mrfpipeline.discovery_runs
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&discoveryCount, &discoveryBad); err != nil {
		return out, dbFailure(ctx)
	}
	if discoveryCount == 0 || discoveryBad != 0 {
		return out, jobs.Failure(jobs.FailureReleaseNotReady)
	}

	var tocCount, tocNonterminal, tocFailed int64
	if err := q.QueryRow(ctx, `
SELECT count(*),
       count(*) FILTER (WHERE download_status IN ('pending', 'running')
                           OR parse_status IN ('pending', 'running')
                           OR import_status IN ('pending', 'running')),
       count(*) FILTER (WHERE download_status = 'failed'
                           OR parse_status = 'failed'
                           OR import_status = 'failed')
FROM mrfpipeline.toc_files
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&tocCount, &tocNonterminal, &tocFailed); err != nil {
		return out, dbFailure(ctx)
	}
	if tocCount == 0 || tocNonterminal != 0 {
		return out, jobs.Failure(jobs.FailureReleaseNotReady)
	}

	var publicationFailed int64
	if err := q.QueryRow(ctx, `
SELECT
  (SELECT count(*)
   FROM mrfpipeline.mrf_snapshots s
   JOIN mrfpipeline.monthly_release_mrf_sources a
     ON a.payer_id = s.payer_id
    AND a.collection_month = s.collection_month
    AND a.mrf_source_id = s.mrf_source_id
   WHERE s.payer_id = $1 AND s.collection_month = $2
     AND s.consume_status = 'failed')
  +
  (SELECT count(*)
   FROM mrfpipeline.plan_attachment_batches b
   JOIN mrfpipeline.mrf_snapshots s ON s.id = b.mrf_snapshot_id
   JOIN mrfpipeline.monthly_release_mrf_sources a
     ON a.payer_id = s.payer_id
    AND a.collection_month = s.collection_month
    AND a.mrf_source_id = s.mrf_source_id
   WHERE s.payer_id = $1 AND s.collection_month = $2
     AND b.status = 'failed')`, payer, month).Scan(&publicationFailed); err != nil {
		return out, dbFailure(ctx)
	}
	if publicationFailed != 0 {
		return out, jobs.Failure(jobs.FailureReleaseNotReady)
	}

	if err := q.QueryRow(ctx, `
SELECT count(*)
FROM mrfpipeline.mrf_snapshots s
JOIN mrfpipeline.monthly_release_mrf_sources a
  ON a.payer_id = s.payer_id
 AND a.collection_month = s.collection_month
 AND a.mrf_source_id = s.mrf_source_id
WHERE s.payer_id = $1 AND s.collection_month = $2`, payer, month).Scan(&out.SnapshotCount); err != nil {
		return out, dbFailure(ctx)
	}
	var mrfFailed int64
	if err := q.QueryRow(ctx, `
SELECT count(*)
FROM mrfpipeline.mrf_sources s
JOIN mrfpipeline.monthly_release_mrf_sources a ON a.mrf_source_id = s.id
WHERE a.payer_id = $1 AND a.collection_month = $2
  AND (s.download_status = 'failed' OR s.parse_status = 'failed')`, payer, month).Scan(&mrfFailed); err != nil {
		return out, dbFailure(ctx)
	}
	if err := q.QueryRow(ctx, `
SELECT count(*)
FROM mrfpipeline.monthly_release_outputs
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&out.PublishedCount); err != nil {
		return out, dbFailure(ctx)
	}
	out.TerminalFileFailureCount = tocFailed + mrfFailed
	return out, nil
}

func mergeTargets(left, right []Target) []Target {
	seen := make(map[int64]struct{}, len(left)+len(right))
	out := make([]Target, 0, len(left)+len(right))
	for _, targets := range [][]Target{left, right} {
		for _, target := range targets {
			if _, ok := seen[target.SnapshotID]; ok {
				continue
			}
			seen[target.SnapshotID] = struct{}{}
			out = append(out, target)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SnapshotID < out[j].SnapshotID })
	return out
}

func Activate(ctx context.Context, pool *pgxpool.Pool, payer string, month time.Time, preflight func([]Target, int64, time.Time) error) (ActivationResult, error) {
	if pool == nil {
		return ActivationResult{}, jobs.Failure(jobs.FailureInvalidArguments)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return ActivationResult{}, dbFailure(ctx)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
SELECT payer_id, collection_month, status, publication_generation
FROM mrfpipeline.monthly_releases
WHERE payer_id = $1
ORDER BY collection_month
FOR UPDATE`, payer)
	if err != nil {
		return ActivationResult{}, dbFailure(ctx)
	}
	var targetStatus string
	var generation int64
	var activeMonth *time.Time
	for rows.Next() {
		var p string
		var m time.Time
		var s string
		var g int64
		if err := rows.Scan(&p, &m, &s, &g); err != nil {
			rows.Close()
			return ActivationResult{}, dbFailure(ctx)
		}
		if m.Equal(month) {
			targetStatus = s
			generation = g
		}
		if s == Active && !m.Equal(month) {
			mm := m
			activeMonth = &mm
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return ActivationResult{}, dbFailure(ctx)
	}
	if targetStatus == "" {
		return ActivationResult{}, jobs.Failure(jobs.FailureReleaseNotFound)
	}
	if targetStatus != Building && generation <= 0 {
		return ActivationResult{}, jobs.Failure(jobs.FailureDomainInvariant)
	}
	if targetStatus != Building && targetStatus != Inactive && targetStatus != Active {
		return ActivationResult{}, jobs.Failure(jobs.FailureDomainInvariant)

	}
	state := publicationCheckpoint{}
	published, err := listPublishedTargets(ctx, tx, payer, month)
	if err != nil {
		return ActivationResult{}, err
	}
	targets := published
	if targetStatus != Inactive {
		state, err = inspectPublicationCheckpoint(ctx, tx, payer, month)
		if err != nil {
			return ActivationResult{}, err
		}
		publishable, err := listPublishableTargets(ctx, tx, payer, month)
		if err != nil {
			return ActivationResult{}, err
		}
		targets = mergeTargets(published, publishable)
	}
	if len(targets) == 0 {
		return ActivationResult{}, jobs.Failure(jobs.FailureReleaseNotReady)
	}

	newTargets := make([]Target, 0, len(targets))
	publishedIDs := make(map[int64]struct{}, len(published))
	for _, target := range published {
		publishedIDs[target.SnapshotID] = struct{}{}
	}
	for _, target := range targets {
		if _, ok := publishedIDs[target.SnapshotID]; !ok {
			newTargets = append(newTargets, target)
		}
	}
	if targetStatus == Building && (len(published) != 0 || len(newTargets) != len(targets)) {
		return ActivationResult{}, jobs.Failure(jobs.FailureDomainInvariant)
	}
	var added int64
	expectedCatalogGeneration := generation
	if len(newTargets) > 0 || targetStatus == Building {
		if generation == math.MaxInt64 {
			return ActivationResult{}, jobs.Failure(jobs.FailureDomainInvariant)
		}
		expectedCatalogGeneration = generation + 1
	}
	allowPublishedCatalog := targetStatus != Building && len(newTargets) == 0
	activationCatalog, err := lockActivationCatalog(ctx, tx, payer, formatMonth(month), expectedCatalogGeneration, targets, allowPublishedCatalog)
	if err != nil {
		return ActivationResult{}, err
	}
	if preflight != nil {
		if err := preflight(targets, *activationCatalog.ProviderSchema, *activationCatalog.ProviderMonth); err != nil {
			return ActivationResult{}, err
		}
	}
	if targetStatus == Active && len(newTargets) == 0 && activationCatalog.Status == "published" {
		skipped := state.SnapshotCount - int64(len(published))
		if skipped < 0 {
			skipped = 0
		}
		return ActivationResult{PayerID: payer, CollectionMonth: formatMonth(month), OutputCount: int64(len(published)), PublicationGeneration: generation, SkippedOutputCount: skipped, TerminalFileFailureCount: state.TerminalFileFailureCount, FilterCatalogID: activationCatalog.ID}, nil
	}
	if targetStatus == Building || len(newTargets) > 0 {
		ids := make([]int64, 0, len(newTargets))
		for _, target := range newTargets {
			ids = append(ids, target.SnapshotID)
		}
		tag, err := tx.Exec(ctx, `INSERT INTO mrfpipeline.monthly_release_outputs(payer_id,collection_month,mrf_snapshot_id,published_generation) SELECT $1,$2,id,$4 FROM mrfpipeline.mrf_snapshots WHERE payer_id=$1 AND collection_month=$2 AND id=ANY($3::bigint[])`, payer, month, ids, expectedCatalogGeneration)
		if err != nil || tag.RowsAffected() != int64(len(newTargets)) {
			return ActivationResult{}, dbFailure(ctx)
		}
		added = tag.RowsAffected()
		generation = expectedCatalogGeneration
	}

	if targetStatus != Active {
		if _, err := tx.Exec(ctx, `
UPDATE mrfpipeline.monthly_releases
SET status = 'inactive', updated_at = transaction_timestamp()
WHERE payer_id = $1 AND status = 'active'`, payer); err != nil {
			return ActivationResult{}, dbFailure(ctx)
		}
	}
	if targetStatus != Active || len(newTargets) > 0 {
		if _, err := tx.Exec(ctx, `
UPDATE mrfpipeline.monthly_releases
SET status = 'active',
    publication_generation = $3,
    sealed_at = COALESCE(sealed_at, transaction_timestamp()),
    last_activated_at = transaction_timestamp(),
    updated_at = transaction_timestamp()
WHERE payer_id = $1 AND collection_month = $2`, payer, month, expectedCatalogGeneration); err != nil {
			return ActivationResult{}, dbFailure(ctx)
		}
	}
	var outputCount int64
	if err := tx.QueryRow(ctx, `
SELECT count(*)
FROM mrfpipeline.monthly_release_outputs
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&outputCount); err != nil {
		return ActivationResult{}, dbFailure(ctx)
	}
	if activationCatalog.Status == "ready" {
		tag, err := tx.Exec(ctx, `UPDATE mrfweb.release_catalogs SET status='published', published_at=transaction_timestamp() WHERE id=$1 AND status='ready'`, activationCatalog.ID)
		if err != nil || tag.RowsAffected() != 1 {
			return ActivationResult{}, dbFailure(ctx)
		}
	}
	if outputCount != int64(len(targets)) {
		return ActivationResult{}, jobs.Failure(jobs.FailureFilterCatalogInconsistent)
	}
	post, err := inspectActivationCatalog(ctx, tx, payer, formatMonth(month), generation, targets)
	if err != nil || post.ID != activationCatalog.ID || post.Status != "published" {
		if err != nil {
			return ActivationResult{}, err
		}
		return ActivationResult{}, jobs.Failure(jobs.FailureFilterCatalogInconsistent)
	}
	if err := tx.Commit(ctx); err != nil {
		return ActivationResult{}, dbFailure(ctx)
	}
	result := ActivationResult{
		PayerID: payer, CollectionMonth: formatMonth(month),
		OutputCount: outputCount, PublicationGeneration: generation,
		AddedOutputCount: added, SkippedOutputCount: state.SnapshotCount - outputCount,
		TerminalFileFailureCount: state.TerminalFileFailureCount,
		FilterCatalogID:          activationCatalog.ID,
	}
	if result.SkippedOutputCount < 0 {
		result.SkippedOutputCount = 0
	}
	if activeMonth != nil {
		text := formatMonth(*activeMonth)
		result.PreviousCollectionMonth = &text
	}
	return result, nil
}

func requireStatus(ctx context.Context, q queryer, payer string, month time.Time, want string, lock bool) error {
	query := `SELECT status FROM mrfpipeline.monthly_releases WHERE payer_id = $1 AND collection_month = $2`
	if lock {
		query += ` FOR UPDATE`
	}
	var status string
	err := q.QueryRow(ctx, query, payer, month).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureReleaseNotFound)
	}
	if err != nil {
		return dbFailure(ctx)
	}
	if status != want {
		return jobs.Failure(jobs.FailureSealedReleaseInconsistent)
	}
	return nil
}

func addBlocker(blockers *[]string, yes bool, code string) {
	if yes {
		*blockers = append(*blockers, code)
	}
}

func formatMonth(month time.Time) string {
	return fmt.Sprintf("%04d-%02d", month.Year(), month.Month())
}

func formatOutputID(id int64) string { return fmt.Sprintf("mrf-%d", id) }

func dbFailure(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return database.ErrDatabase
}
