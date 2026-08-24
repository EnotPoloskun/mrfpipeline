package tocimport

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

type importMeta struct {
	tocID     int64
	payer     string
	month     string
	monthDate time.Time
}

func importAssociations(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], dir string, meta importMeta, afterBatch func(int) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if pool == nil || client == nil {
		return jobs.Failure(jobs.FailureTOCImportInvariant)
	}
	parts, err := listParts(filepath.Join(dir, datasetAssociations))
	if err != nil {
		return mapValidateErr(err)
	}
	batch := make([]assocRow, 0, maxBatchRows)
	n := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := importBatch(ctx, pool, client, meta, batch); err != nil {
			return err
		}
		n++
		if afterBatch != nil {
			if err := afterBatch(n); err != nil {
				return err
			}
		}
		batch = batch[:0]
		return nil
	}
	for _, path := range parts {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := forEachAssocRow(ctx, path, func(row assocRow) error {
			batch = append(batch, row)
			if len(batch) >= maxBatchRows {
				return flush()
			}
			return nil
		})
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if jobs.IsFailure(err, jobs.FailureTOCImportDatabaseFailed) ||
				jobs.IsFailure(err, jobs.FailureDomainInvariant) ||
				jobs.IsFailure(err, jobs.FailureTOCImportInvariant) {
				return err
			}
			return mapValidateErr(err)
		}
	}
	return flush()
}

func importBatch(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], meta importMeta, rows []assocRow) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return classifyImportDB(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var payer string
	var month time.Time
	err = tx.QueryRow(ctx, `
SELECT payer_id, collection_month
FROM mrfpipeline.toc_files
WHERE id = $1
FOR UPDATE`, meta.tocID).Scan(&payer, &month)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return classifyImportDB(ctx, err)
	}
	if payer != meta.payer || !month.Equal(meta.monthDate) {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}

	locs, keys := collectSourceInfos(rows)

	sourceIDs := make([]int64, 0, len(keys))
	seenID := map[int64]bool{}
	sources := map[int64]sourceInfo{}
	for _, loc := range keys {
		info := locs[loc]
		id, inserted, err := upsertSource(ctx, tx, loc, meta.monthDate, info.filename)
		if err != nil {
			return err
		}
		info.sourceID = id
		info.inserted = inserted
		locs[loc] = info
		sources[id] = info
		if !seenID[id] {
			seenID[id] = true
			sourceIDs = append(sourceIDs, id)
		}
	}
	sourceIDs = sourceIDsSorted(sourceIDs)

	locked, err := lockSources(ctx, tx, sourceIDs)
	if err != nil {
		return err
	}
	snapshots := map[int64]int64{}
	for _, src := range locked {
		info, ok := sources[src.id]
		if !ok {
			return jobs.Failure(jobs.FailureTOCImportInvariant)
		}
		if info.filename != nil {
			if _, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_sources
SET first_mrf_filename = COALESCE(first_mrf_filename, $2),
    updated_at = CASE
        WHEN first_mrf_filename IS NULL THEN transaction_timestamp()
        ELSE updated_at
    END
WHERE id = $1`, src.id, info.filename); err != nil {
				return classifyImportDB(ctx, err)
			}
		}
		if info.inserted {
			jobID, err := jobs.InsertTx(ctx, client, tx, &jobs.MRFDownloadArgs{MRFSourceID: src.id})
			if err != nil {
				return classifyImportDB(ctx, err)
			}
			if _, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_sources
SET download_river_job_id = $2, updated_at = transaction_timestamp()
WHERE id = $1`, src.id, jobID); err != nil {
				return classifyImportDB(ctx, err)
			}
		}
		snapID, err := upsertSnapshot(ctx, tx, client, src, meta)
		if err != nil {
			return err
		}
		snapshots[src.id] = snapID
	}

	for _, row := range rows {
		src := locs[row.MRFLocation]
		snapID, ok := snapshots[src.sourceID]
		if !ok {
			return jobs.Failure(jobs.FailureTOCImportInvariant)
		}
		if err := insertProvenance(ctx, tx, meta.tocID, snapID, row); err != nil {
			return err
		}
		if err := insertPlan(ctx, tx, snapID, row); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return classifyImportDB(ctx, err)
	}
	return nil
}

type lockedSource struct {
	id     int64
	parsed string
}

type sourceInfo struct {
	filename *string
	sourceID int64
	inserted bool
}

func collectSourceInfos(rows []assocRow) (map[string]sourceInfo, []string) {
	infos := make(map[string]sourceInfo)
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		info, ok := infos[row.MRFLocation]
		if !ok {
			infos[row.MRFLocation] = sourceInfo{filename: firstNonemptyFilename(nil, row.MRFFilename)}
			keys = append(keys, row.MRFLocation)
			continue
		}
		info.filename = firstNonemptyFilename(info.filename, row.MRFFilename)
		infos[row.MRFLocation] = info
	}
	sort.Strings(keys)
	return infos, keys
}

func firstNonemptyFilename(first, next *string) *string {
	if first != nil && *first != "" {
		return first
	}
	if next != nil && *next != "" {
		return next
	}
	return nil
}

func upsertSource(ctx context.Context, tx pgx.Tx, location string, month time.Time, filename *string) (int64, bool, error) {
	var id int64
	err := tx.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, first_mrf_filename, download_status, parse_status)
VALUES ($1, $2, $3, 'pending', 'blocked')
ON CONFLICT ON CONSTRAINT mrf_sources_source_url_collection_month_key DO NOTHING
RETURNING id`, location, month, filename).Scan(&id)
	if err == nil {
		return id, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, classifyImportDB(ctx, err)
	}
	if err := tx.QueryRow(ctx, `
SELECT id FROM mrfpipeline.mrf_sources
WHERE source_url = $1 AND collection_month = $2`, location, month).Scan(&id); err != nil {
		return 0, false, classifyImportDB(ctx, err)
	}
	return id, false, nil
}

func lockSources(ctx context.Context, tx pgx.Tx, ids []int64) ([]lockedSource, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx, `
SELECT id, parse_status
FROM mrfpipeline.mrf_sources
WHERE id = ANY($1)
ORDER BY id
FOR UPDATE`, ids)
	if err != nil {
		return nil, classifyImportDB(ctx, err)
	}
	defer rows.Close()
	var out []lockedSource
	for rows.Next() {
		var s lockedSource
		if err := rows.Scan(&s.id, &s.parsed); err != nil {
			return nil, classifyImportDB(ctx, err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, classifyImportDB(ctx, err)
	}
	if len(out) != len(ids) {
		return nil, jobs.Failure(jobs.FailureTOCImportInvariant)
	}
	return out, nil
}

func upsertSnapshot(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx], src lockedSource, meta importMeta) (int64, error) {
	parsed := src.parsed == jobs.StatusSucceeded
	consume := jobs.StatusBlocked
	if parsed {
		consume = jobs.StatusPending
	}
	var snapID int64
	var status string
	var jobID *int64
	err := tx.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, $2, $3, $4)
ON CONFLICT ON CONSTRAINT mrf_snapshots_source_payer_month_key DO NOTHING
RETURNING id, consume_status, consume_river_job_id`, src.id, meta.payer, meta.monthDate, consume).Scan(&snapID, &status, &jobID)
	if err == nil {
		if parsed {
			if err := scheduleIngest(ctx, tx, client, snapID); err != nil {
				return 0, err
			}
		}
		return snapID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, classifyImportDB(ctx, err)
	}
	if err := tx.QueryRow(ctx, `
SELECT id, consume_status, consume_river_job_id
FROM mrfpipeline.mrf_snapshots
WHERE mrf_source_id = $1 AND payer_id = $2 AND collection_month = $3
FOR UPDATE`, src.id, meta.payer, meta.monthDate).Scan(&snapID, &status, &jobID); err != nil {
		return 0, classifyImportDB(ctx, err)
	}
	if status == jobs.StatusPending && jobID == nil {
		return 0, jobs.Failure(jobs.FailureDomainInvariant)
	}
	if status == jobs.StatusBlocked && jobID != nil {
		return 0, jobs.Failure(jobs.FailureDomainInvariant)
	}
	if status == jobs.StatusBlocked && parsed && jobID == nil {
		if _, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_snapshots
SET consume_status = $2, updated_at = transaction_timestamp()
WHERE id = $1`, snapID, jobs.StatusPending); err != nil {
			return 0, classifyImportDB(ctx, err)
		}
		if err := scheduleIngest(ctx, tx, client, snapID); err != nil {
			return 0, err
		}
	}
	return snapID, nil
}

func scheduleIngest(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx], snapshotID int64) error {
	jobID, err := jobs.InsertTx(ctx, client, tx, &jobs.ConsumerIngestArgs{MRFSnapshotID: snapshotID})
	if err != nil {
		return classifyImportDB(ctx, err)
	}
	if _, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_snapshots
SET consume_river_job_id = $2, updated_at = transaction_timestamp()
WHERE id = $1`, snapshotID, jobID); err != nil {
		return classifyImportDB(ctx, err)
	}
	return nil
}

func insertProvenance(ctx context.Context, tx pgx.Tx, tocID, snapshotID int64, row assocRow) error {
	_, err := tx.Exec(ctx, `
INSERT INTO mrfpipeline.toc_mrf_plan_associations (
    toc_file_id, mrf_snapshot_id, mrf_location, mrf_filename,
    plan_name, issuer_name, plan_sponsor_name, plan_id_type, plan_id, plan_market_type
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT ON CONSTRAINT toc_mrf_plan_associations_unique_key DO NOTHING`,
		tocID, snapshotID, row.MRFLocation, row.MRFFilename,
		row.PlanName, row.IssuerName, row.PlanSponsorName, row.PlanIDType, row.PlanID, row.PlanMarketType)
	if err != nil {
		return classifyImportDB(ctx, err)
	}
	return nil
}

func canonicalSponsor(row assocRow) *string {
	if row.PlanIDType == "ein" {
		return row.PlanSponsorName
	}
	return nil
}

func insertPlan(ctx context.Context, tx pgx.Tx, snapshotID int64, row assocRow) error {
	sponsor := canonicalSponsor(row)
	_, err := tx.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_plans (
    mrf_snapshot_id, plan_name, issuer_name, plan_sponsor_name,
    plan_id_type, plan_id, plan_market_type
) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT ON CONSTRAINT mrf_plans_identity_key DO NOTHING`,
		snapshotID, row.PlanName, row.IssuerName, sponsor, row.PlanIDType, row.PlanID, row.PlanMarketType)
	if err != nil {
		return classifyImportDB(ctx, err)
	}
	return nil
}

func classifyImportDB(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if err == nil {
		return jobs.Failure(jobs.FailureTOCImportDatabaseFailed)
	}
	return jobs.Failure(jobs.FailureTOCImportDatabaseFailed)
}

func distinctLocations(rows []assocRow) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if seen[row.MRFLocation] {
			continue
		}
		seen[row.MRFLocation] = true
		out = append(out, row.MRFLocation)
	}
	sort.Strings(out)
	return out
}

func sourceIDsSorted(ids []int64) []int64 {
	out := append([]int64(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
