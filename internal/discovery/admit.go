package discovery

import (
	"context"
	"errors"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

type runRow struct {
	PayerID string
	Month   time.Time
	Limit   int32
}

func loadRun(ctx context.Context, pool *pgxpool.Pool, runID int64) (runRow, error) {
	var row runRow
	err := pool.QueryRow(ctx, `
SELECT payer_id, collection_month, toc_limit
FROM mrfpipeline.discovery_runs
WHERE id = $1`, runID).Scan(&row.PayerID, &row.Month, &row.Limit)
	if errors.Is(err, pgx.ErrNoRows) {
		return runRow{}, jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return runRow{}, jobs.Failure(jobs.FailureDiscoveryDatabase)
	}
	return row, nil
}

func admit(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], runID, riverJobID int64, files []TOCFile) error {
	first, err := firstOccurrences(files)
	if err != nil {
		return err
	}
	discovered := int64(len(files))

	tx, err := pool.Begin(ctx)
	if err != nil {
		return jobs.Failure(jobs.FailureDiscoveryDatabase)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var status string
	var assigned *int64
	var payer string
	var month time.Time
	var limit int32
	err = tx.QueryRow(ctx, `
SELECT status, river_job_id, payer_id, collection_month, toc_limit
FROM mrfpipeline.discovery_runs
WHERE id = $1
FOR UPDATE`, runID).Scan(&status, &assigned, &payer, &month, &limit)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return jobs.Failure(jobs.FailureDiscoveryDatabase)
	}
	if assigned == nil || *assigned != riverJobID || status != jobs.StatusRunning {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}

	var existing, admitted, overflow int64
	for _, hit := range first {
		id, found, err := lookupTOC(ctx, tx, payer, hit.URL)
		if err != nil {
			return err
		}
		if found {
			if err := observeExisting(ctx, tx, runID, id, hit.Ordinal); err != nil {
				return err
			}
			existing++
			continue
		}
		if admitted >= int64(limit) {
			overflow++
			continue
		}
		id, inserted, err := insertTOC(ctx, tx, payer, month, hit.URL, runID)
		if err != nil {
			return err
		}
		if !inserted {
			id, found, err = lookupTOC(ctx, tx, payer, hit.URL)
			if err != nil {
				return err
			}
			if !found {
				return jobs.Failure(jobs.FailureDiscoveryDatabase)
			}
			if err := observeExisting(ctx, tx, runID, id, hit.Ordinal); err != nil {
				return err
			}
			existing++
			continue
		}
		if err := insertMembership(ctx, tx, runID, id, hit.Ordinal, true); err != nil {
			return err
		}
		jobID, err := jobs.InsertTx(ctx, client, tx, &jobs.TOCDownloadArgs{TOCFileID: id})
		if err != nil {
			return jobs.Failure(jobs.FailureDiscoveryDatabase)
		}
		tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.toc_files
SET download_river_job_id = $2, updated_at = transaction_timestamp()
WHERE id = $1`, id, jobID)
		if err != nil || tag.RowsAffected() != 1 {
			return jobs.Failure(jobs.FailureDiscoveryDatabase)
		}
		admitted++
	}

	tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.discovery_runs
SET discovered_count = $2,
    existing_count = $3,
    admitted_count = $4,
    overflow_count = $5,
    status = 'succeeded',
    completed_at = transaction_timestamp(),
    updated_at = transaction_timestamp(),
    failure_code = NULL
WHERE id = $1`, runID, discovered, existing, admitted, overflow)
	if err != nil || tag.RowsAffected() != 1 {
		return jobs.Failure(jobs.FailureDiscoveryDatabase)
	}
	if err := tx.Commit(ctx); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return jobs.Failure(jobs.FailureDiscoveryDatabase)
	}
	return nil
}

func lookupTOC(ctx context.Context, tx pgx.Tx, payer, url string) (int64, bool, error) {
	var id int64
	err := tx.QueryRow(ctx, `
SELECT id FROM mrfpipeline.toc_files
WHERE payer_id = $1 AND source_url = $2`, payer, url).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, jobs.Failure(jobs.FailureDiscoveryDatabase)
	}
	return id, true, nil
}

func insertTOC(ctx context.Context, tx pgx.Tx, payer string, month time.Time, url string, runID int64) (int64, bool, error) {
	var id int64
	err := tx.QueryRow(ctx, `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id,
    download_status, parse_status, import_status
) VALUES ($1, $2, $3, $4, 'pending', 'blocked', 'blocked')
ON CONFLICT (payer_id, source_url) DO NOTHING
RETURNING id`, payer, month, url, runID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, jobs.Failure(jobs.FailureDiscoveryDatabase)
	}
	return id, true, nil
}

func observeExisting(ctx context.Context, tx pgx.Tx, runID, tocID, ordinal int64) error {
	tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.toc_files
SET last_seen_at = transaction_timestamp(), updated_at = transaction_timestamp()
WHERE id = $1`, tocID)
	if err != nil || tag.RowsAffected() != 1 {
		return jobs.Failure(jobs.FailureDiscoveryDatabase)
	}
	return insertMembership(ctx, tx, runID, tocID, ordinal, false)
}

func insertMembership(ctx context.Context, tx pgx.Tx, runID, tocID, ordinal int64, wasNew bool) error {
	_, err := tx.Exec(ctx, `
INSERT INTO mrfpipeline.discovery_run_toc_files (
    discovery_run_id, toc_file_id, listing_ordinal, was_new
) VALUES ($1, $2, $3, $4)`, runID, tocID, ordinal, wasNew)
	if err != nil {
		return jobs.Failure(jobs.FailureDiscoveryDatabase)
	}
	return nil
}
