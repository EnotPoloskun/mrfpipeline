package mrfdownload

import (
	"context"
	"errors"
	"fmt"

	"github.com/enotpoloskun/mrfpipeline/internal/admission"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func classifyClaim(download, parse string, stored *int64, riverJobID int64) (string, error) {
	switch download {
	case jobs.StatusSucceeded:
		return jobs.ClaimNoop, nil
	case jobs.StatusFailed:
		return jobs.ClaimNoop, nil
	case jobs.StatusPending, jobs.StatusRunning:
		if stored == nil || *stored != riverJobID {
			return jobs.ClaimNoop, nil
		}
		if parse != jobs.StatusBlocked {
			return "", jobs.Failure(jobs.FailureDomainInvariant)
		}
		return jobs.ClaimWork, nil
	default:
		return "", jobs.Failure(jobs.FailureDomainInvariant)
	}
}

// normalizeUnadmitted clears a stale delivery after the admission boundary
// has been removed.  The row lock and job-identity check make this harmless
// when another delivery has already claimed the source for a new attempt.
func normalizeUnadmitted(ctx context.Context, tx pgx.Tx, sourceID, riverJobID int64) error {
	var download, parse string
	var stored, parseStored *int64
	if err := tx.QueryRow(ctx, `
SELECT download_status, download_river_job_id, parse_status, parse_river_job_id
FROM mrfpipeline.mrf_sources
WHERE id = $1
FOR UPDATE`, sourceID).Scan(&download, &stored, &parse, &parseStored); err != nil {
		return classifyClaimDB(ctx, err)
	}
	admitted, err := admission.SourceAdmitted(ctx, tx, sourceID)
	if err != nil {
		return err
	}
	if admitted {
		return nil
	}
	if download != jobs.StatusPending || (parse != jobs.StatusBlocked && parse != jobs.StatusPending) {
		return nil
	}
	if stored == nil || *stored != riverJobID {
		return nil
	}
	if _, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_sources
SET download_status = 'blocked', download_river_job_id = NULL,
    parse_status = CASE WHEN parse_status = 'pending' THEN 'blocked' ELSE parse_status END,
    parse_river_job_id = CASE WHEN parse_status IN ('pending', 'blocked')
                                   AND (parse_river_job_id IS NULL OR parse_river_job_id = $2)
                              THEN NULL ELSE parse_river_job_id END,
    updated_at = transaction_timestamp()
WHERE id = $1`, sourceID, riverJobID); err != nil {
		return classifyClaimDB(ctx, err)
	}
	return nil
}

func claimDownload(ctx context.Context, pool *pgxpool.Pool, sourceID, riverJobID int64) (jobs.ClaimResult, string, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return jobs.ClaimResult{}, "", err
	}
	if pool == nil || sourceID <= 0 || riverJobID <= 0 {
		return jobs.ClaimResult{}, "", jobs.Failure(jobs.FailureInvalidArguments)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return jobs.ClaimResult{}, "", classifyClaimDB(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var download, parse, sourceURL string
	var stored *int64
	err = tx.QueryRow(ctx, `
SELECT download_status, download_river_job_id, parse_status, source_url
FROM mrfpipeline.mrf_sources
WHERE id = $1`, sourceID).Scan(&download, &stored, &parse, &sourceURL)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.ClaimResult{}, "", jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return jobs.ClaimResult{}, "", classifyClaimDB(ctx, err)
	}
	admitted, err := admission.SourceAdmitted(ctx, tx, sourceID)
	if err != nil {
		return jobs.ClaimResult{}, "", err
	}
	if !admitted {
		if err := normalizeUnadmitted(ctx, tx, sourceID, riverJobID); err != nil {
			return jobs.ClaimResult{}, "", err
		}
		if err := tx.Commit(ctx); err != nil {
			return jobs.ClaimResult{}, "", classifyClaimDB(ctx, err)
		}
		return jobs.ClaimResult{Action: jobs.ClaimNoop}, "", nil
	}
	action, err := classifyClaim(download, parse, stored, riverJobID)
	if err != nil {
		return jobs.ClaimResult{}, "", err
	}
	if action == jobs.ClaimNoop {
		return jobs.ClaimResult{Action: jobs.ClaimNoop}, "", nil
	}
	if err := release.RequireBuildingForStage(ctx, tx, jobs.KindMRFDownload, sourceID); err != nil {
		return jobs.ClaimResult{}, "", err
	}
	err = tx.QueryRow(ctx, `
SELECT download_status, download_river_job_id, parse_status, source_url
FROM mrfpipeline.mrf_sources
WHERE id = $1
FOR UPDATE`, sourceID).Scan(&download, &stored, &parse, &sourceURL)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.ClaimResult{}, "", jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return jobs.ClaimResult{}, "", classifyClaimDB(ctx, err)
	}
	action, err = classifyClaim(download, parse, stored, riverJobID)
	if err != nil {
		return jobs.ClaimResult{}, "", err
	}
	if action == jobs.ClaimNoop {
		return jobs.ClaimResult{Action: jobs.ClaimNoop}, "", nil
	}
	tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_sources
SET download_status = $2, updated_at = transaction_timestamp()
WHERE id = $1`, sourceID, jobs.StatusRunning)
	if err != nil || tag.RowsAffected() != 1 {
		return jobs.ClaimResult{}, "", classifyClaimDB(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.ClaimResult{}, "", classifyClaimDB(ctx, err)
	}
	return jobs.ClaimResult{Action: jobs.ClaimWork}, sourceURL, nil
}

func classifyClaimDB(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("%w: %w: claim", jobs.ErrJob, database.ErrDatabase)
}
