package mrfdownload

import (
	"context"
	"errors"
	"fmt"

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
