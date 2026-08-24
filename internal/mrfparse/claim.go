package mrfparse

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

func classifyClaim(parse, download string, stored *int64, riverJobID int64) (string, error) {
	switch parse {
	case jobs.StatusSucceeded, jobs.StatusFailed:
		return jobs.ClaimNoop, nil
	case jobs.StatusPending, jobs.StatusRunning:
		if stored == nil || *stored != riverJobID {
			return jobs.ClaimNoop, nil
		}
		if download != jobs.StatusSucceeded {
			return "", jobs.Failure(jobs.FailureDomainInvariant)
		}
		return jobs.ClaimWork, nil
	default:
		return "", jobs.Failure(jobs.FailureDomainInvariant)
	}
}

func claimParse(ctx context.Context, pool *pgxpool.Pool, sourceID, riverJobID int64) (jobs.ClaimResult, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return jobs.ClaimResult{}, err
	}
	if pool == nil || sourceID <= 0 || riverJobID <= 0 {
		return jobs.ClaimResult{}, jobs.Failure(jobs.FailureInvalidArguments)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return jobs.ClaimResult{}, classifyClaimDB(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var parse, download string
	var stored *int64
	err = tx.QueryRow(ctx, `
SELECT parse_status, parse_river_job_id, download_status
FROM mrfpipeline.mrf_sources
WHERE id = $1`, sourceID).Scan(&parse, &stored, &download)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.ClaimResult{}, jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return jobs.ClaimResult{}, classifyClaimDB(ctx, err)
	}
	action, err := classifyClaim(parse, download, stored, riverJobID)
	if err != nil {
		return jobs.ClaimResult{}, err
	}
	if action == jobs.ClaimNoop {
		return jobs.ClaimResult{Action: jobs.ClaimNoop}, nil
	}
	if err := release.RequireBuildingForStage(ctx, tx, jobs.KindMRFParse, sourceID); err != nil {
		return jobs.ClaimResult{}, err
	}
	err = tx.QueryRow(ctx, `
SELECT parse_status, parse_river_job_id, download_status
FROM mrfpipeline.mrf_sources
WHERE id = $1
FOR UPDATE`, sourceID).Scan(&parse, &stored, &download)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.ClaimResult{}, jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return jobs.ClaimResult{}, classifyClaimDB(ctx, err)
	}
	action, err = classifyClaim(parse, download, stored, riverJobID)
	if err != nil {
		return jobs.ClaimResult{}, err
	}
	if action == jobs.ClaimNoop {
		return jobs.ClaimResult{Action: jobs.ClaimNoop}, nil
	}
	tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_sources
SET parse_status = $2, updated_at = transaction_timestamp()
WHERE id = $1`, sourceID, jobs.StatusRunning)
	if err != nil || tag.RowsAffected() != 1 {
		return jobs.ClaimResult{}, classifyClaimDB(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.ClaimResult{}, classifyClaimDB(ctx, err)
	}
	return jobs.ClaimResult{Action: jobs.ClaimWork}, nil
}

func classifyClaimDB(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("%w: %w: claim", jobs.ErrJob, database.ErrDatabase)
}
