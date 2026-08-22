package tocparse

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func classifyClaim(parse, download, imp string, stored *int64, riverJobID int64) (string, error) {
	switch parse {
	case jobs.StatusSucceeded, jobs.StatusFailed:
		return jobs.ClaimNoop, nil
	case jobs.StatusPending, jobs.StatusRunning:
		if stored == nil || *stored != riverJobID {
			return jobs.ClaimNoop, nil
		}
		if download != jobs.StatusSucceeded || imp != jobs.StatusBlocked {
			return "", jobs.Failure(jobs.FailureDomainInvariant)
		}
		return jobs.ClaimWork, nil
	default:
		return "", jobs.Failure(jobs.FailureDomainInvariant)
	}
}

func claimParse(ctx context.Context, pool *pgxpool.Pool, tocFileID, riverJobID int64) (jobs.ClaimResult, string, string, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return jobs.ClaimResult{}, "", "", err
	}
	if pool == nil || tocFileID <= 0 || riverJobID <= 0 {
		return jobs.ClaimResult{}, "", "", jobs.Failure(jobs.FailureInvalidArguments)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return jobs.ClaimResult{}, "", "", classifyClaimDB(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var parse, download, imp, payer string
	var stored *int64
	var month time.Time
	err = tx.QueryRow(ctx, `
SELECT parse_status, parse_river_job_id, download_status, import_status, payer_id, collection_month
FROM mrfpipeline.toc_files
WHERE id = $1
FOR UPDATE`, tocFileID).Scan(&parse, &stored, &download, &imp, &payer, &month)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.ClaimResult{}, "", "", jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return jobs.ClaimResult{}, "", "", classifyClaimDB(ctx, err)
	}
	action, err := classifyClaim(parse, download, imp, stored, riverJobID)
	if err != nil {
		return jobs.ClaimResult{}, "", "", err
	}
	if action == jobs.ClaimNoop {
		return jobs.ClaimResult{Action: jobs.ClaimNoop}, "", "", nil
	}
	tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.toc_files
SET parse_status = $2, updated_at = transaction_timestamp()
WHERE id = $1`, tocFileID, jobs.StatusRunning)
	if err != nil || tag.RowsAffected() != 1 {
		return jobs.ClaimResult{}, "", "", classifyClaimDB(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.ClaimResult{}, "", "", classifyClaimDB(ctx, err)
	}
	return jobs.ClaimResult{Action: jobs.ClaimWork}, payer, formatMonth(month), nil
}

func confirmDownloadSucceeded(ctx context.Context, tx pgx.Tx, tocFileID int64) error {
	var download string
	if err := tx.QueryRow(ctx, `
SELECT download_status FROM mrfpipeline.toc_files WHERE id = $1`, tocFileID).Scan(&download); err != nil {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %w: confirm", jobs.ErrJob, database.ErrDatabase)
	}
	if download != jobs.StatusSucceeded {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	return nil
}

func classifyClaimDB(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("%w: %w: claim", jobs.ErrJob, database.ErrDatabase)
}

func formatMonth(d time.Time) string {
	return fmt.Sprintf("%04d-%02d", d.Year(), int(d.Month()))
}
