package tocimport

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/planbatch"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

type claimInfo struct {
	payer     string
	month     string
	monthDate time.Time
}

func classifyClaim(imp, download, parse string, stored *int64, riverJobID int64) (string, error) {
	switch imp {
	case jobs.StatusSucceeded, jobs.StatusFailed:
		return jobs.ClaimNoop, nil
	case jobs.StatusPending, jobs.StatusRunning:
		if stored == nil || *stored != riverJobID {
			return jobs.ClaimNoop, nil
		}
		if download != jobs.StatusSucceeded || parse != jobs.StatusSucceeded {
			return "", jobs.Failure(jobs.FailureDomainInvariant)
		}
		return jobs.ClaimWork, nil
	default:
		return "", jobs.Failure(jobs.FailureDomainInvariant)
	}
}

func claimImport(ctx context.Context, pool *pgxpool.Pool, tocFileID, riverJobID int64) (jobs.ClaimResult, claimInfo, error) {
	if ctx == nil {
		panic("nil context")
	}
	var zero claimInfo
	if err := ctx.Err(); err != nil {
		return jobs.ClaimResult{}, zero, err
	}
	if pool == nil || tocFileID <= 0 || riverJobID <= 0 {
		return jobs.ClaimResult{}, zero, jobs.Failure(jobs.FailureInvalidArguments)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return jobs.ClaimResult{}, zero, classifyClaimDB(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var imp, download, parse, payer string
	var stored *int64
	var month time.Time
	err = tx.QueryRow(ctx, `
SELECT import_status, import_river_job_id, download_status, parse_status, payer_id, collection_month
FROM mrfpipeline.toc_files
WHERE id = $1`, tocFileID).Scan(&imp, &stored, &download, &parse, &payer, &month)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.ClaimResult{}, zero, jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return jobs.ClaimResult{}, zero, classifyClaimDB(ctx, err)
	}
	if err := release.RequireBuildingForTOC(ctx, tx, tocFileID); err != nil {
		return jobs.ClaimResult{}, zero, err
	}
	err = tx.QueryRow(ctx, `
SELECT import_status, import_river_job_id, download_status, parse_status, payer_id, collection_month
FROM mrfpipeline.toc_files
WHERE id = $1
FOR UPDATE`, tocFileID).Scan(&imp, &stored, &download, &parse, &payer, &month)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.ClaimResult{}, zero, jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return jobs.ClaimResult{}, zero, classifyClaimDB(ctx, err)
	}
	action, err := classifyClaim(imp, download, parse, stored, riverJobID)
	if err != nil {
		return jobs.ClaimResult{}, zero, err
	}
	if action == jobs.ClaimNoop {
		return jobs.ClaimResult{Action: jobs.ClaimNoop}, zero, nil
	}
	tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.toc_files
SET import_status = $2, updated_at = transaction_timestamp()
WHERE id = $1`, tocFileID, jobs.StatusRunning)
	if err != nil || tag.RowsAffected() != 1 {
		return jobs.ClaimResult{}, zero, classifyClaimDB(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.ClaimResult{}, zero, classifyClaimDB(ctx, err)
	}
	return jobs.ClaimResult{Action: jobs.ClaimWork}, claimInfo{
		payer: payer, month: formatMonth(month), monthDate: month,
	}, nil
}

func confirmParseSucceeded(ctx context.Context, tx pgx.Tx, tocFileID int64) error {
	var parse string
	if err := tx.QueryRow(ctx, `
SELECT parse_status FROM mrfpipeline.toc_files WHERE id = $1`, tocFileID).Scan(&parse); err != nil {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %w: confirm", jobs.ErrJob, database.ErrDatabase)
	}
	if parse != jobs.StatusSucceeded {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	return nil
}

func confirmImportSuccess(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx], tocFileID int64) error {
	if err := confirmParseSucceeded(ctx, tx, tocFileID); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `
SELECT DISTINCT mrf_snapshot_id
FROM mrfpipeline.toc_mrf_plan_associations
WHERE toc_file_id = $1
ORDER BY 1`, tocFileID)
	if err != nil {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %w: confirm", jobs.ErrJob, database.ErrDatabase)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("%w: %w: confirm", jobs.ErrJob, database.ErrDatabase)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: %w: confirm", jobs.ErrJob, database.ErrDatabase)
	}
	for _, id := range ids {
		if _, err := planbatch.Schedule(ctx, tx, client, id); err != nil {
			return err
		}
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
