package mrfparse

import (
	"context"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
)

func confirmParseSuccess(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx], sourceID int64) error {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if tx == nil || client == nil || sourceID <= 0 {
		return jobs.Failure(jobs.FailureMRFParseDatabaseFailed)
	}
	var download string
	if err := tx.QueryRow(ctx, `
SELECT download_status FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&download); err != nil {
		return classifySuccessDB(ctx, err)
	}
	if download != jobs.StatusSucceeded {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	rows, err := tx.Query(ctx, `
SELECT id, consume_status, consume_river_job_id
FROM mrfpipeline.mrf_snapshots
WHERE mrf_source_id = $1
ORDER BY id
FOR UPDATE`, sourceID)
	if err != nil {
		return classifySuccessDB(ctx, err)
	}
	defer rows.Close()
	type snap struct {
		id     int64
		status string
		jobID  *int64
	}
	var list []snap
	for rows.Next() {
		var s snap
		if err := rows.Scan(&s.id, &s.status, &s.jobID); err != nil {
			return classifySuccessDB(ctx, err)
		}
		list = append(list, s)
	}
	if err := rows.Err(); err != nil {
		return classifySuccessDB(ctx, err)
	}
	for _, s := range list {
		switch s.status {
		case jobs.StatusBlocked:
			if s.jobID != nil {
				return jobs.Failure(jobs.FailureDomainInvariant)
			}
			if _, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_snapshots
SET consume_status = $2, updated_at = transaction_timestamp()
WHERE id = $1`, s.id, jobs.StatusPending); err != nil {
				return classifySuccessDB(ctx, err)
			}
			jobID, err := jobs.InsertTx(ctx, client, tx, &jobs.ConsumerIngestArgs{MRFSnapshotID: s.id})
			if err != nil {
				return classifySuccessDB(ctx, err)
			}
			if _, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_snapshots
SET consume_river_job_id = $2, updated_at = transaction_timestamp()
WHERE id = $1`, s.id, jobID); err != nil {
				return classifySuccessDB(ctx, err)
			}
		case jobs.StatusPending, jobs.StatusRunning, jobs.StatusSucceeded, jobs.StatusFailed:
			if s.jobID == nil {
				return jobs.Failure(jobs.FailureDomainInvariant)
			}
		default:
			return jobs.Failure(jobs.FailureDomainInvariant)
		}
	}
	return nil
}

func classifySuccessDB(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if jobs.IsFailure(err, jobs.FailureDomainInvariant) || jobs.IsFailure(err, jobs.FailureMRFParseDatabaseFailed) {
		return err
	}
	return jobs.Failure(jobs.FailureMRFParseDatabaseFailed)
}
