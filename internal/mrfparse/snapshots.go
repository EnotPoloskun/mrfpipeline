package mrfparse

import (
	"context"

	"github.com/enotpoloskun/mrfpipeline/internal/admission"
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
  AND EXISTS (
      SELECT 1 FROM mrfpipeline.monthly_release_mrf_sources a
      WHERE a.payer_id = mrf_snapshots.payer_id
        AND a.collection_month = mrf_snapshots.collection_month
        AND a.mrf_source_id = mrf_snapshots.mrf_source_id
  )
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
	if err := admission.ReleaseSlotTx(ctx, tx, sourceID); err != nil {
		return err
	}
	if err := admission.WakeTx(ctx, tx, client); err != nil {
		return err
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
