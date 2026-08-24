package planbatch

import (
	"context"
	"errors"
	"strconv"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

const scheduleRetries = 8
const snapshotPage = 100

// FormatPlanBatchID emits plan-batch-<positive batch ID> without padding.
func FormatPlanBatchID(id int64) string {
	return "plan-batch-" + strconv.FormatInt(id, 10)
}

// Schedule freezes currently unassigned plans for one consumed snapshot into
// a pending attachment batch and River job. It must run inside a caller-owned
// transaction. If the caller already holds the snapshot row, FOR UPDATE
// re-reads it. Uniqueness or serialization conflicts retry from a fresh
// snapshot lock in this same transaction.
func Schedule(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx], snapshotID int64) (bool, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if tx == nil || snapshotID <= 0 {
		return false, jobs.Failure(jobs.FailureInvalidArguments)
	}
	var last error
	for i := 0; i < scheduleRetries; i++ {
		if _, err := tx.Exec(ctx, `SAVEPOINT plan_batch_schedule`); err != nil {
			return false, classifyDB(ctx, err)
		}
		created, err := scheduleOnce(ctx, tx, client, snapshotID)
		if err == nil {
			_, _ = tx.Exec(ctx, `RELEASE SAVEPOINT plan_batch_schedule`)
			return created, nil
		}
		_, _ = tx.Exec(ctx, `ROLLBACK TO SAVEPOINT plan_batch_schedule`)
		if !isConflict(err) {
			return false, err
		}
		last = err
	}
	if last == nil {
		return false, jobs.Failure(jobs.FailureDomainInvariant)
	}
	return false, last
}

func scheduleOnce(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx], snapshotID int64) (bool, error) {
	var consume string
	var jobID *int64
	err := tx.QueryRow(ctx, `
SELECT consume_status, consume_river_job_id
FROM mrfpipeline.mrf_snapshots
WHERE id = $1`, snapshotID).Scan(&consume, &jobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return false, classifyDB(ctx, err)
	}
	if err := release.RequireBuildingForSnapshot(ctx, tx, snapshotID); err != nil {
		return false, err
	}
	err = tx.QueryRow(ctx, `
SELECT consume_status, consume_river_job_id
FROM mrfpipeline.mrf_snapshots
WHERE id = $1
FOR UPDATE`, snapshotID).Scan(&consume, &jobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return false, classifyDB(ctx, err)
	}
	if err := snapshotLifecycle(consume, jobID); err != nil {
		return false, err
	}
	if consume != jobs.StatusSucceeded {
		return false, nil
	}

	var blocked bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM mrfpipeline.plan_attachment_batches
    WHERE mrf_snapshot_id = $1 AND status IN ('pending', 'running', 'failed')
)`, snapshotID).Scan(&blocked); err != nil {
		return false, classifyDB(ctx, err)
	}
	if blocked {
		return false, nil
	}

	rows, err := tx.Query(ctx, `
SELECT p.id
FROM mrfpipeline.mrf_plans p
WHERE p.mrf_snapshot_id = $1
  AND NOT EXISTS (
      SELECT 1 FROM mrfpipeline.plan_attachment_batch_items i
      WHERE i.mrf_plan_id = p.id
  )
ORDER BY p.plan_name, p.issuer_name, p.plan_id_type, p.plan_id, p.plan_market_type, p.id
FOR UPDATE`, snapshotID)
	if err != nil {
		return false, classifyDB(ctx, err)
	}
	defer rows.Close()
	var planIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return false, classifyDB(ctx, err)
		}
		planIDs = append(planIDs, id)
	}
	if err := rows.Err(); err != nil {
		return false, classifyDB(ctx, err)
	}
	if len(planIDs) == 0 {
		return false, nil
	}
	if client == nil {
		return false, jobs.Failure(jobs.FailureInvalidArguments)
	}

	var batchID int64
	if err := tx.QueryRow(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batches (mrf_snapshot_id, requested_plan_count)
VALUES ($1, $2)
RETURNING id`, snapshotID, int64(len(planIDs))).Scan(&batchID); err != nil {
		return false, classifyDB(ctx, err)
	}
	for _, planID := range planIDs {
		if _, err := tx.Exec(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batch_items (plan_attachment_batch_id, mrf_plan_id)
VALUES ($1, $2)`, batchID, planID); err != nil {
			return false, classifyDB(ctx, err)
		}
	}
	riverJobID, err := jobs.InsertTx(ctx, client, tx, &jobs.ConsumerAttachPlansArgs{PlanAttachmentBatchID: batchID})
	if err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.plan_attachment_batches
SET river_job_id = $2, updated_at = transaction_timestamp()
WHERE id = $1`, batchID, riverJobID)
	if err != nil || tag.RowsAffected() != 1 {
		return false, classifyDB(ctx, err)
	}
	return true, nil
}

func snapshotLifecycle(consume string, jobID *int64) error {
	switch consume {
	case jobs.StatusSucceeded:
		if jobID == nil {
			return jobs.Failure(jobs.FailureDomainInvariant)
		}
		return nil
	case jobs.StatusPending, jobs.StatusRunning:
		if jobID == nil {
			return jobs.Failure(jobs.FailureDomainInvariant)
		}
		return nil
	case jobs.StatusFailed:
		return nil
	case jobs.StatusBlocked:
		if jobID != nil {
			return jobs.Failure(jobs.FailureDomainInvariant)
		}
		return nil
	default:
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
}

// SweepConsumed pages consumed snapshots by ascending ID and invokes Schedule
// in one short transaction per snapshot. It is repeat-safe and does not reset
// failed or running batches.
func SweepConsumed(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx]) (int, error) {
	return SweepConsumedReleaseAware(ctx, pool, client, nil)
}

// SweepConsumedReleaseAware is SweepConsumed with a skip hook for snapshots
// whose release is sealed. The hook is called after the transaction rolls
// back, and the sweep continues with the next snapshot.
func SweepConsumedReleaseAware(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], onSealed func(int64)) (int, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if pool == nil || client == nil {
		return 0, jobs.Failure(jobs.FailureInvalidArguments)
	}
	var after int64
	created := 0
	for {
		rows, err := pool.Query(ctx, `
SELECT id FROM mrfpipeline.mrf_snapshots
WHERE consume_status = $1 AND id > $2
ORDER BY id
LIMIT $3`, jobs.StatusSucceeded, after, snapshotPage)
		if err != nil {
			return created, classifyDB(ctx, err)
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return created, classifyDB(ctx, err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return created, classifyDB(ctx, err)
		}
		rows.Close()
		if len(ids) == 0 {
			return created, nil
		}
		for _, id := range ids {
			var unassigned, unresolved, succeeded bool
			if err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM mrfpipeline.mrf_plans p
    WHERE p.mrf_snapshot_id = $1
      AND NOT EXISTS (
          SELECT 1 FROM mrfpipeline.plan_attachment_batch_items i
          WHERE i.mrf_plan_id = p.id
      )
), EXISTS (
    SELECT 1 FROM mrfpipeline.plan_attachment_batches b
    WHERE b.mrf_snapshot_id = $1
      AND b.status IN ('pending', 'running', 'failed')
), EXISTS (
    SELECT 1 FROM mrfpipeline.plan_attachment_batches b
    WHERE b.mrf_snapshot_id = $1 AND b.status = 'succeeded'
)`, id).Scan(&unassigned, &unresolved, &succeeded); err != nil {
				return created, classifyDB(ctx, err)
			}
			if !unassigned && !unresolved && succeeded {
				after = id
				continue
			}
			tx, err := pool.Begin(ctx)
			if err != nil {
				return created, classifyDB(ctx, err)
			}
			ok, err := Schedule(ctx, tx, client, id)
			if err != nil {
				_ = tx.Rollback(ctx)
				if onSealed != nil && jobs.IsFailure(err, jobs.FailureSealedReleaseInconsistent) {
					onSealed(id)
					after = id
					continue
				}
				return created, err
			}
			if err := tx.Commit(ctx); err != nil {
				return created, classifyDB(ctx, err)
			}
			if ok {
				created++
			}
			after = id
		}
	}
}

func isConflict(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == "23505" || pgErr.Code == "40001") {
		return true
	}
	return false
}

func classifyDB(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if isConflict(err) {
		return err
	}
	return jobs.Failure(jobs.FailurePlanAttachDatabaseFailed)
}
