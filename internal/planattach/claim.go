package planattach

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type claimIdentity struct {
	BatchID        int64
	SnapshotID     int64
	PayerID        string
	MonthText      string
	RequestedCount int64
	RiverJobID     int64
	Consume        string
}

func claimAttach(ctx context.Context, pool *pgxpool.Pool, batchID, riverJobID int64) (jobs.ClaimResult, claimIdentity, error) {
	if ctx == nil {
		panic("nil context")
	}
	var zero claimIdentity
	if err := ctx.Err(); err != nil {
		return jobs.ClaimResult{}, zero, err
	}
	if pool == nil || batchID <= 0 || riverJobID <= 0 {
		return jobs.ClaimResult{}, zero, jobs.Failure(jobs.FailureInvalidArguments)
	}

	var snapshotID int64
	err := pool.QueryRow(ctx, `
SELECT mrf_snapshot_id FROM mrfpipeline.plan_attachment_batches WHERE id = $1`, batchID).Scan(&snapshotID)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.ClaimResult{}, zero, jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return jobs.ClaimResult{}, zero, classifyDB(ctx, err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return jobs.ClaimResult{}, zero, classifyDB(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	ident, err := lockSnapshotThenBatch(ctx, tx, snapshotID, batchID)
	if err != nil {
		return jobs.ClaimResult{}, zero, err
	}
	ident.RiverJobID = riverJobID

	var status string
	var stored *int64
	var requested int64
	if err := tx.QueryRow(ctx, `
SELECT status, river_job_id, requested_plan_count
FROM mrfpipeline.plan_attachment_batches
WHERE id = $1`, batchID).Scan(&status, &stored, &requested); err != nil {
		return jobs.ClaimResult{}, zero, classifyDB(ctx, err)
	}
	ident.RequestedCount = requested
	if err := requireFrozenItems(ctx, tx, ident); err != nil {
		return jobs.ClaimResult{}, zero, err
	}

	switch status {
	case jobs.StatusSucceeded, jobs.StatusFailed:
		return jobs.ClaimResult{Action: jobs.ClaimNoop}, ident, nil
	case jobs.StatusPending, jobs.StatusRunning:
		if stored == nil || *stored != riverJobID {
			return jobs.ClaimResult{Action: jobs.ClaimNoop}, ident, nil
		}
	default:
		return jobs.ClaimResult{}, zero, jobs.Failure(jobs.FailureDomainInvariant)
	}
	if ident.Consume != jobs.StatusSucceeded {
		return jobs.ClaimResult{}, zero, jobs.Failure(jobs.FailureDomainInvariant)
	}

	tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.plan_attachment_batches
SET status = $2,
    started_at = COALESCE(started_at, transaction_timestamp()),
    updated_at = transaction_timestamp()
WHERE id = $1`, batchID, jobs.StatusRunning)
	if err != nil || tag.RowsAffected() != 1 {
		return jobs.ClaimResult{}, zero, classifyDB(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.ClaimResult{}, zero, classifyDB(ctx, err)
	}
	return jobs.ClaimResult{Action: jobs.ClaimWork}, ident, nil
}

func lockSnapshotThenBatch(ctx context.Context, tx pgx.Tx, snapshotID, batchID int64) (claimIdentity, error) {
	var zero claimIdentity
	var consume, payer string
	var month time.Time
	err := tx.QueryRow(ctx, `
SELECT consume_status, payer_id, collection_month
FROM mrfpipeline.mrf_snapshots
WHERE id = $1
FOR UPDATE`, snapshotID).Scan(&consume, &payer, &month)
	if errors.Is(err, pgx.ErrNoRows) {
		return zero, jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return zero, classifyDB(ctx, err)
	}
	var found int64
	err = tx.QueryRow(ctx, `
SELECT id FROM mrfpipeline.plan_attachment_batches WHERE id = $1 FOR UPDATE`, batchID).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return zero, jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return zero, classifyDB(ctx, err)
	}
	return claimIdentity{
		BatchID: batchID, SnapshotID: snapshotID, PayerID: payer,
		MonthText: formatMonth(month), Consume: consume,
	}, nil
}

func requireFrozenItems(ctx context.Context, tx pgx.Tx, ident claimIdentity) error {
	if ident.RequestedCount <= 0 {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	var items, owned int64
	if err := tx.QueryRow(ctx, `
SELECT count(*) FROM mrfpipeline.plan_attachment_batch_items
WHERE plan_attachment_batch_id = $1`, ident.BatchID).Scan(&items); err != nil {
		return classifyDB(ctx, err)
	}
	if items != ident.RequestedCount {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	if err := tx.QueryRow(ctx, `
SELECT count(*)
FROM mrfpipeline.plan_attachment_batch_items i
JOIN mrfpipeline.mrf_plans p ON p.id = i.mrf_plan_id
WHERE i.plan_attachment_batch_id = $1 AND p.mrf_snapshot_id = $2`, ident.BatchID, ident.SnapshotID).Scan(&owned); err != nil {
		return classifyDB(ctx, err)
	}
	if owned != ident.RequestedCount {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	return nil
}

func lockSnapshotForSucceed(ctx context.Context, tx pgx.Tx, snapshotID int64) error {
	if tx == nil || snapshotID <= 0 {
		return jobs.Failure(jobs.FailurePlanAttachDatabaseFailed)
	}
	var id int64
	err := tx.QueryRow(ctx, `
SELECT id FROM mrfpipeline.mrf_snapshots WHERE id = $1 FOR UPDATE`, snapshotID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return classifyDB(ctx, err)
	}
	return nil
}

func formatMonth(d time.Time) string {
	return fmt.Sprintf("%04d-%02d", d.Year(), int(d.Month()))
}
