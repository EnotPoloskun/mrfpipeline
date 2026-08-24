package reconcile

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

// Retry reopens one failed production stage. It acquires the worker lease,
// inserts a replacement River job, and does not run the job.
func Retry(ctx context.Context, pool *pgxpool.Pool, stage string, domainID int64) (RetryResult, error) {
	if ctx == nil {
		panic("nil context")
	}
	var zero RetryResult
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if pool == nil || domainID <= 0 {
		return zero, jobs.Failure(jobs.FailureInvalidArguments)
	}
	binding, ok := bindingFor(stage)
	if !ok {
		return zero, jobs.Failure(jobs.FailureInvalidArguments)
	}
	if err := database.ValidateCurrent(ctx, pool); err != nil {
		return zero, err
	}
	lease, err := database.AcquireWorkerLease(ctx, pool)
	if err != nil {
		if database.IsLeaseUnavailable(err) {
			return zero, jobs.Failure(jobs.FailureWorkerLeaseUnavailable)
		}
		return zero, err
	}
	defer func() { _ = lease.Release(context.Background()) }()

	client, err := jobs.NewInsertClient(ctx, pool, jobs.NewLogger(nil))
	if err != nil {
		return zero, err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return zero, dbFail(ctx.Err())
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := release.RequireBuildingForStage(ctx, tx, binding.Kind, domainID); err != nil {
		if jobs.IsFailure(err, jobs.FailureSealedReleaseInconsistent) {
			return zero, jobs.Failure(jobs.FailureSealedReleaseRetryForbidden)
		}
		return zero, err
	}
	if err := lockForRetry(ctx, tx, binding, domainID); err != nil {
		return zero, err
	}
	if err := requireFailedRetryable(ctx, tx, binding, domainID); err != nil {
		return zero, err
	}

	args, err := jobs.ArgsFor(binding.Kind, domainID)
	if err != nil {
		return zero, err
	}
	jobID, err := jobs.InsertTx(ctx, client, tx, args)
	if err != nil {
		return zero, err
	}
	if err := reopenFailed(ctx, tx, binding.Spec, domainID, jobID); err != nil {
		return zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, dbFail(ctx.Err())
	}
	return RetryResult{Stage: binding.Kind, DomainID: domainID, RiverJobID: jobID}, nil
}

func bindingFor(kind string) (jobs.KindBinding, bool) {
	for _, b := range jobs.ProductionBindings() {
		if b.Kind == kind {
			return b, true
		}
	}
	return jobs.KindBinding{}, false
}

func lockForRetry(ctx context.Context, tx pgx.Tx, b jobs.KindBinding, domainID int64) error {
	switch b.Kind {
	case jobs.KindConsumerAttachPlans:
		var snapshotID int64
		err := tx.QueryRow(ctx, `
SELECT mrf_snapshot_id FROM mrfpipeline.plan_attachment_batches WHERE id = $1`, domainID).Scan(&snapshotID)
		if errors.Is(err, pgx.ErrNoRows) {
			return jobs.Failure(jobs.FailureMissingRecord)
		}
		if err != nil {
			return dbFail(ctx.Err())
		}
		if err := tx.QueryRow(ctx, `
SELECT id FROM mrfpipeline.mrf_snapshots WHERE id = $1 FOR UPDATE`, snapshotID).Scan(&snapshotID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return jobs.Failure(jobs.FailureMissingRecord)
			}
			return dbFail(ctx.Err())
		}
		_, err = lockStage(ctx, tx, b.Spec, domainID)
		return err
	case jobs.KindConsumerIngest:
		var sourceID int64
		err := tx.QueryRow(ctx, `
SELECT mrf_source_id FROM mrfpipeline.mrf_snapshots WHERE id = $1 FOR UPDATE`, domainID).Scan(&sourceID)
		if errors.Is(err, pgx.ErrNoRows) {
			return jobs.Failure(jobs.FailureMissingRecord)
		}
		if err != nil {
			return dbFail(ctx.Err())
		}
		if err := tx.QueryRow(ctx, `
SELECT id FROM mrfpipeline.mrf_sources WHERE id = $1 FOR UPDATE`, sourceID).Scan(&sourceID); err != nil {
			return dbFail(ctx.Err())
		}
		return nil
	default:
		_, err := lockStage(ctx, tx, b.Spec, domainID)
		return err
	}
}

func requireFailedRetryable(ctx context.Context, tx pgx.Tx, b jobs.KindBinding, domainID int64) error {
	row, err := lockStage(ctx, tx, b.Spec, domainID)
	if err != nil {
		return err
	}
	if row.status != jobs.StatusFailed {
		return jobs.Failure(jobs.FailureRetryStageNotFailed)
	}
	var code *string
	q := fmt.Sprintf(`SELECT %s FROM %s WHERE %s = $1`, b.Spec.FailureCodeColumn, b.Spec.Table, b.Spec.IDColumn)
	if err := tx.QueryRow(ctx, q, domainID).Scan(&code); err != nil {
		return dbFail(ctx.Err())
	}
	if code == nil || !jobs.IsRecognizedFailureCode(*code) {
		return jobs.Failure(jobs.FailureRetryStageInvariant)
	}
	if err := requirePredecessor(ctx, tx, b, domainID); err != nil {
		return err
	}
	if b.Kind == jobs.KindConsumerAttachPlans {
		return requireFrozenBatch(ctx, tx, domainID)
	}
	return nil
}

func requirePredecessor(ctx context.Context, tx pgx.Tx, b jobs.KindBinding, domainID int64) error {
	switch b.Kind {
	case jobs.KindTOCParse:
		return requireColumn(ctx, tx, `SELECT download_status FROM mrfpipeline.toc_files WHERE id = $1`, domainID, jobs.StatusSucceeded)
	case jobs.KindTOCImport:
		return requireColumn(ctx, tx, `SELECT parse_status FROM mrfpipeline.toc_files WHERE id = $1`, domainID, jobs.StatusSucceeded)
	case jobs.KindMRFParse:
		return requireColumn(ctx, tx, `SELECT download_status FROM mrfpipeline.mrf_sources WHERE id = $1`, domainID, jobs.StatusSucceeded)
	case jobs.KindConsumerIngest:
		var sourceID int64
		if err := tx.QueryRow(ctx, `SELECT mrf_source_id FROM mrfpipeline.mrf_snapshots WHERE id = $1`, domainID).Scan(&sourceID); err != nil {
			return dbFail(ctx.Err())
		}
		return requireColumn(ctx, tx, `SELECT parse_status FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID, jobs.StatusSucceeded)
	case jobs.KindConsumerAttachPlans:
		var snapshotID int64
		if err := tx.QueryRow(ctx, `SELECT mrf_snapshot_id FROM mrfpipeline.plan_attachment_batches WHERE id = $1`, domainID).Scan(&snapshotID); err != nil {
			return dbFail(ctx.Err())
		}
		return requireColumn(ctx, tx, `SELECT consume_status FROM mrfpipeline.mrf_snapshots WHERE id = $1`, snapshotID, jobs.StatusSucceeded)
	default:
		return nil
	}
}

func requireColumn(ctx context.Context, tx pgx.Tx, query string, id int64, want string) error {
	var got string
	if err := tx.QueryRow(ctx, query, id).Scan(&got); err != nil {
		return dbFail(ctx.Err())
	}
	if got != want {
		return jobs.Failure(jobs.FailureRetryStageInvariant)
	}
	return nil
}

func requireFrozenBatch(ctx context.Context, tx pgx.Tx, batchID int64) error {
	var snapshotID, requested int64
	if err := tx.QueryRow(ctx, `
SELECT mrf_snapshot_id, requested_plan_count
FROM mrfpipeline.plan_attachment_batches WHERE id = $1`, batchID).Scan(&snapshotID, &requested); err != nil {
		return dbFail(ctx.Err())
	}
	if requested <= 0 {
		return jobs.Failure(jobs.FailureRetryStageInvariant)
	}
	var items, matching int64
	if err := tx.QueryRow(ctx, `
SELECT count(*), count(*) FILTER (WHERE p.mrf_snapshot_id = $2)
FROM mrfpipeline.plan_attachment_batch_items i
JOIN mrfpipeline.mrf_plans p ON p.id = i.mrf_plan_id
WHERE i.plan_attachment_batch_id = $1`, batchID, snapshotID).Scan(&items, &matching); err != nil {
		return dbFail(ctx.Err())
	}
	if items != requested || matching != requested {
		return jobs.Failure(jobs.FailureRetryStageInvariant)
	}
	return nil
}

func reopenFailed(ctx context.Context, tx pgx.Tx, spec jobs.StageSpec, domainID, jobID int64) error {
	sets := fmt.Sprintf(
		`%s = $2, %s = $3, %s = NULL, %s = transaction_timestamp()`,
		spec.StatusColumn, spec.JobIDColumn, spec.FailureCodeColumn, spec.UpdatedAtColumn,
	)
	if spec.CompletedAtColumn != "" {
		sets += fmt.Sprintf(`, %s = NULL`, spec.CompletedAtColumn)
	}
	q := fmt.Sprintf(`UPDATE %s SET %s WHERE %s = $1`, spec.Table, sets, spec.IDColumn)
	tag, err := tx.Exec(ctx, q, domainID, jobs.StatusPending, jobID)
	if err != nil || tag.RowsAffected() != 1 {
		return dbFail(ctx.Err())
	}
	return nil
}
