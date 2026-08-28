package reconcile

import (
	"context"
	"errors"
	"fmt"

	"github.com/enotpoloskun/mrfpipeline/internal/admission"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// Retry reopens one failed production stage under its release and execution
// locks, inserts a replacement River job when capacity permits, and does not
// run the job. MRF parse retries require the artifact workspace so they can
// distinguish a valid raw download from a missing or incomplete one.
func Retry(ctx context.Context, pool *pgxpool.Pool, stage string, domainID int64, workspaces ...*artifact.Workspace) (RetryResult, error) {
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
	var ws *artifact.Workspace
	if binding.Kind == jobs.KindMRFParse {
		if len(workspaces) != 1 || workspaces[0] == nil {
			return zero, jobs.Failure(jobs.FailureInvalidArguments)
		}
		ws = workspaces[0]
	}
	if err := database.ValidateCurrent(ctx, pool); err != nil {
		return zero, err
	}
	lockDomain := domainID
	namespace := jobs.LockNamespaceMRF
	if binding.Kind == jobs.KindConsumerIngest || binding.Kind == jobs.KindConsumerAttachPlans {
		namespace = jobs.LockNamespaceConsumer
	}
	if binding.Kind == jobs.KindConsumerAttachPlans {
		if err := pool.QueryRow(ctx, `
SELECT mrf_snapshot_id FROM mrfpipeline.plan_attachment_batches WHERE id = $1`, domainID).Scan(&lockDomain); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return zero, jobs.Failure(jobs.FailureMissingRecord)
			}
			return zero, dbFail(ctx.Err())
		}
	}
	var result RetryResult
	busy, err := jobs.WithExecutionLock(ctx, pool, namespace, lockDomain, func(ctx context.Context) error {
		client, err := jobs.NewInsertClient(ctx, pool, jobs.NewLogger(nil))
		if err != nil {
			return err
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			return dbFail(ctx.Err())
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := release.RequireBuildingForStage(ctx, tx, binding.Kind, domainID); err != nil {
			if jobs.IsFailure(err, jobs.FailureSealedReleaseInconsistent) {
				return jobs.Failure(jobs.FailureSealedReleaseRetryForbidden)
			}
			return err
		}
		if err := lockForRetry(ctx, tx, binding, domainID); err != nil {
			return err
		}
		if err := requireFailedRetryable(ctx, tx, binding, domainID); err != nil {
			return err
		}
		if binding.Kind == jobs.KindMRFParse {
			if ws == nil {
				return jobs.Failure(jobs.FailureInvalidArguments)
			}
			next, err := retryMRFParse(ctx, tx, client, ws, domainID)
			if err != nil {
				return err
			}
			if err := tx.Commit(ctx); err != nil {
				return dbFail(ctx.Err())
			}
			result = next
			return nil
		}
		if binding.Kind == jobs.KindMRFDownload {
			var selected, hasSlot bool
			if err := tx.QueryRow(ctx, `
SELECT EXISTS (SELECT 1 FROM mrfpipeline.monthly_release_mrf_sources a
               WHERE a.mrf_source_id = $1),
       EXISTS (SELECT 1 FROM mrfpipeline.mrf_materialization_slots s
               WHERE s.mrf_source_id = $1)`, domainID).Scan(&selected, &hasSlot); err != nil {
				return dbFail(ctx.Err())
			}
			if !selected {
				return jobs.Failure(jobs.FailureDomainInvariant)
			}
			if !hasSlot {
				if err := reopenWaiting(ctx, tx, binding.Spec, domainID); err != nil {
					return err
				}
				if err := admission.WakeTx(ctx, tx, client); err != nil {
					return err
				}
				if err := tx.Commit(ctx); err != nil {
					return dbFail(ctx.Err())
				}
				result = RetryResult{Stage: binding.Kind, DomainID: domainID}
				return nil
			}
		}
		args, err := jobs.ArgsFor(binding.Kind, domainID)
		if err != nil {
			return err
		}
		jobID, err := jobs.InsertTx(ctx, client, tx, args)
		if err != nil {
			return err
		}
		if err := reopenFailed(ctx, tx, binding.Spec, domainID, jobID); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return dbFail(ctx.Err())
		}
		result = RetryResult{Stage: binding.Kind, DomainID: domainID, RiverJobID: jobID}
		return nil
	})
	if err != nil {
		return zero, err
	}
	if busy {
		return zero, jobs.Failure(jobs.FailureStageExecutionBusy)
	}
	return result, nil
}

func retryMRFParse(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx], ws *artifact.Workspace, sourceID int64) (RetryResult, error) {
	if ctx == nil {
		panic("nil context")
	}
	var zero RetryResult
	var downloadStatus string
	var selected, hasSlot bool
	if err := tx.QueryRow(ctx, `
SELECT s.download_status,
       EXISTS (
           SELECT 1
           FROM mrfpipeline.monthly_release_mrf_sources a
           JOIN mrfpipeline.monthly_releases r
             ON r.payer_id = a.payer_id AND r.collection_month = a.collection_month
           WHERE a.mrf_source_id = s.id AND r.status IN ('building', 'active')
       ),
       EXISTS (
           SELECT 1
           FROM mrfpipeline.mrf_materialization_slots x
           WHERE x.mrf_source_id = s.id
       )
FROM mrfpipeline.mrf_sources s
WHERE s.id = $1
FOR UPDATE`, sourceID).Scan(&downloadStatus, &selected, &hasSlot); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return zero, jobs.Failure(jobs.FailureMissingRecord)
		}
		return zero, dbFail(ctx.Err())
	}
	if !selected || downloadStatus == jobs.StatusFailed {
		return zero, jobs.Failure(jobs.FailureDomainInvariant)
	}
	state, err := ws.InspectDownloadState(artifact.KindMRF, sourceID)
	if err != nil {
		return zero, retryMRFParseArtifactError(err)
	}
	if state == artifact.DownloadComplete {
		if !hasSlot || downloadStatus == jobs.StatusFailed {
			return zero, jobs.Failure(jobs.FailureDomainInvariant)
		}
		if downloadStatus != jobs.StatusSucceeded {
			tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_sources
SET download_status = 'succeeded',
    download_river_job_id = NULL,
    updated_at = transaction_timestamp()
WHERE id = $1 AND parse_status = 'failed'`, sourceID)
			if err != nil || tag.RowsAffected() != 1 {
				return zero, dbFail(ctx.Err())
			}
		}
		args, err := jobs.ArgsFor(jobs.KindMRFParse, sourceID)
		if err != nil {
			return zero, err
		}
		jobID, err := jobs.InsertTx(ctx, client, tx, args)
		if err != nil {
			return zero, err
		}
		if err := reopenFailed(ctx, tx, jobs.MRFParseStage, sourceID, jobID); err != nil {
			return zero, err
		}
		return RetryResult{Stage: jobs.KindMRFParse, DomainID: sourceID, RiverJobID: jobID}, nil
	}
	if state != artifact.DownloadAbsent && state != artifact.DownloadIncomplete {
		return zero, jobs.Failure(jobs.FailureDomainInvariant)
	}
	if err := removeMRFParseRetryArtifacts(ctx, ws, sourceID, state); err != nil {
		return zero, err
	}
	if hasSlot {
		jobID, err := jobs.InsertTx(ctx, client, tx, &jobs.MRFDownloadArgs{MRFSourceID: sourceID})
		if err != nil {
			return zero, err
		}
		tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_sources
SET download_status = 'pending',
    download_river_job_id = $2,
    parse_status = 'blocked',
    parse_river_job_id = NULL,
    failure_code = NULL,
    updated_at = transaction_timestamp()
WHERE id = $1 AND parse_status = 'failed'`, sourceID, jobID)
		if err != nil || tag.RowsAffected() != 1 {
			return zero, dbFail(ctx.Err())
		}
		return RetryResult{Stage: jobs.KindMRFDownload, DomainID: sourceID, RiverJobID: jobID}, nil
	}
	tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_sources
SET download_status = 'blocked',
    download_river_job_id = NULL,
    parse_status = 'blocked',
    parse_river_job_id = NULL,
    failure_code = NULL,
    updated_at = transaction_timestamp()
WHERE id = $1 AND parse_status = 'failed'`, sourceID)
	if err != nil || tag.RowsAffected() != 1 {
		return zero, dbFail(ctx.Err())
	}
	if err := admission.WakeTx(ctx, tx, client); err != nil {
		return zero, err
	}
	return RetryResult{Stage: jobs.KindMRFParse, DomainID: sourceID}, nil
}

func removeMRFParseRetryArtifacts(ctx context.Context, ws *artifact.Workspace, sourceID int64, state string) error {
	if state == artifact.DownloadIncomplete {
		if err := ws.RemoveDownload(artifact.KindMRF, sourceID); err != nil {
			return retryMRFParseArtifactError(err)
		}
	}
	if err := ws.RemoveMRFParserTemp(sourceID); err != nil {
		return retryMRFParseArtifactError(err)
	}
	return nil
}

func retryMRFParseArtifactError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return jobs.Failure(jobs.FailureArtifactReconciliationFailed)
}

func reopenWaiting(ctx context.Context, tx pgx.Tx, spec jobs.StageSpec, domainID int64) error {
	q := fmt.Sprintf(`UPDATE %s SET %s = $2, %s = NULL, %s = NULL, %s = transaction_timestamp() WHERE %s = $1`,
		spec.Table, spec.StatusColumn, spec.JobIDColumn, spec.FailureCodeColumn, spec.UpdatedAtColumn, spec.IDColumn)
	tag, err := tx.Exec(ctx, q, domainID, jobs.StatusBlocked)
	if err != nil || tag.RowsAffected() != 1 {
		return dbFail(ctx.Err())
	}
	return nil
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
		// Parse retries inspect physical raw state under the source execution
		// lock; download_status is not proof that bytes remain.
		return nil
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
