package reconcile

import (
	"context"
	"errors"
	"fmt"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

const (
	actionKeep      = "keep"
	actionReplace   = "replace"
	actionFail      = "fail"
	actionInvariant = "invariant"
)

// classifyRiver maps a stored job's River row to the reconcile action.
// Unknown states fail closed.
func classifyRiver(found bool, state string) (string, error) {
	if !found {
		return actionReplace, nil
	}
	switch rivertype.JobState(state) {
	case rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateScheduled, rivertype.JobStateRetryable:
		return actionKeep, nil
	case rivertype.JobStateRunning, rivertype.JobStateCompleted:
		return actionReplace, nil
	case rivertype.JobStateCancelled, rivertype.JobStateDiscarded:
		return actionFail, nil
	default:
		return actionInvariant, jobs.Failure(jobs.FailureDomainInvariant)
	}
}

func repairCurrentJobs(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], report *Report) error {
	for _, b := range jobs.ProductionBindings() {
		if err := repairKind(ctx, pool, client, b, report); err != nil {
			return err
		}
	}
	return nil
}

func repairKind(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], b jobs.KindBinding, report *Report) error {
	var after int64
	for {
		ids, err := pageIDs(ctx, pool, fmt.Sprintf(`
SELECT %s FROM %s
WHERE %s IN ('pending', 'running') AND %s > $1
ORDER BY %s
LIMIT $2`, b.Spec.IDColumn, b.Spec.Table, b.Spec.StatusColumn, b.Spec.IDColumn, b.Spec.IDColumn), after, pageSize)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			n, err := repairOneCurrent(ctx, pool, client, b, id)
			if err != nil {
				if jobs.IsFailure(err, jobs.FailureSealedReleaseInconsistent) {
					report.recordSealed(report.logger, b.Kind, id)
					after = id
					continue
				}
				return err
			}
			report.RepairedJobCount += n
			after = id
		}
	}
}

func repairOneCurrent(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], b jobs.KindBinding, domainID int64) (int, error) {
	if b.Kind == jobs.KindMRFDownload || b.Kind == jobs.KindMRFParse || b.Kind == jobs.KindConsumerIngest || b.Kind == jobs.KindConsumerAttachPlans {
		namespace := jobs.LockNamespaceMRF
		lockID := domainID
		if b.Kind == jobs.KindConsumerIngest {
			namespace = jobs.LockNamespaceConsumer
		} else if b.Kind == jobs.KindConsumerAttachPlans {
			namespace = jobs.LockNamespaceConsumer
			if err := pool.QueryRow(ctx, `SELECT mrf_snapshot_id FROM mrfpipeline.plan_attachment_batches WHERE id = $1`, domainID).Scan(&lockID); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return 0, jobs.Failure(jobs.FailureMissingRecord)
				}
				return 0, dbFail(ctx.Err())
			}
		}
		inserted := 0
		busy, err := jobs.WithExecutionLock(ctx, pool, namespace, lockID, func(ctx context.Context) error {
			n, err := repairOneCurrentUnlocked(ctx, pool, client, b, domainID)
			if err == nil {
				inserted = n
			}
			return err
		})
		if err != nil {
			return 0, err
		}
		if busy {
			return 0, nil
		}
		return inserted, nil
	}
	return repairOneCurrentUnlocked(ctx, pool, client, b, domainID)
}

func repairOneCurrentUnlocked(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], b jobs.KindBinding, domainID int64) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, dbFail(ctx.Err())
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := release.RequireBuildingForStage(ctx, tx, b.Kind, domainID); err != nil {
		return 0, err
	}
	row, err := lockStage(ctx, tx, b.Spec, domainID)
	if err != nil {
		return 0, err
	}
	if row.status != jobs.StatusPending && row.status != jobs.StatusRunning {
		if err := tx.Commit(ctx); err != nil {
			return 0, dbFail(ctx.Err())
		}
		return 0, nil
	}
	if b.Kind == jobs.KindMRFDownload || b.Kind == jobs.KindMRFParse {
		var hasSlot bool
		if err := tx.QueryRow(ctx, `
SELECT EXISTS (SELECT 1 FROM mrfpipeline.mrf_materialization_slots WHERE mrf_source_id = $1)`, domainID).Scan(&hasSlot); err != nil {
			return 0, dbFail(ctx.Err())
		}
		if !hasSlot {
			if err := setStatus(ctx, tx, b.Spec, domainID, jobs.StatusBlocked, nil); err != nil {
				return 0, err
			}
			if err := tx.Commit(ctx); err != nil {
				return 0, dbFail(ctx.Err())
			}
			return 0, nil
		}
	}

	found := false
	state := ""
	if row.jobID != nil {
		job, jerr := client.JobGetTx(ctx, tx, *row.jobID)
		if jerr != nil && !errors.Is(jerr, rivertype.ErrNotFound) {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			return 0, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
		}
		if jerr == nil {
			found = true
			state = string(job.State)
			if job.Kind != b.Kind {
				return 0, jobs.Failure(jobs.FailureDomainInvariant)
			}
			got, aerr := jobs.DomainIDFromEncodedArgs(b.Kind, job.EncodedArgs)
			if aerr != nil || got != domainID {
				return 0, jobs.Failure(jobs.FailureDomainInvariant)
			}
		}
	}

	action, cerr := classifyRiver(found && row.jobID != nil, state)
	if cerr != nil {
		return 0, cerr
	}

	inserted := 0
	switch action {
	case actionKeep:
		if row.status == jobs.StatusRunning {
			if err := setStatus(ctx, tx, b.Spec, domainID, jobs.StatusPending, row.jobID); err != nil {
				return 0, err
			}
		}
	case actionReplace:
		args, aerr := jobs.ArgsFor(b.Kind, domainID)
		if aerr != nil {
			return 0, aerr
		}
		jobID, ierr := jobs.InsertTx(ctx, client, tx, args)
		if ierr != nil {
			return 0, ierr
		}
		if err := setStatus(ctx, tx, b.Spec, domainID, jobs.StatusPending, &jobID); err != nil {
			return 0, err
		}
		inserted = 1
	case actionFail:
		if err := markRiverTerminal(ctx, tx, b.Spec, domainID); err != nil {
			return 0, err
		}
	default:
		return 0, jobs.Failure(jobs.FailureDomainInvariant)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, dbFail(ctx.Err())
	}
	return inserted, nil
}

type stageRow struct {
	id     int64
	status string
	jobID  *int64
}

func lockStage(ctx context.Context, tx pgx.Tx, spec jobs.StageSpec, domainID int64) (stageRow, error) {
	q := fmt.Sprintf(
		`SELECT %s, %s, %s FROM %s WHERE %s = $1 FOR UPDATE`,
		spec.IDColumn, spec.StatusColumn, spec.JobIDColumn, spec.Table, spec.IDColumn,
	)
	var row stageRow
	err := tx.QueryRow(ctx, q, domainID).Scan(&row.id, &row.status, &row.jobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return stageRow{}, jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return stageRow{}, dbFail(ctx.Err())
	}
	return row, nil
}

func setStatus(ctx context.Context, tx pgx.Tx, spec jobs.StageSpec, domainID int64, status string, jobID *int64) error {
	q := fmt.Sprintf(
		`UPDATE %s SET %s = $2, %s = $3, %s = transaction_timestamp() WHERE %s = $1`,
		spec.Table, spec.StatusColumn, spec.JobIDColumn, spec.UpdatedAtColumn, spec.IDColumn,
	)
	tag, err := tx.Exec(ctx, q, domainID, status, jobID)
	if err != nil || tag.RowsAffected() != 1 {
		return dbFail(ctx.Err())
	}
	return nil
}

func markRiverTerminal(ctx context.Context, tx pgx.Tx, spec jobs.StageSpec, domainID int64) error {
	sets := fmt.Sprintf(`%s = $2, %s = $3, %s = transaction_timestamp()`, spec.StatusColumn, spec.FailureCodeColumn, spec.UpdatedAtColumn)
	if spec.CompletedAtColumn != "" {
		sets += fmt.Sprintf(`, %s = transaction_timestamp()`, spec.CompletedAtColumn)
	}
	q := fmt.Sprintf(`UPDATE %s SET %s WHERE %s = $1`, spec.Table, sets, spec.IDColumn)
	tag, err := tx.Exec(ctx, q, domainID, jobs.StatusFailed, jobs.FailureRiverTerminalWithoutResult)
	if err != nil || tag.RowsAffected() != 1 {
		return dbFail(ctx.Err())
	}
	return nil
}

func pageIDs(ctx context.Context, pool *pgxpool.Pool, query string, after int64, limit int) ([]int64, error) {
	rows, err := pool.Query(ctx, query, after, limit)
	if err != nil {
		return nil, dbFail(ctx.Err())
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, dbFail(ctx.Err())
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, dbFail(ctx.Err())
	}
	return ids, nil
}

func insertAndStore(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx], spec jobs.StageSpec, kind string, domainID int64) (int64, error) {
	args, err := jobs.ArgsFor(kind, domainID)
	if err != nil {
		return 0, err
	}
	jobID, err := jobs.InsertTx(ctx, client, tx, args)
	if err != nil {
		return 0, err
	}
	if err := setStatus(ctx, tx, spec, domainID, jobs.StatusPending, &jobID); err != nil {
		return 0, err
	}
	return jobID, nil
}
