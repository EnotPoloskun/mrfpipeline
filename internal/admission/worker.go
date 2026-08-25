package admission

import (
	"context"
	"log/slog"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// Worker consumes the one coalesced control wake-up and refills selected
// materialization work under the control lease.
type Worker struct {
	river.WorkerDefaults[jobs.ControlScheduleArgs]
	Pool   *pgxpool.Pool
	Logger *slog.Logger
}

func (w *Worker) Work(ctx context.Context, job *river.Job[jobs.ControlScheduleArgs]) error {
	if w == nil || w.Pool == nil {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	client := river.ClientFromContext[pgx.Tx](ctx)
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := ScheduleSelectedConsumersAllTx(ctx, tx, client); err != nil {
		return err
	}
	if err := ScheduleWaitingTx(ctx, tx, client); err != nil {
		return err
	}
	if job != nil && job.Args.EventID > 0 {
		if err := ConsumeWakeTx(ctx, tx, job.Args.EventID); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	LogCapacityState(ctx, w.Pool, w.Logger)
	return nil
}
