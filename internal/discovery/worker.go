package discovery

import (
	"context"
	"log/slog"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// Worker executes discovery.run: list, admit, and enqueue toc.download jobs.
type Worker struct {
	river.WorkerDefaults[jobs.DiscoveryRunArgs]
	Pool     *pgxpool.Pool
	Discover DiscoverFunc
	Logger   *slog.Logger
}

func (w *Worker) Work(ctx context.Context, job *river.Job[jobs.DiscoveryRunArgs]) error {
	return jobs.Run(ctx, jobs.RunParams{
		Pool:        w.Pool,
		Client:      river.ClientFromContext[pgx.Tx](ctx),
		Spec:        jobs.DiscoveryRunStage,
		DomainID:    job.Args.DiscoveryRunID,
		RiverJobID:  job.ID,
		Attempt:     job.Attempt,
		MaxAttempts: job.MaxAttempts,
		Work:        func(ctx context.Context) error { return w.execute(ctx, job) },
	})
}

func (w *Worker) execute(ctx context.Context, job *river.Job[jobs.DiscoveryRunArgs]) error {
	run, err := loadRun(ctx, w.Pool, job.Args.DiscoveryRunID)
	if err != nil {
		return err
	}
	w.log(ctx, "listing_started", job)
	files, err := resolveDiscover(w.Discover)(ctx, run.PayerID)
	if err != nil {
		return mapDiscoverError(err)
	}
	client := river.ClientFromContext[pgx.Tx](ctx)
	if err := admit(ctx, w.Pool, client, job.Args.DiscoveryRunID, job.ID, files); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if jobs.IsFailure(err, jobs.FailureDiscoveryListing) ||
			jobs.IsFailure(err, jobs.FailureDiscoveryResultInvalid) ||
			jobs.IsFailure(err, jobs.FailureDiscoveryDatabase) ||
			jobs.IsFailure(err, jobs.FailureDomainInvariant) ||
			jobs.IsFailure(err, jobs.FailureMissingRecord) ||
			jobs.IsFailure(err, jobs.FailureInvalidArguments) ||
			jobs.IsFailure(err, jobs.FailureSealedReleaseInconsistent) {
			return err
		}
		return jobs.Failure(jobs.FailureDiscoveryDatabase)
	}
	w.log(ctx, "admission_committed", job)
	return nil
}

func (w *Worker) log(ctx context.Context, event string, job *river.Job[jobs.DiscoveryRunArgs]) {
	if w == nil || w.Logger == nil || job == nil {
		return
	}
	w.Logger.LogAttrs(ctx, slog.LevelInfo, event,
		slog.String("kind", jobs.KindDiscoveryRun),
		slog.Int64("job_id", job.ID),
		slog.Int("attempt", job.Attempt),
	)
}
