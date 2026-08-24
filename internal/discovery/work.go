package discovery

import (
	"context"
	"log/slog"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// Queues is the Story 05 worker map: discovery only, max one worker.
func Queues() map[string]river.QueueConfig {
	return map[string]river.QueueConfig{
		jobs.QueueDiscovery: {MaxWorkers: 1},
	}
}

// RunWorkers starts a River client that consumes only discovery.run, waits
// until ctx is canceled or the client stops, then shuts down. A requested
// shutdown after Start succeeds returns nil.
func RunWorkers(ctx context.Context, pool *pgxpool.Pool, discover DiscoverFunc, logger *slog.Logger) error {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if pool == nil {
		return jobs.Failure("runtime")
	}
	if logger == nil {
		logger = jobs.NewLogger(nil)
	}
	insertClient, err := jobs.NewInsertClient(ctx, pool, logger)
	if err != nil {
		return err
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{Pool: pool, Discover: discover, Logger: logger, InsertClient: insertClient})
	handler := jobs.NewDomainErrorHandler(pool, []jobs.KindBinding{{
		Kind:     jobs.KindDiscoveryRun,
		Spec:     jobs.DiscoveryRunStage,
		ArgField: jobs.FieldDiscoveryRunID,
	}}, logger)
	client, err := jobs.NewRuntime(ctx, pool, workers, Queues(), handler, logger)
	if err != nil {
		return err
	}
	if err := client.Start(context.Background()); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return jobs.Failure("start")
	}
	select {
	case <-ctx.Done():
		if err := jobs.Shutdown(context.Background(), client); err != nil {
			return err
		}
		return nil
	case <-client.Stopped():
		return jobs.Failure("runtime")
	}
}
