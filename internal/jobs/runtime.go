package jobs

import (
	"context"
	"errors"
	"log/slog"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// NewInsertClient validates current schemas and constructs a River client
// for InsertTx only. It does not start workers, register queues, or migrate.
func NewInsertClient(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) (*river.Client[pgx.Tx], error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if pool == nil {
		return nil, jobErr("client")
	}
	if err := database.ValidateCurrent(ctx, pool); err != nil {
		return nil, err
	}
	cfg := ProductionPolicy()
	cfg.Workers = nil
	cfg.Queues = nil
	cfg.Logger = logger
	client, err := river.NewClient(riverpgxv5.New(pool), &cfg)
	if err != nil {
		return nil, jobErr("client")
	}
	return client, nil
}

// NewRuntime validates current application and River schemas, then constructs
// a River client. It never migrates. workers/queues are required to Start.
func NewRuntime(ctx context.Context, pool *pgxpool.Pool, workers *river.Workers, queues map[string]river.QueueConfig, handler river.ErrorHandler, logger *slog.Logger) (*river.Client[pgx.Tx], error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if pool == nil {
		return nil, jobErr("runtime")
	}
	if err := database.ValidateCurrent(ctx, pool); err != nil {
		return nil, err
	}
	cfg := ClientConfig(workers, queues, handler, logger)
	client, err := river.NewClient(riverpgxv5.New(pool), cfg)
	if err != nil {
		return nil, jobErr("client")
	}
	return client, nil
}

// Shutdown stops fetching, waits GracefulStop for cooperative jobs, then
// uses River's canceling stop. The caller closes the pool after this returns.
func Shutdown(ctx context.Context, client *river.Client[pgx.Tx]) error {
	if client == nil {
		return jobErr("stop")
	}
	soft, cancel := context.WithTimeout(context.Background(), GracefulStop)
	defer cancel()
	err := client.Stop(soft)
	if err == nil {
		return nil
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return classifyJob(ctx, "stop", err)
	}
	if err := client.StopAndCancel(context.Background()); err != nil {
		return classifyJob(ctx, "stop", err)
	}
	return nil
}
