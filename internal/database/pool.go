package database

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DiscoverMaxConns is the bounded pool for discover enqueue.
	DiscoverMaxConns int32 = 2
	// WorkMaxConns is the conservative pool shared by role processes and
	// operator reconciliation/retry commands. It leaves room for River's
	// runtime connections in addition to one advisory-lock connection per
	// executing MRF/consumer job and lease/bookkeeping checks. The supported
	// MRF process has two download workers and one parse worker.
	WorkMaxConns int32 = 12
)

// Open parses databaseURL, creates a pool with maxConns, and pings.
// It does not migrate or validate schema versions.
func Open(ctx context.Context, databaseURL string, maxConns int32) (*pgxpool.Pool, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if maxConns < 1 {
		return nil, dbErr("connect")
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, classify(ctx, "parse config", err)
	}
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, classify(ctx, "connect", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, classify(ctx, "ping", err)
	}
	return pool, nil
}
