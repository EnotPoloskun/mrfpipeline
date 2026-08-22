package database

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DiscoverMaxConns is the bounded pool for discover enqueue.
	DiscoverMaxConns int32 = 2
	// WorkMaxConns is the production pool for the River worker process.
	WorkMaxConns int32 = 8
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
