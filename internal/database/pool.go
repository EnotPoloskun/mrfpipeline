package database

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// ControlMaxConns leaves headroom above the control queues' ten workers for
	// River polling and bookkeeping connections.
	ControlMaxConns int32 = 16
	// MRFMaxConns leaves headroom above two download and one parse worker. Each
	// executing MRF stage keeps one advisory-lock connection while external work
	// runs.
	MRFMaxConns int32 = 8
	// ConsumerMaxConns leaves headroom above the singleton warehouse writer and
	// its advisory-lock/bookkeeping connections.
	ConsumerMaxConns int32 = 6
	// OperatorMaxConns is used by short-lived operator commands.
	OperatorMaxConns int32 = 8
)

// MaxConnsForRole returns the fixed pool size for one long-lived role. The
// caller validates the role before opening the pool; the default is the small
// operator pool for non-worker callers.
func MaxConnsForRole(role string) int32 {
	switch role {
	case "control":
		return ControlMaxConns
	case "mrf":
		return MRFMaxConns
	case "consumer":
		return ConsumerMaxConns
	default:
		return OperatorMaxConns
	}
}

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
