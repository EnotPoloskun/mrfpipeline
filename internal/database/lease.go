package database

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Worker lease keys. Distinct from MigrationLockObject. Never print these.
const (
	WorkerLeaseClass  = 7319
	WorkerLeaseObject = 1
)

// Lease is one dedicated pool connection that holds the worker advisory lock.
type Lease struct {
	conn *pgxpool.Conn
}

// AcquireWorkerLease checks out one pool connection and takes the nonwaiting
// session lock. A false try-lock result is returned as errUnavailable so the
// caller can map it without logging the keys.
func AcquireWorkerLease(ctx context.Context, pool *pgxpool.Pool) (*Lease, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if pool == nil {
		return nil, dbErr("acquire")
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, classify(ctx, "acquire", err)
	}
	var locked bool
	err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1, $2)`, WorkerLeaseClass, WorkerLeaseObject).Scan(&locked)
	if err != nil {
		conn.Release()
		return nil, classify(ctx, "lock", err)
	}
	if !locked {
		conn.Release()
		return nil, errLeaseUnavailable
	}
	return &Lease{conn: conn}, nil
}

// Check confirms this backend still holds the granted lock and can ping.
// It must not call pg_try_advisory_lock again.
func (l *Lease) Check(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return dbErr("lease")
	}
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var held bool
	err := l.conn.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_locks
    WHERE locktype = 'advisory'
      AND classid = $1
      AND objid = $2
      AND granted
      AND pid = pg_backend_pid()
)`, WorkerLeaseClass, WorkerLeaseObject).Scan(&held)
	if err != nil {
		return classify(ctx, "lease", err)
	}
	if !held {
		return errLeaseLost
	}
	var one int
	if err := l.conn.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		return classify(ctx, "lease", err)
	}
	return nil
}

// Release unlocks then returns the connection to the pool.
func (l *Lease) Release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := l.conn.Exec(ctx, `SELECT pg_advisory_unlock($1, $2)`, WorkerLeaseClass, WorkerLeaseObject)
	if err != nil {
		raw := l.conn.Hijack()
		l.conn = nil
		if raw != nil {
			_ = raw.Close(context.Background())
		}
		return dbErr("unlock")
	}
	l.conn.Release()
	l.conn = nil
	return nil
}

var (
	errLeaseUnavailable = errorsLease("unavailable")
	errLeaseLost        = errorsLease("lost")
)

type leaseSentinel string

func (e leaseSentinel) Error() string { return string(e) }

func errorsLease(kind string) error {
	return leaseSentinel(kind)
}

// IsLeaseUnavailable reports a failed nonwaiting lock attempt.
func IsLeaseUnavailable(err error) bool {
	return err == errLeaseUnavailable
}

// IsLeaseLost reports a failed health check on a previously held lease.
func IsLeaseLost(err error) bool {
	return err == errLeaseLost
}
