package jobs

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

const (
	LockNamespaceMRF      int32 = 7321
	LockNamespaceConsumer int32 = 7322
	lockPollInterval            = 5 * time.Second
)

type executionLease struct {
	mu       sync.Mutex
	conn     *pgxpool.Conn
	key      int64
	classID  uint32
	objectID uint32
}

// executionKey maps the two mutable domains into one advisory bigint key.
// Positive keys are MRF sources and negative keys are consumer outputs. This
// is deterministic, collision-free for the positive bigint domain IDs, and
// deliberately does not hash or expose the database identity.
func executionKey(namespace int32, domainID int64) (int64, error) {
	if domainID <= 0 {
		return 0, Failure(FailureInvalidArguments)
	}
	switch namespace {
	case LockNamespaceMRF:
		return domainID, nil
	case LockNamespaceConsumer:
		return -domainID, nil
	default:
		return 0, Failure(FailureInvalidArguments)
	}
}

func acquireExecutionLease(ctx context.Context, pool *pgxpool.Pool, namespace int32, domainID int64) (*executionLease, bool, error) {
	if pool == nil {
		return nil, false, Failure(FailureInvalidArguments)
	}
	key, err := executionKey(namespace, domainID)
	if err != nil {
		return nil, false, err
	}
	classID := uint32(uint64(key) >> 32)
	objectID := uint32(uint64(key))
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1::bigint)`, key).Scan(&locked); err != nil {
		conn.Release()
		return nil, false, err
	}
	if !locked {
		conn.Release()
		return nil, true, nil
	}
	return &executionLease{conn: conn, key: key, classID: classID, objectID: objectID}, false, nil
}

func (l *executionLease) release(ctx context.Context) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return
	}
	_, _ = l.conn.Exec(ctx, `SELECT pg_advisory_unlock($1::bigint)`, l.key)
	l.conn.Release()
	l.conn = nil
}

func (l *executionLease) check(ctx context.Context) error {
	if l == nil {
		return Failure(FailureStageExecutionInterrupted)
	}
	if ctx == nil {
		return Failure(FailureStageExecutionInterrupted)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return Failure(FailureStageExecutionInterrupted)
	}
	var held bool
	if err := l.conn.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1 FROM pg_locks
  WHERE locktype = 'advisory' AND objsubid = 1 AND objid = $2::oid
    AND classid = $1::oid
    AND granted AND pid = pg_backend_pid()
)`, l.classID, l.objectID).Scan(&held); err != nil {
		return err
	}
	if !held {
		return Failure(FailureStageExecutionInterrupted)
	}
	return nil
}

func (l *executionLease) watch(ctx context.Context, cancel context.CancelFunc, lost chan<- struct{}) {
	if l == nil || l.conn == nil {
		return
	}
	check := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		checkCtx, checkCancel := context.WithTimeout(context.Background(), time.Second)
		defer checkCancel()
		return l.check(checkCtx)
	}
	if err := check(); err != nil {
		if ctx.Err() == nil {
			lost <- struct{}{}
			cancel()
		}
		return
	}
	ticker := time.NewTicker(lockPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := check(); err != nil {
				if ctx.Err() == nil {
					lost <- struct{}{}
				}
				cancel()
				return
			}
		}
	}
}

// RunWithExecutionLock holds the cross-process source/output lock across the
// complete claim, external work, and confirmation lifecycle. A busy lock is
// snoozed so River does not consume a pipeline retry attempt.
func RunWithExecutionLock(ctx context.Context, p RunParams, namespace int32, domainID int64) error {
	lease, busy, err := acquireExecutionLease(ctx, p.Pool, namespace, domainID)
	if err != nil {
		return err
	}
	if busy {
		if p.Logger != nil {
			p.Logger.LogAttrs(ctx, slog.LevelInfo, "job_execution_deferred",
				slog.String("kind", p.Kind), slog.String("queue", p.Queue),
				slog.Int64("job_id", p.RiverJobID), slog.Int64("domain_id", p.DomainID),
				slog.String("failure", FailureStageExecutionBusy))
		}
		return river.JobSnooze(5 * time.Second)
	}
	if err := lease.check(ctx); err != nil {
		cleanupCtx, cleanupCancel := executionCleanupContext()
		lease.release(cleanupCtx)
		cleanupCancel()
		return Failure(FailureStageExecutionInterrupted)
	}
	workCtx, cancel := context.WithCancel(ctx)
	p.LeaseHealth = lease.check
	lost := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() { defer close(done); lease.watch(workCtx, cancel, lost) }()
	err = Run(workCtx, p)
	cancel()
	<-done
	lostLease := false
	select {
	case <-lost:
		lostLease = true
	default:
	}
	cleanupCtx, cleanupCancel := executionCleanupContext()
	lease.release(cleanupCtx)
	cleanupCancel()
	if lostLease {
		return Failure(FailureStageExecutionInterrupted)
	}
	return err
}

// WithExecutionLock is the short-operation variant used by control
// reconciliation and retry. It returns busy=true without invoking fn, so the
// caller can skip that domain without mutating it.
func WithExecutionLock(ctx context.Context, pool *pgxpool.Pool, namespace int32, domainID int64, fn func(context.Context) error) (busy bool, err error) {
	if fn == nil {
		return false, Failure(FailureInvalidArguments)
	}
	lease, busy, err := acquireExecutionLease(ctx, pool, namespace, domainID)
	if err != nil || busy {
		return busy, err
	}
	if err := lease.check(ctx); err != nil {
		cleanupCtx, cleanupCancel := executionCleanupContext()
		lease.release(cleanupCtx)
		cleanupCancel()
		return false, Failure(FailureStageExecutionInterrupted)
	}
	workCtx, cancel := context.WithCancel(ctx)
	lost := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() { defer close(done); lease.watch(workCtx, cancel, lost) }()
	err = fn(workCtx)
	cancel()
	<-done
	lostLease := false
	select {
	case <-lost:
		lostLease = true
	default:
	}
	if err == nil {
		checkCtx, checkCancel := executionCleanupContext()
		checkErr := lease.check(checkCtx)
		checkCancel()
		if checkErr != nil {
			err = Failure(FailureStageExecutionInterrupted)
		}
	}
	cleanupCtx, cleanupCancel := executionCleanupContext()
	lease.release(cleanupCtx)
	cleanupCancel()
	if lostLease {
		return false, Failure(FailureStageExecutionInterrupted)
	}
	return false, err
}

func executionCleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Second)
}
