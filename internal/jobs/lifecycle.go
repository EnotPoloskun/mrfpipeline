package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

const (
	ClaimWork = "work"
	ClaimNoop = "noop"
)

// ClaimResult is the outcome of the claim transaction.
type ClaimResult struct {
	Action string
}

// Successor is an immediate next stage published in the success transaction.
type Successor struct {
	Spec     StageSpec
	DomainID int64
	Args     river.JobArgs
}

// RunParams is the shared worker lifecycle used by Stories 05–12.
type RunParams struct {
	Pool        *pgxpool.Pool
	Client      *river.Client[pgx.Tx]
	Spec        StageSpec
	DomainID    int64
	RiverJobID  int64
	Attempt     int
	MaxAttempts int
	Work        func(context.Context) error
	Successor   *Successor
	Claim       func(context.Context) (ClaimResult, error)
	ClaimGate   func(context.Context, pgx.Tx) error
	Confirm     func(context.Context, pgx.Tx) error
	PreLock     func(context.Context, pgx.Tx) error
	Kind        string
	Queue       string
	Logger      *slog.Logger
}

// Run executes claim, external work, success/successor, retry bookkeeping,
// or terminal failure. Discovery uses DiscoveryRunStage, TOC download
// uses TOCDownloadStage, and TOC parse uses TOCParseStage with an optional
// Claim hook; later stories pass their own StageSpec.
func Run(ctx context.Context, p RunParams) error {
	if ctx == nil {
		panic("nil context")
	}
	if p.DomainID <= 0 {
		return river.JobCancel(jobErr(FailureInvalidArguments))
	}
	unlock := lockStageFlight(p.Spec, p.DomainID)
	defer unlock()
	claimFn := p.Claim
	if claimFn == nil {
		claimFn = func(ctx context.Context) (ClaimResult, error) {
			return ClaimWithGate(ctx, p.Pool, p.Spec, p.DomainID, p.RiverJobID, p.ClaimGate)
		}
	}
	claim, err := claimFn(ctx)
	if err != nil {
		if isFailure(err, FailureSealedReleaseInconsistent) {
			p.logLifecycle(ctx, slog.LevelWarn, "sealed_delivery_suppressed", "claim", err, "")
		} else {
			p.logLifecycle(ctx, slog.LevelInfo, "job_lifecycle_failure", "claim", err, "")
		}
		if isFailure(err, FailureMissingRecord) || isFailure(err, FailureDomainInvariant) || isFailure(err, FailureInvalidArguments) || isFailure(err, FailureSealedReleaseInconsistent) {
			return river.JobCancel(err)
		}
		return err
	}
	if claim.Action == ClaimNoop {
		return nil
	}
	workErr := p.Work(ctx)
	if workErr == nil {
		if err := Succeed(ctx, p.Pool, p.Client, p.Spec, p.DomainID, p.RiverJobID, p.Successor, p.Confirm, p.PreLock); err != nil {
			p.logLifecycle(ctx, slog.LevelInfo, "job_lifecycle_failure", "success", err, "")
			return err
		}
		return nil
	}
	if isFailure(workErr, FailureWorkerLeaseLost) || ctx.Err() != nil {
		return nil
	}
	if isImmediateFail(workErr) {
		code := terminalFailureCode(workErr)
		if ferr := MarkFailed(ctx, p.Pool, p.Spec, p.DomainID, p.RiverJobID, code); ferr != nil {
			p.logBookkeeping(ctx, "terminal_bookkeeping")
			return ferr
		}
		p.logLifecycle(ctx, slog.LevelInfo, "job_attempt_failed", "", workErr, "terminal")
		return river.JobCancel(jobErr(code))
	}
	max := p.MaxAttempts
	if max <= 0 {
		max = MaxAttempts
	}
	if p.Attempt >= max {
		code := terminalFailureCode(workErr)
		if ferr := MarkFailed(ctx, p.Pool, p.Spec, p.DomainID, p.RiverJobID, code); ferr != nil {
			p.logBookkeeping(ctx, "terminal_bookkeeping")
			return ferr
		}
		p.logLifecycle(ctx, slog.LevelInfo, "job_attempt_failed", "", workErr, "terminal")
		return river.JobCancel(jobErr(code))
	}
	if ferr := MarkRetryable(ctx, p.Pool, p.Spec, p.DomainID, p.RiverJobID); ferr != nil {
		p.logBookkeeping(ctx, "retry_bookkeeping")
		return fmt.Errorf("%w: %w", workErr, ferr)
	}
	p.logLifecycle(ctx, slog.LevelInfo, "job_attempt_failed", "", workErr, "retrying")
	return workErr
}

func (p RunParams) logLifecycle(ctx context.Context, level slog.Level, event, phase string, err error, outcome string) {
	if p.Logger == nil {
		return
	}
	attrs := []any{
		slog.String("kind", p.Kind),
		slog.String("queue", p.Queue),
		slog.Int64("job_id", p.RiverJobID),
		slog.Int64("domain_id", p.DomainID),
		slog.Int("attempt", p.Attempt),
		slog.String("failure", lifecycleFailureCode(err)),
	}
	if phase != "" {
		attrs = append(attrs, slog.String("phase", phase))
	}
	if outcome != "" {
		attrs = append(attrs, slog.String("outcome", outcome))
	}
	p.Logger.Log(ctx, level, event, attrs...)
}

func (p RunParams) logBookkeeping(ctx context.Context, phase string) {
	if p.Logger == nil {
		return
	}
	p.Logger.LogAttrs(ctx, slog.LevelError, "job_lifecycle_failure",
		slog.String("kind", p.Kind),
		slog.String("queue", p.Queue),
		slog.Int64("job_id", p.RiverJobID),
		slog.Int64("domain_id", p.DomainID),
		slog.Int("attempt", p.Attempt),
		slog.String("failure", FailureJobBookkeeping),
		slog.String("phase", phase),
	)
}

func lifecycleFailureCode(err error) string {
	if err == nil {
		return FailureJobLifecycle
	}
	var f *failCodeError
	if errors.As(err, &f) && allowedFailureCode(f.code) {
		return f.code
	}
	return FailureJobLifecycle
}

// Claim locks the domain row and transitions pending/running for this
// River job ID to running. Succeeded, failed, a missing job ID, and a
// mismatched job ID are no-ops. Blocked is an invariant failure.
func Claim(ctx context.Context, pool *pgxpool.Pool, spec StageSpec, domainID, riverJobID int64) (ClaimResult, error) {
	return ClaimWithGate(ctx, pool, spec, domainID, riverJobID, nil)
}

// ClaimWithGate performs the generic stage claim after an optional caller
// supplied release gate. The unlocked read avoids taking a release lock for a
// stale delivery; the stage is re-read under lock after the gate before it is
// changed.
func ClaimWithGate(ctx context.Context, pool *pgxpool.Pool, spec StageSpec, domainID, riverJobID int64, gate func(context.Context, pgx.Tx) error) (ClaimResult, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return ClaimResult{}, err
	}
	if pool == nil || domainID <= 0 || riverJobID <= 0 {
		return ClaimResult{}, jobErr(FailureInvalidArguments)
	}
	if err := spec.validate(); err != nil {
		return ClaimResult{}, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return ClaimResult{}, classifyJob(ctx, "claim", fmt.Errorf("%w", database.ErrDatabase))
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row, err := readStage(ctx, tx, spec, domainID)
	if err != nil {
		return ClaimResult{}, err
	}
	switch row.status {
	case StatusSucceeded:
		return ClaimResult{Action: ClaimNoop}, nil
	case StatusFailed:
		return ClaimResult{Action: ClaimNoop}, nil
	case StatusBlocked:
		return ClaimResult{}, jobErr(FailureDomainInvariant)
	case StatusPending, StatusRunning:
		if row.jobID == nil || *row.jobID != riverJobID {
			return ClaimResult{Action: ClaimNoop}, nil
		}
	default:
		return ClaimResult{}, jobErr(FailureDomainInvariant)
	}
	if gate != nil {
		if err := gate(ctx, tx); err != nil {
			return ClaimResult{}, err
		}
	}
	row, err = lockStage(ctx, tx, spec, domainID)
	if err != nil {
		return ClaimResult{}, err
	}
	switch row.status {
	case StatusSucceeded, StatusFailed:
		return ClaimResult{Action: ClaimNoop}, nil
	case StatusBlocked:
		return ClaimResult{}, jobErr(FailureDomainInvariant)
	case StatusPending, StatusRunning:
		if row.jobID == nil || *row.jobID != riverJobID {
			return ClaimResult{Action: ClaimNoop}, nil
		}
	default:
		return ClaimResult{}, jobErr(FailureDomainInvariant)
	}
	if err := setRunning(ctx, tx, spec, domainID); err != nil {
		return ClaimResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ClaimResult{}, classifyJob(ctx, "claim", fmt.Errorf("%w", database.ErrDatabase))
	}
	return ClaimResult{Action: ClaimWork}, nil
}

// Succeed marks the assigned running stage succeeded and may publish a
// successor in the same transaction. An already-succeeded row is a no-op
// and does not enqueue another successor.
func Succeed(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], spec StageSpec, domainID, riverJobID int64, succ *Successor, confirm, preLock func(context.Context, pgx.Tx) error) error {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if pool == nil || client == nil || domainID <= 0 || riverJobID <= 0 {
		return jobErr("succeed")
	}
	if err := spec.validate(); err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return classifyJob(ctx, "succeed", fmt.Errorf("%w", database.ErrDatabase))
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row, err := lockForSucceed(ctx, tx, spec, domainID, preLock)
	if err != nil {
		return err
	}
	if row.jobID == nil || *row.jobID != riverJobID {
		return nil
	}
	if row.status == StatusSucceeded {
		if err := tx.Commit(ctx); err != nil {
			return classifyJob(ctx, "succeed", fmt.Errorf("%w", database.ErrDatabase))
		}
		return nil
	}
	if row.status != StatusRunning {
		return jobErr(FailureDomainInvariant)
	}
	if confirm != nil {
		if err := confirm(ctx, tx); err != nil {
			return err
		}
	}
	if succ != nil {
		if err := succ.Spec.validate(); err != nil {
			return err
		}
		next, err := lockStage(ctx, tx, succ.Spec, succ.DomainID)
		if err != nil {
			return err
		}
		if next.status != StatusBlocked {
			return jobErr(FailureDomainInvariant)
		}
	}
	if err := setSucceeded(ctx, tx, spec, domainID); err != nil {
		return err
	}
	if succ != nil {
		if err := unblockIfNeeded(ctx, tx, succ.Spec, succ.DomainID); err != nil {
			return err
		}
		if _, err := Schedule(ctx, tx, client, succ.Spec, succ.DomainID, succ.Args); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return classifyJob(ctx, "succeed", fmt.Errorf("%w", database.ErrDatabase))
	}
	return nil
}

func lockForSucceed(ctx context.Context, tx pgx.Tx, spec StageSpec, domainID int64, preLock func(context.Context, pgx.Tx) error) (stageRow, error) {
	if preLock != nil {
		if err := preLock(ctx, tx); err != nil {
			return stageRow{}, err
		}
	}
	return lockStage(ctx, tx, spec, domainID)
}

// MarkRetryable puts a still-assigned running stage back to pending.
func MarkRetryable(ctx context.Context, pool *pgxpool.Pool, spec StageSpec, domainID, riverJobID int64) error {
	return updateAssigned(ctx, pool, spec, domainID, riverJobID, func(tx pgx.Tx) error {
		q := fmt.Sprintf(
			`UPDATE %s SET %s = $2, %s = transaction_timestamp() WHERE %s = $1 AND %s = $3 AND %s = $4`,
			spec.Table, spec.StatusColumn, spec.UpdatedAtColumn,
			spec.IDColumn, spec.JobIDColumn, spec.StatusColumn,
		)
		_, err := tx.Exec(ctx, q, domainID, StatusPending, riverJobID, StatusRunning)
		if err != nil {
			return classifyJob(ctx, "retry bookkeeping", fmt.Errorf("%w", database.ErrDatabase))
		}
		return nil
	})
}

// MarkFailed records a terminal safe failure_code on a still-assigned row.
func MarkFailed(ctx context.Context, pool *pgxpool.Pool, spec StageSpec, domainID, riverJobID int64, code string) error {
	if !allowedFailureCode(code) {
		code = FailureAttemptsExhausted
	}
	return updateAssigned(ctx, pool, spec, domainID, riverJobID, func(tx pgx.Tx) error {
		return setFailed(ctx, tx, spec, domainID, riverJobID, code)
	})
}

func updateAssigned(ctx context.Context, pool *pgxpool.Pool, spec StageSpec, domainID, riverJobID int64, fn func(pgx.Tx) error) error {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if pool == nil || domainID <= 0 || riverJobID <= 0 {
		return jobErr("stage update")
	}
	if err := spec.validate(); err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return classifyJob(ctx, "stage update", fmt.Errorf("%w", database.ErrDatabase))
	}
	defer func() { _ = tx.Rollback(ctx) }()
	row, err := lockStage(ctx, tx, spec, domainID)
	if err != nil {
		return err
	}
	if row.jobID == nil || *row.jobID != riverJobID {
		return nil
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return classifyJob(ctx, "stage update", fmt.Errorf("%w", database.ErrDatabase))
	}
	return nil
}

func setRunning(ctx context.Context, tx pgx.Tx, spec StageSpec, domainID int64) error {
	started := spec.UpdatedAtColumn + ` = transaction_timestamp()`
	if spec.StartedAtColumn != "" {
		started = spec.StartedAtColumn + ` = COALESCE(` + spec.StartedAtColumn + `, transaction_timestamp()), ` + started
	}
	q := fmt.Sprintf(
		`UPDATE %s SET %s = $2, %s WHERE %s = $1`,
		spec.Table, spec.StatusColumn, started, spec.IDColumn,
	)
	_, err := tx.Exec(ctx, q, domainID, StatusRunning)
	if err != nil {
		return classifyJob(ctx, "claim", fmt.Errorf("%w", database.ErrDatabase))
	}
	return nil
}

func setSucceeded(ctx context.Context, tx pgx.Tx, spec StageSpec, domainID int64) error {
	sets := fmt.Sprintf(`%s = $2, %s = transaction_timestamp()`, spec.StatusColumn, spec.UpdatedAtColumn)
	if spec.FailureCodeColumn != "" {
		sets += fmt.Sprintf(`, %s = NULL`, spec.FailureCodeColumn)
	}
	if spec.CompletedAtColumn != "" {
		sets += fmt.Sprintf(`, %s = transaction_timestamp()`, spec.CompletedAtColumn)
	}
	q := fmt.Sprintf(`UPDATE %s SET %s WHERE %s = $1`, spec.Table, sets, spec.IDColumn)
	_, err := tx.Exec(ctx, q, domainID, StatusSucceeded)
	if err != nil {
		return classifyJob(ctx, "succeed", fmt.Errorf("%w", database.ErrDatabase))
	}
	return nil
}

func setFailed(ctx context.Context, tx pgx.Tx, spec StageSpec, domainID, riverJobID int64, code string) error {
	sets := fmt.Sprintf(`%s = $2, %s = transaction_timestamp()`, spec.StatusColumn, spec.UpdatedAtColumn)
	args := []any{domainID, StatusFailed}
	if spec.FailureCodeColumn != "" {
		args = append(args, code)
		sets += fmt.Sprintf(`, %s = $%d`, spec.FailureCodeColumn, len(args))
	}
	if spec.CompletedAtColumn != "" {
		sets += fmt.Sprintf(`, %s = transaction_timestamp()`, spec.CompletedAtColumn)
	}
	args = append(args, riverJobID)
	q := fmt.Sprintf(
		`UPDATE %s SET %s WHERE %s = $1 AND %s = $%d`,
		spec.Table, sets, spec.IDColumn, spec.JobIDColumn, len(args),
	)
	_, err := tx.Exec(ctx, q, args...)
	if err != nil {
		return classifyJob(ctx, "fail", fmt.Errorf("%w", database.ErrDatabase))
	}
	return nil
}

func unblockIfNeeded(ctx context.Context, tx pgx.Tx, spec StageSpec, domainID int64) error {
	if err := spec.validate(); err != nil {
		return err
	}
	q := fmt.Sprintf(
		`UPDATE %s SET %s = $2, %s = transaction_timestamp() WHERE %s = $1 AND %s = $3`,
		spec.Table, spec.StatusColumn, spec.UpdatedAtColumn, spec.IDColumn, spec.StatusColumn,
	)
	_, err := tx.Exec(ctx, q, domainID, StatusPending, StatusBlocked)
	if err != nil {
		return classifyJob(ctx, "unblock", fmt.Errorf("%w", database.ErrDatabase))
	}
	return nil
}
