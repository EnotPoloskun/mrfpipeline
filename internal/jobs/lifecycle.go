package jobs

import (
	"context"
	"fmt"

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
	Confirm     func(context.Context, pgx.Tx) error
	PreLock     func(context.Context, pgx.Tx) error
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
	claimFn := p.Claim
	if claimFn == nil {
		claimFn = func(ctx context.Context) (ClaimResult, error) {
			return Claim(ctx, p.Pool, p.Spec, p.DomainID, p.RiverJobID)
		}
	}
	claim, err := claimFn(ctx)
	if err != nil {
		if isFailure(err, FailureMissingRecord) || isFailure(err, FailureDomainInvariant) || isFailure(err, FailureInvalidArguments) {
			return river.JobCancel(err)
		}
		return err
	}
	if claim.Action == ClaimNoop {
		return nil
	}
	workErr := p.Work(ctx)
	if workErr == nil {
		return Succeed(ctx, p.Pool, p.Client, p.Spec, p.DomainID, p.RiverJobID, p.Successor, p.Confirm, p.PreLock)
	}
	if isFailure(workErr, FailureWorkerLeaseLost) || ctx.Err() != nil {
		return nil
	}
	if isImmediateFail(workErr) {
		code := terminalFailureCode(workErr)
		if ferr := MarkFailed(ctx, p.Pool, p.Spec, p.DomainID, p.RiverJobID, code); ferr != nil {
			return ferr
		}
		return river.JobCancel(jobErr(code))
	}
	max := p.MaxAttempts
	if max <= 0 {
		max = MaxAttempts
	}
	if p.Attempt >= max {
		code := terminalFailureCode(workErr)
		if ferr := MarkFailed(ctx, p.Pool, p.Spec, p.DomainID, p.RiverJobID, code); ferr != nil {
			return ferr
		}
		return river.JobCancel(jobErr(code))
	}
	if ferr := MarkRetryable(ctx, p.Pool, p.Spec, p.DomainID, p.RiverJobID); ferr != nil {
		return fmt.Errorf("%w: %w", workErr, ferr)
	}
	return workErr
}

// Claim locks the domain row and transitions pending/running for this
// River job ID to running. Succeeded, failed, a missing job ID, and a
// mismatched job ID are no-ops. Blocked is an invariant failure.
func Claim(ctx context.Context, pool *pgxpool.Pool, spec StageSpec, domainID, riverJobID int64) (ClaimResult, error) {
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

	row, err := lockStage(ctx, tx, spec, domainID)
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
