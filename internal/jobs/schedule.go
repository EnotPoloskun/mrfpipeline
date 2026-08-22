package jobs

import (
	"context"
	"errors"
	"fmt"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
)

const (
	ScheduleInserted = "inserted"
	ScheduleExisting = "existing"
	ScheduleNoop     = "noop"
)

// ScheduleResult is the outcome of the eight-step enqueue protocol.
type ScheduleResult struct {
	JobID   int64
	Outcome string
}

// Schedule runs the eight-step protocol in a caller-owned transaction:
// lock, re-read, skip if pending/running with a job ID, no-op if succeeded,
// reject failed, InsertTx if eligible, store job ID + pending. The caller
// commits or rolls back.
func Schedule(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx], spec StageSpec, domainID int64, args river.JobArgs) (ScheduleResult, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return ScheduleResult{}, err
	}
	if tx == nil || client == nil || args == nil || domainID <= 0 {
		return ScheduleResult{}, jobErr("schedule")
	}
	if err := spec.validate(); err != nil {
		return ScheduleResult{}, err
	}
	row, err := lockStage(ctx, tx, spec, domainID)
	if err != nil {
		return ScheduleResult{}, err
	}
	switch row.status {
	case StatusSucceeded:
		return ScheduleResult{JobID: jobIDValue(row.jobID), Outcome: ScheduleNoop}, nil
	case StatusFailed:
		return ScheduleResult{}, jobErr(FailureDomainInvariant)
	case StatusBlocked:
		return ScheduleResult{}, jobErr(FailureDomainInvariant)
	case StatusPending, StatusRunning:
		if row.jobID != nil && *row.jobID > 0 {
			return ScheduleResult{JobID: *row.jobID, Outcome: ScheduleExisting}, nil
		}
	default:
		return ScheduleResult{}, jobErr(FailureDomainInvariant)
	}
	jobID, err := InsertTx(ctx, client, tx, args)
	if err != nil {
		return ScheduleResult{}, err
	}
	if err := storePendingJob(ctx, tx, spec, domainID, jobID); err != nil {
		return ScheduleResult{}, err
	}
	return ScheduleResult{JobID: jobID, Outcome: ScheduleInserted}, nil
}

func lockStage(ctx context.Context, tx pgx.Tx, spec StageSpec, domainID int64) (stageRow, error) {
	q := fmt.Sprintf(
		`SELECT %s, %s, %s FROM %s WHERE %s = $1 FOR UPDATE`,
		spec.IDColumn, spec.StatusColumn, spec.JobIDColumn, spec.Table, spec.IDColumn,
	)
	var row stageRow
	err := tx.QueryRow(ctx, q, domainID).Scan(&row.id, &row.status, &row.jobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return stageRow{}, jobErr(FailureMissingRecord)
	}
	if err != nil {
		return stageRow{}, classifyJob(ctx, "lock stage", fmt.Errorf("%w", database.ErrDatabase))
	}
	return row, nil
}

func storePendingJob(ctx context.Context, tx pgx.Tx, spec StageSpec, domainID, jobID int64) error {
	q := fmt.Sprintf(
		`UPDATE %s SET %s = $2, %s = $3, %s = transaction_timestamp() WHERE %s = $1`,
		spec.Table, spec.StatusColumn, spec.JobIDColumn, spec.UpdatedAtColumn, spec.IDColumn,
	)
	tag, err := tx.Exec(ctx, q, domainID, StatusPending, jobID)
	if err != nil {
		return classifyJob(ctx, "store job", fmt.Errorf("%w", database.ErrDatabase))
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: %w: store job", ErrJob, database.ErrDatabase)
	}
	return nil
}

func jobIDValue(id *int64) int64 {
	if id == nil {
		return 0
	}
	return *id
}
