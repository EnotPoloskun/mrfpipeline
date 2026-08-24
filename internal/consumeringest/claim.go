package consumeringest

import (
	"context"
	"errors"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/planbatch"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

type claimIdentity struct {
	SnapshotID   int64
	SourceID     int64
	PayerID      string
	Month        time.Time
	MonthText    string
	ConsumeJobID int64
}

func classifyClaim(consume, parse string, stored *int64, riverJobID int64) (string, error) {
	switch consume {
	case jobs.StatusSucceeded, jobs.StatusFailed:
		return jobs.ClaimNoop, nil
	case jobs.StatusPending:
		if stored == nil {
			return "", jobs.Failure(jobs.FailureDomainInvariant)
		}
		if *stored != riverJobID {
			return jobs.ClaimNoop, nil
		}
		if parse != jobs.StatusSucceeded {
			return "", jobs.Failure(jobs.FailureDomainInvariant)
		}
		return jobs.ClaimWork, nil
	case jobs.StatusRunning:
		if stored == nil {
			return "", jobs.Failure(jobs.FailureDomainInvariant)
		}
		if *stored != riverJobID {
			return jobs.ClaimNoop, nil
		}
		if parse != jobs.StatusSucceeded {
			return "", jobs.Failure(jobs.FailureDomainInvariant)
		}
		return jobs.ClaimWork, nil
	default:
		return "", jobs.Failure(jobs.FailureDomainInvariant)
	}
}

func claimIngest(ctx context.Context, pool *pgxpool.Pool, snapshotID, riverJobID int64) (jobs.ClaimResult, claimIdentity, error) {
	if ctx == nil {
		panic("nil context")
	}
	var zero claimIdentity
	if err := ctx.Err(); err != nil {
		return jobs.ClaimResult{}, zero, err
	}
	if pool == nil || snapshotID <= 0 || riverJobID <= 0 {
		return jobs.ClaimResult{}, zero, jobs.Failure(jobs.FailureInvalidArguments)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return jobs.ClaimResult{}, zero, classifyDB(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var consume, payer string
	var stored *int64
	var sourceID int64
	var month time.Time
	err = tx.QueryRow(ctx, `
SELECT consume_status, consume_river_job_id, mrf_source_id, payer_id, collection_month
FROM mrfpipeline.mrf_snapshots
WHERE id = $1`, snapshotID).Scan(&consume, &stored, &sourceID, &payer, &month)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.ClaimResult{}, zero, jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return jobs.ClaimResult{}, zero, classifyDB(ctx, err)
	}

	var parse string
	err = tx.QueryRow(ctx, `
	SELECT parse_status FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&parse)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.ClaimResult{}, zero, jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return jobs.ClaimResult{}, zero, classifyDB(ctx, err)
	}

	action, err := classifyClaim(consume, parse, stored, riverJobID)
	if err != nil {
		return jobs.ClaimResult{}, zero, err
	}
	ident := claimIdentity{
		SnapshotID: snapshotID, SourceID: sourceID, PayerID: payer,
		Month: month, MonthText: formatMonth(month),
		ConsumeJobID: riverJobID,
	}
	if action == jobs.ClaimNoop {
		return jobs.ClaimResult{Action: jobs.ClaimNoop}, ident, nil
	}
	if err := release.RequireBuildingForSnapshot(ctx, tx, snapshotID); err != nil {
		return jobs.ClaimResult{}, zero, err
	}
	var lockedSourceID int64
	var lockedPayer string
	var lockedMonth time.Time
	err = tx.QueryRow(ctx, `
SELECT consume_status, consume_river_job_id, mrf_source_id, payer_id, collection_month
FROM mrfpipeline.mrf_snapshots
WHERE id = $1
FOR UPDATE`, snapshotID).Scan(&consume, &stored, &lockedSourceID, &lockedPayer, &lockedMonth)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.ClaimResult{}, zero, jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return jobs.ClaimResult{}, zero, classifyDB(ctx, err)
	}
	if lockedSourceID != sourceID || lockedPayer != payer || !lockedMonth.Equal(month) {
		return jobs.ClaimResult{}, zero, jobs.Failure(jobs.FailureDomainInvariant)
	}
	err = tx.QueryRow(ctx, `
SELECT parse_status FROM mrfpipeline.mrf_sources WHERE id = $1 FOR UPDATE`, lockedSourceID).Scan(&parse)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.ClaimResult{}, zero, jobs.Failure(jobs.FailureMissingRecord)
	}
	if err != nil {
		return jobs.ClaimResult{}, zero, classifyDB(ctx, err)
	}
	action, err = classifyClaim(consume, parse, stored, riverJobID)
	if err != nil {
		return jobs.ClaimResult{}, zero, err
	}
	if action == jobs.ClaimNoop {
		return jobs.ClaimResult{Action: jobs.ClaimNoop}, zero, nil
	}
	sourceID = lockedSourceID
	payer = lockedPayer
	month = lockedMonth
	ident = claimIdentity{
		SnapshotID: snapshotID, SourceID: sourceID, PayerID: payer,
		Month: month, MonthText: formatMonth(month), ConsumeJobID: riverJobID,
	}
	tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_snapshots
SET consume_status = $2, failure_code = NULL, updated_at = transaction_timestamp()
WHERE id = $1`, snapshotID, jobs.StatusRunning)
	if err != nil || tag.RowsAffected() != 1 {
		return jobs.ClaimResult{}, zero, classifyDB(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.ClaimResult{}, zero, classifyDB(ctx, err)
	}
	return jobs.ClaimResult{Action: jobs.ClaimWork}, ident, nil
}

func confirmIngestSuccess(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx], ident claimIdentity) error {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if tx == nil || ident.SnapshotID <= 0 {
		return jobs.Failure(jobs.FailureConsumerIngestDatabaseFailed)
	}
	var consume, payer string
	var stored *int64
	var sourceID int64
	var month time.Time
	if err := tx.QueryRow(ctx, `
SELECT consume_status, consume_river_job_id, mrf_source_id, payer_id, collection_month
FROM mrfpipeline.mrf_snapshots WHERE id = $1 FOR UPDATE`, ident.SnapshotID).Scan(&consume, &stored, &sourceID, &payer, &month); err != nil {
		return classifyDB(ctx, err)
	}
	if stored == nil || *stored != ident.ConsumeJobID {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}

	var parse string
	var sourceMonth time.Time
	if err := tx.QueryRow(ctx, `
SELECT parse_status, collection_month FROM mrfpipeline.mrf_sources WHERE id = $1 FOR UPDATE`, sourceID).Scan(&parse, &sourceMonth); err != nil {
		return classifyDB(ctx, err)
	}
	if sourceID != ident.SourceID || formatMonth(month) != ident.MonthText ||
		formatMonth(sourceMonth) != ident.MonthText || payer != ident.PayerID ||
		parse != jobs.StatusSucceeded {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	if consume == jobs.StatusSucceeded {
		if _, err := planbatch.Schedule(ctx, tx, client, ident.SnapshotID); err != nil {
			return err
		}
		return nil
	}
	if consume != jobs.StatusRunning {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_snapshots
SET consume_status = $2, failure_code = NULL, updated_at = transaction_timestamp()
WHERE id = $1`, ident.SnapshotID, jobs.StatusSucceeded)
	if err != nil || tag.RowsAffected() != 1 {
		return classifyDB(ctx, err)
	}
	if _, err := planbatch.Schedule(ctx, tx, client, ident.SnapshotID); err != nil {
		return err
	}
	return nil
}

func classifyDB(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return jobs.Failure(jobs.FailureConsumerIngestDatabaseFailed)
}
