package discovery

import (
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Enqueue inserts one discovery run and its discovery.run job in one
// transaction, then returns the compact enqueue report.
func Enqueue(ctx context.Context, pool *pgxpool.Pool, payer, month string, limit int64) (string, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if pool == nil || payer == "" || limit < 1 || limit > math.MaxInt32 {
		return "", jobs.Failure("enqueue")
	}
	monthDate, err := parseCollectionMonth(month)
	if err != nil {
		return "", jobs.Failure("enqueue")
	}
	client, err := jobs.NewInsertClient(ctx, pool, jobs.NewLogger(io.Discard))
	if err != nil {
		return "", err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", dbFail(ctx, "begin", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := release.EnsureBuilding(ctx, tx, payer, monthDate); err != nil {
		return "", err
	}

	var runID int64
	err = tx.QueryRow(ctx, `
INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, toc_limit, status)
VALUES ($1, $2, $3, 'pending')
RETURNING id`, payer, monthDate, int32(limit)).Scan(&runID)
	if err != nil {
		return "", dbFail(ctx, "insert run", err)
	}

	jobID, err := jobs.InsertTx(ctx, client, tx, &jobs.DiscoveryRunArgs{DiscoveryRunID: runID})
	if err != nil {
		return "", err
	}
	tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.discovery_runs
SET river_job_id = $2, updated_at = transaction_timestamp()
WHERE id = $1`, runID, jobID)
	if err != nil {
		return "", dbFail(ctx, "store job", err)
	}
	if tag.RowsAffected() != 1 {
		return "", fmt.Errorf("%w: store job", database.ErrDatabase)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", dbFail(ctx, "commit", err)
	}
	return FormatReport(Report{
		DiscoveryRunID:  runID,
		RiverJobID:      jobID,
		PayerID:         payer,
		CollectionMonth: month,
		TOCLimit:        limit,
	})
}

func parseCollectionMonth(month string) (time.Time, error) {
	if len(month) != 7 || month[4] != '-' {
		return time.Time{}, jobs.Failure("enqueue")
	}
	year, err := strconv.Atoi(month[:4])
	if err != nil {
		return time.Time{}, jobs.Failure("enqueue")
	}
	m, err := strconv.Atoi(month[5:])
	if err != nil || m < 1 || m > 12 {
		return time.Time{}, jobs.Failure("enqueue")
	}
	return time.Date(year, time.Month(m), 1, 0, 0, 0, 0, time.UTC), nil
}
