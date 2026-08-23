package planbatch

import (
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	raw := os.Getenv("MRFPIPELINE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("MRFPIPELINE_TEST_DATABASE_URL is not set")
	}
	cfg, err := pgxpool.ParseConfig(raw)
	if err != nil {
		t.Fatal("invalid test database url")
	}
	if !strings.HasPrefix(cfg.ConnConfig.Database, "mrfpipeline_test_") {
		t.Fatal("test database name must start with mrfpipeline_test_")
	}
	database.TestDBMu.Lock()
	pool, err := pgxpool.New(context.Background(), raw)
	if err != nil {
		database.TestDBMu.Unlock()
		t.Fatal("connect test database")
	}
	if err := resetPipelineSchemas(context.Background(), pool); err != nil {
		pool.Close()
		database.TestDBMu.Unlock()
		t.Fatal("reset schema")
	}
	t.Cleanup(func() {
		_ = resetPipelineSchemas(context.Background(), pool)
		pool.Close()
		database.TestDBMu.Unlock()
	})
	if _, err := database.Migrate(context.Background(), raw); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func resetPipelineSchemas(ctx context.Context, pool *pgxpool.Pool) error {
	for _, schema := range []string{database.RiverSchema, database.ApplicationSchema, "mrfpipeline_test"} {
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			return err
		}
	}
	return nil
}

var sourceURLSeq atomic.Int64

func insertClient(t *testing.T, pool *pgxpool.Pool) *river.Client[pgx.Tx] {
	t.Helper()
	client, err := jobs.NewInsertClient(context.Background(), pool, jobs.NewLogger(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func insertConsumedSnapshot(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	url := "https://files.test/planbatch/" + strconv.FormatInt(sourceURLSeq.Add(1), 10)
	var sourceID, feedID, snapID int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_sources (source_url, download_status, parse_status)
VALUES ($1, 'succeeded', 'succeeded') RETURNING id`, url).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_feeds (payer_id, feed_id)
VALUES ('uhc', $1) RETURNING id`, "mrf-source-"+strconv.FormatInt(sourceID, 10)).Scan(&feedID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, mrf_feed_id, collection_month, consume_status, consume_river_job_id)
VALUES ($1, $2, DATE '2026-08-01', 'succeeded', 1) RETURNING id`, sourceID, feedID).Scan(&snapID); err != nil {
		t.Fatal(err)
	}
	return snapID
}

func insertPlan(t *testing.T, pool *pgxpool.Pool, snapID int64, name, idType, planID string, sponsor *string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_plans (
    mrf_snapshot_id, plan_name, issuer_name, plan_sponsor_name, plan_id_type, plan_id, plan_market_type
) VALUES ($1, $2, 'issuer', $3, $4, $5, 'group') RETURNING id`, snapID, name, sponsor, idType, planID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func schedule(t *testing.T, pool *pgxpool.Pool, client *river.Client[pgx.Tx], snapID int64) {
	t.Helper()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := Schedule(context.Background(), tx, client, snapID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFormatPlanBatchID(t *testing.T) {
	t.Parallel()
	if FormatPlanBatchID(230) != "plan-batch-230" || FormatPlanBatchID(1) != "plan-batch-1" {
		t.Fatal(FormatPlanBatchID(230), FormatPlanBatchID(1))
	}
	if strings.Contains(FormatPlanBatchID(12), "012") {
		t.Fatal("padded")
	}
}

func TestSchedulerCreatesOneBatch(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	snap := insertConsumedSnapshot(t, pool)
	insertPlan(t, pool, snap, "B", "hios", "2", nil)
	insertPlan(t, pool, snap, "A", "hios", "1", nil)
	schedule(t, pool, client, snap)
	schedule(t, pool, client, snap)
	var batches, items, jobsN int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.plan_attachment_batches`).Scan(&batches); err != nil || batches != 1 {
		t.Fatalf("batches %d", batches)
	}
	if err := pool.QueryRow(context.Background(), `SELECT requested_plan_count FROM mrfpipeline.plan_attachment_batches`).Scan(&items); err != nil || items != 2 {
		t.Fatalf("requested %d", items)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.plan_attachment_batch_items`).Scan(&items); err != nil || items != 2 {
		t.Fatalf("items %d", items)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindConsumerAttachPlans).Scan(&jobsN); err != nil || jobsN != 1 {
		t.Fatalf("jobs %d", jobsN)
	}
	var firstName string
	if err := pool.QueryRow(context.Background(), `
SELECT p.plan_name FROM mrfpipeline.plan_attachment_batch_items i
JOIN mrfpipeline.mrf_plans p ON p.id = i.mrf_plan_id
ORDER BY p.plan_name LIMIT 1`).Scan(&firstName); err != nil || firstName != "A" {
		t.Fatalf("order %s", firstName)
	}
}

func TestSchedulerZeroCountNeverCreated(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	snap := insertConsumedSnapshot(t, pool)
	schedule(t, pool, client, snap)
	var batches int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.plan_attachment_batches`).Scan(&batches); err != nil || batches != 0 {
		t.Fatalf("batches %d", batches)
	}
}

func TestSchedulerBlocksUnresolved(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	for _, status := range []string{jobs.StatusPending, jobs.StatusRunning, jobs.StatusFailed} {
		t.Run(status, func(t *testing.T) {
			snap := insertConsumedSnapshot(t, pool)
			insertPlan(t, pool, snap, "A", "hios", "1", nil)
			schedule(t, pool, client, snap)
			if status != jobs.StatusPending {
				if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.plan_attachment_batches
SET status = $2, completed_at = CASE WHEN $2 = 'failed' THEN transaction_timestamp() ELSE completed_at END,
    failure_code = CASE WHEN $2 = 'failed' THEN 'plan_attach_output_failed' ELSE failure_code END
WHERE mrf_snapshot_id = $1`, snap, status); err != nil {
					t.Fatal(err)
				}
			}
			insertPlan(t, pool, snap, "B", "hios", "2", nil)
			schedule(t, pool, client, snap)
			var n int
			if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id = $1`, snap).Scan(&n); err != nil || n != 1 {
				t.Fatalf("batches %d", n)
			}
		})
	}
}

func TestSchedulerSucceededAllowsNext(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	snap := insertConsumedSnapshot(t, pool)
	insertPlan(t, pool, snap, "A", "hios", "1", nil)
	schedule(t, pool, client, snap)
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.plan_attachment_batches
SET status = 'succeeded', added_plan_count = 1, completed_at = transaction_timestamp()
WHERE mrf_snapshot_id = $1`, snap); err != nil {
		t.Fatal(err)
	}
	insertPlan(t, pool, snap, "B", "hios", "2", nil)
	schedule(t, pool, client, snap)
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id = $1`, snap).Scan(&n); err != nil || n != 2 {
		t.Fatalf("batches %d", n)
	}
}

func TestReadinessStates(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	planless := insertConsumedSnapshot(t, pool)
	ready, err := IsPlanReady(context.Background(), pool, planless)
	if err != nil || ready {
		t.Fatalf("planless %v %v", ready, err)
	}
	unassigned := insertConsumedSnapshot(t, pool)
	insertPlan(t, pool, unassigned, "A", "hios", "1", nil)
	ready, err = IsPlanReady(context.Background(), pool, unassigned)
	if err != nil || ready {
		t.Fatalf("unassigned %v %v", ready, err)
	}
	pending := insertConsumedSnapshot(t, pool)
	insertPlan(t, pool, pending, "A", "hios", "1", nil)
	schedule(t, pool, client, pending)
	ready, err = IsPlanReady(context.Background(), pool, pending)
	if err != nil || ready {
		t.Fatalf("pending %v %v", ready, err)
	}
	failed := insertConsumedSnapshot(t, pool)
	insertPlan(t, pool, failed, "A", "hios", "1", nil)
	schedule(t, pool, client, failed)
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.plan_attachment_batches
SET status = 'failed', failure_code = 'plan_attach_output_failed', completed_at = transaction_timestamp()
WHERE mrf_snapshot_id = $1`, failed); err != nil {
		t.Fatal(err)
	}
	ready, err = IsPlanReady(context.Background(), pool, failed)
	if err != nil || ready {
		t.Fatalf("failed %v %v", ready, err)
	}
	ok := insertConsumedSnapshot(t, pool)
	insertPlan(t, pool, ok, "A", "hios", "1", nil)
	schedule(t, pool, client, ok)
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.plan_attachment_batches
SET status = 'succeeded', added_plan_count = 1, completed_at = transaction_timestamp()
WHERE mrf_snapshot_id = $1`, ok); err != nil {
		t.Fatal(err)
	}
	ready, err = IsPlanReady(context.Background(), pool, ok)
	if err != nil || !ready {
		t.Fatalf("ready %v %v", ready, err)
	}
}

func TestSweepConsumedBacklog(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	snap := insertConsumedSnapshot(t, pool)
	insertPlan(t, pool, snap, "A", "hios", "1", nil)
	if n, err := SweepConsumed(context.Background(), pool, client); err != nil || n != 1 {
		t.Fatalf("first sweep %d %v", n, err)
	}
	if n, err := SweepConsumed(context.Background(), pool, client); err != nil || n != 0 {
		t.Fatalf("second sweep %d %v", n, err)
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.plan_attachment_batches`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("batches %d", n)
	}
}
