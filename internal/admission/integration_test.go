package admission

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5/pgxpool"
)

func admissionTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	raw := os.Getenv("MRFPIPELINE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("MRFPIPELINE_TEST_DATABASE_URL is not set")
	}
	cfg, err := pgxpool.ParseConfig(raw)
	if err != nil || !strings.HasPrefix(cfg.ConnConfig.Database, "mrfpipeline_test_") {
		t.Fatal("invalid test database url")
	}
	database.TestDBMu.Lock()
	pool, err := pgxpool.New(context.Background(), raw)
	if err != nil {
		database.TestDBMu.Unlock()
		t.Fatal("connect test database")
	}
	reset := func() {
		for _, schema := range []string{database.RiverSchema, database.ApplicationSchema, "mrfpipeline_test"} {
			_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		}
	}
	reset()
	t.Cleanup(func() {
		reset()
		pool.Close()
		database.TestDBMu.Unlock()
	})
	if _, err := database.Migrate(context.Background(), raw); err != nil {
		t.Fatal("migrate: ", err)
	}
	return pool
}

func TestIntegrationSetTargetMissingReleaseDoesNotMutate(t *testing.T) {
	pool := admissionTestDB(t)
	ctx := context.Background()
	month := time.Date(2026, time.October, 1, 0, 0, 0, 0, time.UTC)
	if _, err := SetTarget(ctx, pool, "uhc", month, Target{Kind: TargetNumeric, Count: 1}); !jobs.IsFailure(err, jobs.FailureReleaseNotFound) {
		t.Fatalf("missing release error = %v", err)
	}
	var releases, events, jobsCount int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.monthly_releases`).Scan(&releases); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.control_schedule_events`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindControlSchedule).Scan(&jobsCount); err != nil {
		t.Fatal(err)
	}
	if releases != 0 || events != 0 || jobsCount != 0 {
		t.Fatalf("missing release mutated state: releases=%d events=%d jobs=%d", releases, events, jobsCount)
	}
}

func TestIntegrationTargetSelectionIsStableAndCumulative(t *testing.T) {
	pool := admissionTestDB(t)
	ctx := context.Background()
	month := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('uhc', $1)`, month); err != nil {
		t.Fatal(err)
	}
	urls := []string{
		"https://files.test/z",
		"https://files.test/a-2",
		"https://files.test/a-1",
	}
	for _, rawURL := range urls {
		var sourceID int64
		if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month)
VALUES ($1, $2) RETURNING id`, rawURL, month).Scan(&sourceID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month)
VALUES ($1, 'uhc', $2)`, sourceID, month); err != nil {
			t.Fatal(err)
		}
	}
	result, err := SetTarget(ctx, pool, "uhc", month, Target{Kind: TargetNumeric, Count: 2})
	if err != nil || result.NewlySelected != 2 {
		t.Fatalf("initial target %+v: %v", result, err)
	}
	assertSelectedURLs(t, pool, month, []string{"https://files.test/a-1", "https://files.test/a-2"})

	// Discover must not silently change an initialized target. It may reuse an
	// equal target, but a different value belongs to set-total.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = PrepareDiscoverTarget(ctx, tx, "uhc", month, Target{Kind: TargetNumeric, Count: 3})
	_ = tx.Rollback(ctx)
	if !jobs.IsFailure(err, jobs.FailureSourceTargetConflict) {
		t.Fatalf("discover changed target: %v", err)
	}
	var kind string
	var count int64
	if err := pool.QueryRow(ctx, `
SELECT mrf_source_target_kind, mrf_source_target_count
FROM mrfpipeline.monthly_releases WHERE payer_id = 'uhc' AND collection_month = $1`, month).Scan(&kind, &count); err != nil {
		t.Fatal(err)
	}
	if kind != TargetNumeric || count != 2 {
		t.Fatalf("target mutated after conflict: %s %d", kind, count)
	}

	result, err = SetTarget(ctx, pool, "uhc", month, Target{Kind: TargetNumeric, Count: 3})
	if err != nil || result.NewlySelected != 1 {
		t.Fatalf("increase %+v: %v", result, err)
	}
	assertSelectedURLs(t, pool, month, []string{"https://files.test/a-1", "https://files.test/a-2", "https://files.test/z"})
	result, err = SetTarget(ctx, pool, "uhc", month, Target{Kind: TargetNumeric, Count: 3})
	if err != nil || result.NewlySelected != 0 {
		t.Fatalf("repeat %+v: %v", result, err)
	}

	// A later TOC import can add a source without changing the target; the
	// repeated durable target selection fills only the remaining prefix.
	var sourceID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month)
VALUES ('https://files.test/later', $1) RETURNING id`, month).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month)
VALUES ($1, 'uhc', $2)`, sourceID, month); err != nil {
		t.Fatal(err)
	}
	result, err = SetTarget(ctx, pool, "uhc", month, Target{Kind: TargetNumeric, Count: 3})
	if err != nil || result.NewlySelected != 0 {
		t.Fatalf("target should remain full %+v: %v", result, err)
	}

	// Concurrent cumulative increases serialize on the release row and cannot
	// select more records than the final durable target.
	for i := 0; i < 2; i++ {
		var id int64
		if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month)
VALUES ($1, $2) RETURNING id`, "https://files.test/concurrent/"+string(rune('a'+i)), month).Scan(&id); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month)
VALUES ($1, 'uhc', $2)`, id, month); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, n := range []int64{4, 5} {
		wg.Add(1)
		go func(i int, n int64) {
			defer wg.Done()
			_, errs[i] = SetTarget(ctx, pool, "uhc", month, Target{Kind: TargetNumeric, Count: n})
		}(i, n)
	}
	wg.Wait()
	if errs[0] != nil && errs[1] != nil {
		t.Fatalf("both concurrent increases failed: %v %v", errs[0], errs[1])
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM mrfpipeline.monthly_release_mrf_sources
WHERE payer_id = 'uhc' AND collection_month = $1`, month).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count > 5 {
		t.Fatalf("over-selected %d sources", count)
	}
}

func TestIntegrationConcurrentSchedulersRespectResidentCapacity(t *testing.T) {
	pool := admissionTestDB(t)
	ctx := context.Background()
	month := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('uhc', $1);
INSERT INTO mrfpipeline.pipeline_runtime (artifact_root, resident_capacity)
VALUES ('/tmp/admission-test', 2)`, month); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		var sourceID int64
		if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month)
VALUES ($1, $2) RETURNING id`, "https://files.test/capacity/"+string(rune('a'+i)), month).Scan(&sourceID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month)
VALUES ($1, 'uhc', $2);
INSERT INTO mrfpipeline.monthly_release_mrf_sources (payer_id, collection_month, mrf_source_id)
VALUES ('uhc', $2, $1)`, sourceID, month); err != nil {
			t.Fatal(err)
		}
	}
	client, err := jobs.NewInsertClient(ctx, pool, jobs.NewLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = ScheduleWaiting(ctx, pool, client)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var held, jobsCount int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.mrf_materialization_slots`).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFDownload).Scan(&jobsCount); err != nil {
		t.Fatal(err)
	}
	if held != 2 || jobsCount != 2 {
		t.Fatalf("capacity was not enforced: held=%d jobs=%d", held, jobsCount)
	}
	if err := ReleaseSlotAndWake(ctx, pool, 1); err != nil {
		t.Fatal(err)
	}
	if err := ScheduleWaiting(ctx, pool, client); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.mrf_materialization_slots`).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != 2 {
		t.Fatalf("refill did not stay at capacity: %d", held)
	}
}

func TestIntegrationBackfillPreservesRetryableAndFailsBelowHeldCapacity(t *testing.T) {
	pool := admissionTestDB(t)
	ctx := context.Background()
	month := time.Date(2026, time.November, 1, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('uhc', $1)`, month); err != nil {
		t.Fatal(err)
	}
	client, err := jobs.NewInsertClient(ctx, pool, jobs.NewLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	insertRetryable := func(url string, state string) int64 {
		var sourceID, jobID int64
		if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ($1, $2, 'pending', 'blocked') RETURNING id`, url, month).Scan(&sourceID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month)
VALUES ($1, 'uhc', $2)`, sourceID, month); err != nil {
			t.Fatal(err)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		jobID, err = jobs.InsertTx(ctx, client, tx, &jobs.MRFDownloadArgs{MRFSourceID: sourceID})
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE mrfpipeline.mrf_sources SET download_river_job_id = $2 WHERE id = $1`, sourceID, jobID); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE mrfpipeline_river.river_job SET state = $2, scheduled_at = now() WHERE id = $1`, jobID, state); err != nil {
			t.Fatal(err)
		}
		return sourceID
	}
	first := insertRetryable("https://files.test/retryable", "retryable")
	second := insertRetryable("https://files.test/retryable-2", "retryable")
	untouched := insertRetryable("https://files.test/untouched", "available")
	if err := NormalizePending(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var status string
	var jobID *int64
	if err := pool.QueryRow(ctx, `SELECT download_status, download_river_job_id FROM mrfpipeline.mrf_sources WHERE id = $1`, first).Scan(&status, &jobID); err != nil {
		t.Fatal(err)
	}
	if status != jobs.StatusPending || jobID == nil {
		t.Fatalf("retryable source was normalized: %s %v", status, jobID)
	}
	if err := pool.QueryRow(ctx, `SELECT download_status, download_river_job_id FROM mrfpipeline.mrf_sources WHERE id = $1`, untouched).Scan(&status, &jobID); err != nil {
		t.Fatal(err)
	}
	if status != jobs.StatusBlocked || jobID != nil {
		t.Fatalf("untouched source was not normalized: %s %v", status, jobID)
	}
	ws, err := artifact.Init(ctx, filepath.Join(t.TempDir(), "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	if err := BackfillSlots(ctx, pool, ws); err != nil {
		t.Fatal(err)
	}
	var held int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.mrf_materialization_slots`).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != 2 {
		t.Fatalf("retryable backfill held=%d, want 2", held)
	}
	if _, err := ConfigureRuntime(ctx, pool, ws.Root, 1); !jobs.IsFailure(err, jobs.FailureCapacityBelowHeld) {
		t.Fatalf("capacity below held error=%v", err)
	}
	_ = second
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.mrf_materialization_slots`).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != 2 {
		t.Fatalf("failed capacity configuration changed held=%d", held)
	}
}

func assertSelectedURLs(t *testing.T, pool *pgxpool.Pool, month time.Time, want []string) {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
SELECT m.source_url
FROM mrfpipeline.monthly_release_mrf_sources a
JOIN mrfpipeline.mrf_sources m ON m.id = a.mrf_source_id
WHERE a.payer_id = 'uhc' AND a.collection_month = $1
ORDER BY m.source_url, m.id`, month)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var rawURL string
		if err := rows.Scan(&rawURL); err != nil {
			t.Fatal(err)
		}
		got = append(got, rawURL)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("selected urls %v want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("selected urls %v want %v", got, want)
		}
	}
}
