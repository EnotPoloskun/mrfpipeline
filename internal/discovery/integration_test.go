package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EnotPoloskun/mrfdiscoverer"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

func testDB(t *testing.T) (string, *pgxpool.Pool) {
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
	return raw, pool
}

func resetPipelineSchemas(ctx context.Context, pool *pgxpool.Pool) error {
	for _, schema := range []string{database.RiverSchema, database.ApplicationSchema, "mrfpipeline_test"} {
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			return err
		}
	}
	return nil
}

func parseReport(t *testing.T, text string) Report {
	t.Helper()
	var r Report
	if err := json.Unmarshal([]byte(strings.TrimSpace(text)), &r); err != nil {
		t.Fatal(err)
	}
	return r
}

func mustEnqueue(t *testing.T, pool *pgxpool.Pool, limit int64) Report {
	t.Helper()
	return mustEnqueueMonth(t, pool, "2026-08", limit)
}

func mustEnqueueMonth(t *testing.T, pool *pgxpool.Pool, month string, limit int64) Report {
	t.Helper()
	text, err := Enqueue(context.Background(), pool, "uhc", month, limit)
	if err != nil {
		t.Fatal(err)
	}
	r := parseReport(t, text)
	if r.TOCLimit != limit || r.PayerID != "uhc" || r.CollectionMonth != month {
		t.Fatalf("%+v", r)
	}
	return r
}

func markRunning(t *testing.T, pool *pgxpool.Pool, runID int64) {
	t.Helper()
	tag, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.discovery_runs
SET status = 'running', started_at = COALESCE(started_at, transaction_timestamp()), updated_at = transaction_timestamp()
WHERE id = $1`, runID)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("mark running: %v", err)
	}
}

func insertClient(t *testing.T, pool *pgxpool.Pool) *river.Client[pgx.Tx] {
	t.Helper()
	client, err := jobs.NewInsertClient(context.Background(), pool, jobs.NewLogger(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func waitRun(t *testing.T, pool *pgxpool.Pool, id int64, want string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		err := pool.QueryRow(context.Background(), `SELECT status FROM mrfpipeline.discovery_runs WHERE id = $1`, id).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for run %d status %s", id, want)
}

func TestIntegrationEnqueueAtomicAndDistinct(t *testing.T) {
	_, pool := testDB(t)
	first := mustEnqueue(t, pool, 5)
	second := mustEnqueue(t, pool, 5)
	if first.DiscoveryRunID == second.DiscoveryRunID || first.RiverJobID == second.RiverJobID {
		t.Fatalf("runs were reused: %+v %+v", first, second)
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.discovery_runs`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("runs %d %v", n, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindDiscoveryRun).Scan(&n); err != nil || n != 2 {
		t.Fatalf("jobs %d %v", n, err)
	}
	var stored int64
	if err := pool.QueryRow(context.Background(), `SELECT river_job_id FROM mrfpipeline.discovery_runs WHERE id = $1`, first.DiscoveryRunID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != first.RiverJobID {
		t.Fatalf("stored %d want %d", stored, first.RiverJobID)
	}
	var limit *int32
	if err := pool.QueryRow(context.Background(), `SELECT toc_limit FROM mrfpipeline.discovery_runs WHERE id = $1`, first.DiscoveryRunID).Scan(&limit); err != nil {
		t.Fatal(err)
	}
	if limit == nil || *limit != 5 {
		t.Fatalf("toc_limit %v", limit)
	}
}

func TestIntegrationEnqueueRollback(t *testing.T) {
	_, pool := testDB(t)
	_, err := pool.Exec(context.Background(), `
CREATE FUNCTION mrfpipeline_test_reject_river() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'injected insert failure';
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER mrfpipeline_test_reject_river
BEFORE INSERT ON mrfpipeline_river.river_job
FOR EACH ROW EXECUTE FUNCTION mrfpipeline_test_reject_river();`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Enqueue(context.Background(), pool, "uhc", "2026-08", 3)
	if err == nil {
		t.Fatal("expected failure")
	}
	if !errors.Is(err, database.ErrDatabase) && !errors.Is(err, jobs.ErrJob) {
		t.Fatalf("got %v", err)
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.discovery_runs`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("runs survived rollback: %d %v", n, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("jobs survived rollback: %d %v", n, err)
	}
}

func TestIntegrationAdmissionAndRepeat(t *testing.T) {
	_, pool := testDB(t)
	client := insertClient(t, pool)
	files := []TOCFile{
		{URL: "https://example.invalid/known"},
		{URL: "https://example.invalid/new-a"},
		{URL: "https://example.invalid/new-a"},
		{URL: "https://example.invalid/new-b"},
		{URL: "https://example.invalid/new-c"},
	}
	r1 := mustEnqueue(t, pool, 2)
	markRunning(t, pool, r1.DiscoveryRunID)
	if err := admit(context.Background(), pool, client, r1.DiscoveryRunID, r1.RiverJobID, files); err != nil {
		t.Fatal(err)
	}
	assertCounts(t, pool, r1.DiscoveryRunID, 5, 0, 2, 2)
	assertTOC(t, pool, "2026-08", "https://example.invalid/known", true)
	assertTOC(t, pool, "2026-08", "https://example.invalid/new-a", true)
	assertTOC(t, pool, "2026-08", "https://example.invalid/new-b", false)
	assertTOC(t, pool, "2026-08", "https://example.invalid/new-c", false)
	assertMembership(t, pool, r1.DiscoveryRunID, 2)
	assertDownloadJobs(t, pool, 2)

	r2 := mustEnqueue(t, pool, 2)
	markRunning(t, pool, r2.DiscoveryRunID)
	if err := admit(context.Background(), pool, client, r2.DiscoveryRunID, r2.RiverJobID, files); err != nil {
		t.Fatal(err)
	}
	assertCounts(t, pool, r2.DiscoveryRunID, 5, 2, 2, 0)
	var wasNew bool
	if err := pool.QueryRow(context.Background(), `
SELECT was_new FROM mrfpipeline.discovery_run_toc_files
WHERE discovery_run_id = $1 AND listing_ordinal = 0`, r2.DiscoveryRunID).Scan(&wasNew); err != nil || wasNew {
		t.Fatalf("known was_new=%v %v", wasNew, err)
	}
	assertTOC(t, pool, "2026-08", "https://example.invalid/new-b", true)
	assertTOC(t, pool, "2026-08", "https://example.invalid/new-c", true)
	assertDownloadJobs(t, pool, 4)
	r3 := mustEnqueueMonth(t, pool, "2026-09", 1)
	markRunning(t, pool, r3.DiscoveryRunID)
	if err := admit(context.Background(), pool, client, r3.DiscoveryRunID, r3.RiverJobID, []TOCFile{{URL: "https://example.invalid/known"}}); err != nil {
		t.Fatal(err)
	}
	assertCounts(t, pool, r3.DiscoveryRunID, 1, 0, 1, 0)
	assertTOC(t, pool, "2026-08", "https://example.invalid/known", true)
	assertTOC(t, pool, "2026-09", "https://example.invalid/known", true)
	var n int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline.toc_files
WHERE payer_id = 'uhc' AND source_url = 'https://example.invalid/known'`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("monthly captures %d %v", n, err)
	}
	assertDownloadJobs(t, pool, 5)
}

func TestIntegrationEmptyListingAndRetryNoop(t *testing.T) {
	_, pool := testDB(t)
	client := insertClient(t, pool)
	r := mustEnqueue(t, pool, 3)
	markRunning(t, pool, r.DiscoveryRunID)
	if err := admit(context.Background(), pool, client, r.DiscoveryRunID, r.RiverJobID, nil); err != nil {
		t.Fatal(err)
	}
	assertCounts(t, pool, r.DiscoveryRunID, 0, 0, 0, 0)
	if err := admit(context.Background(), pool, client, r.DiscoveryRunID, r.RiverJobID, []TOCFile{{URL: "https://example.invalid/late"}}); !jobs.IsFailure(err, jobs.FailureDomainInvariant) {
		t.Fatalf("retry after success: %v", err)
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.toc_files`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("partial publish %d %v", n, err)
	}
	claim, err := jobs.Claim(context.Background(), pool, jobs.DiscoveryRunStage, r.DiscoveryRunID, r.RiverJobID)
	if err != nil || claim.Action != jobs.ClaimNoop {
		t.Fatalf("succeeded claim %+v %v", claim, err)
	}
}

func TestIntegrationInvalidListingPublishesNothing(t *testing.T) {
	_, pool := testDB(t)
	client := insertClient(t, pool)
	r := mustEnqueue(t, pool, 3)
	markRunning(t, pool, r.DiscoveryRunID)
	err := admit(context.Background(), pool, client, r.DiscoveryRunID, r.RiverJobID, []TOCFile{
		{URL: "https://example.invalid/ok"},
		{URL: "https://example.invalid/bad\n"},
	})
	if !jobs.IsFailure(err, jobs.FailureDiscoveryResultInvalid) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), "example.invalid") {
		t.Fatalf("exposed: %v", err)
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.toc_files`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("partial %d", n)
	}
	var status string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM mrfpipeline.discovery_runs WHERE id = $1`, r.DiscoveryRunID).Scan(&status); err != nil || status != jobs.StatusRunning {
		t.Fatalf("status %s %v", status, err)
	}
}

func TestIntegrationClaimsAndTerminalCode(t *testing.T) {
	_, pool := testDB(t)
	client := insertClient(t, pool)
	missing := mustEnqueue(t, pool, 1)
	if _, err := jobs.Claim(context.Background(), pool, jobs.DiscoveryRunStage, missing.DiscoveryRunID+99, missing.RiverJobID); !jobs.IsFailure(err, jobs.FailureMissingRecord) {
		t.Fatalf("missing: %v", err)
	}
	stale := mustEnqueue(t, pool, 1)
	res, err := jobs.Claim(context.Background(), pool, jobs.DiscoveryRunStage, stale.DiscoveryRunID, stale.RiverJobID+1)
	if err != nil || res.Action != jobs.ClaimNoop {
		t.Fatalf("stale %+v %v", res, err)
	}
	failed := mustEnqueue(t, pool, 1)
	if _, err := pool.Exec(context.Background(), `UPDATE mrfpipeline.discovery_runs SET status = 'failed', completed_at = transaction_timestamp() WHERE id = $1`, failed.DiscoveryRunID); err != nil {
		t.Fatal(err)
	}
	res, err = jobs.Claim(context.Background(), pool, jobs.DiscoveryRunStage, failed.DiscoveryRunID, failed.RiverJobID)
	if err != nil || res.Action != jobs.ClaimNoop {
		t.Fatalf("failed %+v %v", res, err)
	}
	run := mustEnqueue(t, pool, 1)
	res, err = jobs.Claim(context.Background(), pool, jobs.DiscoveryRunStage, run.DiscoveryRunID, run.RiverJobID)
	if err != nil || res.Action != jobs.ClaimWork {
		t.Fatalf("claim %+v %v", res, err)
	}
	var started1, started2 time.Time
	if err := pool.QueryRow(context.Background(), `SELECT started_at FROM mrfpipeline.discovery_runs WHERE id = $1`, run.DiscoveryRunID).Scan(&started1); err != nil {
		t.Fatal(err)
	}
	res, err = jobs.Claim(context.Background(), pool, jobs.DiscoveryRunStage, run.DiscoveryRunID, run.RiverJobID)
	if err != nil || res.Action != jobs.ClaimWork {
		t.Fatalf("reclaim %+v %v", res, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT started_at FROM mrfpipeline.discovery_runs WHERE id = $1`, run.DiscoveryRunID).Scan(&started2); err != nil {
		t.Fatal(err)
	}
	if !started1.Equal(started2) {
		t.Fatalf("started_at moved %v %v", started1, started2)
	}

	term := mustEnqueue(t, pool, 1)
	markRunning(t, pool, term.DiscoveryRunID)
	err = jobs.Run(context.Background(), jobs.RunParams{
		Pool:        pool,
		Client:      client,
		Spec:        jobs.DiscoveryRunStage,
		DomainID:    term.DiscoveryRunID,
		RiverJobID:  term.RiverJobID,
		Attempt:     8,
		MaxAttempts: 8,
		Work:        func(context.Context) error { return mapDiscoverError(mrfdiscoverer.ErrListing) },
	})
	if err == nil {
		t.Fatal("expected cancel")
	}
	var code string
	if err := pool.QueryRow(context.Background(), `SELECT failure_code FROM mrfpipeline.discovery_runs WHERE id = $1`, term.DiscoveryRunID).Scan(&code); err != nil {
		t.Fatal(err)
	}
	if code != jobs.FailureDiscoveryListing {
		t.Fatalf("code %s", code)
	}
}

func TestIntegrationConcurrentAdmission(t *testing.T) {
	_, pool := testDB(t)
	client := insertClient(t, pool)
	files := []TOCFile{{URL: "https://example.invalid/x"}, {URL: "https://example.invalid/y"}}
	a := mustEnqueue(t, pool, 1)
	b := mustEnqueue(t, pool, 1)
	markRunning(t, pool, a.DiscoveryRunID)
	markRunning(t, pool, b.DiscoveryRunID)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = admit(context.Background(), pool, client, a.DiscoveryRunID, a.RiverJobID, files)
	}()
	go func() {
		defer wg.Done()
		errs[1] = admit(context.Background(), pool, client, b.DiscoveryRunID, b.RiverJobID, files)
	}()
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("admit %d: %v", i, err)
		}
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.toc_files`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("tocs %d %v", n, err)
	}
	assertDownloadJobs(t, pool, 2)
	var admitted int64
	if err := pool.QueryRow(context.Background(), `SELECT sum(admitted_count) FROM mrfpipeline.discovery_runs`).Scan(&admitted); err != nil || admitted != 2 {
		t.Fatalf("admitted sum %d %v", admitted, err)
	}
}

func TestIntegrationWorkerRuntime(t *testing.T) {
	_, pool := testDB(t)
	root := t.TempDir()
	art := root + "/artifacts"
	ws, err := artifact.Init(context.Background(), art)
	if err != nil {
		t.Fatal(err)
	}
	old := os.Getenv("TMPDIR")
	if err := os.Setenv("TMPDIR", ws.StagingDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Setenv("TMPDIR", old) })

	started := make(chan struct{})
	discover := func(ctx context.Context, payer string) ([]TOCFile, error) {
		if payer != "uhc" {
			t.Errorf("payer %q", payer)
		}
		select {
		case <-started:
		default:
			close(started)
		}
		return []TOCFile{
			{URL: "https://example.invalid/one"},
			{URL: "https://example.invalid/two"},
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunWorkers(ctx, pool, discover, jobs.NewLogger(io.Discard))
	}()
	r := mustEnqueue(t, pool, 1)
	waitRun(t, pool, r.DiscoveryRunID, jobs.StatusSucceeded)
	assertCounts(t, pool, r.DiscoveryRunID, 2, 0, 1, 1)
	assertDownloadJobs(t, pool, 1)
	var state, kind string
	if err := pool.QueryRow(context.Background(), `
SELECT state, kind FROM mrfpipeline_river.river_job
WHERE kind = $1`, jobs.KindTOCDownload).Scan(&state, &kind); err != nil {
		t.Fatal(err)
	}
	if kind != jobs.KindTOCDownload {
		t.Fatalf("kind %s", kind)
	}
	if state == "completed" || state == "discarded" {
		t.Fatalf("toc.download was consumed: %s", state)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown timeout")
	}
	entries, err := os.ReadDir(ws.Root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		switch e.Name() {
		case "workspace.json", ".staging", "toc", "mrf", "plan-batches":
		default:
			t.Fatalf("unexpected workspace entry %s", e.Name())
		}
	}
}

func TestIntegrationShutdownCancelsListing(t *testing.T) {
	_, pool := testDB(t)
	entered := make(chan struct{})
	discover := func(ctx context.Context, _ string) ([]TOCFile, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunWorkers(ctx, pool, discover, jobs.NewLogger(io.Discard))
	}()
	r := mustEnqueue(t, pool, 1)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("listing did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown timeout")
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.toc_files`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("canceled listing published %d", n)
	}
	_ = r
}

func TestIntegrationRetryAfterPrecommitFailure(t *testing.T) {
	_, pool := testDB(t)
	attempts := 0
	discover := func(context.Context, string) ([]TOCFile, error) {
		attempts++
		if attempts == 1 {
			return nil, mrfdiscoverer.ErrListing
		}
		return []TOCFile{{URL: "https://example.invalid/ok"}}, nil
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{Pool: pool, Discover: discover, Logger: jobs.NewLogger(io.Discard)})
	cfg := jobs.ClientConfig(workers, Queues(), nil, jobs.NewLogger(io.Discard))
	cfg.MaxAttempts = 8
	cfg.RetryPolicy = immediateRetry{}
	cfg.FetchPollInterval = 50 * time.Millisecond
	client, err := river.NewClient(riverpgxv5.New(pool), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = jobs.Shutdown(context.Background(), client) })
	r := mustEnqueue(t, pool, 1)
	waitRun(t, pool, r.DiscoveryRunID, jobs.StatusSucceeded)
	assertCounts(t, pool, r.DiscoveryRunID, 1, 0, 1, 0)
	if attempts < 2 {
		t.Fatalf("attempts %d", attempts)
	}
}

type immediateRetry struct{}

func (immediateRetry) NextRetry(*rivertype.JobRow) time.Time { return time.Now() }

func assertCounts(t *testing.T, pool *pgxpool.Pool, id, discovered, existing, admitted, overflow int64) {
	t.Helper()
	var d, e, a, o int64
	var status string
	var failed *string
	if err := pool.QueryRow(context.Background(), `
SELECT discovered_count, existing_count, admitted_count, overflow_count, status, failure_code
FROM mrfpipeline.discovery_runs WHERE id = $1`, id).Scan(&d, &e, &a, &o, &status, &failed); err != nil {
		t.Fatal(err)
	}
	if d != discovered || e != existing || a != admitted || o != overflow || status != jobs.StatusSucceeded || failed != nil {
		t.Fatalf("counts d=%d e=%d a=%d o=%d status=%s fail=%v", d, e, a, o, status, failed)
	}
}

func assertTOC(t *testing.T, pool *pgxpool.Pool, month, url string, want bool) {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline.toc_files
WHERE collection_month = $1 AND source_url = $2`, month, url).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if want && n != 1 {
		t.Fatalf("%s missing", url)
	}
	if !want && n != 0 {
		t.Fatalf("%s present", url)
	}
}

func assertMembership(t *testing.T, pool *pgxpool.Pool, runID int64, want int) {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.discovery_run_toc_files WHERE discovery_run_id = $1`, runID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != want {
		t.Fatalf("membership %d want %d", n, want)
	}
}

func assertDownloadJobs(t *testing.T, pool *pgxpool.Pool, want int) {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindTOCDownload).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != want {
		t.Fatalf("download jobs %d want %d", n, want)
	}
}
