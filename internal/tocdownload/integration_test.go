package tocdownload

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

const publicTestIP = "203.0.113.10"

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

func hookDownloader(ws *artifact.Workspace, ts *httptest.Server) *artifact.Downloader {
	return artifact.NewTestDownloader(ws, jobs.NewProgress(jobs.NewLogger(io.Discard)),
		func(context.Context, string) ([]net.IP, error) {
			return []net.IP{net.ParseIP(publicTestIP)}, nil
		},
		func(ctx context.Context, network, address string) (net.Conn, error) {
			var nd net.Dialer
			return nd.DialContext(ctx, "tcp", ts.Listener.Addr().String())
		},
	)
}

func storedURL(path string) string {
	if path == "" {
		path = "/data"
	}
	return "http://files.test" + path
}

func insertClient(t *testing.T, pool *pgxpool.Pool) *river.Client[pgx.Tx] {
	t.Helper()
	client, err := jobs.NewInsertClient(context.Background(), pool, jobs.NewLogger(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func insertTOCJob(t *testing.T, pool *pgxpool.Pool, client *river.Client[pgx.Tx], sourceURL string) (tocID, jobID int64) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('uhc', DATE '2026-08-01') ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	var runID int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, toc_limit, status, completed_at)
VALUES ('uhc', DATE '2026-08-01', 1, 'succeeded', transaction_timestamp())
RETURNING id`).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id,
    download_status, parse_status, import_status
) VALUES ('uhc', DATE '2026-08-01', $1, $2, 'pending', 'blocked', 'blocked')
RETURNING id`, sourceURL, runID).Scan(&tocID); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	jobID, err = jobs.InsertTx(context.Background(), client, tx, &jobs.TOCDownloadArgs{TOCFileID: tocID})
	if err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `
UPDATE mrfpipeline.toc_files SET download_river_job_id = $2, updated_at = transaction_timestamp()
WHERE id = $1`, tocID, jobID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	return tocID, jobID
}

func waitDownload(t *testing.T, pool *pgxpool.Pool, tocID int64, want string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		err := pool.QueryRow(context.Background(), `SELECT download_status FROM mrfpipeline.toc_files WHERE id = $1`, tocID).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for toc %d download %s", tocID, want)
}

func tocRow(t *testing.T, pool *pgxpool.Pool, tocID int64) (download, parse string, parseJob *int64, fail *string) {
	t.Helper()
	if err := pool.QueryRow(context.Background(), `
SELECT download_status, parse_status, parse_river_job_id, failure_code
FROM mrfpipeline.toc_files WHERE id = $1`, tocID).Scan(&download, &parse, &parseJob, &fail); err != nil {
		t.Fatal(err)
	}
	return download, parse, parseJob, fail
}

type immediateRetry struct{}

func (immediateRetry) NextRetry(*rivertype.JobRow) time.Time { return time.Now() }

func startDownloadRuntime(t *testing.T, pool *pgxpool.Pool, dl *artifact.Downloader, maxAttempts int) *river.Client[pgx.Tx] {
	t.Helper()
	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{Pool: pool, Downloader: dl, Logger: jobs.NewLogger(io.Discard)})
	cfg := jobs.ClientConfig(workers, map[string]river.QueueConfig{jobs.QueueTOCDownload: {MaxWorkers: 4}}, nil, jobs.NewLogger(io.Discard))
	cfg.SkipUnknownJobCheck = true
	cfg.MaxAttempts = maxAttempts
	cfg.RetryPolicy = immediateRetry{}
	cfg.FetchCooldown = 50 * time.Millisecond
	cfg.FetchPollInterval = 50 * time.Millisecond
	client, err := river.NewClient(riverpgxv5.New(pool), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = jobs.Shutdown(context.Background(), client) })
	return client
}

func TestIntegrationDownloadSuccessAndReuse(t *testing.T) {
	_, pool := testDB(t)
	body := []byte(`{"toc":true}`)
	var hits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Host != "files.test" || r.URL.Path != "/data" {
			t.Errorf("unexpected request %s %s", r.Host, r.URL.Path)
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	dl := hookDownloader(ws, ts)
	client := insertClient(t, pool)
	tocID, jobID := insertTOCJob(t, pool, client, storedURL("/data"))
	startDownloadRuntime(t, pool, dl, 8)
	waitDownload(t, pool, tocID, jobs.StatusSucceeded)
	download, parse, parseJob, fail := tocRow(t, pool, tocID)
	if download != jobs.StatusSucceeded || parse != jobs.StatusPending || parseJob == nil || *parseJob <= 0 || fail != nil {
		t.Fatalf("row %s %s job=%v fail=%v", download, parse, parseJob, fail)
	}
	n, err := ws.InspectDownload(artifact.KindTOC, tocID)
	if err != nil || n != int64(len(body)) {
		t.Fatalf("inspect %d %v", n, err)
	}
	got, err := os.ReadFile(filepath.Join(ws.Root, "toc", "toc-"+strconv.FormatInt(tocID, 10), "download", "data"))
	if err != nil || string(got) != string(body) {
		t.Fatalf("bytes %q %v", got, err)
	}
	var parseKind, parseState string
	if err := pool.QueryRow(context.Background(), `
SELECT kind, state FROM mrfpipeline_river.river_job WHERE id = $1`, *parseJob).Scan(&parseKind, &parseState); err != nil {
		t.Fatal(err)
	}
	if parseKind != jobs.KindTOCParse || parseState == "completed" || parseState == "discarded" {
		t.Fatalf("parse job %s %s", parseKind, parseState)
	}
	if hits.Load() != 1 {
		t.Fatalf("hits %d", hits.Load())
	}

	if err := jobs.Run(context.Background(), jobs.RunParams{
		Pool:        pool,
		Client:      client,
		Spec:        jobs.TOCDownloadStage,
		DomainID:    tocID,
		RiverJobID:  jobID,
		Attempt:     1,
		MaxAttempts: 8,
		Claim: func(ctx context.Context) (jobs.ClaimResult, error) {
			res, _, err := claimDownload(ctx, pool, tocID, jobID)
			return res, err
		},
		Work: func(context.Context) error {
			t.Fatal("succeeded claim must not download")
			return nil
		},
		Successor: &jobs.Successor{
			Spec:     jobs.TOCParseStage,
			DomainID: tocID,
			Args:     &jobs.TOCParseArgs{TOCFileID: tocID},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatalf("second http %d", hits.Load())
	}
	var parseCount int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindTOCParse).Scan(&parseCount); err != nil || parseCount != 1 {
		t.Fatalf("parse jobs %d %v", parseCount, err)
	}
}

func TestIntegrationRenameBeforeCommitReuses(t *testing.T) {
	_, pool := testDB(t)
	var hits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("ready"))
	}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	dl := hookDownloader(ws, ts)
	client := insertClient(t, pool)
	tocID, jobID := insertTOCJob(t, pool, client, storedURL("/data"))
	if _, err := dl.Download(context.Background(), artifact.KindTOC, tocID, storedURL("/data"), artifact.ProgressID{JobID: jobID, Kind: jobs.KindTOCDownload, Queue: jobs.QueueTOCDownload}); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatalf("prep hits %d", hits.Load())
	}
	startDownloadRuntime(t, pool, dl, 8)
	waitDownload(t, pool, tocID, jobs.StatusSucceeded)
	if hits.Load() != 1 {
		t.Fatalf("reused download hit http %d", hits.Load())
	}
	_, parse, parseJob, _ := tocRow(t, pool, tocID)
	if parse != jobs.StatusPending || parseJob == nil {
		t.Fatal("parse not scheduled")
	}
}

func TestIntegrationMidStreamRestart(t *testing.T) {
	_, pool := testDB(t)
	var hits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if n == 1 {
			w.Header().Set("Content-Length", "20")
			fl, _ := w.(http.Flusher)
			_, _ = w.Write([]byte("partial"))
			if fl != nil {
				fl.Flush()
			}
			if hj, ok := w.(http.Hijacker); ok {
				conn, _, err := hj.Hijack()
				if err == nil {
					_ = conn.Close()
				}
			}
			return
		}
		_, _ = w.Write([]byte("complete-download"))
	}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	dl := hookDownloader(ws, ts)
	client := insertClient(t, pool)
	tocID, _ := insertTOCJob(t, pool, client, storedURL("/data"))
	startDownloadRuntime(t, pool, dl, 8)
	waitDownload(t, pool, tocID, jobs.StatusSucceeded)
	if hits.Load() < 2 {
		t.Fatalf("did not restart %d", hits.Load())
	}
	n, err := ws.InspectDownload(artifact.KindTOC, tocID)
	if err != nil || n != int64(len("complete-download")) {
		t.Fatalf("final %d %v", n, err)
	}
}

func TestIntegrationZeroByteSchedulesParse(t *testing.T) {
	_, pool := testDB(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	client := insertClient(t, pool)
	tocID, _ := insertTOCJob(t, pool, client, storedURL("/data"))
	startDownloadRuntime(t, pool, hookDownloader(ws, ts), 8)
	waitDownload(t, pool, tocID, jobs.StatusSucceeded)
	n, err := ws.InspectDownload(artifact.KindTOC, tocID)
	if err != nil || n != 0 {
		t.Fatalf("zero %d %v", n, err)
	}
	_, parse, parseJob, _ := tocRow(t, pool, tocID)
	if parse != jobs.StatusPending || parseJob == nil {
		t.Fatal("zero-byte must schedule parse")
	}
}

func TestIntegrationEighthFailureCode(t *testing.T) {
	_, pool := testDB(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	client := insertClient(t, pool)
	tocID, _ := insertTOCJob(t, pool, client, storedURL("/data"))
	startDownloadRuntime(t, pool, hookDownloader(ws, ts), 1)
	waitDownload(t, pool, tocID, jobs.StatusFailed)
	download, parse, parseJob, fail := tocRow(t, pool, tocID)
	if download != jobs.StatusFailed || parse != jobs.StatusBlocked || parseJob != nil || fail == nil || *fail != jobs.FailureTOCDownload {
		t.Fatalf("terminal %s %s job=%v fail=%v", download, parse, parseJob, fail)
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.toc_files WHERE id = $1`, tocID).Scan(&n); err != nil || n != 1 {
		t.Fatal("deleted toc row")
	}
}

func TestIntegrationStaleJobLeavesArtifact(t *testing.T) {
	_, pool := testDB(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("owned"))
	}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	dl := hookDownloader(ws, ts)
	client := insertClient(t, pool)
	tocID, jobID := insertTOCJob(t, pool, client, storedURL("/data"))
	startDownloadRuntime(t, pool, dl, 8)
	waitDownload(t, pool, tocID, jobs.StatusSucceeded)
	before, err := os.ReadFile(filepath.Join(ws.Root, "toc", "toc-"+strconv.FormatInt(tocID, 10), "download", "data"))
	if err != nil {
		t.Fatal(err)
	}
	res, url, err := claimDownload(context.Background(), pool, tocID, jobID+99)
	if err != nil || res.Action != jobs.ClaimNoop || url != "" {
		t.Fatalf("stale %+v %q %v", res, url, err)
	}
	after, err := os.ReadFile(filepath.Join(ws.Root, "toc", "toc-"+strconv.FormatInt(tocID, 10), "download", "data"))
	if err != nil || string(after) != string(before) {
		t.Fatal("stale job touched artifact")
	}
}

func TestIntegrationClaimCombinations(t *testing.T) {
	_, pool := testDB(t)
	client := insertClient(t, pool)
	tocID, jobID := insertTOCJob(t, pool, client, storedURL("/data"))
	if _, _, err := claimDownload(context.Background(), pool, tocID+99, jobID); !jobs.IsFailure(err, jobs.FailureMissingRecord) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.toc_files SET parse_status = 'blocked', download_status = 'failed', failure_code = $2 WHERE id = $1`, tocID, jobs.FailureTOCDownload); err != nil {
		t.Fatal(err)
	}
	res, _, err := claimDownload(context.Background(), pool, tocID, jobID)
	if err != nil || res.Action != jobs.ClaimNoop {
		t.Fatalf("failed %+v %v", res, err)
	}
}

func TestIntegrationWorkQueuesAndConcurrency(t *testing.T) {
	_, pool := testDB(t)
	var current, max atomic.Int64
	var gate sync.WaitGroup
	gate.Add(1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := current.Add(1)
		for {
			old := max.Load()
			if n <= old || max.CompareAndSwap(old, n) {
				break
			}
		}
		gate.Wait()
		current.Add(-1)
		_, _ = w.Write([]byte("x"))
	}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	client := insertClient(t, pool)
	var ids []int64
	for i := 0; i < 6; i++ {
		id, _ := insertTOCJob(t, pool, client, storedURL("/data/"+strconv.Itoa(i)))
		ids = append(ids, id)
	}
	startDownloadRuntime(t, pool, hookDownloader(ws, ts), 8)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if max.Load() >= 4 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if max.Load() > 4 {
		t.Fatalf("concurrency %d", max.Load())
	}
	if max.Load() < 4 {
		gate.Done()
		t.Fatalf("did not reach 4 workers: %d", max.Load())
	}
	gate.Done()
	for _, id := range ids {
		waitDownload(t, pool, id, jobs.StatusSucceeded)
	}
}
