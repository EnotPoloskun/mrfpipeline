package mrfdownload

import (
	"bytes"
	"context"
	"errors"
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
	cfg.MaxConns = database.MRFMaxConns
	database.TestDBMu.Lock()
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
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

var urlSeq atomic.Int64

func storedURL(path string) string {
	if path == "" {
		path = "/data/" + strconv.FormatInt(urlSeq.Add(1), 10)
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

func insertSourceJob(t *testing.T, pool *pgxpool.Pool, client *river.Client[pgx.Tx], sourceURL string) (sourceID, jobID int64) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('uhc', DATE '2026-08-01'), ('aetna', DATE '2026-08-01')
ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ($1, DATE '2026-08-01', 'pending', 'blocked')
RETURNING id`, sourceURL).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.mrf_snapshots
    (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, 'uhc', DATE '2026-08-01', 'blocked')`, sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.monthly_release_mrf_sources
    (payer_id, collection_month, mrf_source_id)
VALUES ('uhc', DATE '2026-08-01', $1)
ON CONFLICT DO NOTHING`, sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.mrf_materialization_slots (mrf_source_id)
VALUES ($1) ON CONFLICT DO NOTHING`, sourceID); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	jobID, err = jobs.InsertTx(context.Background(), client, tx, &jobs.MRFDownloadArgs{MRFSourceID: sourceID})
	if err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `
UPDATE mrfpipeline.mrf_sources SET download_river_job_id = $2, updated_at = transaction_timestamp()
WHERE id = $1`, sourceID, jobID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	return sourceID, jobID
}

func insertBlockedSnapshot(t *testing.T, pool *pgxpool.Pool, sourceID int64) int64 {
	t.Helper()
	var snapID int64
	err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, 'uhc', DATE '2026-08-01', 'blocked')
ON CONFLICT ON CONSTRAINT mrf_snapshots_source_payer_month_key DO NOTHING
RETURNING id`, sourceID).Scan(&snapID)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := pool.QueryRow(context.Background(), `
	INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_status)
	VALUES ($1, 'aetna', DATE '2026-08-01', 'blocked')
	ON CONFLICT ON CONSTRAINT mrf_snapshots_source_payer_month_key DO NOTHING
	RETURNING id`, sourceID).Scan(&snapID); err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				t.Fatal(err)
			}
			if err := pool.QueryRow(context.Background(), `
		SELECT id FROM mrfpipeline.mrf_snapshots
		WHERE mrf_source_id = $1 AND payer_id = 'aetna' AND collection_month = DATE '2026-08-01'`, sourceID).Scan(&snapID); err != nil {
				t.Fatal(err)
			}
		}
		return snapID
	}
	if err != nil {
		t.Fatal(err)
	}
	return snapID
}

func waitDownload(t *testing.T, pool *pgxpool.Pool, sourceID int64, want string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		err := pool.QueryRow(context.Background(), `SELECT download_status FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for source %d download %s", sourceID, want)
}

func sourceRow(t *testing.T, pool *pgxpool.Pool, sourceID int64) (download, parse string, parseJob *int64, fail *string) {
	t.Helper()
	if err := pool.QueryRow(context.Background(), `
SELECT download_status, parse_status, parse_river_job_id, failure_code
FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&download, &parse, &parseJob, &fail); err != nil {
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
	cfg := jobs.ClientConfig(workers, map[string]river.QueueConfig{jobs.QueueMRFDownload: {MaxWorkers: 2}}, nil, jobs.NewLogger(io.Discard))
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
	pool := testDB(t)
	body := bytes.Repeat([]byte("m"), 1024)
	var hits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Host != "files.test" || r.URL.Path != "/shared" {
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
	sourceID, jobID := insertSourceJob(t, pool, client, storedURL("/shared"))
	insertBlockedSnapshot(t, pool, sourceID)
	insertBlockedSnapshot(t, pool, sourceID)
	startDownloadRuntime(t, pool, dl, 8)
	waitDownload(t, pool, sourceID, jobs.StatusSucceeded)
	download, parse, parseJob, fail := sourceRow(t, pool, sourceID)
	if download != jobs.StatusSucceeded || parse != jobs.StatusPending || parseJob == nil || *parseJob <= 0 || fail != nil {
		t.Fatalf("row %s %s job=%v fail=%v", download, parse, parseJob, fail)
	}
	n, err := ws.InspectDownload(artifact.KindMRF, sourceID)
	if err != nil || n != int64(len(body)) {
		t.Fatalf("inspect %d %v", n, err)
	}
	got, err := os.ReadFile(filepath.Join(ws.Root, "mrf", "mrf-source-"+strconv.FormatInt(sourceID, 10), "download", "data"))
	if err != nil || len(got) != len(body) {
		t.Fatalf("bytes %d %v", len(got), err)
	}
	var parseKind, parseState string
	if err := pool.QueryRow(context.Background(), `
SELECT kind, state FROM mrfpipeline_river.river_job WHERE id = $1`, *parseJob).Scan(&parseKind, &parseState); err != nil {
		t.Fatal(err)
	}
	if parseKind != jobs.KindMRFParse || parseState == "completed" || parseState == "discarded" {
		t.Fatalf("parse job %s %s", parseKind, parseState)
	}
	if hits.Load() != 1 {
		t.Fatalf("hits %d", hits.Load())
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFParse) != 1 {
		t.Fatal("parse jobs")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_snapshots WHERE consume_status = 'blocked'`) != 2 {
		t.Fatal("snapshots")
	}

	if err := jobs.Run(context.Background(), jobs.RunParams{
		Pool:        pool,
		Client:      client,
		Spec:        jobs.MRFDownloadStage,
		DomainID:    sourceID,
		RiverJobID:  jobID,
		Attempt:     1,
		MaxAttempts: 8,
		Claim: func(ctx context.Context) (jobs.ClaimResult, error) {
			res, _, err := claimDownload(ctx, pool, sourceID, jobID)
			return res, err
		},
		Work: func(context.Context) error {
			t.Fatal("succeeded claim must not download")
			return nil
		},
		Successor: &jobs.Successor{
			Spec:     jobs.MRFParseStage,
			DomainID: sourceID,
			Args:     &jobs.MRFParseArgs{MRFSourceID: sourceID},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 || count(t, pool, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFParse) != 1 {
		t.Fatal("redelivery")
	}
}

func TestIntegrationRenameBeforeCommitReuses(t *testing.T) {
	pool := testDB(t)
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
	url := storedURL("")
	sourceID, jobID := insertSourceJob(t, pool, client, url)
	if _, err := dl.Download(context.Background(), artifact.KindMRF, sourceID, url, artifact.ProgressID{JobID: jobID, Kind: jobs.KindMRFDownload, Queue: jobs.QueueMRFDownload}); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatalf("prep hits %d", hits.Load())
	}
	startDownloadRuntime(t, pool, dl, 8)
	waitDownload(t, pool, sourceID, jobs.StatusSucceeded)
	if hits.Load() != 1 {
		t.Fatalf("reused download hit http %d", hits.Load())
	}
}

func TestIntegrationSuccessRollbackRetainsDownload(t *testing.T) {
	pool := testDB(t)
	var hits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("published"))
	}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	dl := hookDownloader(ws, ts)
	client := insertClient(t, pool)
	sourceID, jobID := insertSourceJob(t, pool, client, storedURL(""))
	res, url, err := claimDownload(context.Background(), pool, sourceID, jobID)
	if err != nil || res.Action != jobs.ClaimWork || url == "" {
		t.Fatalf("claim %+v %q %v", res, url, err)
	}
	if err := (&Worker{Downloader: dl}).download(context.Background(), downloadJob(jobID, sourceID), url); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatal("hits")
	}
	once := true
	if err := jobs.Succeed(context.Background(), pool, client, jobs.MRFDownloadStage, sourceID, jobID, &jobs.Successor{
		Spec: jobs.MRFParseStage, DomainID: sourceID, Args: &jobs.MRFParseArgs{MRFSourceID: sourceID},
	}, func(context.Context, pgx.Tx) error {
		if once {
			once = false
			return errors.New("rollback")
		}
		return nil
	}, nil); err == nil {
		t.Fatal("expected rollback")
	}
	download, parse, parseJob, _ := sourceRow(t, pool, sourceID)
	if download == jobs.StatusSucceeded || parse != jobs.StatusBlocked || parseJob != nil {
		t.Fatalf("after rollback %s %s job=%v", download, parse, parseJob)
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFParse) != 0 {
		t.Fatal("visible parse")
	}
	if _, err := ws.InspectDownload(artifact.KindMRF, sourceID); err != nil {
		t.Fatal(err)
	}
	startDownloadRuntime(t, pool, dl, 8)
	waitDownload(t, pool, sourceID, jobs.StatusSucceeded)
	if hits.Load() != 1 {
		t.Fatalf("retried http %d", hits.Load())
	}
	_, parse, parseJob, _ = sourceRow(t, pool, sourceID)
	if parse != jobs.StatusPending || parseJob == nil {
		t.Fatal("parse")
	}
}

func TestIntegrationMidStreamRestart(t *testing.T) {
	pool := testDB(t)
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
	client := insertClient(t, pool)
	sourceID, _ := insertSourceJob(t, pool, client, storedURL(""))
	startDownloadRuntime(t, pool, hookDownloader(ws, ts), 8)
	waitDownload(t, pool, sourceID, jobs.StatusSucceeded)
	if hits.Load() < 2 {
		t.Fatalf("did not restart %d", hits.Load())
	}
}

func TestIntegrationCorruptManifestPreserved(t *testing.T) {
	pool := testDB(t)
	var hits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("fresh"))
	}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	client := insertClient(t, pool)
	sourceID, _ := insertSourceJob(t, pool, client, storedURL(""))
	name, err := artifact.RecordDirName(artifact.KindMRF, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(ws.Root, "mrf", name)
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "bogus")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(parent, "download")); err != nil {
		t.Fatal(err)
	}
	startDownloadRuntime(t, pool, hookDownloader(ws, ts), 1)
	waitDownload(t, pool, sourceID, jobs.StatusFailed)
	info, err := os.Lstat(filepath.Join(parent, "download"))
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("corrupt mutated")
	}
	if hits.Load() != 0 {
		t.Fatalf("http %d", hits.Load())
	}
}

func TestIntegrationIncompleteResetOnlyClaimed(t *testing.T) {
	pool := testDB(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("complete"))
	}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	client := insertClient(t, pool)
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('uhc', DATE '2026-08-01') ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	aID, _ := insertSourceJob(t, pool, client, storedURL(""))
	var bID int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ($1, DATE '2026-08-01', 'pending', 'blocked')
RETURNING id`, storedURL("")).Scan(&bID); err != nil {
		t.Fatal(err)
	}
	writeIncomplete(t, ws, bID)
	startDownloadRuntime(t, pool, hookDownloader(ws, ts), 8)
	waitDownload(t, pool, aID, jobs.StatusSucceeded)
	if _, err := ws.InspectDownload(artifact.KindMRF, aID); err != nil {
		t.Fatal(err)
	}
	bName, err := artifact.RecordDirName(artifact.KindMRF, bID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(ws.Root, "mrf", bName, "download", "data"))
	if err != nil || string(got) != "partial" {
		t.Fatal("other source reset")
	}
}

func TestIntegrationDifferentURLsIndependent(t *testing.T) {
	pool := testDB(t)
	var hits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("same-body"))
	}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	client := insertClient(t, pool)
	aID, _ := insertSourceJob(t, pool, client, storedURL("/one"))
	bID, _ := insertSourceJob(t, pool, client, storedURL("/two"))
	startDownloadRuntime(t, pool, hookDownloader(ws, ts), 8)
	waitDownload(t, pool, aID, jobs.StatusSucceeded)
	waitDownload(t, pool, bID, jobs.StatusSucceeded)
	if hits.Load() != 2 {
		t.Fatalf("hits %d", hits.Load())
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFParse) != 2 {
		t.Fatal("parse")
	}
}

func TestIntegrationEighthFailureLeavesSnapshots(t *testing.T) {
	pool := testDB(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	client := insertClient(t, pool)
	sourceID, _ := insertSourceJob(t, pool, client, storedURL(""))
	snapID := insertBlockedSnapshot(t, pool, sourceID)
	startDownloadRuntime(t, pool, hookDownloader(ws, ts), 1)
	waitDownload(t, pool, sourceID, jobs.StatusFailed)
	download, parse, parseJob, fail := sourceRow(t, pool, sourceID)
	if download != jobs.StatusFailed || parse != jobs.StatusBlocked || parseJob != nil || fail == nil || *fail != jobs.FailureMRFDownloadNotFound {
		t.Fatalf("terminal %s %s job=%v fail=%v", download, parse, parseJob, fail)
	}
	state, err := ws.InspectDownloadState(artifact.KindMRF, sourceID)
	if err != nil || state != artifact.DownloadAbsent {
		t.Fatalf("download state %s %v", state, err)
	}
	staging, err := ws.HasDownloadStaging(artifact.KindMRF, sourceID)
	if err != nil || staging {
		t.Fatalf("staging %v %v", staging, err)
	}
	var consume string
	var snapFail *string
	if err := pool.QueryRow(context.Background(), `
SELECT consume_status, failure_code FROM mrfpipeline.mrf_snapshots WHERE id = $1`, snapID).Scan(&consume, &snapFail); err != nil {
		t.Fatal(err)
	}
	if consume != jobs.StatusBlocked || snapFail != nil {
		t.Fatalf("snapshot %s %v", consume, snapFail)
	}
}

func TestIntegrationStaleJobLeavesArtifact(t *testing.T) {
	pool := testDB(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("owned"))
	}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	client := insertClient(t, pool)
	sourceID, jobID := insertSourceJob(t, pool, client, storedURL(""))
	startDownloadRuntime(t, pool, hookDownloader(ws, ts), 8)
	waitDownload(t, pool, sourceID, jobs.StatusSucceeded)
	before, err := os.ReadFile(filepath.Join(ws.Root, "mrf", "mrf-source-"+strconv.FormatInt(sourceID, 10), "download", "data"))
	if err != nil {
		t.Fatal(err)
	}
	res, url, err := claimDownload(context.Background(), pool, sourceID, jobID+99)
	if err != nil || res.Action != jobs.ClaimNoop || url != "" {
		t.Fatalf("stale %+v %q %v", res, url, err)
	}
	after, err := os.ReadFile(filepath.Join(ws.Root, "mrf", "mrf-source-"+strconv.FormatInt(sourceID, 10), "download", "data"))
	if err != nil || string(after) != string(before) {
		t.Fatal("stale job touched artifact")
	}
}

func TestIntegrationUnadmittedClaimNormalizesStaleJob(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	var sourceID, jobID int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ('https://files.test/unadmitted', DATE '2026-08-01', 'pending', 'blocked')
RETURNING id`).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	jobID, err = jobs.InsertTx(context.Background(), client, tx, &jobs.MRFDownloadArgs{MRFSourceID: sourceID})
	if err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `
UPDATE mrfpipeline.mrf_sources SET download_river_job_id = $2 WHERE id = $1`, sourceID, jobID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, _, err := claimDownload(context.Background(), pool, sourceID, jobID)
	if err != nil || result.Action != jobs.ClaimNoop {
		t.Fatalf("claim result=%+v err=%v", result, err)
	}
	var download, parse string
	var downloadJob, parseJob *int64
	if err := pool.QueryRow(context.Background(), `
SELECT download_status, download_river_job_id, parse_status, parse_river_job_id
FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&download, &downloadJob, &parse, &parseJob); err != nil {
		t.Fatal(err)
	}
	if download != jobs.StatusBlocked || parse != jobs.StatusBlocked || downloadJob != nil || parseJob != nil {
		t.Fatalf("stale job not normalized: download=%s/%v parse=%s/%v", download, downloadJob, parse, parseJob)
	}
}

func TestIntegrationClaimCombinations(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	sourceID, jobID := insertSourceJob(t, pool, client, storedURL(""))
	if _, _, err := claimDownload(context.Background(), pool, sourceID+99, jobID); !jobs.IsFailure(err, jobs.FailureMissingRecord) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.mrf_sources SET parse_status = 'blocked', download_status = 'failed', failure_code = $2 WHERE id = $1`, sourceID, jobs.FailureMRFDownload); err != nil {
		t.Fatal(err)
	}
	res, url, err := claimDownload(context.Background(), pool, sourceID, jobID)
	if err != nil || res.Action != jobs.ClaimNoop || url != "" {
		t.Fatalf("failed %+v %q %v", res, url, err)
	}
}

func TestIntegrationQueueConcurrencyTwo(t *testing.T) {
	pool := testDB(t)
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
	for i := 0; i < 4; i++ {
		id, _ := insertSourceJob(t, pool, client, storedURL("/c/"+strconv.Itoa(i)))
		ids = append(ids, id)
	}
	startDownloadRuntime(t, pool, hookDownloader(ws, ts), 8)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if max.Load() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if max.Load() > 2 {
		t.Fatalf("concurrency %d", max.Load())
	}
	if max.Load() < 2 {
		gate.Done()
		t.Fatalf("did not reach 2 workers: %d", max.Load())
	}
	gate.Done()
	for _, id := range ids {
		waitDownload(t, pool, id, jobs.StatusSucceeded)
	}
}

func count(t *testing.T, pool *pgxpool.Pool, q string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func writeIncomplete(t *testing.T, ws *artifact.Workspace, id int64) {
	t.Helper()
	name, err := artifact.RecordDirName(artifact.KindMRF, id)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(ws.Root, "mrf", name, "download")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "data"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
}
