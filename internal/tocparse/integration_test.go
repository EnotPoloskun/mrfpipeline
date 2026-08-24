package tocparse

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EnotPoloskun/mrftocparser"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
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

type immediateRetry struct{}

func (immediateRetry) NextRetry(*rivertype.JobRow) time.Time { return time.Now() }

func insertClient(t *testing.T, pool *pgxpool.Pool) *river.Client[pgx.Tx] {
	t.Helper()
	client, err := jobs.NewInsertClient(context.Background(), pool, jobs.NewLogger(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

var tocURLSeq atomic.Int64

func insertParseJob(t *testing.T, pool *pgxpool.Pool, client *river.Client[pgx.Tx]) (tocID, jobID int64) {
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
	sourceURL := "http://files.test/data/" + strconv.FormatInt(tocURLSeq.Add(1), 10)
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id,
    download_status, parse_status, import_status
) VALUES ('uhc', DATE '2026-08-01', $1, $2, 'succeeded', 'pending', 'blocked')
RETURNING id`, sourceURL, runID).Scan(&tocID); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	jobID, err = jobs.InsertTx(context.Background(), client, tx, &jobs.TOCParseArgs{TOCFileID: tocID})
	if err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `
UPDATE mrfpipeline.toc_files SET parse_river_job_id = $2, updated_at = transaction_timestamp()
WHERE id = $1`, tocID, jobID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	return tocID, jobID
}

func useStagingTMPDIR(t *testing.T, ws *artifact.Workspace) {
	t.Helper()
	t.Setenv("TMPDIR", ws.StagingDir())
}

func startParseRuntime(t *testing.T, pool *pgxpool.Pool, ws *artifact.Workspace, parse ParseFunc, maxAttempts int) *river.Client[pgx.Tx] {
	t.Helper()
	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{
		Pool: pool, Workspace: ws, Parse: parse, Logger: jobs.NewLogger(io.Discard),
	})
	cfg := jobs.ClientConfig(workers, map[string]river.QueueConfig{jobs.QueueTOCParse: {MaxWorkers: 2}}, nil, jobs.NewLogger(io.Discard))
	cfg.MaxAttempts = maxAttempts
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
	return client
}

func waitParse(t *testing.T, pool *pgxpool.Pool, tocID int64, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		err := pool.QueryRow(context.Background(), `SELECT parse_status FROM mrfpipeline.toc_files WHERE id = $1`, tocID).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for toc %d parse %s", tocID, want)
}

func tocRow(t *testing.T, pool *pgxpool.Pool, tocID int64) (download, parse, imp string, importJob *int64, fail *string) {
	t.Helper()
	if err := pool.QueryRow(context.Background(), `
SELECT download_status, parse_status, import_status, import_river_job_id, failure_code
FROM mrfpipeline.toc_files WHERE id = $1`, tocID).Scan(&download, &parse, &imp, &importJob, &fail); err != nil {
		t.Fatal(err)
	}
	return download, parse, imp, importJob, fail
}

func gzipBytes(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestIntegrationJSONAndGzipParse(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	useStagingTMPDIR(t, ws)
	jsonID, _ := insertParseJob(t, pool, client)
	writeDownload(t, ws, jsonID, testdata(t, "toc.json"))
	gzID, _ := insertParseJob(t, pool, client)
	writeDownload(t, ws, gzID, gzipBytes(t, testdata(t, "toc.json")))
	startParseRuntime(t, pool, ws, nil, 8)
	waitParse(t, pool, jsonID, jobs.StatusSucceeded)
	waitParse(t, pool, gzID, jobs.StatusSucceeded)
	for _, id := range []int64{jsonID, gzID} {
		download, parse, imp, importJob, fail := tocRow(t, pool, id)
		if download != jobs.StatusSucceeded || parse != jobs.StatusSucceeded || imp != jobs.StatusPending || importJob == nil || fail != nil {
			t.Fatalf("row %d %s %s %s job=%v fail=%v", id, download, parse, imp, importJob, fail)
		}
		if _, err := os.Lstat(filepath.Join(ws.Root, "toc", "toc-"+strconv.FormatInt(id, 10), "download")); !os.IsNotExist(err) {
			t.Fatal("download remained")
		}
		var kind, state string
		if err := pool.QueryRow(context.Background(), `
SELECT kind, state FROM mrfpipeline_river.river_job WHERE id = $1`, *importJob).Scan(&kind, &state); err != nil {
			t.Fatal(err)
		}
		if kind != jobs.KindTOCImport || state == "completed" || state == "discarded" {
			t.Fatalf("import consumed %s %s", kind, state)
		}
	}
}

func TestIntegrationEmptyAssociationsStillImport(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	useStagingTMPDIR(t, ws)
	tocID, _ := insertParseJob(t, pool, client)
	writeDownload(t, ws, tocID, testdata(t, "toc_empty.json"))
	startParseRuntime(t, pool, ws, nil, 8)
	waitParse(t, pool, tocID, jobs.StatusSucceeded)
	_, _, imp, importJob, _ := tocRow(t, pool, tocID)
	if imp != jobs.StatusPending || importJob == nil {
		t.Fatalf("import %s %v", imp, importJob)
	}
	output, err := ws.ParsedDir(artifact.KindTOC, tocID)
	if err != nil {
		t.Fatal(err)
	}
	m, err := validateCompletedOutput(output, "toc-"+strconv.FormatInt(tocID, 10), "uhc", "2026-08",
		mustPath(t, ws, tocID))
	if err != nil || m.Counts.MRFPlanAssociations != 0 {
		t.Fatalf("%+v %v", m, err)
	}
}

func TestIntegrationReuseAfterPublication(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	useStagingTMPDIR(t, ws)
	tocID, jobID := insertParseJob(t, pool, client)
	body := testdata(t, "toc.json")
	writeDownload(t, ws, tocID, body)
	input, output, err := generatedPaths(ws, tocID)
	if err != nil {
		t.Fatal(err)
	}
	report, err := mrftocparser.Parse(context.Background(), mrftocparser.Config{
		InputPath: input, OutputPath: output, TOCOutputID: "toc-" + strconv.FormatInt(tocID, 10),
		PayerID: "uhc", CollectionMonth: "2026-08",
	})
	if err != nil || report.Counts.TOCFiles != 1 {
		t.Fatalf("preparse %v %+v", err, report)
	}
	var calls atomic.Int64
	startParseRuntime(t, pool, ws, func(ctx context.Context, cfg mrftocparser.Config) (mrftocparser.Report, error) {
		calls.Add(1)
		return mrftocparser.Report{}, errors.New("should reuse")
	}, 8)
	waitParse(t, pool, tocID, jobs.StatusSucceeded)
	if calls.Load() != 0 {
		t.Fatalf("reparsed %d", calls.Load())
	}
	if err := jobs.Run(context.Background(), jobs.RunParams{
		Pool: pool, Client: client, Spec: jobs.TOCParseStage, DomainID: tocID, RiverJobID: jobID,
		Attempt: 1, MaxAttempts: 8,
		Claim: func(ctx context.Context) (jobs.ClaimResult, error) {
			res, _, _, err := claimParse(ctx, pool, tocID, jobID)
			return res, err
		},
		Work: func(context.Context) error {
			t.Fatal("succeeded claim must not parse")
			return nil
		},
		Successor: &jobs.Successor{Spec: jobs.TOCImportStage, DomainID: tocID, Args: &jobs.TOCImportArgs{TOCFileID: tocID}},
	}); err != nil {
		t.Fatal(err)
	}
	var importCount int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindTOCImport).Scan(&importCount); err != nil || importCount != 1 {
		t.Fatalf("import jobs %d %v", importCount, err)
	}
}

func TestIntegrationSuccessRollbackRetainsOutput(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	tocID, jobID := insertParseJob(t, pool, client)
	input, output, err := generatedPaths(ws, tocID)
	if err != nil {
		t.Fatal(err)
	}
	writeValidParsed(t, output, "toc-"+strconv.FormatInt(tocID, 10), "uhc", "2026-08", input, 0)
	if _, err := os.Lstat(filepath.Join(ws.Root, "toc", "toc-"+strconv.FormatInt(tocID, 10), "download")); !os.IsNotExist(err) {
		t.Fatal("download should be absent")
	}
	var parses atomic.Int64
	w := &Worker{
		Workspace: ws,
		Parse: func(context.Context, mrftocparser.Config) (mrftocparser.Report, error) {
			parses.Add(1)
			return mrftocparser.Report{}, errors.New("should not parse")
		},
	}
	res, payer, month, err := claimParse(context.Background(), pool, tocID, jobID)
	if err != nil || res.Action != jobs.ClaimWork {
		t.Fatalf("claim %+v %v", res, err)
	}
	if err := w.parse(context.Background(), parseJob(jobID, tocID), payer, month); err != nil {
		t.Fatal(err)
	}
	succ := &jobs.Successor{Spec: jobs.TOCImportStage, DomainID: tocID, Args: &jobs.TOCImportArgs{TOCFileID: tocID}}
	err = jobs.Succeed(context.Background(), pool, client, jobs.TOCParseStage, tocID, jobID, succ, func(context.Context, pgx.Tx) error {
		return errors.New("forced success rollback")
	}, nil)
	if err == nil {
		t.Fatal("succeed should roll back")
	}
	_, parse, imp, importJob, _ := tocRow(t, pool, tocID)
	if parse == jobs.StatusSucceeded || imp != jobs.StatusBlocked || importJob != nil {
		t.Fatalf("after rollback parse=%s import=%s job=%v", parse, imp, importJob)
	}
	var importCount int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindTOCImport).Scan(&importCount); err != nil || importCount != 0 {
		t.Fatalf("visible import %d %v", importCount, err)
	}
	if _, err := os.Lstat(filepath.Join(output, fileManifest)); err != nil {
		t.Fatal("parsed output lost")
	}
	if err := jobs.Run(context.Background(), jobs.RunParams{
		Pool: pool, Client: client, Spec: jobs.TOCParseStage, DomainID: tocID, RiverJobID: jobID,
		Attempt: 1, MaxAttempts: 8,
		Claim: func(ctx context.Context) (jobs.ClaimResult, error) {
			res, p, m, err := claimParse(ctx, pool, tocID, jobID)
			payer, month = p, m
			return res, err
		},
		Work: func(ctx context.Context) error {
			return w.parse(ctx, parseJob(jobID, tocID), payer, month)
		},
		Successor: succ,
		Confirm: func(ctx context.Context, tx pgx.Tx) error {
			return confirmDownloadSucceeded(ctx, tx, tocID)
		},
	}); err != nil {
		t.Fatal(err)
	}
	if parses.Load() != 0 {
		t.Fatalf("reparsed %d", parses.Load())
	}
	download, parse, imp, importJob, fail := tocRow(t, pool, tocID)
	if download != jobs.StatusSucceeded || parse != jobs.StatusSucceeded || imp != jobs.StatusPending || importJob == nil || fail != nil {
		t.Fatalf("retry row %s %s %s job=%v fail=%v", download, parse, imp, importJob, fail)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindTOCImport).Scan(&importCount); err != nil || importCount != 1 {
		t.Fatalf("import jobs %d %v", importCount, err)
	}
	if _, err := os.Lstat(filepath.Join(ws.Root, "toc", "toc-"+strconv.FormatInt(tocID, 10), "download")); !os.IsNotExist(err) {
		t.Fatal("download recreated")
	}
}

func TestIntegrationCleanupRetryDoesNotReparse(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	useStagingTMPDIR(t, ws)
	tocID, _ := insertParseJob(t, pool, client)
	writeDownload(t, ws, tocID, testdata(t, "toc.json"))
	var parses, cleans atomic.Int64
	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{
		Pool: pool, Workspace: ws, Logger: jobs.NewLogger(io.Discard),
		Parse: func(ctx context.Context, cfg mrftocparser.Config) (mrftocparser.Report, error) {
			parses.Add(1)
			return DefaultParse(ctx, cfg)
		},
		removeDownload: func(kind string, id int64) error {
			n := cleans.Add(1)
			if n == 1 {
				return artifact.ErrArtifact
			}
			return ws.RemoveDownload(kind, id)
		},
	})
	cfg := jobs.ClientConfig(workers, map[string]river.QueueConfig{jobs.QueueTOCParse: {MaxWorkers: 2}}, nil, jobs.NewLogger(io.Discard))
	cfg.MaxAttempts = 8
	cfg.RetryPolicy = immediateRetry{}
	cfg.FetchPollInterval = 50 * time.Millisecond
	rt, err := river.NewClient(riverpgxv5.New(pool), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = jobs.Shutdown(context.Background(), rt) })
	waitParse(t, pool, tocID, jobs.StatusSucceeded)
	if parses.Load() != 1 || cleans.Load() < 2 {
		t.Fatalf("parses %d cleans %d", parses.Load(), cleans.Load())
	}
}

func TestIntegrationCorruptManifestPreserved(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	tocID, _ := insertParseJob(t, pool, client)
	writeDownload(t, ws, tocID, testdata(t, "toc.json"))
	output, err := ws.ParsedDir(artifact.KindTOC, tocID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(output, 0700); err != nil {
		t.Fatal(err)
	}
	corrupt := []byte(`{"manifest_schema_version":"9.0.0"}`)
	if err := os.WriteFile(filepath.Join(output, fileManifest), corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	var parses atomic.Int64
	startParseRuntime(t, pool, ws, func(ctx context.Context, cfg mrftocparser.Config) (mrftocparser.Report, error) {
		parses.Add(1)
		return DefaultParse(ctx, cfg)
	}, 8)
	waitParse(t, pool, tocID, jobs.StatusFailed)
	_, _, imp, _, fail := tocRow(t, pool, tocID)
	if imp != jobs.StatusBlocked || fail == nil || *fail != jobs.FailureTOCParseOutputInvalid {
		t.Fatalf("imp %s fail %v", imp, fail)
	}
	if parses.Load() != 0 {
		t.Fatalf("parsed corrupt output %d", parses.Load())
	}
	got, err := os.ReadFile(filepath.Join(output, fileManifest))
	if err != nil || !bytes.Equal(got, corrupt) {
		t.Fatal("corrupt output mutated")
	}
}

func TestIntegrationInjectedFailureThenRetry(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	useStagingTMPDIR(t, ws)
	tocID, _ := insertParseJob(t, pool, client)
	writeDownload(t, ws, tocID, testdata(t, "toc.json"))
	var n atomic.Int64
	startParseRuntime(t, pool, ws, func(ctx context.Context, cfg mrftocparser.Config) (mrftocparser.Report, error) {
		if n.Add(1) == 1 {
			if err := os.WriteFile(filepath.Join(cfg.OutputPath, "partial"), []byte("x"), 0600); err != nil {
				return mrftocparser.Report{}, err
			}
			return mrftocparser.Report{}, mrftocparser.ErrInvalidInput
		}
		return DefaultParse(ctx, cfg)
	}, 8)
	waitParse(t, pool, tocID, jobs.StatusSucceeded)
	if n.Load() < 2 {
		t.Fatalf("attempts %d", n.Load())
	}
	output, _ := ws.ParsedDir(artifact.KindTOC, tocID)
	if _, err := os.Lstat(filepath.Join(output, "partial")); !os.IsNotExist(err) {
		t.Fatal("partial survived")
	}
}

func TestIntegrationStaleJobDoesNotTouchArtifacts(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	tocID, jobID := insertParseJob(t, pool, client)
	body := testdata(t, "toc.json")
	writeDownload(t, ws, tocID, body)
	before, err := os.ReadFile(filepath.Join(ws.Root, "toc", "toc-"+strconv.FormatInt(tocID, 10), "download", "data"))
	if err != nil {
		t.Fatal(err)
	}
	res, payer, month, err := claimParse(context.Background(), pool, tocID, jobID+99)
	if err != nil || res.Action != jobs.ClaimNoop || payer != "" || month != "" {
		t.Fatalf("stale %+v %q %q %v", res, payer, month, err)
	}
	after, err := os.ReadFile(filepath.Join(ws.Root, "toc", "toc-"+strconv.FormatInt(tocID, 10), "download", "data"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("stale job touched artifact")
	}
}

func TestIntegrationQueueConcurrencyTwo(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	useStagingTMPDIR(t, ws)
	var current, max atomic.Int64
	var gate sync.WaitGroup
	gate.Add(1)
	var ids []int64
	for i := 0; i < 4; i++ {
		id, _ := insertParseJob(t, pool, client)
		writeDownload(t, ws, id, testdata(t, "toc.json"))
		ids = append(ids, id)
	}
	startParseRuntime(t, pool, ws, func(ctx context.Context, cfg mrftocparser.Config) (mrftocparser.Report, error) {
		n := current.Add(1)
		for {
			old := max.Load()
			if n <= old || max.CompareAndSwap(old, n) {
				break
			}
		}
		gate.Wait()
		current.Add(-1)
		return DefaultParse(ctx, cfg)
	}, 8)
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
		waitParse(t, pool, id, jobs.StatusSucceeded)
	}
}

func TestIntegrationMissingDownloadIsInvariant(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	tocID, jobID := insertParseJob(t, pool, client)
	res, _, _, err := claimParse(context.Background(), pool, tocID, jobID)
	if err != nil || res.Action != jobs.ClaimWork {
		t.Fatalf("claim %+v %v", res, err)
	}
	w := &Worker{Workspace: ws}
	err = w.parse(context.Background(), parseJob(jobID, tocID), "uhc", "2026-08")
	if !jobs.IsFailure(err, jobs.FailureDomainInvariant) {
		t.Fatalf("missing download: %v", err)
	}
	download, _, _, _, _ := tocRow(t, pool, tocID)
	if download != jobs.StatusSucceeded {
		t.Fatalf("download status %s", download)
	}
}

func mustPath(t *testing.T, ws *artifact.Workspace, id int64) string {
	t.Helper()
	p, _, err := generatedPaths(ws, id)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
