package mrfparse

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EnotPoloskun/mrfparser"
	"github.com/enotpoloskun/mrfpipeline/internal/admission"
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
	for _, schema := range []string{"mrfweb", database.RiverSchema, database.ApplicationSchema, "mrfpipeline_test"} {
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			return err
		}
	}
	return nil
}

type immediateRetry struct{}

func (immediateRetry) NextRetry(*rivertype.JobRow) time.Time { return time.Now() }

var sourceURLSeq atomic.Int64

func insertClient(t *testing.T, pool *pgxpool.Pool) *river.Client[pgx.Tx] {
	t.Helper()
	client, err := jobs.NewInsertClient(context.Background(), pool, jobs.NewLogger(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func insertParseJob(t *testing.T, pool *pgxpool.Pool, client *river.Client[pgx.Tx]) (sourceID, jobID int64) {
	t.Helper()
	url := "https://files.test/mrf/" + strconv.FormatInt(sourceURLSeq.Add(1), 10)
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('uhc', DATE '2026-08-01') ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ($1, DATE '2026-08-01', 'succeeded', 'pending')
RETURNING id`, url).Scan(&sourceID); err != nil {
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
	jobID, err = jobs.InsertTx(context.Background(), client, tx, &jobs.MRFParseArgs{MRFSourceID: sourceID})
	if err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `
UPDATE mrfpipeline.mrf_sources SET parse_river_job_id = $2, updated_at = transaction_timestamp()
WHERE id = $1`, sourceID, jobID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	return sourceID, jobID
}

func insertBlockedSnapshot(t *testing.T, pool *pgxpool.Pool, sourceID int64, month string) int64 {
	return insertBlockedSnapshotForPayer(t, pool, sourceID, "uhc", month)
}

func insertBlockedSnapshotForPayer(t *testing.T, pool *pgxpool.Pool, sourceID int64, payer, month string) int64 {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ($1, $2::date) ON CONFLICT DO NOTHING`, payer, month); err != nil {
		t.Fatal(err)
	}
	var snapID int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, $2, $3::date, 'blocked')
RETURNING id`, sourceID, payer, month).Scan(&snapID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.monthly_release_mrf_sources
    (payer_id, collection_month, mrf_source_id)
VALUES ($1, $2::date, $3)
ON CONFLICT DO NOTHING`, payer, month, sourceID); err != nil {
		t.Fatal(err)
	}
	return snapID
}

func startParseRuntime(t *testing.T, pool *pgxpool.Pool, ws *artifact.Workspace, parse ParseFunc, svc ServicesID, maxAttempts int) *river.Client[pgx.Tx] {
	t.Helper()
	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{
		Pool: pool, Workspace: ws, Parse: parse, Services: svc, Logger: jobs.NewLogger(io.Discard),
	})
	cfg := jobs.ClientConfig(workers, map[string]river.QueueConfig{jobs.QueueMRFParse: {MaxWorkers: 1}}, nil, jobs.NewLogger(io.Discard))
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

func waitParse(t *testing.T, pool *pgxpool.Pool, sourceID int64, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		err := pool.QueryRow(context.Background(), `SELECT parse_status FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for source %d parse %s", sourceID, want)
}

func waitDownload(t *testing.T, pool *pgxpool.Pool, sourceID int64, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
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

func waitRiverJob(t *testing.T, pool *pgxpool.Pool, jobID int64, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var state string
		err := pool.QueryRow(context.Background(), `SELECT state FROM mrfpipeline_river.river_job WHERE id = $1`, jobID).Scan(&state)
		if err == nil && state == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for river job %d %s", jobID, want)
}

func TestIntegrationSuccessUnblocksSnapshots(t *testing.T) {
	pool := testDB(t)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	svc := mustServices(t)
	client := insertClient(t, pool)
	sourceID, _ := insertParseJob(t, pool, client)
	insertBlockedSnapshot(t, pool, sourceID, "2026-08-01")
	insertBlockedSnapshotForPayer(t, pool, sourceID, "aetna", "2026-08-01")
	writeDownload(t, ws, sourceID, testdata(t, "mrf.json"))
	startParseRuntime(t, pool, ws, func(ctx context.Context, cfg mrfparser.Config) error {
		input, _, _ := generatedPaths(ws, sourceID)
		writeValidParsed(t, cfg.Output, expectedSourceURI(input), svc.Path)
		return nil
	}, svc, 8)
	waitParse(t, pool, sourceID, jobs.StatusSucceeded)
	var pending, blocked, ingests int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.mrf_snapshots WHERE consume_status = 'pending'`).Scan(&pending); err != nil || pending != 2 {
		t.Fatalf("pending %d %v", pending, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.mrf_snapshots WHERE consume_status = 'blocked'`).Scan(&blocked); err != nil || blocked != 0 {
		t.Fatalf("blocked %d %v", blocked, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindConsumerIngest).Scan(&ingests); err != nil || ingests != 2 {
		t.Fatalf("ingests %d %v", ingests, err)
	}
	if _, err := os.Lstat(filepath.Join(ws.Root, "mrf", "mrf-source-"+strconv.FormatInt(sourceID, 10), "download")); !os.IsNotExist(err) {
		t.Fatal("download remained")
	}
	var held int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline.mrf_materialization_slots
WHERE mrf_source_id = $1`, sourceID).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != 0 {
		t.Fatalf("success retained slot %d", held)
	}
}

func TestIntegrationSuccessRollbackLeavesBlocked(t *testing.T) {
	pool := testDB(t)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	svc := mustServices(t)
	client := insertClient(t, pool)
	sourceID, jobID := insertParseJob(t, pool, client)
	insertBlockedSnapshot(t, pool, sourceID, "2026-08-01")
	writeDownload(t, ws, sourceID, []byte("x"))
	w := &Worker{Workspace: ws, Services: svc, Parse: func(ctx context.Context, cfg mrfparser.Config) error {
		input, _, _ := generatedPaths(ws, sourceID)
		writeValidParsed(t, cfg.Output, expectedSourceURI(input), svc.Path)
		return nil
	}}
	res, err := claimParse(context.Background(), pool, sourceID, jobID)
	if err != nil || res.Action != jobs.ClaimWork {
		t.Fatalf("claim %+v %v", res, err)
	}
	if err := w.parse(context.Background(), parseJob(jobID, sourceID)); err != nil {
		t.Fatal(err)
	}
	once := true
	if err := jobs.Succeed(context.Background(), pool, client, jobs.MRFParseStage, sourceID, jobID, nil, func(context.Context, pgx.Tx) error {
		if once {
			once = false
			return errors.New("rollback")
		}
		return nil
	}, nil); err == nil {
		t.Fatal("expected rollback")
	}
	var parse string
	var consume string
	if err := pool.QueryRow(context.Background(), `SELECT parse_status FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&parse); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT consume_status FROM mrfpipeline.mrf_snapshots LIMIT 1`).Scan(&consume); err != nil {
		t.Fatal(err)
	}
	if parse == jobs.StatusSucceeded || consume != jobs.StatusBlocked {
		t.Fatalf("after rollback parse=%s consume=%s", parse, consume)
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindConsumerIngest).Scan(&n); err != nil || n != 0 {
		t.Fatalf("visible ingest %d", n)
	}
}

func TestIntegrationFourthFailureReleasesTerminalOccupancy(t *testing.T) {
	pool := testDB(t)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	svc := mustServices(t)
	client := insertClient(t, pool)
	sourceID, parseJobID := insertParseJob(t, pool, client)
	snapID := insertBlockedSnapshot(t, pool, sourceID, "2026-08-01")
	var waitingID int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ($1, DATE '2026-08-01', 'blocked', 'blocked')
RETURNING id`, "https://files.test/mrf/waiting-"+strconv.FormatInt(sourceURLSeq.Add(1), 10)).Scan(&waitingID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.monthly_release_mrf_sources
    (payer_id, collection_month, mrf_source_id)
VALUES ('uhc', DATE '2026-08-01', $1)`, waitingID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.pipeline_runtime (artifact_root, resident_capacity)
VALUES ($1, 1)`, ws.Root); err != nil {
		t.Fatal(err)
	}
	writeDownload(t, ws, sourceID, []byte("x"))
	startParseRuntime(t, pool, ws, func(context.Context, mrfparser.Config) error {
		return errors.New("hostile parser text /tmp/mrf-source-9")
	}, svc, 8)
	waitParse(t, pool, sourceID, jobs.StatusFailed)
	waitDownload(t, pool, sourceID, jobs.StatusBlocked)
	waitRiverJob(t, pool, parseJobID, "cancelled")

	var fail, download, parse string
	var downloadJob, parseJob *int64
	if err := pool.QueryRow(context.Background(), `
SELECT download_status, download_river_job_id, parse_status, parse_river_job_id
FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&download, &downloadJob, &parse, &parseJob); err != nil {
		t.Fatal(err)
	}
	if download != jobs.StatusBlocked || downloadJob != nil || parse != jobs.StatusFailed || parseJob == nil {
		t.Fatalf("terminal state download=%s download_job=%v parse=%s parse_job=%v", download, downloadJob, parse, parseJob)
	}
	if err := pool.QueryRow(context.Background(), `
SELECT failure_code FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&fail); err != nil {
		t.Fatal(err)
	}
	if fail != jobs.FailureMRFParseExecutionFailed {
		t.Fatalf("failure code %s", fail)
	}
	var consume string
	if err := pool.QueryRow(context.Background(), `
SELECT consume_status FROM mrfpipeline.mrf_snapshots WHERE id = $1`, snapID).Scan(&consume); err != nil {
		t.Fatal(err)
	}
	if consume != jobs.StatusBlocked {
		t.Fatalf("snapshot %s", consume)
	}
	var attempt, maxAttempts int
	var riverState string
	if err := pool.QueryRow(context.Background(), `
SELECT attempt, max_attempts, state
FROM mrfpipeline_river.river_job WHERE id = $1`, parseJobID).Scan(&attempt, &maxAttempts, &riverState); err != nil {
		t.Fatal(err)
	}
	if attempt != 4 || maxAttempts != 4 || riverState != "cancelled" {
		t.Fatalf("river attempt=%d max_attempts=%d state=%s", attempt, maxAttempts, riverState)
	}
	raw, err := ws.InspectDownloadState(artifact.KindMRF, sourceID)
	if err != nil || raw != artifact.DownloadAbsent {
		t.Fatalf("raw state %s %v", raw, err)
	}
	parserTemp, err := ws.HasMRFParserTemp(sourceID)
	if err != nil || parserTemp {
		t.Fatalf("parser temp %v %v", parserTemp, err)
	}
	parsed, err := ws.InspectParsed(artifact.KindMRF, sourceID)
	if err != nil || parsed != artifact.ParsedAbsent {
		t.Fatalf("parsed state %s %v", parsed, err)
	}
	var held int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline.mrf_materialization_slots`).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != 0 {
		t.Fatalf("held slots %d", held)
	}
	if err := admission.ScheduleWaiting(context.Background(), pool, client); err != nil {
		t.Fatal(err)
	}
	var admitted bool
	if err := pool.QueryRow(context.Background(), `
SELECT EXISTS (
    SELECT 1 FROM mrfpipeline.mrf_materialization_slots WHERE mrf_source_id = $1
)`, waitingID).Scan(&admitted); err != nil {
		t.Fatal(err)
	}
	if !admitted {
		t.Fatal("waiting source was not admitted")
	}
}

func TestIntegrationFirstThreeParseFailuresRetainOccupancy(t *testing.T) {
	pool := testDB(t)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	svc := mustServices(t)
	client := insertClient(t, pool)
	sourceID, _ := insertParseJob(t, pool, client)
	insertBlockedSnapshot(t, pool, sourceID, "2026-08-01")
	writeDownload(t, ws, sourceID, []byte("x"))
	started := make(chan int, 4)
	completed := make(chan int, 4)
	release := make(chan struct{}, 4)
	var calls atomic.Int32
	startParseRuntime(t, pool, ws, func(context.Context, mrfparser.Config) error {
		n := int(calls.Add(1))
		started <- n
		<-release
		completed <- n
		return errors.New("parse attempt failed")
	}, svc, 8)
	for want := 1; want <= 3; want++ {
		select {
		case got := <-started:
			if got != want {
				t.Fatalf("attempt order got %d want %d", got, want)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("timed out waiting for attempt %d", want)
		}
		release <- struct{}{}
		select {
		case got := <-completed:
			if got != want {
				t.Fatalf("completed attempt %d want %d", got, want)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("timed out completing attempt %d", want)
		}
		var downloadStatus, parseStatus string
		if err := pool.QueryRow(context.Background(), `
SELECT download_status, parse_status
FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&downloadStatus, &parseStatus); err != nil {
			t.Fatal(err)
		}
		if downloadStatus != jobs.StatusSucceeded || parseStatus == jobs.StatusFailed {
			t.Fatalf("attempt %d statuses download=%s parse=%s", want, downloadStatus, parseStatus)
		}
		var held int
		if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline.mrf_materialization_slots
WHERE mrf_source_id = $1`, sourceID).Scan(&held); err != nil {
			t.Fatal(err)
		}
		if held != 1 {
			t.Fatalf("attempt %d slot count %d", want, held)
		}
		state, err := ws.InspectDownloadState(artifact.KindMRF, sourceID)
		if err != nil || state != artifact.DownloadComplete {
			t.Fatalf("attempt %d raw state %s %v", want, state, err)
		}
		var downloads int
		if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`,
			jobs.KindMRFDownload).Scan(&downloads); err != nil {
			t.Fatal(err)
		}
		if downloads != 0 {
			t.Fatalf("attempt %d inserted %d download jobs", want, downloads)
		}
	}
	select {
	case got := <-started:
		if got != 4 {
			t.Fatalf("attempt order got %d want 4", got)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for attempt 4")
	}
	release <- struct{}{}
	select {
	case got := <-completed:
		if got != 4 {
			t.Fatalf("completed attempt %d want 4", got)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out completing attempt 4")
	}
	waitParse(t, pool, sourceID, jobs.StatusFailed)
}

func TestIntegrationTerminalParseCleanupFailureRetainsSlot(t *testing.T) {
	pool := testDB(t)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	svc := mustServices(t)
	client := insertClient(t, pool)
	sourceID, _ := insertParseJob(t, pool, client)
	insertBlockedSnapshot(t, pool, sourceID, "2026-08-01")
	var waitingID int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ($1, DATE '2026-08-01', 'blocked', 'blocked')
RETURNING id`, "https://files.test/mrf/cleanup-waiting-"+strconv.FormatInt(sourceURLSeq.Add(1), 10)).Scan(&waitingID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.monthly_release_mrf_sources
    (payer_id, collection_month, mrf_source_id)
VALUES ('uhc', DATE '2026-08-01', $1)`, waitingID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.pipeline_runtime (artifact_root, resident_capacity)
VALUES ($1, 1)`, ws.Root); err != nil {
		t.Fatal(err)
	}
	writeDownload(t, ws, sourceID, []byte("x"))
	staging := filepath.Join(ws.StagingDir(), "mrf-download-"+strconv.FormatInt(sourceID, 10)+"-old")
	if err := os.Mkdir(staging, 0700); err != nil {
		t.Fatal(err)
	}
	startParseRuntime(t, pool, ws, func(context.Context, mrfparser.Config) error {
		return errors.New("hostile parser text /tmp/mrf-source-9")
	}, svc, 8)
	waitParse(t, pool, sourceID, jobs.StatusFailed)
	var parse, failure, download string
	if err := pool.QueryRow(context.Background(), `
SELECT parse_status, failure_code, download_status
FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&parse, &failure, &download); err != nil {
		t.Fatal(err)
	}
	if parse != jobs.StatusFailed || failure != jobs.FailureMRFParseExecutionFailed || download != jobs.StatusSucceeded {
		t.Fatalf("state parse=%s failure=%s download=%s", parse, failure, download)
	}
	var held int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline.mrf_materialization_slots
WHERE mrf_source_id = $1`, sourceID).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != 1 {
		t.Fatalf("slot count %d", held)
	}
	hasStaging, err := ws.HasDownloadStaging(artifact.KindMRF, sourceID)
	if err != nil || !hasStaging {
		t.Fatalf("download staging %v %v", hasStaging, err)
	}
	if err := admission.ScheduleWaiting(context.Background(), pool, client); err != nil {
		t.Fatal(err)
	}
	var admitted bool
	if err := pool.QueryRow(context.Background(), `
SELECT EXISTS (
    SELECT 1 FROM mrfpipeline.mrf_materialization_slots WHERE mrf_source_id = $1
)`, waitingID).Scan(&admitted); err != nil {
		t.Fatal(err)
	}
	if admitted {
		t.Fatal("waiting source admitted while terminal cleanup retained slot")
	}
}

func TestIntegrationNoSnapshotDoesNotClaim(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	sourceID, jobID := insertParseJob(t, pool, client)
	result, err := claimParse(context.Background(), pool, sourceID, jobID)
	if !jobs.IsFailure(err, jobs.FailureMissingRecord) {
		t.Fatalf("claim result=%+v error=%v", result, err)
	}
	if result.Action != "" {
		t.Fatalf("claim action %q", result.Action)
	}
	var status string
	if err := pool.QueryRow(context.Background(), `
SELECT parse_status FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != jobs.StatusPending {
		t.Fatalf("parse status %q", status)
	}
}

func TestIntegrationSuspiciousSnapshotInvariant(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	sourceID, jobID := insertParseJob(t, pool, client)
	snapID := insertBlockedSnapshot(t, pool, sourceID, "2026-08-01")
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.mrf_snapshots SET consume_river_job_id = 99 WHERE id = $1`, snapID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.mrf_sources SET parse_status = 'running' WHERE id = $1`, sourceID); err != nil {
		t.Fatal(err)
	}
	err := jobs.Succeed(context.Background(), pool, client, jobs.MRFParseStage, sourceID, jobID, nil, func(ctx context.Context, tx pgx.Tx) error {
		return confirmParseSuccess(ctx, tx, client, sourceID)
	}, nil)
	if !jobs.IsFailure(err, jobs.FailureDomainInvariant) {
		t.Fatalf("got %v", err)
	}
}
