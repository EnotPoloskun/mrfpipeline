package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/admission"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/planbatch"
	"github.com/jackc/pgx/v5/pgxpool"
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

var urlSeq atomic.Int64

func uniqueURL(prefix string) string {
	return "https://files.test/" + prefix + "/" + strconv.FormatInt(urlSeq.Add(1), 10)
}

func workspace(t *testing.T) *artifact.Workspace {
	t.Helper()
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

func insertTerminalParseSource(t *testing.T, pool *pgxpool.Pool, download string, slot bool) int64 {
	t.Helper()
	ctx := context.Background()
	month := "2026-08-01"
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('uhc', $1) ON CONFLICT DO NOTHING`, month); err != nil {
		t.Fatal(err)
	}
	var sourceID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources
    (source_url, collection_month, download_status, parse_status, failure_code)
VALUES ($1, $2, $3, 'failed', $4)
RETURNING id`, uniqueURL("terminal-parse"), month, download, jobs.FailureMRFParseExecutionFailed).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots
    (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, 'uhc', $2, 'blocked')`, sourceID, month); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_release_mrf_sources
    (payer_id, collection_month, mrf_source_id)
VALUES ('uhc', $2, $1)`, sourceID, month); err != nil {
		t.Fatal(err)
	}
	if slot {
		if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_materialization_slots (mrf_source_id)
VALUES ($1)`, sourceID); err != nil {
			t.Fatal(err)
		}
	}
	return sourceID
}

func writeRawDownload(t *testing.T, ws *artifact.Workspace, sourceID int64) {
	t.Helper()
	data, err := ws.DownloadDataPath(artifact.KindMRF, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(data)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	body := []byte("raw")
	if err := os.WriteFile(data, body, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(`{"schema_version":"1.0.0","byte_count":3}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationMRFStagingCleanupRespectsSourceLock(t *testing.T) {
	pool := testDB(t)
	ws := workspace(t)
	entry := filepath.Join(ws.StagingDir(), "mrf-download-42-old")
	if err := os.Mkdir(entry, 0700); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(entry, old, old); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := jobs.WithExecutionLock(context.Background(), pool, jobs.LockNamespaceMRF, 42, func(context.Context) error {
			close(started)
			<-release
			return nil
		})
		done <- err
	}()
	<-started
	var report Report
	if err := cleanStaging(context.Background(), pool, ws, &report); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(entry); err != nil {
		t.Fatalf("locked staging entry was removed: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	report = Report{}
	if err := cleanStaging(context.Background(), pool, ws, &report); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(entry); !os.IsNotExist(err) {
		t.Fatalf("unlocked staging entry remained: %v", err)
	}
}

func runPass(t *testing.T, pool *pgxpool.Pool, ws *artifact.Workspace) Report {
	t.Helper()
	svc := filepath.Join(t.TempDir(), "services.csv")
	if err := os.WriteFile(svc, []byte("billing_code_type,billing_code\nCPT,99213\n"), 0600); err != nil {
		t.Fatal(err)
	}
	rep, err := Run(context.Background(), Params{
		Pool: pool, Workspace: ws, ServicesPath: svc,
		WarehousePath:       filepath.Join(t.TempDir(), "wh"),
		ProviderCatalogPath: filepath.Join(t.TempDir(), "cat"),
		Logger:              jobs.NewLogger(io.Discard),
	})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func insertDiscovery(t *testing.T, pool *pgxpool.Pool) int64 {
	return insertDiscoveryFor(t, pool, "uhc", "2026-08")
}

func insertDiscoveryFor(t *testing.T, pool *pgxpool.Pool, payer, month string) int64 {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ($1, $2)
ON CONFLICT DO NOTHING`, payer, month+"-01"); err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.discovery_runs (
    payer_id, collection_month, status, river_job_id, started_at, completed_at
) VALUES ($1, $2, 'succeeded', 1, transaction_timestamp(), transaction_timestamp())
RETURNING id`, payer, month+"-01").Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func insertTOC(t *testing.T, pool *pgxpool.Pool, download, parse, imp string) int64 {
	return insertTOCFor(t, pool, "uhc", "2026-08", download, parse, imp)
}

func insertTOCFor(t *testing.T, pool *pgxpool.Pool, payer, month, download, parse, imp string) int64 {
	t.Helper()
	runID := insertDiscoveryFor(t, pool, payer, month)
	var fail any
	if download == jobs.StatusFailed || parse == jobs.StatusFailed || imp == jobs.StatusFailed {
		fail = jobs.FailureTOCDownload
		if parse == jobs.StatusFailed {
			fail = jobs.FailureTOCParseOutputInvalid
		}
		if imp == jobs.StatusFailed {
			fail = jobs.FailureTOCImportInvariant
		}
	}
	var id int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id,
    download_status, parse_status, import_status, failure_code
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING id`, payer, month+"-01", uniqueURL("toc"), runID, download, parse, imp, fail).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestIntegrationSuccessorAndCurrentJobs(t *testing.T) {
	pool := testDB(t)
	ws := workspace(t)
	toc := insertTOC(t, pool, jobs.StatusSucceeded, jobs.StatusBlocked, jobs.StatusBlocked)
	downloadData, err := ws.DownloadDataPath(artifact.KindTOC, toc)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("toc-download")
	if err := os.MkdirAll(filepath.Dir(downloadData), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(downloadData, body, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(downloadData), "manifest.json"), []byte(fmt.Sprintf(`{"schema_version":"1.0.0","byte_count":%d}`, len(body))), 0600); err != nil {
		t.Fatal(err)
	}
	rep := runPass(t, pool, ws)
	if rep.UnblockedStageCount < 1 || rep.RepairedJobCount < 1 {
		t.Fatalf("report %+v", rep)
	}
	var parse, status string
	var jobID *int64
	if err := pool.QueryRow(context.Background(), `
SELECT parse_status, parse_river_job_id FROM mrfpipeline.toc_files WHERE id = $1`, toc).Scan(&status, &jobID); err != nil {
		t.Fatal(err)
	}
	if status != jobs.StatusPending || jobID == nil {
		t.Fatalf("parse %s %v", status, jobID)
	}
	_ = parse
	second := runPass(t, pool, ws)
	if second.RepairedJobCount != 0 || second.UnblockedStageCount != 0 || second.ScheduledPlanBatchCount != 0 || second.CleanedArtifactCount != 0 {
		t.Fatalf("second %+v", second)
	}
}

func TestIntegrationMissingCurrentJob(t *testing.T) {
	pool := testDB(t)
	ws := workspace(t)
	toc := insertTOC(t, pool, jobs.StatusPending, jobs.StatusBlocked, jobs.StatusBlocked)
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.toc_files SET download_river_job_id = NULL WHERE id = $1`, toc); err != nil {
		t.Fatal(err)
	}
	rep := runPass(t, pool, ws)
	if rep.RepairedJobCount < 1 {
		t.Fatalf("report %+v", rep)
	}
	var jobID *int64
	if err := pool.QueryRow(context.Background(), `
SELECT download_river_job_id FROM mrfpipeline.toc_files WHERE id = $1`, toc).Scan(&jobID); err != nil || jobID == nil {
		t.Fatal("missing replacement")
	}
}

func TestIntegrationSQLStaleStagesExecutesAgainstRiver(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	toc := insertTOC(t, pool, jobs.StatusPending, jobs.StatusBlocked, jobs.StatusBlocked)
	client, err := jobs.NewInsertClient(ctx, pool, jobs.NewLogger(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	jobID, err := jobs.InsertTx(ctx, client, tx, &jobs.TOCDownloadArgs{TOCFileID: toc})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline.toc_files
SET download_river_job_id = $2, updated_at = transaction_timestamp() - interval '1 hour'
WHERE id = $1`, toc, jobID); err != nil {
		t.Fatal(err)
	}
	read := func() (int, *int32, error) {
		rows, err := pool.Query(ctx, SQLStaleStages, "30 minutes")
		if err != nil {
			return 0, nil, err
		}
		defer rows.Close()
		count := 0
		var gotAttempt *int32
		for rows.Next() {
			var stage, status string
			var domainID, gotJobID, ageSeconds int64
			var attempt *int32
			if err := rows.Scan(&stage, &domainID, &status, &gotJobID, &attempt, &ageSeconds); err != nil {
				return 0, nil, err
			}
			if stage == jobs.KindTOCDownload && domainID == toc {
				count++
				gotAttempt = attempt
				if status != jobs.StatusPending || gotJobID != jobID || ageSeconds < 1800 {
					return 0, nil, fmt.Errorf("unexpected stale row %s %d %s %d %d", stage, domainID, status, gotJobID, ageSeconds)
				}
			}
		}
		if err := rows.Err(); err != nil {
			return 0, nil, err
		}
		return count, gotAttempt, nil
	}
	count, attempt, err := read()
	if err != nil || count != 1 || attempt == nil {
		t.Fatalf("valid current job stale result count=%d attempt=%v err=%v", count, attempt, err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline_river.river_job
SET args = jsonb_build_object('toc_file_id', $2::bigint, 'extra', 1)
WHERE id = $1`, jobID, toc); err != nil {
		t.Fatal(err)
	}
	count, attempt, err = read()
	if err != nil || count != 1 || attempt != nil {
		t.Fatalf("extra args were accepted count=%d attempt=%v err=%v", count, attempt, err)
	}
}

func TestIntegrationCancelledRiverFailsDomain(t *testing.T) {
	pool := testDB(t)
	ws := workspace(t)
	client, err := jobs.NewInsertClient(context.Background(), pool, jobs.NewLogger(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	toc := insertTOC(t, pool, jobs.StatusPending, jobs.StatusBlocked, jobs.StatusBlocked)
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	jobID, err := jobs.InsertTx(context.Background(), client, tx, &jobs.TOCDownloadArgs{TOCFileID: toc})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `
UPDATE mrfpipeline.toc_files SET download_river_job_id = $2 WHERE id = $1`, toc, jobID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline_river.river_job SET state = 'cancelled', finalized_at = now() WHERE id = $1`, jobID); err != nil {
		t.Fatal(err)
	}
	rep := runPass(t, pool, ws)
	if rep.RepairedJobCount != 0 {
		t.Fatalf("should not insert replacement %+v", rep)
	}
	var status, code string
	if err := pool.QueryRow(context.Background(), `
SELECT download_status, failure_code FROM mrfpipeline.toc_files WHERE id = $1`, toc).Scan(&status, &code); err != nil {
		t.Fatal(err)
	}
	if status != jobs.StatusFailed || code != jobs.FailureRiverTerminalWithoutResult {
		t.Fatalf("%s %s", status, code)
	}
}

func TestIntegrationRetryFailedStage(t *testing.T) {
	pool := testDB(t)
	toc := insertTOC(t, pool, jobs.StatusFailed, jobs.StatusBlocked, jobs.StatusBlocked)
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.toc_files SET failure_code = $2, download_river_job_id = 9 WHERE id = $1`, toc, jobs.FailureTOCDownload); err != nil {
		t.Fatal(err)
	}
	lease, err := database.AcquireWorkerLease(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}
	_ = lease.Release(context.Background())

	res, err := Retry(context.Background(), pool, jobs.KindTOCDownload, toc)
	if err != nil {
		t.Fatal(err)
	}
	if res.Stage != jobs.KindTOCDownload || res.DomainID != toc || res.RiverJobID <= 0 {
		t.Fatalf("%+v", res)
	}
	var status string
	var code *string
	var jobID int64
	if err := pool.QueryRow(context.Background(), `
SELECT download_status, failure_code, download_river_job_id FROM mrfpipeline.toc_files WHERE id = $1`, toc).Scan(&status, &code, &jobID); err != nil {
		t.Fatal(err)
	}
	if status != jobs.StatusPending || code != nil || jobID != res.RiverJobID {
		t.Fatalf("%s %v %d", status, code, jobID)
	}
	_, err = Retry(context.Background(), pool, jobs.KindTOCDownload, toc)
	if !jobs.IsFailure(err, jobs.FailureRetryStageNotFailed) {
		t.Fatalf("retry pending: %v", err)
	}
}

func TestIntegrationRetryDownloadWaitsForResidentCapacity(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	month := "2026-08-01"
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('uhc', $1)`, month); err != nil {
		t.Fatal(err)
	}
	var sourceID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources
    (source_url, collection_month, download_status, parse_status, failure_code)
VALUES ('https://files.test/capacity-retry', $1, 'failed', 'blocked', $2)
RETURNING id`, month, jobs.FailureMRFDownload).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month)
VALUES ($1, 'uhc', $2)`, sourceID, month); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_release_mrf_sources (payer_id, collection_month, mrf_source_id)
VALUES ('uhc', $2, $1)`, sourceID, month); err != nil {
		t.Fatal(err)
	}
	result, err := Retry(ctx, pool, jobs.KindMRFDownload, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	if result.RiverJobID != 0 || result.DomainID != sourceID {
		t.Fatalf("capacity retry inserted executable job: %+v", result)
	}
	var status string
	var jobID *int64
	if err := pool.QueryRow(ctx, `
SELECT download_status, download_river_job_id
FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&status, &jobID); err != nil {
		t.Fatal(err)
	}
	if status != jobs.StatusBlocked || jobID != nil {
		t.Fatalf("retry did not leave source waiting: %s %v", status, jobID)
	}
	var downloadJobs, wakeJobs, events int64
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFDownload).Scan(&downloadJobs); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindControlSchedule).Scan(&wakeJobs); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.control_schedule_events`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if downloadJobs != 0 || wakeJobs != 1 || events != 1 {
		t.Fatalf("waiting retry jobs=%d wake=%d events=%d", downloadJobs, wakeJobs, events)
	}
}

func TestIntegrationRetryParseArtifactTable(t *testing.T) {
	t.Run("valid raw with held slot inserts parse", func(t *testing.T) {
		pool := testDB(t)
		ws := workspace(t)
		sourceID := insertTerminalParseSource(t, pool, jobs.StatusSucceeded, true)
		writeRawDownload(t, ws, sourceID)
		result, err := Retry(context.Background(), pool, jobs.KindMRFParse, sourceID, ws)
		if err != nil {
			t.Fatal(err)
		}
		if result.Stage != jobs.KindMRFParse || result.RiverJobID <= 0 {
			t.Fatalf("result %+v", result)
		}
		var parseStatus string
		var parseJob int64
		if err := pool.QueryRow(context.Background(), `
SELECT parse_status, parse_river_job_id
FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&parseStatus, &parseJob); err != nil {
			t.Fatal(err)
		}
		if parseStatus != jobs.StatusPending || parseJob != result.RiverJobID {
			t.Fatalf("parse state %s %d", parseStatus, parseJob)
		}
		var maxAttempts int
		if err := pool.QueryRow(context.Background(), `
SELECT max_attempts FROM mrfpipeline_river.river_job WHERE id = $1`, result.RiverJobID).Scan(&maxAttempts); err != nil {
			t.Fatal(err)
		}
		if maxAttempts != 4 {
			t.Fatalf("max attempts %d", maxAttempts)
		}
		var downloads int
		if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFDownload).Scan(&downloads); err != nil {
			t.Fatal(err)
		}
		if downloads != 0 {
			t.Fatalf("download jobs %d", downloads)
		}
	})
	t.Run("no raw with held slot inserts download", func(t *testing.T) {
		pool := testDB(t)
		ws := workspace(t)
		sourceID := insertTerminalParseSource(t, pool, jobs.StatusBlocked, true)
		result, err := Retry(context.Background(), pool, jobs.KindMRFParse, sourceID, ws)
		if err != nil {
			t.Fatal(err)
		}
		if result.Stage != jobs.KindMRFDownload || result.RiverJobID <= 0 {
			t.Fatalf("result %+v", result)
		}
		var download, parse string
		var downloadJob, parseJob *int64
		if err := pool.QueryRow(context.Background(), `
SELECT download_status, parse_status, download_river_job_id, parse_river_job_id
FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&download, &parse, &downloadJob, &parseJob); err != nil {
			t.Fatal(err)
		}
		if download != jobs.StatusPending || parse != jobs.StatusBlocked || downloadJob == nil || parseJob != nil {
			t.Fatalf("state download=%s parse=%s download_job=%v parse_job=%v", download, parse, downloadJob, parseJob)
		}
	})
	t.Run("no raw and no slot waits for admission", func(t *testing.T) {
		pool := testDB(t)
		ws := workspace(t)
		sourceID := insertTerminalParseSource(t, pool, jobs.StatusBlocked, false)
		if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.pipeline_runtime (artifact_root, resident_capacity)
VALUES ($1, 1)`, ws.Root); err != nil {
			t.Fatal(err)
		}
		client, err := jobs.NewInsertClient(context.Background(), pool, jobs.NewLogger(io.Discard))
		if err != nil {
			t.Fatal(err)
		}
		result, err := Retry(context.Background(), pool, jobs.KindMRFParse, sourceID, ws)
		if err != nil {
			t.Fatal(err)
		}
		if result.Stage != jobs.KindMRFParse || result.RiverJobID != 0 {
			t.Fatalf("result %+v", result)
		}
		var download, parse string
		var downloadJob, parseJob *int64
		var failure *string
		if err := pool.QueryRow(context.Background(), `
SELECT download_status, parse_status, download_river_job_id, parse_river_job_id, failure_code
FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&download, &parse, &downloadJob, &parseJob, &failure); err != nil {
			t.Fatal(err)
		}
		if download != jobs.StatusBlocked || parse != jobs.StatusBlocked || downloadJob != nil || parseJob != nil || failure != nil {
			t.Fatalf("waiting state download=%s parse=%s download_job=%v parse_job=%v failure=%v", download, parse, downloadJob, parseJob, failure)
		}
		if err := admission.ScheduleWaiting(context.Background(), pool, client); err != nil {
			t.Fatal(err)
		}
		var held bool
		if err := pool.QueryRow(context.Background(), `
SELECT EXISTS (
    SELECT 1 FROM mrfpipeline.mrf_materialization_slots WHERE mrf_source_id = $1
)`, sourceID).Scan(&held); err != nil {
			t.Fatal(err)
		}
		if !held {
			t.Fatal("source was not admitted")
		}
		if err := pool.QueryRow(context.Background(), `
SELECT download_status, download_river_job_id, parse_river_job_id
FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&download, &downloadJob, &parseJob); err != nil {
			t.Fatal(err)
		}
		if download != jobs.StatusPending || downloadJob == nil || parseJob != nil {
			t.Fatalf("scheduled state download=%s download_job=%v parse_job=%v", download, downloadJob, parseJob)
		}
		var downloadJobs, parseJobs int
		if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFDownload).Scan(&downloadJobs); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFParse).Scan(&parseJobs); err != nil {
			t.Fatal(err)
		}
		if downloadJobs != 1 || parseJobs != 0 {
			t.Fatalf("scheduled jobs download=%d parse=%d", downloadJobs, parseJobs)
		}
	})
}

func TestIntegrationReconcileReleasesTerminalSlotAfterStagingCleanup(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()

	month := "2026-08-01"
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('uhc', $1)`, month); err != nil {
		t.Fatal(err)
	}
	var sourceID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources
    (source_url, collection_month, download_status, parse_status, failure_code)
VALUES ('https://files.test/terminal-staging', $1, 'failed', 'blocked', $2)
RETURNING id`, month, jobs.FailureMRFDownload).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month)
VALUES ($1, 'uhc', $2)`, sourceID, month); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_release_mrf_sources (payer_id, collection_month, mrf_source_id)
VALUES ('uhc', $2, $1)`, sourceID, month); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_materialization_slots (mrf_source_id) VALUES ($1)`, sourceID); err != nil {
		t.Fatal(err)
	}
	ws := workspace(t)
	// Use the downloader-owned name; parser parents are intentionally skipped
	// by the generic staging sweep.
	staging := filepath.Join(ws.StagingDir(), "mrf-download-"+strconv.FormatInt(sourceID, 10)+"-old")
	if err := os.Mkdir(staging, 0700); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(staging, old, old); err != nil {
		t.Fatal(err)
	}
	_ = runPass(t, pool, ws)
	var held, events int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.mrf_materialization_slots`).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.control_schedule_events`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if held != 0 || events != 1 {
		t.Fatalf("post-cleanup terminal repair held=%d events=%d", held, events)
	}
}

func TestIntegrationReconcileTerminalParseOccupancy(t *testing.T) {
	t.Run("held slot with raw", func(t *testing.T) {
		pool := testDB(t)
		ws := workspace(t)
		sourceID := insertTerminalParseSource(t, pool, jobs.StatusSucceeded, true)
		writeRawDownload(t, ws, sourceID)
		_ = runPass(t, pool, ws)
		assertTerminalParseReleased(t, pool, ws, sourceID)
	})
	t.Run("held slot without raw", func(t *testing.T) {
		pool := testDB(t)
		ws := workspace(t)
		sourceID := insertTerminalParseSource(t, pool, jobs.StatusSucceeded, true)
		_ = runPass(t, pool, ws)
		assertTerminalParseReleased(t, pool, ws, sourceID)
	})
	t.Run("cleaned terminal parse is not reopened", func(t *testing.T) {
		pool := testDB(t)
		ws := workspace(t)
		sourceID := insertTerminalParseSource(t, pool, jobs.StatusBlocked, false)
		_ = runPass(t, pool, ws)
		var status string
		var jobID *int64
		if err := pool.QueryRow(context.Background(), `
SELECT parse_status, parse_river_job_id
FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&status, &jobID); err != nil {
			t.Fatal(err)
		}
		if status != jobs.StatusFailed || jobID != nil {
			t.Fatalf("terminal parse reopened: status=%s job=%v", status, jobID)
		}
		var parseJobs int
		if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`,
			jobs.KindMRFParse).Scan(&parseJobs); err != nil {
			t.Fatal(err)
		}
		if parseJobs != 0 {
			t.Fatalf("parse jobs %d", parseJobs)
		}
	})
}

func assertTerminalParseReleased(t *testing.T, pool *pgxpool.Pool, ws *artifact.Workspace, sourceID int64) {
	t.Helper()
	var download, parse string
	var downloadJob *int64
	if err := pool.QueryRow(context.Background(), `
SELECT download_status, parse_status, download_river_job_id
FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&download, &parse, &downloadJob); err != nil {
		t.Fatal(err)
	}
	if download != jobs.StatusBlocked || parse != jobs.StatusFailed || downloadJob != nil {
		t.Fatalf("terminal parse state download=%s parse=%s job=%v", download, parse, downloadJob)
	}
	var held, events int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline.mrf_materialization_slots`).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline.control_schedule_events`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if held != 0 || events != 1 {
		t.Fatalf("terminal parse occupancy held=%d events=%d", held, events)
	}
	state, err := ws.InspectDownloadState(artifact.KindMRF, sourceID)
	if err != nil || state != artifact.DownloadAbsent {
		t.Fatalf("raw state %s %v", state, err)
	}
}

func TestIntegrationRetrySealedReleaseForbidden(t *testing.T) {
	pool := testDB(t)
	toc := insertTOC(t, pool, jobs.StatusFailed, jobs.StatusBlocked, jobs.StatusBlocked)
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.monthly_releases
SET status = 'active', sealed_at = transaction_timestamp(), last_activated_at = transaction_timestamp()
WHERE payer_id = 'uhc' AND collection_month = DATE '2026-08-01'`); err != nil {
		t.Fatal(err)
	}
	var before int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindTOCDownload).Scan(&before); err != nil {
		t.Fatal(err)
	}
	_, err := Retry(context.Background(), pool, jobs.KindTOCDownload, toc)
	if !jobs.IsFailure(err, jobs.FailureSealedReleaseRetryForbidden) {
		t.Fatalf("retry sealed: %v", err)
	}
	var after int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindTOCDownload).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("sealed retry inserted a job: %d -> %d", before, after)
	}
}

func TestIntegrationSharedSourceSealedRetryAndReconcile(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	month := "2026-08-01"
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases
    (payer_id, collection_month, status, sealed_at, last_activated_at)
VALUES
    ('uhc', $1, 'active', transaction_timestamp(), transaction_timestamp()),
    ('aetna', $1, 'building', NULL, NULL)`, month); err != nil {
		t.Fatal(err)
	}
	var sourceID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources
    (source_url, collection_month, download_status, parse_status, failure_code)
VALUES ('https://files.test/shared-source', $1, 'failed', 'blocked', $2)
RETURNING id`, month, jobs.FailureMRFDownload).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	for _, payer := range []string{"uhc", "aetna"} {
		if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots
    (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, $2, $3, 'blocked')`, sourceID, payer, month); err != nil {
			t.Fatal(err)
		}
	}
	var jobsBefore int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFDownload).Scan(&jobsBefore); err != nil {
		t.Fatal(err)
	}
	if _, err := Retry(ctx, pool, jobs.KindMRFDownload, sourceID); !jobs.IsFailure(err, jobs.FailureSealedReleaseRetryForbidden) {
		t.Fatalf("shared sealed retry: %v", err)
	}
	var status, parse string
	var jobID *int64
	if err := pool.QueryRow(ctx, `
SELECT download_status, parse_status, download_river_job_id
FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&status, &parse, &jobID); err != nil {
		t.Fatal(err)
	}
	if status != jobs.StatusFailed || parse != jobs.StatusBlocked || jobID != nil {
		t.Fatalf("sealed source mutated: %s %s %v", status, parse, jobID)
	}
	var jobsAfter int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFDownload).Scan(&jobsAfter); err != nil {
		t.Fatal(err)
	}
	if jobsAfter != jobsBefore {
		t.Fatalf("sealed retry inserted a job: %d -> %d", jobsBefore, jobsAfter)
	}
	if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline.mrf_sources
SET download_status = 'succeeded', parse_status = 'pending', failure_code = NULL,
    download_river_job_id = NULL, parse_river_job_id = NULL
WHERE id = $1`, sourceID); err != nil {
		t.Fatal(err)
	}
	buildingTOC := insertTOCFor(t, pool, "cigna", "2026-08", jobs.StatusPending, jobs.StatusBlocked, jobs.StatusBlocked)
	report := runPass(t, pool, workspace(t))
	if report.SealedReleaseInconsistencyCount != 2 || report.RepairedJobCount != 1 {
		t.Fatalf("shared source report: %+v", report)
	}
	var buildingJob *int64
	if err := pool.QueryRow(ctx, `
SELECT download_river_job_id FROM mrfpipeline.toc_files WHERE id = $1`, buildingTOC).Scan(&buildingJob); err != nil {
		t.Fatal(err)
	}
	if buildingJob == nil {
		t.Fatal("unrelated building repair was not scheduled")
	}
}

func TestIntegrationReconcileSealedAndBuildingRows(t *testing.T) {
	pool := testDB(t)
	ws := workspace(t)
	sealed := insertTOC(t, pool, jobs.StatusPending, jobs.StatusBlocked, jobs.StatusBlocked)
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.monthly_releases
SET status = 'active', sealed_at = transaction_timestamp(), last_activated_at = transaction_timestamp()
WHERE payer_id = 'uhc' AND collection_month = DATE '2026-08-01'`); err != nil {
		t.Fatal(err)
	}
	building := insertTOCFor(t, pool, "aetna", "2026-08", jobs.StatusPending, jobs.StatusBlocked, jobs.StatusBlocked)
	report := runPass(t, pool, ws)
	if report.SealedReleaseInconsistencyCount != 1 || report.RepairedJobCount != 1 {
		t.Fatalf("report %+v", report)
	}
	var sealedStatus string
	var sealedJob *int64
	if err := pool.QueryRow(context.Background(), `
SELECT download_status, download_river_job_id
FROM mrfpipeline.toc_files WHERE id = $1`, sealed).Scan(&sealedStatus, &sealedJob); err != nil {
		t.Fatal(err)
	}
	if sealedStatus != jobs.StatusPending || sealedJob != nil {
		t.Fatalf("sealed row changed: %s %v", sealedStatus, sealedJob)
	}
	var buildingJob *int64
	if err := pool.QueryRow(context.Background(), `
SELECT download_river_job_id FROM mrfpipeline.toc_files WHERE id = $1`, building).Scan(&buildingJob); err != nil {
		t.Fatal(err)
	}
	if buildingJob == nil {
		t.Fatal("building row was not repaired")
	}
}

func TestIntegrationReconcileHealthySealedReleaseIsQuiet(t *testing.T) {
	pool := testDB(t)
	ws := workspace(t)
	ctx := context.Background()
	month := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month, status, sealed_at, last_activated_at)
VALUES ('uhc', $1, 'active', transaction_timestamp(), transaction_timestamp())`, month); err != nil {
		t.Fatal(err)
	}
	var runID, sourceID, snapshotID, planID, batchID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, status, completed_at)
VALUES ('uhc', $1, 'succeeded', transaction_timestamp()) RETURNING id`, month).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id,
    download_status, parse_status, import_status
) VALUES ('uhc', $1, 'https://files.test/quiet-toc', $2, 'succeeded', 'succeeded', 'succeeded')`, month, runID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ('https://files.test/quiet-mrf', $1, 'succeeded', 'succeeded') RETURNING id`, month).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, 'uhc', $2, 'succeeded') RETURNING id`, sourceID, month).Scan(&snapshotID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_plans (mrf_snapshot_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type)
VALUES ($1, 'A', 'issuer', 'hios', '1', 'group') RETURNING id`, snapshotID).Scan(&planID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batches (
    mrf_snapshot_id, status, requested_plan_count, added_plan_count, completed_at
) VALUES ($1, 'succeeded', 1, 1, transaction_timestamp()) RETURNING id`, snapshotID).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batch_items (plan_attachment_batch_id, mrf_plan_id)
VALUES ($1, $2)`, batchID, planID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline.monthly_releases
SET sealed_at = transaction_timestamp(), last_activated_at = transaction_timestamp()
WHERE payer_id = 'uhc' AND collection_month = $1`, month); err != nil {
		t.Fatal(err)
	}
	services := filepath.Join(t.TempDir(), "services.csv")
	if err := os.WriteFile(services, []byte("billing_code_type,billing_code\nCPT,99213\n"), 0600); err != nil {
		t.Fatal(err)
	}
	warehouse := t.TempDir()
	writeRecognizedSnapshot(t, warehouse, "uhc", "2026-08", "mrf-"+strconv.FormatInt(snapshotID, 10))
	partDir := filepath.Join(warehouse, "plan_associations", "output_id=mrf-"+strconv.FormatInt(snapshotID, 10))
	if err := os.MkdirAll(partDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partDir, "plan-batch-"+strconv.FormatInt(batchID, 10)+"-part-00000.parquet"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	report, err := Run(ctx, Params{
		Pool: pool, Workspace: ws, ServicesPath: services,
		WarehousePath: warehouse, ProviderCatalogPath: filepath.Join(t.TempDir(), "cat"),
		Logger: jobs.NewLogger(io.Discard),
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.SealedReleaseInconsistencyCount != 0 || report.RepairedJobCount != 0 || report.UnblockedStageCount != 0 {
		t.Fatalf("healthy sealed release was not quiet: %+v", report)
	}
}

func TestIntegrationSealedPlanSetDriftReported(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	month := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases
    (payer_id, collection_month, status, sealed_at, last_activated_at)
VALUES ('uhc', $1, 'active', transaction_timestamp(), transaction_timestamp())`, month); err != nil {
		t.Fatal(err)
	}
	var sourceID, snapshotID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ('https://files.test/sealed-drift', $1, 'succeeded', 'succeeded') RETURNING id`, month).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, 'uhc', $2, 'succeeded') RETURNING id`, sourceID, month).Scan(&snapshotID); err != nil {
		t.Fatal(err)
	}
	var batchID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batches
    (mrf_snapshot_id, status, requested_plan_count, added_plan_count, completed_at)
VALUES ($1, 'succeeded', 1, 1, transaction_timestamp())
RETURNING id`, snapshotID).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_plans
    (mrf_snapshot_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type)
VALUES ($1, 'out-of-band', 'issuer', 'hios', 'sealed-drift', 'group')`, snapshotID); err != nil {
		t.Fatal(err)
	}
	var planID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM mrfpipeline.mrf_plans WHERE mrf_snapshot_id = $1`, snapshotID).Scan(&planID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batch_items (plan_attachment_batch_id, mrf_plan_id)
VALUES ($1, $2)`, batchID, planID); err != nil {
		t.Fatal(err)
	}
	warehouse := t.TempDir()
	if err := os.WriteFile(filepath.Join(warehouse, "warehouse.json"), []byte(`{"warehouse_schema_version":"2.0.0","provider_catalog":{"schema_version":1,"release_month":"2026-08"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	parts := filepath.Join(warehouse, "plan_associations", "output_id=mrf-"+strconv.FormatInt(snapshotID, 10))
	if err := os.MkdirAll(parts, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parts, "plan-batch-999-part-00000.parquet"), []byte("not inspected"), 0600); err != nil {
		t.Fatal(err)
	}
	var report Report
	report.logger = jobs.NewLogger(io.Discard)
	if err := auditSealedPlanSets(ctx, pool, warehouse, &report); err != nil {
		t.Fatal(err)
	}
	if report.SealedReleaseInconsistencyCount != 4 {
		t.Fatalf("drift count = %d", report.SealedReleaseInconsistencyCount)
	}
}

func TestIntegrationSealedPlanSetSymlinkedInventoryReported(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	month := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('uhc', $1)`, month); err != nil {
		t.Fatal(err)
	}
	var sourceID, snapshotID, planID, batchID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ('https://files.test/sealed-symlink', $1, 'succeeded', 'succeeded')
RETURNING id`, month).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, 'uhc', $2, 'succeeded') RETURNING id`, sourceID, month).Scan(&snapshotID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_plans
    (mrf_snapshot_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type)
VALUES ($1, 'symlink-plan', 'issuer', 'hios', 'symlink-plan', 'group')
RETURNING id`, snapshotID).Scan(&planID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batches
    (mrf_snapshot_id, status, requested_plan_count, added_plan_count, completed_at)
VALUES ($1, 'succeeded', 1, 1, transaction_timestamp())
RETURNING id`, snapshotID).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batch_items (plan_attachment_batch_id, mrf_plan_id)
VALUES ($1, $2)`, batchID, planID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline.monthly_releases
SET status = 'active', sealed_at = transaction_timestamp(), last_activated_at = transaction_timestamp()
WHERE payer_id = 'uhc' AND collection_month = $1`, month); err != nil {
		t.Fatal(err)
	}
	warehouse := t.TempDir()
	outputID := "mrf-" + strconv.FormatInt(snapshotID, 10)
	writeRecognizedSnapshot(t, warehouse, "uhc", "2026-08", outputID)
	external := filepath.Join(t.TempDir(), "plan-parts")
	if err := os.MkdirAll(external, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(external, "plan-batch-"+strconv.FormatInt(batchID, 10)+"-part-00000.parquet"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	parts := filepath.Join(warehouse, "plan_associations", "output_id="+outputID)
	if err := os.MkdirAll(filepath.Dir(parts), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, parts); err != nil {
		t.Fatal(err)
	}
	var report Report
	report.logger = jobs.NewLogger(io.Discard)
	if err := auditSealedPlanSets(ctx, pool, warehouse, &report); err != nil {
		t.Fatal(err)
	}
	if report.SealedReleaseInconsistencyCount != 1 {
		t.Fatalf("symlinked inventory count = %d", report.SealedReleaseInconsistencyCount)
	}
}

func writeRecognizedSnapshot(t testing.TB, warehouse, payer, month, outputID string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(warehouse, "warehouse.json"), []byte(`{"warehouse_schema_version":"2.0.0","provider_catalog":{"schema_version":1,"release_month":"2026-08"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(warehouse, "snapshots", "collection_month="+month, "payer_id="+payer, "output_id="+outputID)
	datasets := []string{"rate_facts", "rate_provider_groups", "provider_groups", "provider_group_memberships", "ingestions", "network_names"}
	counts := map[string]map[string]int64{}
	for _, name := range datasets {
		if err := os.MkdirAll(filepath.Join(final, name), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(final, name, outputID+"-part-00000.parquet"), []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
		rowCount := int64(0)
		if name == "ingestions" {
			rowCount = 1
		}
		counts[name] = map[string]int64{"row_count": rowCount, "part_count": 1}
	}
	manifest := map[string]any{
		"manifest_schema_version": "2.0.0", "output_schema_version": "2.0.0",
		"output_id": outputID, "payer_id": payer, "collection_month": month,
		"provider_catalog": map[string]any{"schema_version": 1, "release_month": "2026-08"},
		"datasets":         counts,
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(final, "manifest.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationPrerequisiteRestore(t *testing.T) {
	pool := testDB(t)
	ws := workspace(t)
	toc := insertTOC(t, pool, jobs.StatusSucceeded, jobs.StatusPending, jobs.StatusBlocked)
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.toc_files SET parse_river_job_id = 4, download_river_job_id = 3 WHERE id = $1`, toc); err != nil {
		t.Fatal(err)
	}
	rep := runPass(t, pool, ws)
	if rep.RepairedJobCount < 1 {
		t.Fatalf("report %+v", rep)
	}
	var download, parse string
	var parseJob, downJob *int64
	if err := pool.QueryRow(context.Background(), `
SELECT download_status, parse_status, parse_river_job_id, download_river_job_id
FROM mrfpipeline.toc_files WHERE id = $1`, toc).Scan(&download, &parse, &parseJob, &downJob); err != nil {
		t.Fatal(err)
	}
	if download != jobs.StatusPending || parse != jobs.StatusBlocked || parseJob != nil || downJob == nil {
		t.Fatalf("%s %s parseJob=%v downJob=%v", download, parse, parseJob, downJob)
	}
}

func TestIntegrationFailedParsePreserved(t *testing.T) {
	pool := testDB(t)
	ws := workspace(t)
	toc := insertTOC(t, pool, jobs.StatusSucceeded, jobs.StatusFailed, jobs.StatusBlocked)
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.toc_files SET failure_code = $2, parse_river_job_id = 8, download_river_job_id = 7 WHERE id = $1`, toc, jobs.FailureTOCParseOutputInvalid); err != nil {
		t.Fatal(err)
	}
	rep := runPass(t, pool, ws)
	if rep.RepairedJobCount != 0 {
		t.Fatalf("touched failed parse %+v", rep)
	}
	var parse, code string
	if err := pool.QueryRow(context.Background(), `
SELECT parse_status, failure_code FROM mrfpipeline.toc_files WHERE id = $1`, toc).Scan(&parse, &code); err != nil {
		t.Fatal(err)
	}
	if parse != jobs.StatusFailed || code != jobs.FailureTOCParseOutputInvalid {
		t.Fatalf("%s %s", parse, code)
	}
}

func TestIntegrationCleanupStagingAndAbsentLeaf(t *testing.T) {
	pool := testDB(t)
	ws := workspace(t)
	old := filepath.Join(ws.StagingDir(), "stale")
	if err := os.Mkdir(old, 0700); err != nil {
		t.Fatal(err)
	}
	oldTime := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(old, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	toc := insertTOC(t, pool, jobs.StatusSucceeded, jobs.StatusSucceeded, jobs.StatusSucceeded)
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.toc_files
SET download_river_job_id = 2, parse_river_job_id = 3, import_river_job_id = 4
WHERE id = $1`, toc); err != nil {
		t.Fatal(err)
	}
	rep := runPass(t, pool, ws)
	if rep.CleanedArtifactCount < 1 {
		t.Fatalf("report %+v", rep)
	}
	if _, err := os.Lstat(old); !os.IsNotExist(err) {
		t.Fatal("staging remained")
	}
}

func TestIntegrationPlanBatchScheduleAndRetry(t *testing.T) {
	pool := testDB(t)
	ws := workspace(t)
	url := uniqueURL("mrf")
	var sourceID, snapID, planID int64
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month, mrf_source_target_kind)
VALUES ('uhc', DATE '2026-08-01', 'all')
ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status, download_river_job_id, parse_river_job_id)
VALUES ($1, DATE '2026-08-01', 'succeeded', 'succeeded', 11, 12) RETURNING id`, url).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.monthly_release_mrf_sources
    (payer_id, collection_month, mrf_source_id)
VALUES ('uhc', DATE '2026-08-01', $1)`, sourceID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_status, consume_river_job_id)
VALUES ($1, 'uhc', DATE '2026-08-01', 'succeeded', 13) RETURNING id`, sourceID).Scan(&snapID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_plans (mrf_snapshot_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type)
VALUES ($1, 'A', 'issuer', 'hios', '1', 'group') RETURNING id`, snapID).Scan(&planID); err != nil {
		t.Fatal(err)
	}
	rep := runPass(t, pool, ws)
	if rep.ScheduledPlanBatchCount != 1 {
		t.Fatalf("batches %+v", rep)
	}
	var batchID int64
	var status string
	if err := pool.QueryRow(context.Background(), `
SELECT id, status FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id = $1`, snapID).Scan(&batchID, &status); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.plan_attachment_batches
SET status = 'failed', failure_code = $2, completed_at = transaction_timestamp()
WHERE id = $1`, batchID, jobs.FailurePlanAttachOutputFailed); err != nil {
		t.Fatal(err)
	}
	res, err := Retry(context.Background(), pool, jobs.KindConsumerAttachPlans, batchID)
	if err != nil {
		t.Fatal(err)
	}
	if res.DomainID != batchID {
		t.Fatalf("%+v", res)
	}
	var items int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline.plan_attachment_batch_items WHERE plan_attachment_batch_id = $1`, batchID).Scan(&items); err != nil || items != 1 {
		t.Fatalf("items %d", items)
	}
	_ = planID
	ready, err := planbatch.IsPlanReady(context.Background(), pool, snapID)
	if err != nil || ready {
		t.Fatalf("retry pending batch must not be plan-ready %v %v", ready, err)
	}
}
