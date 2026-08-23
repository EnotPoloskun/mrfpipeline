package reconcile

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

func runPass(t *testing.T, pool *pgxpool.Pool, ws *artifact.Workspace) Report {
	t.Helper()
	svc := filepath.Join(t.TempDir(), "services.csv")
	if err := os.WriteFile(svc, []byte("billing_code_type,billing_code\nCPT,99213\n"), 0600); err != nil {
		t.Fatal(err)
	}
	rep, err := Run(context.Background(), Params{
		Pool: pool, Workspace: ws, ServicesPath: svc,
		WarehousePath: filepath.Join(t.TempDir(), "wh"),
		ProviderCatalogPath: filepath.Join(t.TempDir(), "cat"),
		Logger: jobs.NewLogger(io.Discard),
	})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func insertDiscovery(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.discovery_runs (
    payer_id, collection_month, status, river_job_id, started_at, completed_at
) VALUES ('uhc', DATE '2026-08-01', 'succeeded', 1, transaction_timestamp(), transaction_timestamp())
RETURNING id`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func insertTOC(t *testing.T, pool *pgxpool.Pool, download, parse, imp string) int64 {
	t.Helper()
	runID := insertDiscovery(t, pool)
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
) VALUES ('uhc', DATE '2026-08-01', $1, $2, $3, $4, $5, $6)
RETURNING id`, uniqueURL("toc"), runID, download, parse, imp, fail).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestIntegrationSuccessorAndCurrentJobs(t *testing.T) {
	pool := testDB(t)
	ws := workspace(t)
	toc := insertTOC(t, pool, jobs.StatusSucceeded, jobs.StatusBlocked, jobs.StatusBlocked)
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
	var sourceID, feedID, snapID, planID int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_sources (source_url, download_status, parse_status, download_river_job_id, parse_river_job_id)
VALUES ($1, 'succeeded', 'succeeded', 11, 12) RETURNING id`, url).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_feeds (payer_id, feed_id) VALUES ('uhc', $1) RETURNING id`, "mrf-source-"+strconv.FormatInt(sourceID, 10)).Scan(&feedID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, mrf_feed_id, collection_month, consume_status, consume_river_job_id)
VALUES ($1, $2, DATE '2026-08-01', 'succeeded', 13) RETURNING id`, sourceID, feedID).Scan(&snapID); err != nil {
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
