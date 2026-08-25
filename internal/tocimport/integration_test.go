package tocimport

import (
	"context"
	"fmt"
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
	"github.com/enotpoloskun/mrfpipeline/internal/planbatch"
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

func insertImportJob(t *testing.T, pool *pgxpool.Pool, client *river.Client[pgx.Tx], month time.Time) (tocID, jobID int64) {
	return insertImportJobForPayer(t, pool, client, "uhc", month)
}

func insertImportJobForPayer(t *testing.T, pool *pgxpool.Pool, client *river.Client[pgx.Tx], payer string, month time.Time) (tocID, jobID int64) {
	t.Helper()
	if month.IsZero() {
		month = time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month, mrf_source_target_kind)
VALUES ($1, $2, 'all') ON CONFLICT DO NOTHING`, payer, month); err != nil {
		t.Fatal(err)
	}
	var runID int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, toc_limit, status, completed_at)
VALUES ($1, $2, 1, 'succeeded', transaction_timestamp())
RETURNING id`, payer, month).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	sourceURL := "http://files.test/data/" + strconv.FormatInt(tocURLSeq.Add(1), 10)
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id,
    download_status, parse_status, import_status
) VALUES ($1, $2, $3, $4, 'succeeded', 'succeeded', 'pending')
RETURNING id`, payer, month, sourceURL, runID).Scan(&tocID); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	jobID, err = jobs.InsertTx(context.Background(), client, tx, &jobs.TOCImportArgs{TOCFileID: tocID})
	if err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `
UPDATE mrfpipeline.toc_files SET import_river_job_id = $2, updated_at = transaction_timestamp()
WHERE id = $1`, tocID, jobID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	return tocID, jobID
}

func startImportRuntime(t *testing.T, pool *pgxpool.Pool, ws *artifact.Workspace, maxAttempts int, mutate func(*Worker)) *river.Client[pgx.Tx] {
	t.Helper()
	w := &Worker{Pool: pool, Workspace: ws, Logger: jobs.NewLogger(io.Discard)}
	if mutate != nil {
		mutate(w)
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, w)
	cfg := jobs.ClientConfig(workers, map[string]river.QueueConfig{jobs.QueueTOCImport: {MaxWorkers: 2}}, nil, jobs.NewLogger(io.Discard))
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

func waitImport(t *testing.T, pool *pgxpool.Pool, tocID int64, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		err := pool.QueryRow(context.Background(), `SELECT import_status FROM mrfpipeline.toc_files WHERE id = $1`, tocID).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	var download, parse, status, failure string
	var jobID *int64
	if err := pool.QueryRow(context.Background(), `
SELECT download_status, parse_status, import_status,
       COALESCE(failure_code, ''), import_river_job_id
FROM mrfpipeline.toc_files WHERE id = $1`, tocID).
		Scan(&download, &parse, &status, &failure, &jobID); err != nil {
		t.Fatalf("timed out waiting for toc %d import %s (row query: %v)", tocID, want, err)
	}
	var state, errorsJSON string
	if jobID != nil {
		_ = pool.QueryRow(context.Background(), `
SELECT state, COALESCE(errors::text, '')
FROM mrfpipeline_river.river_job WHERE id = $1`, *jobID).Scan(&state, &errorsJSON)
	}
	t.Fatalf("timed out waiting for toc %d import %s (download=%s parse=%s import=%s failure=%s job=%v state=%s errors=%s)",
		tocID, want, download, parse, status, failure, jobID, state, errorsJSON)
}

func writeParsedRows(t *testing.T, ws *artifact.Workspace, tocID int64, month string, assocs []assocRow) {
	writeParsedRowsForPayer(t, ws, tocID, "uhc", month, assocs)
}

func writeParsedRowsForPayer(t *testing.T, ws *artifact.Workspace, tocID int64, payer, month string, assocs []assocRow) {
	t.Helper()
	source, output, err := generatedPaths(ws, tocID)
	if err != nil {
		t.Fatal(err)
	}
	id, err := artifact.RecordDirName(artifact.KindTOC, tocID)
	if err != nil {
		t.Fatal(err)
	}
	for i := range assocs {
		assocs[i].TOCOutputID = id
		assocs[i].PayerID = payer
		assocs[i].CollectionMonth = month
	}
	writeCompleted(t, output, id, payer, month, source, sampleTOCRow(id, payer, month, source), assocs)
}

func writeParsedTOC(t *testing.T, ws *artifact.Workspace, tocID int64, month, body string) {
	t.Helper()
	source, output, err := generatedPaths(ws, tocID)
	if err != nil {
		t.Fatal(err)
	}
	id, err := artifact.RecordDirName(artifact.KindTOC, tocID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(source), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", ws.StagingDir())
	if _, err := mrftocparser.Parse(context.Background(), mrftocparser.Config{
		InputPath: source, OutputPath: output, TOCOutputID: id, PayerID: "uhc", CollectionMonth: month,
	}); err != nil {
		t.Fatal(err)
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

func TestIntegrationValidImport(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	tocID, _ := insertImportJob(t, pool, client, time.Time{})
	writeParsedTOC(t, ws, tocID, "2026-08", sampleTOC("https://example.test/a.json", "plan", "issuer", "hios", "id", "group", ""))
	startImportRuntime(t, pool, ws, 8, nil)
	waitImport(t, pool, tocID, jobs.StatusSucceeded)
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_sources`) != 1 {
		t.Fatal("sources")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_snapshots`) != 1 {
		t.Fatal("snapshots")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.toc_mrf_plan_associations`) != 1 {
		t.Fatal("assoc")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_plans`) != 1 {
		t.Fatal("plans")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.plan_attachment_batches`) != 0 {
		t.Fatal("attachment")
	}
	var sourceID int64
	var payer string
	var month time.Time
	if err := pool.QueryRow(context.Background(), `
SELECT s.id, n.payer_id, n.collection_month FROM mrfpipeline.mrf_sources s
JOIN mrfpipeline.mrf_snapshots n ON n.mrf_source_id = s.id
WHERE s.source_url = 'https://example.test/a.json'`).Scan(&sourceID, &payer, &month); err != nil {
		t.Fatal(err)
	}
	if sourceID <= 0 || payer != "uhc" || !month.Equal(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("snapshot identity source=%d payer=%s month=%s", sourceID, payer, month.Format("2006-01-02"))
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFDownload) != 1 {
		t.Fatal("download job")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1 AND state IN ('completed','discarded')`, jobs.KindMRFDownload) != 0 {
		t.Fatal("download consumed")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindConsumerIngest) != 0 {
		t.Fatal("ingest")
	}
}

func TestIntegrationSameMonthlySourceDifferentPayers(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	const location = "https://example.test/shared-payer.json"
	uhcID, _ := insertImportJobForPayer(t, pool, client, "uhc", time.Time{})
	writeParsedRowsForPayer(t, ws, uhcID, "uhc", "2026-08", []assocRow{
		validAssoc(location, "uhc-plan", "issuer", nil, "hios", "uhc-id", "group"),
	})
	aetnaID, _ := insertImportJobForPayer(t, pool, client, "aetna", time.Time{})
	writeParsedRowsForPayer(t, ws, aetnaID, "aetna", "2026-08", []assocRow{
		validAssoc(location, "aetna-plan", "issuer", nil, "hios", "aetna-id", "group"),
	})
	startImportRuntime(t, pool, ws, 8, nil)
	waitImport(t, pool, uhcID, jobs.StatusSucceeded)
	waitImport(t, pool, aetnaID, jobs.StatusSucceeded)
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_sources`) != 1 {
		t.Fatal("sources")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_snapshots`) != 2 {
		t.Fatal("snapshots")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_snapshots WHERE payer_id IN ('uhc', 'aetna') AND collection_month = DATE '2026-08-01'`) != 2 {
		t.Fatal("payer snapshots")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFDownload) != 1 {
		t.Fatal("download jobs")
	}
}

func TestIntegrationZeroAssociations(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	tocID, _ := insertImportJob(t, pool, client, time.Time{})
	writeParsedRows(t, ws, tocID, "2026-08", nil)
	startImportRuntime(t, pool, ws, 8, nil)
	waitImport(t, pool, tocID, jobs.StatusSucceeded)
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_sources`) != 0 {
		t.Fatal("source")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFDownload) != 0 {
		t.Fatal("job")
	}
}

func TestIntegrationInvalidLastPartNoMutation(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	tocID, _ := insertImportJob(t, pool, client, time.Time{})
	source, output, err := generatedPaths(ws, tocID)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := artifact.RecordDirName(artifact.KindTOC, tocID)
	a := validAssoc("https://example.test/a.json", "plan", "issuer", nil, "hios", "id", "group")
	a.TOCOutputID, a.CollectionMonth = id, "2026-08"
	writeParquet(t, filepath.Join(output, datasetTOCFiles, "part-00000.parquet"), []tocFileRow{sampleTOCRow(id, "uhc", "2026-08", source)})
	writeParquet(t, filepath.Join(output, datasetAssociations, "part-00000.parquet"), []assocRow{a})
	type bad struct {
		X int32 `parquet:"x"`
	}
	writeParquet(t, filepath.Join(output, datasetAssociations, "part-00001.parquet"), []bad{{X: 1}})
	writeManifest(t, output, id, "uhc", "2026-08", source, 2)
	startImportRuntime(t, pool, ws, 8, nil)
	waitImport(t, pool, tocID, jobs.StatusFailed)
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_sources`) != 0 {
		t.Fatal("mutated")
	}
	var code *string
	if err := pool.QueryRow(context.Background(), `SELECT failure_code FROM mrfpipeline.toc_files WHERE id = $1`, tocID).Scan(&code); err != nil || code == nil || *code != jobs.FailureTOCImportSchemaInvalid {
		t.Fatalf("code %v", code)
	}
}

func TestIntegrationMultiPartAndBatchCrash(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	tocID, _ := insertImportJob(t, pool, client, time.Time{})
	source, output, err := generatedPaths(ws, tocID)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := artifact.RecordDirName(artifact.KindTOC, tocID)
	var first, second []assocRow
	for i := 0; i < 1001; i++ {
		loc := fmt.Sprintf("https://example.test/p/%05d.json", i)
		row := validAssoc(loc, "plan", "issuer", nil, "hios", strconv.Itoa(i), "group")
		row.TOCOutputID, row.CollectionMonth = id, "2026-08"
		if i < 600 {
			first = append(first, row)
		} else {
			second = append(second, row)
		}
	}
	writeParquet(t, filepath.Join(output, datasetTOCFiles, "part-00000.parquet"), []tocFileRow{sampleTOCRow(id, "uhc", "2026-08", source)})
	writeParquet(t, filepath.Join(output, datasetAssociations, "part-00000.parquet"), first)
	writeParquet(t, filepath.Join(output, datasetAssociations, "part-00001.parquet"), second)
	writeManifest(t, output, id, "uhc", "2026-08", source, 1001)
	var attempts atomic.Int64
	startImportRuntime(t, pool, ws, 8, func(w *Worker) {
		w.afterBatch = func(int) error {
			if attempts.Add(1) == 1 {
				return jobs.Failure(jobs.FailureTOCImportDatabaseFailed)
			}
			return nil
		}
	})
	waitImport(t, pool, tocID, jobs.StatusSucceeded)
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_sources`) != 1001 {
		t.Fatalf("sources %d", count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_sources`))
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.toc_mrf_plan_associations`) != 1001 {
		t.Fatal("assocs")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFDownload) != 1001 {
		t.Fatal("jobs")
	}
	if attempts.Load() < 2 {
		t.Fatal("no retry")
	}
}

func TestIntegrationReimportAndOverlap(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	aID, _ := insertImportJob(t, pool, client, time.Time{})
	writeParsedRows(t, ws, aID, "2026-08", []assocRow{
		validAssoc("https://example.test/shared.json", "planA", "issuer", nil, "hios", "a", "group"),
		validAssoc("https://example.test/shared.json", "planB", "issuer", nil, "hios", "b", "group"),
	})
	startImportRuntime(t, pool, ws, 8, nil)
	waitImport(t, pool, aID, jobs.StatusSucceeded)
	bID, _ := insertImportJob(t, pool, client, time.Time{})
	writeParsedRows(t, ws, bID, "2026-08", []assocRow{
		validAssoc("https://example.test/shared.json", "planB", "issuer", nil, "hios", "b", "group"),
		validAssoc("https://example.test/shared.json", "planC", "issuer", nil, "hios", "c", "group"),
	})
	waitImport(t, pool, bID, jobs.StatusSucceeded)
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_sources`) != 1 {
		t.Fatal("sources")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_plans`) != 3 {
		t.Fatal("plans")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.toc_mrf_plan_associations`) != 4 {
		t.Fatal("assoc")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFDownload) != 1 {
		t.Fatal("download")
	}

	res, _, err := claimImport(context.Background(), pool, aID, 1)
	if err != nil || res.Action != jobs.ClaimNoop {
		t.Fatalf("redelivery %+v %v", res, err)
	}
}

func TestIntegrationSponsorVariantsAndHIOS(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	tocID, _ := insertImportJob(t, pool, client, time.Time{})
	s1, s2, hios := "First", "Second", "Shown"
	writeParsedRows(t, ws, tocID, "2026-08", []assocRow{
		validAssoc("https://example.test/e.json", "plan", "issuer", &s1, "ein", "11-1", "group"),
		validAssoc("https://example.test/e.json", "plan", "issuer", &s2, "ein", "11-1", "group"),
		validAssoc("https://example.test/h.json", "plan", "issuer", &hios, "hios", "H1", "group"),
	})
	startImportRuntime(t, pool, ws, 8, nil)
	waitImport(t, pool, tocID, jobs.StatusSucceeded)
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.toc_mrf_plan_associations`) != 3 {
		t.Fatal("prov")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_plans`) != 2 {
		t.Fatal("plans")
	}
	var einSponsor, hiosSponsor *string
	if err := pool.QueryRow(context.Background(), `
SELECT plan_sponsor_name FROM mrfpipeline.mrf_plans WHERE plan_id_type = 'ein'`).Scan(&einSponsor); err != nil || einSponsor == nil || *einSponsor != "First" {
		t.Fatalf("ein %v", einSponsor)
	}
	if err := pool.QueryRow(context.Background(), `
SELECT plan_sponsor_name FROM mrfpipeline.mrf_plans WHERE plan_id_type = 'hios'`).Scan(&hiosSponsor); err != nil || hiosSponsor != nil {
		t.Fatalf("hios %v", hiosSponsor)
	}
}

func TestIntegrationSameURLDifferentMonth(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	const loc = "https://example.test/same.json"
	var sourceID int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ($1, DATE '2026-08-01', 'succeeded', 'succeeded')
RETURNING id`, loc).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	aug := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	sep := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	aID, _ := insertImportJob(t, pool, client, aug)
	writeParsedRows(t, ws, aID, "2026-08", []assocRow{
		validAssoc(loc, "plan", "issuer", nil, "hios", "id", "group"),
	})
	startImportRuntime(t, pool, ws, 8, nil)
	waitImport(t, pool, aID, jobs.StatusSucceeded)
	bID, _ := insertImportJob(t, pool, client, sep)
	writeParsedRows(t, ws, bID, "2026-09", []assocRow{
		validAssoc(loc, "plan", "issuer", nil, "hios", "id", "group"),
	})
	waitImport(t, pool, bID, jobs.StatusSucceeded)
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_sources`) != 2 {
		t.Fatal("sources")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_snapshots`) != 2 {
		t.Fatal("snapshots")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_snapshots WHERE consume_status = 'pending'`) != 1 {
		t.Fatal("pending snapshots")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_snapshots WHERE consume_status = 'blocked'`) != 1 {
		t.Fatal("blocked snapshots")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindConsumerIngest) != 1 {
		t.Fatal("ingest")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFDownload) != 1 {
		t.Fatal("download")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.plan_attachment_batches`) != 0 {
		t.Fatal("attachment")
	}
}

func TestIntegrationDistinctURLsAndParsedIngest(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ('https://example.test/parsed.json', DATE '2026-08-01', 'succeeded', 'succeeded')`); err != nil {
		t.Fatal(err)
	}
	tocID, _ := insertImportJob(t, pool, client, time.Time{})
	writeParsedRows(t, ws, tocID, "2026-08", []assocRow{
		validAssoc("https://example.test/A.json", "plan", "issuer", nil, "hios", "2", "group"),
		validAssoc("https://example.test/a.json", "plan", "issuer", nil, "hios", "1", "group"),
		validAssoc("https://example.test/a.json?x=1", "plan", "issuer", nil, "hios", "3", "group"),
		validAssoc("https://example.test/parsed.json", "plan", "issuer", nil, "hios", "4", "group"),
	})
	startImportRuntime(t, pool, ws, 8, nil)
	waitImport(t, pool, tocID, jobs.StatusSucceeded)
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_sources`) != 4 {
		t.Fatal("sources")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFDownload) != 3 {
		t.Fatal("new downloads")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindConsumerIngest) != 1 {
		t.Fatal("ingest")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_snapshots WHERE consume_status = 'blocked'`) != 3 {
		t.Fatal("blocked")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_snapshots WHERE consume_status = 'pending'`) != 1 {
		t.Fatal("pending")
	}
}

func TestIntegrationConcurrentUniqueness(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	var ids []int64
	for i := 0; i < 2; i++ {
		id, _ := insertImportJob(t, pool, client, time.Time{})
		writeParsedRows(t, ws, id, "2026-08", []assocRow{
			validAssoc("https://example.test/shared.json", "plan"+strconv.Itoa(i), "issuer", nil, "hios", strconv.Itoa(i), "group"),
		})
		ids = append(ids, id)
	}
	startImportRuntime(t, pool, ws, 8, nil)
	for _, id := range ids {
		waitImport(t, pool, id, jobs.StatusSucceeded)
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_sources`) != 1 {
		t.Fatal("sources")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFDownload) != 1 {
		t.Fatal("jobs")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_plans`) != 2 {
		t.Fatal("plans")
	}
}

func TestIntegrationConcurrentEINRace(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	s1, s2 := "Alpha", "Beta"
	var ids []int64
	for _, sponsor := range []string{s1, s2} {
		sp := sponsor
		id, _ := insertImportJob(t, pool, client, time.Time{})
		writeParsedRows(t, ws, id, "2026-08", []assocRow{
			validAssoc("https://example.test/ein.json", "plan", "issuer", &sp, "ein", "99-9", "group"),
		})
		ids = append(ids, id)
	}
	startImportRuntime(t, pool, ws, 8, nil)
	for _, id := range ids {
		waitImport(t, pool, id, jobs.StatusSucceeded)
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.mrf_plans`) != 1 {
		t.Fatal("plans")
	}
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.toc_mrf_plan_associations`) != 2 {
		t.Fatal("prov")
	}
	var sponsor string
	if err := pool.QueryRow(context.Background(), `SELECT plan_sponsor_name FROM mrfpipeline.mrf_plans`).Scan(&sponsor); err != nil {
		t.Fatal(err)
	}
	if sponsor != s1 && sponsor != s2 {
		t.Fatalf("sponsor %s", sponsor)
	}
}

func TestIntegrationQueueConcurrencyTwo(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	var current, max atomic.Int64
	var gate sync.WaitGroup
	gate.Add(1)
	var ids []int64
	for i := 0; i < 4; i++ {
		id, _ := insertImportJob(t, pool, client, time.Time{})
		writeParsedRows(t, ws, id, "2026-08", []assocRow{
			validAssoc("https://example.test/"+strconv.Itoa(i)+".json", "plan", "issuer", nil, "hios", strconv.Itoa(i), "group"),
		})
		ids = append(ids, id)
	}
	startImportRuntime(t, pool, ws, 8, func(w *Worker) {
		w.hold = func() {
			n := current.Add(1)
			for {
				old := max.Load()
				if n <= old || max.CompareAndSwap(old, n) {
					break
				}
			}
			gate.Wait()
			current.Add(-1)
		}
	})
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
		waitImport(t, pool, id, jobs.StatusSucceeded)
	}
}

func TestIntegrationTOCImportAfterConsumeCreatesBatch(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	loc := "https://files.test/toc-after/" + strconv.FormatInt(tocURLSeq.Add(1), 10) + ".json"
	tocID, _ := insertImportJob(t, pool, client, time.Time{})
	writeParsedTOC(t, ws, tocID, "2026-08", sampleTOC(loc, "A", "issuer", "hios", "1", "group", ""))
	startImportRuntime(t, pool, ws, 8, nil)
	waitImport(t, pool, tocID, jobs.StatusSucceeded)
	var snapID int64
	if err := pool.QueryRow(context.Background(), `SELECT id FROM mrfpipeline.mrf_snapshots`).Scan(&snapID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.mrf_snapshots
SET consume_status = 'succeeded', consume_river_job_id = 1, updated_at = transaction_timestamp()
WHERE id = $1`, snapID); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := planbatch.Schedule(context.Background(), tx, client, snapID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.plan_attachment_batches
SET status = 'succeeded', added_plan_count = 1, completed_at = transaction_timestamp()
WHERE mrf_snapshot_id = $1`, snapID); err != nil {
		t.Fatal(err)
	}
	toc2, _ := insertImportJob(t, pool, client, time.Time{})
	writeParsedTOC(t, ws, toc2, "2026-08", sampleTOC(loc, "C", "issuer", "hios", "3", "group", ""))
	waitImport(t, pool, toc2, jobs.StatusSucceeded)
	if count(t, pool, `SELECT count(*) FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id = $1`, snapID) != 2 {
		t.Fatal("second batch")
	}
	var name string
	if err := pool.QueryRow(context.Background(), `
SELECT p.plan_name FROM mrfpipeline.plan_attachment_batches b
JOIN mrfpipeline.plan_attachment_batch_items i ON i.plan_attachment_batch_id = b.id
JOIN mrfpipeline.mrf_plans p ON p.id = i.mrf_plan_id
WHERE b.mrf_snapshot_id = $1
ORDER BY b.id DESC LIMIT 1`, snapID).Scan(&name); err != nil || name != "C" {
		t.Fatalf("second plan %s", name)
	}
}

func TestIntegrationStaleJobNoOp(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	tocID, jobID := insertImportJob(t, pool, client, time.Time{})
	res, info, err := claimImport(context.Background(), pool, tocID, jobID+99)
	if err != nil || res.Action != jobs.ClaimNoop || info.payer != "" {
		t.Fatalf("%+v %+v %v", res, info, err)
	}
}
