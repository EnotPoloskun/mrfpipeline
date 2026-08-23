package consumeringest

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
	"github.com/enotpoloskun/mrfconsumer"
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

var sourceURLSeq atomic.Int64

func insertClient(t *testing.T, pool *pgxpool.Pool) *river.Client[pgx.Tx] {
	t.Helper()
	client, err := jobs.NewInsertClient(context.Background(), pool, jobs.NewLogger(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func insertIngestJob(t *testing.T, pool *pgxpool.Pool, client *river.Client[pgx.Tx], month string) (sourceID, snapID, jobID int64) {
	t.Helper()
	url := "https://files.test/mrf/" + strconv.FormatInt(sourceURLSeq.Add(1), 10)
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_sources (source_url, download_status, parse_status)
VALUES ($1, 'succeeded', 'succeeded')
RETURNING id`, url).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	var feedID int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_feeds (payer_id, feed_id)
VALUES ('uhc', $1)
RETURNING id`, "mrf-source-"+strconv.FormatInt(sourceID, 10)).Scan(&feedID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, mrf_feed_id, collection_month, consume_status)
VALUES ($1, $2, $3::date, 'pending')
RETURNING id`, sourceID, feedID, month).Scan(&snapID); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	jobID, err = jobs.InsertTx(context.Background(), client, tx, &jobs.ConsumerIngestArgs{MRFSnapshotID: snapID})
	if err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `
UPDATE mrfpipeline.mrf_snapshots SET consume_river_job_id = $2, updated_at = transaction_timestamp()
WHERE id = $1`, snapID, jobID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	return sourceID, snapID, jobID
}

func startIngestRuntime(t *testing.T, pool *pgxpool.Pool, w *Worker, maxAttempts int) *river.Client[pgx.Tx] {
	t.Helper()
	workers := river.NewWorkers()
	river.AddWorker(workers, w)
	cfg := jobs.ClientConfig(workers, map[string]river.QueueConfig{jobs.QueueConsumer: {MaxWorkers: 1}}, nil, jobs.NewLogger(io.Discard))
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

func waitConsume(t *testing.T, pool *pgxpool.Pool, snapID int64, want string) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		err := pool.QueryRow(context.Background(), `SELECT consume_status FROM mrfpipeline.mrf_snapshots WHERE id = $1`, snapID).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for snapshot %d consume %s", snapID, want)
}

func writeRealParsed(t *testing.T, ws *artifact.Workspace, sourceID int64, services string) {
	t.Helper()
	data, err := ws.DownloadDataPath(artifact.KindMRF, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ws.ParsedDir(artifact.KindMRF, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(data), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(data, testdata(t, "mrf.json"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := mrfparser.DefaultConfig()
	cfg.Input = data
	cfg.Output = parsed
	cfg.Services = services
	cfg.TempDir = ws.StagingDir()
	if err := mrfparser.Parse(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationFirstIngestPublishesSnapshot(t *testing.T) {
	pool := testDB(t)
	ws := mustWorkspace(t)
	t.Setenv("TMPDIR", ws.StagingDir())
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	client := insertClient(t, pool)
	sourceID, snapID, _ := insertIngestJob(t, pool, client, "2026-08-01")
	writeRealParsed(t, ws, sourceID, svc)
	startIngestRuntime(t, pool, &Worker{
		Pool: pool, Workspace: ws, WarehousePath: warehouse, ServicesPath: svc, Catalog: cat,
		Logger: jobs.NewLogger(io.Discard),
	}, 8)
	waitConsume(t, pool, snapID, jobs.StatusSucceeded)
	outputID := formatSnapshotOutputID(snapID)
	final, err := expectedFinalPath(warehouse, "uhc", "2026-08", outputID)
	if err != nil {
		t.Fatal(err)
	}
	wh, err := InspectWarehouse(warehouse)
	if err != nil || wh.Kind != warehouseRecognized {
		t.Fatalf("%+v %v", wh, err)
	}
	if err := inspectCompletedSnapshot(warehouse, "uhc", "mrf-source-"+strconv.FormatInt(sourceID, 10), "2026-08", outputID, wh.Catalog); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(final, "plans")); !os.IsNotExist(err) {
		t.Fatal("snapshot plans")
	}
	if _, err := os.Stat(filepath.Join(warehouse, "provider_catalog", fileManifest)); err != nil {
		t.Fatal("catalog not pinned")
	}
	if _, err := os.Stat(filepath.Join(warehouse, "plan_associations", "_schema", "part-00000.parquet")); err != nil {
		t.Fatal("seed missing")
	}
	var batches, plans, attaches int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.plan_attachment_batches`).Scan(&batches); err != nil || batches != 0 {
		t.Fatalf("batches %d", batches)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.mrf_plans`).Scan(&plans); err != nil || plans != 0 {
		t.Fatalf("plans %d", plans)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindConsumerAttachPlans).Scan(&attaches); err != nil || attaches != 0 {
		t.Fatalf("attach %d", attaches)
	}
}

func TestIntegrationRecognizeSkipsSecondIngest(t *testing.T) {
	pool := testDB(t)
	ws := mustWorkspace(t)
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	client := insertClient(t, pool)
	sourceID, snapID, jobID := insertIngestJob(t, pool, client, "2026-08-01")
	writeValidParsed(t, ws, sourceID, svc)
	var calls atomic.Int64
	w := &Worker{Workspace: ws, WarehousePath: warehouse, ServicesPath: svc, Catalog: cat,
		Ingest: func(ctx context.Context, cfg mrfconsumer.Config) (mrfconsumer.Report, error) {
			calls.Add(1)
			writePublishedSnapshot(t, warehouse, cfg.PayerID, cfg.FeedID, cfg.CollectionMonth, cfg.OutputID)
			final, _ := expectedFinalPath(warehouse, cfg.PayerID, cfg.CollectionMonth, cfg.OutputID)
			return mrfconsumer.Report{OutputID: cfg.OutputID, FinalPath: final}, nil
		}}
	res, ident, err := claimIngest(context.Background(), pool, snapID, jobID)
	if err != nil || res.Action != jobs.ClaimWork {
		t.Fatalf("%+v %v", res, err)
	}
	if err := w.ingest(context.Background(), ingestJob(jobID, snapID), ident); err != nil {
		t.Fatal(err)
	}
	once := true
	if err := jobs.Succeed(context.Background(), pool, client, jobs.ConsumerIngestStage, snapID, jobID, nil, func(context.Context, pgx.Tx) error {
		if once {
			once = false
			return errors.New("rollback")
		}
		return nil
	}, nil); err == nil {
		t.Fatal("expected rollback")
	}
	if err := w.ingest(context.Background(), ingestJob(jobID, snapID), ident); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls %d", calls.Load())
	}
	if err := jobs.Succeed(context.Background(), pool, client, jobs.ConsumerIngestStage, snapID, jobID, nil, func(ctx context.Context, tx pgx.Tx) error {
		return confirmIngestSuccess(ctx, tx, client, ident)
	}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationTwoSnapshotsSerial(t *testing.T) {
	pool := testDB(t)
	ws := mustWorkspace(t)
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	client := insertClient(t, pool)
	s1, snap1, _ := insertIngestJob(t, pool, client, "2026-08-01")
	s2, snap2, _ := insertIngestJob(t, pool, client, "2026-09-01")
	writeValidParsed(t, ws, s1, svc)
	writeValidParsed(t, ws, s2, svc)
	var inflight atomic.Int64
	var overlap atomic.Int64
	startIngestRuntime(t, pool, &Worker{
		Pool: pool, Workspace: ws, WarehousePath: warehouse, ServicesPath: svc, Catalog: cat,
		Logger: jobs.NewLogger(io.Discard),
		Ingest: func(ctx context.Context, cfg mrfconsumer.Config) (mrfconsumer.Report, error) {
			if inflight.Add(1) > 1 {
				overlap.Add(1)
			}
			defer inflight.Add(-1)
			writePublishedSnapshot(t, warehouse, cfg.PayerID, cfg.FeedID, cfg.CollectionMonth, cfg.OutputID)
			final, _ := expectedFinalPath(warehouse, cfg.PayerID, cfg.CollectionMonth, cfg.OutputID)
			return mrfconsumer.Report{OutputID: cfg.OutputID, FinalPath: final}, nil
		},
	}, 8)
	waitConsume(t, pool, snap1, jobs.StatusSucceeded)
	waitConsume(t, pool, snap2, jobs.StatusSucceeded)
	if overlap.Load() != 0 {
		t.Fatal("overlapping ingest")
	}
}

func TestIntegrationEighthFailureLeavesParse(t *testing.T) {
	pool := testDB(t)
	ws := mustWorkspace(t)
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	if err := os.Mkdir(warehouse, 0700); err != nil {
		t.Fatal(err)
	}
	client := insertClient(t, pool)
	sourceID, snapID, _ := insertIngestJob(t, pool, client, "2026-08-01")
	writeValidParsed(t, ws, sourceID, svc)
	startIngestRuntime(t, pool, &Worker{
		Pool: pool, Workspace: ws, WarehousePath: warehouse, ServicesPath: svc, Catalog: cat,
		Logger: jobs.NewLogger(io.Discard),
		Ingest: func(context.Context, mrfconsumer.Config) (mrfconsumer.Report, error) {
			return mrfconsumer.Report{}, errors.Join(mrfconsumer.ErrOutput, errors.New("hostile /tmp/catalog"))
		},
	}, 1)
	waitConsume(t, pool, snapID, jobs.StatusFailed)
	var fail *string
	var parse string
	if err := pool.QueryRow(context.Background(), `SELECT failure_code FROM mrfpipeline.mrf_snapshots WHERE id = $1`, snapID).Scan(&fail); err != nil {
		t.Fatal(err)
	}
	if fail == nil || *fail != jobs.FailureConsumerIngestOutputFailed {
		t.Fatalf("fail %v", fail)
	}
	if err := pool.QueryRow(context.Background(), `SELECT parse_status FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&parse); err != nil {
		t.Fatal(err)
	}
	if parse != jobs.StatusSucceeded {
		t.Fatalf("parse %s", parse)
	}
	var batches int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.plan_attachment_batches`).Scan(&batches); err != nil || batches != 0 {
		t.Fatalf("batches %d", batches)
	}
}

func TestIntegrationCancelLeavesNoTarget(t *testing.T) {
	ws := mustWorkspace(t)
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	if err := os.Mkdir(warehouse, 0700); err != nil {
		t.Fatal(err)
	}
	writeValidParsed(t, ws, 11, svc)
	w := &Worker{Workspace: ws, WarehousePath: warehouse, ServicesPath: svc, Catalog: cat,
		Ingest: func(ctx context.Context, cfg mrfconsumer.Config) (mrfconsumer.Report, error) {
			return mrfconsumer.Report{}, context.Canceled
		}}
	err := w.ingest(context.Background(), ingestJob(1, 11), claimIdentity{
		SnapshotID: 11, SourceID: 11, FeedRowID: 1, PayerID: "uhc", FeedID: "mrf-source-11",
		Month: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), MonthText: "2026-08", ConsumeJobID: 1,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	final, _ := expectedFinalPath(warehouse, "uhc", "2026-08", "mrf-11")
	if _, err := os.Lstat(final); !os.IsNotExist(err) {
		t.Fatal("target created")
	}
}

func TestIntegrationIngestWithPlansCreatesBatch(t *testing.T) {
	pool := testDB(t)
	ws := mustWorkspace(t)
	t.Setenv("TMPDIR", ws.StagingDir())
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	client := insertClient(t, pool)
	sourceID, snapID, _ := insertIngestJob(t, pool, client, "2026-08-01")
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.mrf_plans (
    mrf_snapshot_id, plan_name, issuer_name, plan_sponsor_name, plan_id_type, plan_id, plan_market_type
) VALUES ($1, 'plan', 'issuer', NULL, 'hios', 'id', 'group')`, snapID); err != nil {
		t.Fatal(err)
	}
	writeRealParsed(t, ws, sourceID, svc)
	startIngestRuntime(t, pool, &Worker{
		Pool: pool, Workspace: ws, WarehousePath: warehouse, ServicesPath: svc, Catalog: cat,
		Logger: jobs.NewLogger(io.Discard),
	}, 8)
	waitConsume(t, pool, snapID, jobs.StatusSucceeded)
	var batches, items, attaches int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id = $1`, snapID).Scan(&batches); err != nil || batches != 1 {
		t.Fatalf("batches %d", batches)
	}
	if err := pool.QueryRow(context.Background(), `SELECT requested_plan_count FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id = $1`, snapID).Scan(&items); err != nil || items != 1 {
		t.Fatalf("items %d", items)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindConsumerAttachPlans).Scan(&attaches); err != nil || attaches != 1 {
		t.Fatalf("attach %d", attaches)
	}
}

func TestIntegrationCorruptOwnedCatalogFailsClosed(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	parsed, _ := mustRealParsed(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	cfg := mrfconsumer.Config{
		InputPath: parsed, ProviderCatalogPath: cat.Path, OutputPath: warehouse,
		PayerID: "uhc", FeedID: "mrf-source-1", CollectionMonth: "2026-08", OutputID: "mrf-1",
	}
	if _, err := mrfconsumer.Ingest(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(warehouse, "provider_catalog", fileManifest), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg.OutputID = "mrf-2"
	_, err := mrfconsumer.Ingest(context.Background(), cfg)
	if !errors.Is(err, mrfconsumer.ErrOutput) {
		t.Fatalf("got %v", err)
	}
	if !jobs.IsFailure(mapWorkError(err), jobs.FailureConsumerIngestOutputFailed) {
		t.Fatalf("map %v", mapWorkError(err))
	}
}
