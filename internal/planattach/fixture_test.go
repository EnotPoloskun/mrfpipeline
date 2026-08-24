package planattach

import (
	"context"
	"encoding/json"
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
	"github.com/enotpoloskun/mrfpipeline/internal/consumeringest"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/mrfparse"
	"github.com/enotpoloskun/mrfpipeline/internal/planbatch"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/parquet-go/parquet-go"
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

func insertConsumed(t *testing.T, pool *pgxpool.Pool) (sourceID, snapID int64) {
	t.Helper()
	url := "https://files.test/planattach/" + strconv.FormatInt(sourceURLSeq.Add(1), 10)
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('uhc', DATE '2026-08-01') ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ($1, DATE '2026-08-01', 'succeeded', 'succeeded') RETURNING id`, url).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
	INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_status, consume_river_job_id)
VALUES ($1, 'uhc', DATE '2026-08-01', 'succeeded', 1) RETURNING id`, sourceID).Scan(&snapID); err != nil {
		t.Fatal(err)
	}
	return sourceID, snapID
}

func insertPlan(t *testing.T, pool *pgxpool.Pool, snapID int64, name, idType, planID string, sponsor *string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_plans (
    mrf_snapshot_id, plan_name, issuer_name, plan_sponsor_name, plan_id_type, plan_id, plan_market_type
) VALUES ($1, $2, 'issuer', $3, $4, $5, 'group') RETURNING id`, snapID, name, sponsor, idType, planID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func scheduleBatch(t *testing.T, pool *pgxpool.Pool, client *river.Client[pgx.Tx], snapID int64) int64 {
	t.Helper()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := planbatch.Schedule(context.Background(), tx, client, snapID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	var batchID int64
	if err := pool.QueryRow(context.Background(), `
SELECT id FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id = $1 ORDER BY id DESC LIMIT 1`, snapID).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	return batchID
}

func mustWorkspace(t *testing.T) *artifact.Workspace {
	t.Helper()
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

func testdata(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "mrfparse", "testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func writeCatalog(t testing.TB, root string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "providers"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "provider_taxonomies"), 0700); err != nil {
		t.Fatal(err)
	}
	state, city, zip := "fl", "miami", "33101"
	type prow struct {
		NPI        string  `parquet:"npi"`
		State      *string `parquet:"state,optional"`
		City       *string `parquet:"city,optional"`
		PostalCode *string `parquet:"postal_code,optional"`
	}
	type trow struct {
		NPI          string `parquet:"npi"`
		TaxonomyCode string `parquet:"taxonomy_code"`
	}
	writeParquet(t, filepath.Join(root, "providers", "part-00000.parquet"), []prow{{
		NPI: "1111111111", State: &state, City: &city, PostalCode: &zip,
	}})
	writeParquet(t, filepath.Join(root, "provider_taxonomies", "part-00000.parquet"), []trow{{
		NPI: "1111111111", TaxonomyCode: "207Q00000X",
	}})
	man, err := json.Marshal(map[string]any{
		"schema_version": 1, "release_month": "2026-08",
		"providers":           map[string]any{"path": "providers", "rows": 1},
		"provider_taxonomies": map[string]any{"path": "provider_taxonomies", "rows": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), append(man, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func writeParquet[T any](t testing.TB, path string, rows []T) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[T](f)
	if _, err := w.Write(rows); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func mustCatalog(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "catalog")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	writeCatalog(t, root)
	return root
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
	source := data
	if resolved, rerr := filepath.EvalSymlinks(data); rerr == nil {
		source = resolved
	}
	if err := mrfparse.ValidateCompletedOutput(parsed, mrfparse.ExpectedSourceURI(source), services); err != nil {
		t.Fatal(err)
	}
}

func mustServices(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "services.csv")
	if err := os.WriteFile(path, testdata(t, "services.csv"), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func ingestWarehouse(t *testing.T, sourceID, snapID int64, ws *artifact.Workspace, catalog, warehouse, services string) {
	t.Helper()
	t.Setenv("TMPDIR", ws.StagingDir())
	writeRealParsed(t, ws, sourceID, services)
	parsed, err := ws.ParsedDir(artifact.KindMRF, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mrfconsumer.Ingest(context.Background(), mrfconsumer.Config{
		InputPath: parsed, ProviderCatalogPath: catalog, OutputPath: warehouse,
		PayerID:         "uhc",
		CollectionMonth: "2026-08", OutputID: consumeringest.FormatSnapshotOutputID(snapID),
	}); err != nil {
		t.Fatal(err)
	}
}

func startAttachRuntime(t *testing.T, pool *pgxpool.Pool, w *Worker, maxAttempts int) *river.Client[pgx.Tx] {
	t.Helper()
	workers := river.NewWorkers()
	river.AddWorker(workers, w)
	cfg := jobs.ClientConfig(workers, map[string]river.QueueConfig{jobs.QueueConsumer: {MaxWorkers: 1}}, nil, jobs.NewLogger(io.Discard))
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

func waitBatch(t *testing.T, pool *pgxpool.Pool, batchID int64, want string) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		err := pool.QueryRow(context.Background(), `SELECT status FROM mrfpipeline.plan_attachment_batches WHERE id = $1`, batchID).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for batch %d %s", batchID, want)
}
