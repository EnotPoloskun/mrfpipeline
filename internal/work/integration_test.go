package work

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
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
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

const workTOCJSON = `{"reporting_entity_name":"entity","reporting_entity_type":"issuer","version":"2.2.1","reporting_structure":[{"reporting_plans":[{"plan_name":"plan","issuer_name":"issuer","plan_id_type":"hios","plan_id":"id","plan_market_type":"group"}],"in_network_files":[{"description":"file","location":"https://example.test/a.json"}]}]}`

const workMRFJSON = `{"reporting_entity_name":"Example","reporting_entity_type":"issuer","last_updated_on":"2026-07-27","version":"2.2.1","in_network":[{"billing_code_type":"CPT","billing_code":"99213","negotiated_rates":[]}]}`

func TestIntegrationRuntimeDownloadsAndParses(t *testing.T) {
	pool := testDB(t)
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/a.json" {
			_, _ = w.Write([]byte(workMRFJSON))
			return
		}
		_, _ = w.Write([]byte(workTOCJSON))
	}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", ws.StagingDir())
	dl := artifact.NewTestDownloader(ws, jobs.NewProgress(jobs.NewLogger(io.Discard)),
		func(context.Context, string) ([]net.IP, error) {
			return []net.IP{net.ParseIP("203.0.113.10")}, nil
		},
		func(ctx context.Context, network, address string) (net.Conn, error) {
			var nd net.Dialer
			return nd.DialContext(ctx, "tcp", ts.Listener.Addr().String())
		},
	)
	client, err := jobs.NewInsertClient(context.Background(), pool, jobs.NewLogger(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	var runID, tocID int64
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month, mrf_source_target_kind)
VALUES ('uhc', DATE '2026-08-01', 'all')
ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal(err)
	}
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
) VALUES ('uhc', DATE '2026-08-01', 'https://files.test/data', $1, 'pending', 'blocked', 'blocked')
RETURNING id`, runID).Scan(&tocID); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	jobID, err := jobs.InsertTx(context.Background(), client, tx, &jobs.TOCDownloadArgs{TOCFileID: tocID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `
UPDATE mrfpipeline.toc_files SET download_river_job_id = $2 WHERE id = $1`, tocID, jobID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}

	svcPath := filepath.Join(t.TempDir(), "services.csv")
	if err := os.WriteFile(svcPath, []byte("billing_code_type,billing_code\nCPT,99213\n"), 0600); err != nil {
		t.Fatal(err)
	}
	catPath := filepath.Join(t.TempDir(), "catalog")
	if err := os.Mkdir(catPath, 0700); err != nil {
		t.Fatal(err)
	}
	writeWorkCatalog(t, catPath)
	warehouse := filepath.Join(t.TempDir(), "warehouse")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 3)
	go func() {
		done <- Runtime{
			Role: "control", ResidentCapacity: 2, Pool: pool, Workspace: ws, Downloader: dl,
			Logger: jobs.NewLogger(io.Discard), ServicesPath: svcPath, WarehousePath: warehouse,
			ProviderCatalogPath: catPath,
		}.Run(ctx)
	}()
	go func() {
		done <- Runtime{
			Role: "mrf", Pool: pool, Workspace: ws, Downloader: dl,
			Logger: jobs.NewLogger(io.Discard), ServicesPath: svcPath,
		}.Run(ctx)
	}()
	go func() {
		done <- Runtime{
			Role: "consumer", Pool: pool, Workspace: ws,
			Logger: jobs.NewLogger(io.Discard), ServicesPath: svcPath, WarehousePath: warehouse,
			ProviderCatalogPath: catPath,
		}.Run(ctx)
	}()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		var status, parse, imp, mrfDL, mrfParse, consume, ingestState, attachState, batch string
		err := pool.QueryRow(context.Background(), `
SELECT t.download_status, t.parse_status, t.import_status,
       COALESCE(s.download_status, ''), COALESCE(s.parse_status, ''),
       COALESCE(n.consume_status, ''),
       COALESCE((SELECT state FROM mrfpipeline_river.river_job WHERE kind = $2 LIMIT 1), ''),
       COALESCE((SELECT state FROM mrfpipeline_river.river_job WHERE kind = $3 LIMIT 1), ''),
       COALESCE((SELECT status FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id = n.id LIMIT 1), '')
FROM mrfpipeline.toc_files t
LEFT JOIN mrfpipeline.mrf_sources s ON true
LEFT JOIN mrfpipeline.mrf_snapshots n ON n.mrf_source_id = s.id
WHERE t.id = $1`, tocID, jobs.KindConsumerIngest, jobs.KindConsumerAttachPlans).Scan(
			&status, &parse, &imp, &mrfDL, &mrfParse, &consume, &ingestState, &attachState, &batch)
		if err == nil && status == jobs.StatusSucceeded && parse == jobs.StatusSucceeded && imp == jobs.StatusSucceeded &&
			mrfDL == jobs.StatusSucceeded && mrfParse == jobs.StatusSucceeded &&
			consume == jobs.StatusSucceeded && ingestState == "completed" &&
			attachState == "completed" && batch == jobs.StatusSucceeded {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	var status, parse, imp, mrfDL, mrfParse, consume string
	if err := pool.QueryRow(context.Background(), `
SELECT t.download_status, t.parse_status, t.import_status,
       s.download_status, s.parse_status, n.consume_status
FROM mrfpipeline.toc_files t, mrfpipeline.mrf_sources s, mrfpipeline.mrf_snapshots n
WHERE t.id = $1`, tocID).Scan(&status, &parse, &imp, &mrfDL, &mrfParse, &consume); err != nil {
		cancel()
		t.Fatal(err)
	}
	if status != jobs.StatusSucceeded || parse != jobs.StatusSucceeded || imp != jobs.StatusSucceeded ||
		mrfDL != jobs.StatusSucceeded || mrfParse != jobs.StatusSucceeded || consume != jobs.StatusSucceeded {
		cancel()
		t.Fatalf("toc %s %s %s mrf %s %s consume %s", status, parse, imp, mrfDL, mrfParse, consume)
	}
	var sources, parses, ingests, batches, pending, blocked, attaches int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.mrf_sources`).Scan(&sources); err != nil || sources != 1 {
		cancel()
		t.Fatalf("sources %d %v", sources, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFParse).Scan(&parses); err != nil || parses != 1 {
		cancel()
		t.Fatalf("parses %d %v", parses, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1 AND state = 'completed'`, jobs.KindConsumerIngest).Scan(&ingests); err != nil || ingests != 1 {
		cancel()
		t.Fatalf("ingests %d %v", ingests, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1 AND state = 'completed'`, jobs.KindConsumerAttachPlans).Scan(&attaches); err != nil || attaches != 1 {
		cancel()
		t.Fatalf("attach %d %v", attaches, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.plan_attachment_batches WHERE status = 'succeeded'`).Scan(&batches); err != nil || batches != 1 {
		cancel()
		t.Fatalf("batches %d %v", batches, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.mrf_snapshots WHERE consume_status = 'pending'`).Scan(&pending); err != nil || pending != 0 {
		cancel()
		t.Fatalf("pending snapshots %d %v", pending, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.mrf_snapshots WHERE consume_status = 'blocked'`).Scan(&blocked); err != nil || blocked != 0 {
		cancel()
		t.Fatalf("blocked snapshots %d %v", blocked, err)
	}
	var sourceID, snapID int64
	if err := pool.QueryRow(context.Background(), `SELECT s.id, n.id FROM mrfpipeline.mrf_sources s, mrfpipeline.mrf_snapshots n`).Scan(&sourceID, &snapID); err != nil {
		cancel()
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(ws.Root, "mrf", "mrf-source-"+strconv.FormatInt(sourceID, 10), "download")); !os.IsNotExist(err) {
		cancel()
		t.Fatal("download leaf remained")
	}
	if _, err := os.Lstat(filepath.Join(ws.Root, "mrf", "mrf-source-"+strconv.FormatInt(sourceID, 10), "parsed", "manifest.json")); err != nil {
		cancel()
		t.Fatal("parsed output missing")
	}
	final := filepath.Join(warehouse, "snapshots", "collection_month=2026-08", "payer_id=uhc", "output_id=mrf-"+strconv.FormatInt(snapID, 10))
	for _, name := range []string{"rate_facts", "rate_provider_groups", "provider_groups", "provider_group_memberships", "ingestions", "network_names", "manifest.json"} {
		if _, err := os.Lstat(filepath.Join(final, name)); err != nil {
			cancel()
			t.Fatalf("snapshot %s: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(final, "plans")); !os.IsNotExist(err) {
		cancel()
		t.Fatal("snapshot plans")
	}
	cancel()
	for i := 0; i < 3; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("shutdown")
		}
	}
}
