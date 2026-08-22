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

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Runtime{
			Pool: pool, Workspace: ws, Downloader: dl, Logger: jobs.NewLogger(io.Discard), ServicesPath: svcPath,
		}.Run(ctx)
	}()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var status, parse, imp, mrfDL, mrfParse, parseState string
		err := pool.QueryRow(context.Background(), `
SELECT t.download_status, t.parse_status, t.import_status,
       COALESCE(s.download_status, ''), COALESCE(s.parse_status, ''),
       COALESCE((SELECT state FROM mrfpipeline_river.river_job WHERE kind = $2 LIMIT 1), '')
FROM mrfpipeline.toc_files t
LEFT JOIN mrfpipeline.mrf_sources s ON true
WHERE t.id = $1`, tocID, jobs.KindMRFParse).Scan(&status, &parse, &imp, &mrfDL, &mrfParse, &parseState)
		if err == nil && status == jobs.StatusSucceeded && parse == jobs.StatusSucceeded && imp == jobs.StatusSucceeded &&
			mrfDL == jobs.StatusSucceeded && mrfParse == jobs.StatusSucceeded && parseState == "completed" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	var status, parse, imp, mrfDL, mrfParse string
	if err := pool.QueryRow(context.Background(), `
SELECT t.download_status, t.parse_status, t.import_status,
       s.download_status, s.parse_status
FROM mrfpipeline.toc_files t, mrfpipeline.mrf_sources s
WHERE t.id = $1`, tocID).Scan(&status, &parse, &imp, &mrfDL, &mrfParse); err != nil {
		cancel()
		t.Fatal(err)
	}
	if status != jobs.StatusSucceeded || parse != jobs.StatusSucceeded || imp != jobs.StatusSucceeded ||
		mrfDL != jobs.StatusSucceeded || mrfParse != jobs.StatusSucceeded {
		cancel()
		t.Fatalf("toc %s %s %s mrf %s %s", status, parse, imp, mrfDL, mrfParse)
	}
	var sources, parses, ingests, batches, pending, blocked int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.mrf_sources`).Scan(&sources); err != nil || sources != 1 {
		cancel()
		t.Fatalf("sources %d %v", sources, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFParse).Scan(&parses); err != nil || parses != 1 {
		cancel()
		t.Fatalf("parses %d %v", parses, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1 AND state <> 'completed' AND state <> 'discarded'`, jobs.KindConsumerIngest).Scan(&ingests); err != nil || ingests != 1 {
		cancel()
		t.Fatalf("ingests %d %v", ingests, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.plan_attachment_batches`).Scan(&batches); err != nil || batches != 0 {
		cancel()
		t.Fatalf("batches %d %v", batches, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.mrf_snapshots WHERE consume_status = 'pending'`).Scan(&pending); err != nil || pending != 1 {
		cancel()
		t.Fatalf("pending snapshots %d %v", pending, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.mrf_snapshots WHERE consume_status = 'blocked'`).Scan(&blocked); err != nil || blocked != 0 {
		cancel()
		t.Fatalf("blocked snapshots %d %v", blocked, err)
	}
	var sourceID int64
	if err := pool.QueryRow(context.Background(), `SELECT id FROM mrfpipeline.mrf_sources`).Scan(&sourceID); err != nil {
		cancel()
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(ws.Root, "mrf", "mrf-source-"+strconv.FormatInt(sourceID, 10), "download")); !os.IsNotExist(err) {
		cancel()
		t.Fatal("download leaf remained")
	}
	var parseState string
	if err := pool.QueryRow(context.Background(), `
SELECT state FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindMRFParse).Scan(&parseState); err != nil {
		cancel()
		t.Fatal(err)
	}
	if parseState != "completed" {
		cancel()
		t.Fatalf("mrf parse %s", parseState)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("shutdown")
	}
}
