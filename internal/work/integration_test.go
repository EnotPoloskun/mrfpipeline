package work

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

func TestIntegrationRuntimeDownloadsAndLeavesParse(t *testing.T) {
	pool := testDB(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("from-work"))
	}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
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
) VALUES ('uhc', DATE '2026-08-01', 'http://files.test/data', $1, 'pending', 'blocked', 'blocked')
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

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Runtime{Pool: pool, Workspace: ws, Downloader: dl, Logger: jobs.NewLogger(io.Discard)}.Run(ctx)
	}()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var status, parse string
		err := pool.QueryRow(context.Background(), `
SELECT download_status, parse_status FROM mrfpipeline.toc_files WHERE id = $1`, tocID).Scan(&status, &parse)
		if err == nil && status == jobs.StatusSucceeded && parse == jobs.StatusPending {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	var status, parse, kind, state string
	if err := pool.QueryRow(context.Background(), `
SELECT download_status, parse_status FROM mrfpipeline.toc_files WHERE id = $1`, tocID).Scan(&status, &parse); err != nil {
		t.Fatal(err)
	}
	if status != jobs.StatusSucceeded || parse != jobs.StatusPending {
		cancel()
		t.Fatalf("download=%s parse=%s", status, parse)
	}
	if err := pool.QueryRow(context.Background(), `
SELECT kind, state FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindTOCParse).Scan(&kind, &state); err != nil {
		cancel()
		t.Fatal(err)
	}
	if kind != jobs.KindTOCParse || state == "completed" || state == "discarded" {
		cancel()
		t.Fatalf("parse consumed %s %s", kind, state)
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
