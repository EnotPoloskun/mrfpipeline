package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/config"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/discovery"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testDiscoverEnv(t *testing.T) (func(string) string, *pgxpool.Pool) {
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
	pool, err := pgxpool.New(context.Background(), raw)
	if err != nil {
		t.Fatal("connect test database")
	}
	t.Cleanup(pool.Close)
	database.TestDBMu.Lock()
	t.Cleanup(database.TestDBMu.Unlock)
	if err := resetPipelineSchemas(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resetPipelineSchemas(context.Background(), pool) })
	if _, err := database.Migrate(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	return envMap(map[string]string{config.EnvDatabaseURL: raw}), pool
}

func TestIntegrationDiscoverCommand(t *testing.T) {
	env, pool := testDiscoverEnv(t)
	code, stdout, stderr := runCLI(context.Background(), discoverArgs("5"), env)
	if code != 0 {
		t.Fatalf("exit %d stderr=%q", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr %q", stderr)
	}
	var r discovery.Report
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &r); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(stdout, "\n") || strings.Contains(stdout, " ") {
		t.Fatalf("compact report %q", stdout)
	}
	if r.PayerID != "uhc" || r.CollectionMonth != "2026-08" || r.TOCLimit != 5 || r.DiscoveryRunID <= 0 || r.RiverJobID <= 0 {
		t.Fatalf("%+v", r)
	}
	if strings.Contains(stdout, "null") {
		t.Fatalf("null in report %q", stdout)
	}

	code, stdout2, stderr := runCLI(context.Background(), discoverArgs("5"), env)
	if code != 0 || stderr != "" {
		t.Fatalf("second exit %d stderr=%q", code, stderr)
	}
	var r2 discovery.Report
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout2)), &r2); err != nil {
		t.Fatal(err)
	}
	if r2.DiscoveryRunID == r.DiscoveryRunID {
		t.Fatal("second invocation reused the run")
	}

	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.discovery_runs`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("runs %d %v", n, err)
	}

	var buf bytes.Buffer
	code = Run(context.Background(), discoverArgs("2"), env, failWriter{}, &buf)
	if code != 1 {
		t.Fatalf("write failure exit %d", code)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.discovery_runs`).Scan(&n); err != nil || n != 3 {
		t.Fatalf("write failure created extra runs: %d", n)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE kind = $1`, jobs.KindDiscoveryRun).Scan(&n); err != nil || n != 3 {
		t.Fatalf("write failure jobs %d", n)
	}
}

func TestIntegrationWorkOrderlyShutdown(t *testing.T) {
	_, pool := testDiscoverEnv(t)
	oldTmp := os.Getenv("TMPDIR")
	t.Cleanup(func() { _ = os.Setenv("TMPDIR", oldTmp) })
	base := t.TempDir()
	if err := os.MkdirAll(filepath.Join(base, "catalog"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "catalog", "manifest.json"), []byte(`{"schema_version":1,"release_month":"2026-08"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "services.csv"), []byte("service_id\n"), 0600); err != nil {
		t.Fatal(err)
	}
	getenv := envMap(map[string]string{
		config.EnvDatabaseURL:         os.Getenv("MRFPIPELINE_TEST_DATABASE_URL"),
		config.EnvArtifactRoot:        filepath.Join(base, "artifacts"),
		config.EnvWarehousePath:       filepath.Join(base, "warehouse"),
		config.EnvProviderCatalogPath: filepath.Join(base, "catalog"),
		config.EnvServicesPath:        filepath.Join(base, "services.csv"),
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	var stdout, stderr bytes.Buffer
	go func() {
		done <- Run(ctx, []string{"work", "--role", "control"}, getenv, &stdout, &stderr)
	}()
	deadline := time.Now().Add(10 * time.Second)
	started := false
	for time.Now().Before(deadline) {
		var ready bool
		if err := pool.QueryRow(context.Background(), `
SELECT EXISTS (
    SELECT 1 FROM pg_catalog.pg_locks
    WHERE locktype = 'advisory' AND classid = $1 AND objid = $2 AND granted
) AND EXISTS (
    SELECT 1 FROM mrfpipeline_river.river_queue
)`, database.WorkerLeaseClass, database.WorkerLeaseObject).Scan(&ready); err == nil && ready {
			started = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !started {
		cancel()
		t.Fatal("work client did not start")
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("orderly shutdown exit %d", code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("work did not stop")
	}
}
