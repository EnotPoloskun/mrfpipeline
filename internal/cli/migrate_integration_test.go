package cli

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/enotpoloskun/mrfpipeline/internal/config"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testMigrateEnv(t *testing.T) func(string) string {
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
		t.Fatal("reset schema")
	}
	t.Cleanup(func() {
		_ = resetPipelineSchemas(context.Background(), pool)
	})
	return envMap(map[string]string{config.EnvDatabaseURL: raw})
}

func resetPipelineSchemas(ctx context.Context, pool *pgxpool.Pool) error {
	for _, schema := range []string{database.RiverSchema, database.ApplicationSchema, "mrfpipeline_test"} {
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			return err
		}
	}
	return nil
}

func TestIntegrationMigrateCommand(t *testing.T) {
	env := testMigrateEnv(t)
	code, stdout, stderr := runCLI(context.Background(), []string{"migrate"}, env)
	if code != 0 {
		t.Fatalf("exit %d stderr=%q", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr %q", stderr)
	}
	if stdout != "{\"application_version\":5,\"applied_migration_count\":5,\"river_version\":6,\"applied_river_migration_count\":6}\n" {
		t.Fatalf("stdout %q", stdout)
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &obj); err != nil {
		t.Fatal(err)
	}
	if len(obj) != 4 {
		t.Fatalf("fields %v", obj)
	}

	code, stdout, stderr = runCLI(context.Background(), []string{"migrate"}, env)
	if code != 0 || stderr != "" {
		t.Fatalf("repeat exit %d stderr=%q", code, stderr)
	}
	if stdout != "{\"application_version\":5,\"applied_migration_count\":0,\"river_version\":6,\"applied_river_migration_count\":0}\n" {
		t.Fatalf("repeat stdout %q", stdout)
	}

	fail := Run(context.Background(), []string{"migrate"}, env, failWriter{}, io.Discard)
	if fail != 1 {
		t.Fatalf("post-commit write failure exit %d", fail)
	}
}
