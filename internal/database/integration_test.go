package database

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const testDatabaseURLEnv = "MRFPIPELINE_TEST_DATABASE_URL"

func testDatabaseURL(t *testing.T) string {
	t.Helper()
	raw := os.Getenv(testDatabaseURLEnv)
	if raw == "" {
		t.Skip(testDatabaseURLEnv + " is not set")
	}
	cfg, err := pgxpool.ParseConfig(raw)
	if err != nil {
		t.Fatal("invalid test database url")
	}
	if !strings.HasPrefix(cfg.ConnConfig.Database, "mrfpipeline_test_") {
		t.Fatal("test database name must start with mrfpipeline_test_")
	}
	return raw
}

func openTestPool(t *testing.T, url string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal("connect test database")
	}
	t.Cleanup(pool.Close)
	return pool
}

func resetSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if err := resetPipelineSchemas(context.Background(), pool); err != nil {
		t.Fatal("reset schema")
	}
}

func withTestDB(t *testing.T) (string, *pgxpool.Pool) {
	t.Helper()
	url := testDatabaseURL(t)
	TestDBMu.Lock()
	pool := openTestPool(t, url)
	resetSchema(t, pool)
	t.Cleanup(func() {
		resetSchema(t, pool)
		TestDBMu.Unlock()
	})
	return url, pool
}

func mustMigrate(t *testing.T, url string) Result {
	t.Helper()
	result, err := Migrate(context.Background(), url)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return result
}

func TestIntegrationMigrateFreshAndRepeat(t *testing.T) {
	url, pool := withTestDB(t)
	first := mustMigrate(t, url)
	if first.ApplicationVersion != 7 || first.AppliedMigrationCount != 7 {
		t.Fatalf("first %+v", first)
	}
	if first.RiverVersion != ExpectedRiverVersion || first.AppliedRiverMigrationCount != ExpectedRiverVersion {
		t.Fatalf("first river %+v", first)
	}
	second := mustMigrate(t, url)
	if second.ApplicationVersion != 7 || second.AppliedMigrationCount != 0 {
		t.Fatalf("second %+v", second)
	}
	if second.RiverVersion != ExpectedRiverVersion || second.AppliedRiverMigrationCount != 0 {
		t.Fatalf("second river %+v", second)
	}

	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.schema_migrations`).Scan(&n); err != nil {
		t.Fatal("count ledger")
	}
	if n != 7 {
		t.Fatalf("ledger rows %d", n)
	}

	required := []string{
		"schema_migrations",
		"monthly_releases",
		"discovery_runs",
		"toc_files",
		"discovery_run_toc_files",
		"mrf_sources",
		"mrf_snapshots",
		"toc_mrf_plan_associations",
		"mrf_plans",
		"plan_attachment_batches",
		"plan_attachment_batch_items",
		"monthly_release_mrf_sources",
		"mrf_materialization_slots",
		"pipeline_runtime",
		"control_schedule_events",
	}
	for _, table := range required {
		var exists bool
		err := pool.QueryRow(context.Background(), `
SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'mrfpipeline' AND table_name = $1
)`, table).Scan(&exists)
		if err != nil || !exists {
			t.Fatalf("missing table %s: %v", table, err)
		}
	}
	var feedTable bool
	if err := pool.QueryRow(context.Background(), `
SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'mrfpipeline' AND table_name = 'mrf_feeds'
)`).Scan(&feedTable); err != nil {
		t.Fatal(err)
	}
	if feedTable {
		t.Fatal("feed table remains")
	}
	var feedColumns int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*)
FROM information_schema.columns
WHERE table_schema = 'mrfpipeline' AND column_name IN ('feed_id', 'mrf_feed_id')`).Scan(&feedColumns); err != nil {
		t.Fatal(err)
	}
	if feedColumns != 0 {
		t.Fatalf("feed columns remain: %d", feedColumns)
	}
	var feedConstraints int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*)
FROM pg_constraint c
JOIN pg_namespace n ON n.oid = c.connamespace
WHERE n.nspname = 'mrfpipeline'
  AND (c.conname ILIKE '%feed%' OR pg_get_constraintdef(c.oid) ILIKE '%feed%')`).Scan(&feedConstraints); err != nil {
		t.Fatal(err)
	}
	if feedConstraints != 0 {
		t.Fatalf("feed constraints remain: %d", feedConstraints)
	}
	var feedIndexes int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*)
FROM pg_indexes
WHERE schemaname = 'mrfpipeline' AND indexname ILIKE '%feed%'`).Scan(&feedIndexes); err != nil {
		t.Fatal(err)
	}
	if feedIndexes != 0 {
		t.Fatalf("feed indexes remain: %d", feedIndexes)
	}

	indexes := []string{
		"discovery_runs_status_idx",
		"toc_files_download_status_idx",
		"toc_files_parse_status_idx",
		"toc_files_import_status_idx",
		"mrf_sources_download_status_idx",
		"mrf_sources_parse_status_idx",
		"mrf_snapshots_mrf_source_id_idx",
		"mrf_snapshots_consume_status_idx",
		"plan_attachment_batches_mrf_snapshot_id_idx",
		"plan_attachment_batches_status_idx",
		"mrf_plans_mrf_snapshot_id_idx",
		"monthly_releases_one_active_per_payer_idx",
	}
	for _, name := range indexes {
		var exists bool
		err := pool.QueryRow(context.Background(), `
SELECT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname = 'mrfpipeline' AND indexname = $1
)`, name).Scan(&exists)
		if err != nil || !exists {
			t.Fatalf("missing index %s: %v", name, err)
		}
	}
}

func TestIntegrationTerminalParseSourceConstraint(t *testing.T) {
	url, pool := withTestDB(t)
	mustMigrate(t, url)
	ctx := context.Background()
	accepted := []struct {
		download, parse, failure string
	}{
		{"blocked", "failed", "mrf_parse_execution_failed"},
		{"succeeded", "failed", "mrf_parse_execution_failed"},
		{"pending", "blocked", ""},
		{"running", "blocked", ""},
		{"failed", "blocked", "mrf_download_failed"},
	}
	for i, tc := range accepted {
		if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_sources
    (source_url, collection_month, download_status, parse_status, failure_code)
VALUES ($1, DATE '2026-08-01', $2, $3, NULLIF($4, ''))`,
			"https://example.invalid/terminal-accepted-"+strconv.Itoa(i), tc.download, tc.parse, tc.failure); err != nil {
			t.Fatalf("accepted %d: %v", i, err)
		}
	}
	rejected := []struct {
		download, parse, failure string
	}{
		{"blocked", "pending", ""},
		{"blocked", "running", ""},
		{"blocked", "succeeded", ""},
		{"pending", "failed", "mrf_parse_execution_failed"},
		{"running", "failed", "mrf_parse_execution_failed"},
		{"failed", "failed", "mrf_parse_execution_failed"},
	}
	for i, tc := range rejected {
		if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_sources
    (source_url, collection_month, download_status, parse_status, failure_code)
VALUES ($1, DATE '2026-08-01', $2, $3, NULLIF($4, ''))`,
			"https://example.invalid/terminal-rejected-"+strconv.Itoa(i), tc.download, tc.parse, tc.failure); err == nil {
			t.Fatalf("rejected %d was accepted", i)
		}
	}
}

func TestIntegrationMonthlyReleaseBackfillAndForeignKeys(t *testing.T) {
	url, pool := withTestDB(t)
	files, err := loadEmbeddedMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if result, err := applyMigrations(context.Background(), url, files[:3]); err != nil || result.ApplicationVersion != 3 {
		t.Fatalf("apply first three migrations: %+v %v", result, err)
	}
	ctx := context.Background()
	var runID, sourceID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, status, completed_at)
VALUES ('aetna', DATE '2026-08-01', 'succeeded', transaction_timestamp()) RETURNING id`).Scan(&runID); err != nil {
		t.Fatal("discovery: ", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_files (payer_id, collection_month, source_url, first_discovery_run_id)
VALUES ('aetna', DATE '2026-08-01', 'https://example.invalid/toc.json', $1)`, runID); err != nil {
		t.Fatal("toc: ", err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month)
VALUES ('https://example.invalid/mrf.json', DATE '2026-08-01') RETURNING id`).Scan(&sourceID); err != nil {
		t.Fatal("source: ", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month)
VALUES ($1, 'aetna', DATE '2026-08-01')`, sourceID); err != nil {
		t.Fatal("snapshot: ", err)
	}
	if _, err := applyMigrations(ctx, url, files); err != nil {
		t.Fatal("apply release migration: ", err)
	}
	var status string
	if err := pool.QueryRow(ctx, `
SELECT status FROM mrfpipeline.monthly_releases
WHERE payer_id = 'aetna' AND collection_month = DATE '2026-08-01'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "building" {
		t.Fatalf("backfilled status %q", status)
	}
	var foreignKeys int
	if err := pool.QueryRow(ctx, `
SELECT count(*)
FROM pg_constraint
WHERE connamespace = 'mrfpipeline'::regnamespace
  AND conname IN (
      'discovery_runs_monthly_release_fkey',
      'toc_files_monthly_release_fkey',
      'mrf_snapshots_monthly_release_fkey')`).Scan(&foreignKeys); err != nil {
		t.Fatal(err)
	}
	if foreignKeys != 3 {
		t.Fatalf("monthly release foreign keys %d", foreignKeys)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month, status, sealed_at, last_activated_at)
VALUES ('aetna', DATE '2026-09-01', 'active', transaction_timestamp(), transaction_timestamp())`); err != nil {
		t.Fatal("active release: ", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month, status, sealed_at, last_activated_at)
VALUES ('aetna', DATE '2026-10-01', 'active', transaction_timestamp(), transaction_timestamp())`); err == nil {
		t.Fatal("second active release should be rejected")
	}
	if _, err := pool.Exec(ctx, `
DELETE FROM mrfpipeline.monthly_releases
WHERE payer_id = 'aetna' AND collection_month = DATE '2026-08-01'`); err == nil {
		t.Fatal("release referenced by domain rows should be protected")
	}
	invalid := []string{
		`INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month) VALUES ('AETNA', DATE '2026-11-01')`,
		`INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month) VALUES ('aetna', DATE '2026-11-15')`,
		`INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month, status, sealed_at, last_activated_at) VALUES ('aetna', DATE '2026-11-01', 'unknown', NULL, NULL)`,
		`INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month, status, sealed_at, last_activated_at) VALUES ('aetna', DATE '2026-12-01', 'building', transaction_timestamp(), NULL)`,
		`INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month, status, sealed_at, last_activated_at) VALUES ('aetna', DATE '2027-02-01', 'inactive', NULL, NULL)`,
		`INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month, status, sealed_at, last_activated_at) VALUES ('aetna', DATE '2027-01-01', 'active', NULL, transaction_timestamp())`,
		`INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month, status, sealed_at, last_activated_at) VALUES ('aetna', DATE '2027-03-01', 'inactive', transaction_timestamp(), timestamp '2026-01-01')`,
	}
	for _, sql := range invalid {
		if _, err := pool.Exec(ctx, sql); err == nil {
			t.Fatalf("expected release constraint rejection: %s", sql)
		}
	}
}

func TestIntegrationConcurrentMigrators(t *testing.T) {
	url, pool := withTestDB(t)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	results := make([]Result, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			defer wg.Done()
			results[i], errs[i] = Migrate(context.Background(), url)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("migrator %d: %v", i, err)
		}
	}
	if results[0].AppliedMigrationCount+results[1].AppliedMigrationCount != 7 {
		t.Fatalf("applied %+v %+v", results[0], results[1])
	}
	if results[0].AppliedRiverMigrationCount+results[1].AppliedRiverMigrationCount != ExpectedRiverVersion {
		t.Fatalf("river applied %+v %+v", results[0], results[1])
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.schema_migrations`).Scan(&n); err != nil {
		t.Fatal("count ledger")
	}
	if n != 7 {
		t.Fatalf("ledger rows %d", n)
	}
}

func TestIntegrationFailingMigrationRollsBack(t *testing.T) {
	url, pool := withTestDB(t)
	files, err := loadEmbeddedMigrations()
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, migrationFile{
		Version: 8,
		Name:    "0008_fail.sql",
		SQL:     "CREATE TABLE mrfpipeline.should_not_exist (id int);\nSELECT 1 / 0;",
	})
	_, err = applyMigrations(context.Background(), url, files)
	if !errors.Is(err, ErrDatabase) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), url) || strings.Contains(err.Error(), "should_not_exist") {
		t.Fatalf("exposed detail: %v", err)
	}
	var exists bool
	if err := pool.QueryRow(context.Background(), `
SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'mrfpipeline' AND table_name = 'should_not_exist'
)`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("failed migration table remained")
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.schema_migrations`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 7 {
		t.Fatalf("ledger rows %d", n)
	}
}

func TestIntegrationPopulatedV1RejectedWithoutMutation(t *testing.T) {
	url, pool := withTestDB(t)
	files, err := loadEmbeddedMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyMigrations(context.Background(), url, files[:1]); err != nil {
		t.Fatal("apply v1", err)
	}
	var sourceID int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_sources (source_url)
VALUES ('https://example.invalid/mrf.json')
RETURNING id`).Scan(&sourceID); err != nil {
		t.Fatal("insert v1 row", err)
	}

	_, err = Migrate(context.Background(), url)
	if !errors.Is(err, ErrDatabase) {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(err.Error(), "new pipeline database and warehouse") {
		t.Fatalf("missing safe migration instruction: %v", err)
	}
	var got int64
	if err := pool.QueryRow(context.Background(), `SELECT id FROM mrfpipeline.mrf_sources`).Scan(&got); err != nil {
		t.Fatal("read source", err)
	}
	if got != sourceID {
		t.Fatalf("source changed from %d to %d", sourceID, got)
	}
	var collectionMonth bool
	if err := pool.QueryRow(context.Background(), `
SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'mrfpipeline'
      AND table_name = 'mrf_sources'
      AND column_name = 'collection_month'
)`).Scan(&collectionMonth); err != nil {
		t.Fatal(err)
	}
	if collectionMonth {
		t.Fatal("failed migration changed schema")
	}
}

func TestIntegrationInvalidLedgerRejected(t *testing.T) {
	url, pool := withTestDB(t)
	mustMigrate(t, url)

	cases := []struct {
		name string
		sql  string
	}{
		{"filename mismatch", `UPDATE mrfpipeline.schema_migrations SET name = 'wrong.sql'`},
		{"unknown future", `INSERT INTO mrfpipeline.schema_migrations (version, name) VALUES (8, '0008_future.sql')`},
		{"missing", `DELETE FROM mrfpipeline.schema_migrations; INSERT INTO mrfpipeline.schema_migrations (version, name) VALUES (6, '0006_future.sql')`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetSchema(t, pool)
			mustMigrate(t, url)
			if _, err := pool.Exec(context.Background(), tc.sql); err != nil {
				t.Fatal("setup ledger")
			}
			_, err := Migrate(context.Background(), url)
			if !errors.Is(err, ErrDatabase) {
				t.Fatalf("got %v", err)
			}
			if strings.Contains(err.Error(), url) {
				t.Fatalf("exposed url: %v", err)
			}
		})
	}
}

func TestIntegrationRiverFailureDoesNotRewriteApplication(t *testing.T) {
	url, pool := withTestDB(t)
	if _, err := pool.Exec(context.Background(), `
CREATE SCHEMA mrfpipeline_river;
CREATE TABLE mrfpipeline_river.river_migration (broken int)`); err != nil {
		t.Fatal(err)
	}
	_, err := Migrate(context.Background(), url)
	if !errors.Is(err, ErrDatabase) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), url) {
		t.Fatalf("exposed url: %v", err)
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.schema_migrations`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 7 {
		t.Fatalf("application ledger rewritten: %d", n)
	}
	if _, err := pool.Exec(context.Background(), `DROP TABLE mrfpipeline_river.river_migration`); err != nil {
		t.Fatal(err)
	}
	result := mustMigrate(t, url)
	if result.ApplicationVersion != 7 || result.AppliedMigrationCount != 0 {
		t.Fatalf("app %+v", result)
	}
	if result.RiverVersion != ExpectedRiverVersion || result.AppliedRiverMigrationCount != ExpectedRiverVersion {
		t.Fatalf("river %+v", result)
	}
}

func TestIntegrationInvalidRiverLedgerRejected(t *testing.T) {
	url, pool := withTestDB(t)
	mustMigrate(t, url)
	if _, err := pool.Exec(context.Background(), `INSERT INTO mrfpipeline_river.river_migration (line, version) VALUES ('main', 99)`); err != nil {
		t.Fatal(err)
	}
	_, err := Migrate(context.Background(), url)
	if !errors.Is(err, ErrDatabase) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), url) {
		t.Fatalf("exposed url: %v", err)
	}
	err = ValidateCurrent(context.Background(), pool)
	if !errors.Is(err, ErrDatabase) {
		t.Fatalf("validate: %v", err)
	}
	if !strings.Contains(err.Error(), "mrfpipeline migrate") {
		t.Fatalf("missing migrate instruction: %v", err)
	}
}

func TestIntegrationValidateCurrent(t *testing.T) {
	url, pool := withTestDB(t)
	err := ValidateCurrent(context.Background(), pool)
	if !errors.Is(err, ErrDatabase) {
		t.Fatalf("missing schema: %v", err)
	}
	if !strings.Contains(err.Error(), "mrfpipeline migrate") {
		t.Fatalf("missing migrate instruction: %v", err)
	}
	mustMigrate(t, url)
	if err := ValidateCurrent(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationSchemaConstraints(t *testing.T) {
	url, pool := withTestDB(t)
	mustMigrate(t, url)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('aetna', DATE '2026-08-01'), ('aetna', DATE '2026-09-01'), ('uhc', DATE '2026-08-01')
ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal("seed releases: ", err)
	}

	var runID int64
	err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.discovery_runs (
    payer_id, collection_month, toc_limit, status, discovered_count, existing_count, admitted_count, overflow_count
) VALUES ('aetna', DATE '2026-08-01', 5, 'pending', 0, 0, 0, 0)
RETURNING id`).Scan(&runID)
	if err != nil {
		t.Fatalf("valid discovery run: %v", err)
	}

	rejects := []string{
		`INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month) VALUES ('', DATE '2026-08-01')`,
		`INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month) VALUES ('AETNA', DATE '2026-08-01')`,
		`INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month) VALUES ('aetna corp', DATE '2026-08-01')`,
		`INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month) VALUES (repeat('a', 129), DATE '2026-08-01')`,
		`INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month) VALUES ('aetna', DATE '2026-08-15')`,
		`INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, toc_limit) VALUES ('aetna', DATE '2026-08-01', 0)`,
		`INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, discovered_count, existing_count) VALUES ('aetna', DATE '2026-08-01', 1, 2)`,
		`INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, discovered_count, existing_count, admitted_count, overflow_count) VALUES ('aetna', DATE '2026-08-01', 2, 0, 2, 1)`,
		`INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, toc_limit, admitted_count) VALUES ('aetna', DATE '2026-08-01', 1, 2)`,
		`INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, river_job_id) VALUES ('aetna', DATE '2026-08-01', 0)`,
		`INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, river_job_id) VALUES ('aetna', DATE '2026-08-01', -1)`,
		`INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, status, completed_at) VALUES ('aetna', DATE '2026-08-01', 'pending', transaction_timestamp())`,
		`INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, status, failure_code) VALUES ('aetna', DATE '2026-08-01', 'succeeded', 'x')`,
	}
	for _, sql := range rejects {
		if _, err := pool.Exec(ctx, sql); err == nil {
			t.Fatalf("expected reject: %s", sql)
		}
	}

	var tocID int64
	err = pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id
) VALUES ('aetna', DATE '2026-08-01', 'https://example.invalid/toc.json', $1)
RETURNING id`, runID).Scan(&tocID)
	if err != nil {
		t.Fatalf("valid toc: %v", err)
	}
	var oldTOCConstraint, monthlyTOCConstraint bool
	if err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE connamespace = 'mrfpipeline'::regnamespace
      AND conname = 'toc_files_payer_source_url_key'
), EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE connamespace = 'mrfpipeline'::regnamespace
      AND conname = 'toc_files_payer_collection_month_source_url_key'
)`).Scan(&oldTOCConstraint, &monthlyTOCConstraint); err != nil {
		t.Fatal(err)
	}
	if oldTOCConstraint || !monthlyTOCConstraint {
		t.Fatalf("toc identity constraints old=%v monthly=%v", oldTOCConstraint, monthlyTOCConstraint)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id
	) VALUES ('aetna', DATE '2026-08-01', 'https://example.invalid/toc.json', $1)`, runID); err == nil {
		t.Fatal("duplicate toc url")
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id
) VALUES ('aetna', DATE '2026-08-01', E'https://example.invalid/toc\n.json', $1)`, runID); err == nil {
		t.Fatal("newline url")
	}
	var otherPayerRunID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month)
VALUES ('uhc', DATE '2026-08-01')
RETURNING id`).Scan(&otherPayerRunID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id
) VALUES ('uhc', DATE '2026-08-01', 'https://example.invalid/toc.json', $1)`, otherPayerRunID); err != nil {
		t.Fatalf("same url for another payer: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id
) VALUES ('aetna', DATE '2026-09-01', 'https://example.invalid/toc.json', $1)`, runID); err != nil {
		t.Fatalf("same url for another month: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id, download_status, parse_status
) VALUES ('aetna', DATE '2026-08-01', 'https://example.invalid/toc2.json', $1, 'pending', 'pending')`, runID); err == nil {
		t.Fatal("parse left blocked too early")
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id, download_status, parse_status, import_status, failure_code
) VALUES ('aetna', DATE '2026-08-01', 'https://example.invalid/toc3.json', $1, 'failed', 'failed', 'blocked', 'download')`, runID); err == nil {
		t.Fatal("failed download left parse failed")
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id
) VALUES ('AETNA', DATE '2026-08-01', 'https://example.invalid/other.json', $1)`, runID); err == nil {
		t.Fatal("uppercase toc payer")
	}

	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.discovery_run_toc_files (
    discovery_run_id, toc_file_id, listing_ordinal, was_new
) VALUES ($1, $2, 0, TRUE)`, runID, tocID); err != nil {
		t.Fatalf("valid membership: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.discovery_run_toc_files (
    discovery_run_id, toc_file_id, listing_ordinal, was_new
) VALUES ($1, $2, 1, FALSE)`, runID, tocID); err == nil {
		t.Fatal("duplicate membership")
	}

	var toc2 int64
	err = pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id
) VALUES ('aetna', DATE '2026-08-01', 'https://example.invalid/toc-b.json', $1)
RETURNING id`, runID).Scan(&toc2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.discovery_run_toc_files (
    discovery_run_id, toc_file_id, listing_ordinal, was_new
) VALUES ($1, $2, 0, FALSE)`, runID, toc2); err == nil {
		t.Fatal("duplicate listing ordinal")
	}

	var sourceID, septemberSourceID int64
	err = pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month)
VALUES ('https://example.invalid/mrf.json', DATE '2026-08-01')
RETURNING id`).Scan(&sourceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
	INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month)
VALUES ('https://example.invalid/mrf.json', DATE '2026-08-01')`); err == nil {
		t.Fatal("duplicate monthly mrf url")
	}
	if _, err := pool.Exec(ctx, `
	INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month)
VALUES ('https://example.invalid/mrf.json', DATE '2026-09-01')`); err != nil {
		t.Fatalf("same url in another month should be distinct: %v", err)
	}
	err = pool.QueryRow(ctx, `
SELECT id FROM mrfpipeline.mrf_sources
WHERE source_url = 'https://example.invalid/mrf.json' AND collection_month = DATE '2026-09-01'`).Scan(&septemberSourceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month)
VALUES ('https://EXAMPLE.invalid/mrf.json', DATE '2026-08-01')`); err != nil {
		t.Fatalf("case-different url should be distinct: %v", err)
	}
	if _, err := pool.Exec(ctx, `
	INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month)
VALUES ('https://example.invalid/mrf.json?q=1', DATE '2026-08-01')`); err != nil {
		t.Fatalf("query-different url should be distinct: %v", err)
	}
	if _, err := pool.Exec(ctx, `
	INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month)
VALUES ('https://example.invalid/mrf.json#frag', DATE '2026-08-01')`); err != nil {
		t.Fatalf("fragment-different url should be distinct: %v", err)
	}
	if _, err := pool.Exec(ctx, `
	INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ('https://example.invalid/early.json', DATE '2026-08-15', 'pending', 'pending')`); err == nil {
		t.Fatal("non-month source capture")
	}
	if _, err := pool.Exec(ctx, `
	INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
	VALUES ('https://example.invalid/early.json', DATE '2026-08-01', 'pending', 'pending')`); err == nil {
		t.Fatal("source parse before download")
	}

	var snap1, snap2 int64
	err = pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month)
VALUES ($1, 'uhc', DATE '2026-08-01') RETURNING id`, sourceID).Scan(&snap1)
	if err != nil {
		t.Fatal(err)
	}
	err = pool.QueryRow(ctx, `
	INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month)
VALUES ($1, 'aetna', DATE '2026-08-01') RETURNING id`, sourceID).Scan(&snap2)
	if err != nil {
		t.Fatalf("second month snapshot: %v", err)
	}
	if _, err := pool.Exec(ctx, `
	INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month)
VALUES ($1, 'uhc', DATE '2026-08-01')`, sourceID); err == nil {
		t.Fatal("duplicate snapshot")
	}
	if _, err := pool.Exec(ctx, `
	INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month)
VALUES ($1, 'uhc', DATE '2026-09-01')`, sourceID); err == nil {
		t.Fatal("snapshot month must match source month")
	}
	if _, err := pool.Exec(ctx, `
	INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_river_job_id)
VALUES ($1, 'uhc', DATE '2026-08-01', 0)`, sourceID); err == nil {
		t.Fatal("zero river job")
	}
	if _, err := pool.Exec(ctx, `
	INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month)
VALUES ($1, 'AETNA', DATE '2026-08-01')`, sourceID); err == nil {
		t.Fatal("uppercase snapshot payer")
	}
	if _, err := pool.Exec(ctx, `
	INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month)
VALUES ($1, 'aetna corp', DATE '2026-08-01')`, sourceID); err == nil {
		t.Fatal("whitespace snapshot payer")
	}

	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_mrf_plan_associations (
    toc_file_id, mrf_snapshot_id, mrf_location, plan_name, issuer_name, plan_sponsor_name, plan_id_type, plan_id, plan_market_type
	) VALUES ($1, $2, 'https://example.invalid/mrf.json', 'Gold', 'Issuer', NULL, 'ein', '12-3456789', 'group')`, tocID, snap1); err != nil {
		t.Fatalf("ein without sponsor: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_mrf_plan_associations (
    toc_file_id, mrf_snapshot_id, mrf_location, plan_name, issuer_name, plan_sponsor_name, plan_id_type, plan_id, plan_market_type
) VALUES ($1, $2, 'https://example.invalid/mrf.json', 'Gold', 'Issuer', '', 'hios', 'H1', 'individual')`, tocID, snap1); err == nil {
		t.Fatal("empty hios sponsor")
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_mrf_plan_associations (
    toc_file_id, mrf_snapshot_id, mrf_location, plan_name, issuer_name, plan_sponsor_name, plan_id_type, plan_id, plan_market_type
) VALUES ($1, $2, 'https://example.invalid/mrf.json', 'Empty EIN', 'Issuer', '', 'ein', '12-empty', 'group')`, tocID, snap1); err == nil {
		t.Fatal("empty ein sponsor")
	}

	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_mrf_plan_associations (
    toc_file_id, mrf_snapshot_id, mrf_location, mrf_filename, plan_name, issuer_name, plan_sponsor_name, plan_id_type, plan_id, plan_market_type
) VALUES ($1, $2, 'https://example.invalid/mrf.json', NULL, 'Gold', 'Issuer', 'Acme', 'ein', '12-3456789', 'group')`, tocID, snap1); err != nil {
		t.Fatalf("valid association: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_mrf_plan_associations (
    toc_file_id, mrf_snapshot_id, mrf_location, mrf_filename, plan_name, issuer_name, plan_sponsor_name, plan_id_type, plan_id, plan_market_type
) VALUES ($1, $2, 'https://example.invalid/mrf.json', NULL, 'Gold', 'Issuer', 'Acme', 'ein', '12-3456789', 'group')`, tocID, snap1); err == nil {
		t.Fatal("duplicate association with null filename")
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_mrf_plan_associations (
    toc_file_id, mrf_snapshot_id, mrf_location, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type
) VALUES ($1, $2, 'https://example.invalid/mrf.json', 'Silver', 'Issuer', 'hios', 'H1', 'individual')`, toc2, snap1); err != nil {
		t.Fatalf("second toc overlapping snapshot: %v", err)
	}

	var planID int64
	err = pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_plans (
    mrf_snapshot_id, plan_name, issuer_name, plan_sponsor_name, plan_id_type, plan_id, plan_market_type
) VALUES ($1, 'Gold', 'Issuer', 'Acme', 'ein', '12-3456789', 'group')
RETURNING id`, snap1).Scan(&planID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_plans (
    mrf_snapshot_id, plan_name, issuer_name, plan_sponsor_name, plan_id_type, plan_id, plan_market_type
) VALUES ($1, 'Null EIN', 'Issuer', NULL, 'ein', '12-null', 'group')`, snap1); err != nil {
		t.Fatalf("ein null sponsor: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_plans (
    mrf_snapshot_id, plan_name, issuer_name, plan_sponsor_name, plan_id_type, plan_id, plan_market_type
) VALUES ($1, 'Empty EIN', 'Issuer', '', 'ein', '12-empty', 'group')`, snap1); err == nil {
		t.Fatal("empty ein sponsor")
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_plans (
    mrf_snapshot_id, plan_name, issuer_name, plan_sponsor_name, plan_id_type, plan_id, plan_market_type
) VALUES ($1, 'Gold', 'Issuer', 'Other', 'ein', '12-3456789', 'group')`, snap1); err == nil {
		t.Fatal("sponsor variant created another plan")
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_plans (
    mrf_snapshot_id, plan_name, issuer_name, plan_sponsor_name, plan_id_type, plan_id, plan_market_type
) VALUES ($1, 'Silver', 'Issuer', 'Nope', 'hios', 'H1', 'individual')`, snap1); err == nil {
		t.Fatal("hios projection must store null sponsor")
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_plans (
    mrf_snapshot_id, plan_name, issuer_name, plan_sponsor_name, plan_id_type, plan_id, plan_market_type
) VALUES ($1, 'Silver', 'Issuer', NULL, 'hios', 'H1', 'individual')`, snap1); err != nil {
		t.Fatalf("hios null sponsor: %v", err)
	}

	var batchID int64
	err = pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batches (mrf_snapshot_id, requested_plan_count)
VALUES ($1, 1) RETURNING id`, snap1).Scan(&batchID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batch_items (plan_attachment_batch_id, mrf_plan_id)
VALUES ($1, $2)`, batchID, planID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batch_items (plan_attachment_batch_id, mrf_plan_id)
VALUES ($1, $2)`, batchID, planID); err == nil {
		t.Fatal("duplicate batch item")
	}

	if _, err := pool.Exec(ctx, `DELETE FROM mrfpipeline.discovery_runs WHERE id = $1`, runID); err == nil {
		t.Fatal("delete admitting run should restrict")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID); err == nil {
		t.Fatal("delete source with snapshot should restrict")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM mrfpipeline.mrf_plans WHERE id = $1`, planID); err == nil {
		t.Fatal("delete assigned plan should restrict")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM mrfpipeline.plan_attachment_batches WHERE id = $1`, batchID); err != nil {
		t.Fatalf("batch delete should cascade items: %v", err)
	}
	var items int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.plan_attachment_batch_items`).Scan(&items); err != nil {
		t.Fatal(err)
	}
	if items != 0 {
		t.Fatalf("cascade left %d items", items)
	}
}
