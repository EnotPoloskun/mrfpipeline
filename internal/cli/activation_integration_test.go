package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/reconcile"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5/pgxpool"
)

func activationTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	raw := os.Getenv("MRFPIPELINE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("MRFPIPELINE_TEST_DATABASE_URL is not set")
	}
	cfg, err := pgxpool.ParseConfig(raw)
	if err != nil || !strings.HasPrefix(cfg.ConnConfig.Database, "mrfpipeline_test_") {
		t.Fatal("invalid test database url")
	}
	database.TestDBMu.Lock()
	pool, err := pgxpool.New(context.Background(), raw)
	if err != nil {
		database.TestDBMu.Unlock()
		t.Fatal(err)
	}
	reset := func() {
		for _, schema := range []string{database.RiverSchema, database.ApplicationSchema, "mrfpipeline_test"} {
			_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		}
	}
	reset()
	if _, err := database.Migrate(context.Background(), raw); err != nil {
		pool.Close()
		database.TestDBMu.Unlock()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		reset()
		pool.Close()
		database.TestDBMu.Unlock()
	})
	return pool
}

type activationFixture struct {
	target       release.Target
	warehouse    string
	expectedPart string
	basePart     string
}

func seedActivationFixture(t *testing.T, pool *pgxpool.Pool, sealed bool) activationFixture {
	t.Helper()
	ctx := context.Background()
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases
    (payer_id, collection_month, status, sealed_at, last_activated_at)
VALUES ('uhc', DATE '2026-07-01', 'active', transaction_timestamp(), transaction_timestamp()),
       ('uhc', $1, 'building', NULL, NULL)`, month); err != nil {
		t.Fatal(err)
	}
	var runID, sourceID, snapshotID, planID, batchID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, status, completed_at)
VALUES ('uhc', $1, 'succeeded', transaction_timestamp()) RETURNING id`, month).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.toc_files
    (payer_id, collection_month, source_url, first_discovery_run_id,
     download_status, parse_status, import_status)
VALUES ('uhc', $1, 'https://files.test/activation-toc', $2, 'succeeded', 'succeeded', 'succeeded')
RETURNING id`, month, runID).Scan(new(int64)); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ('https://files.test/activation-mrf', $1, 'succeeded', 'succeeded') RETURNING id`, month).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, 'uhc', $2, 'succeeded') RETURNING id`, sourceID, month).Scan(&snapshotID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_plans
    (mrf_snapshot_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type)
VALUES ($1, 'activation-plan', 'issuer', 'hios', 'activation-id', 'group') RETURNING id`, snapshotID).Scan(&planID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batches
    (mrf_snapshot_id, status, requested_plan_count, added_plan_count, completed_at)
VALUES ($1, 'succeeded', 1, 1, transaction_timestamp()) RETURNING id`, snapshotID).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batch_items (plan_attachment_batch_id, mrf_plan_id)
VALUES ($1, $2)`, batchID, planID); err != nil {
		t.Fatal(err)
	}
	if sealed {
		if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline.monthly_releases
SET status = 'inactive', sealed_at = transaction_timestamp(), last_activated_at = transaction_timestamp()
WHERE payer_id = 'uhc' AND collection_month = $1`, month); err != nil {
			t.Fatal(err)
		}
	}

	warehouse := t.TempDir()
	outputID := "mrf-" + strconv.FormatInt(snapshotID, 10)
	writeActivationWarehouse(t, warehouse, "uhc", "2026-08", outputID)
	partDir := filepath.Join(warehouse, "plan_associations", "output_id="+outputID)
	if err := os.MkdirAll(partDir, 0700); err != nil {
		t.Fatal(err)
	}
	expectedPart := filepath.Join(partDir, "plan-batch-"+strconv.FormatInt(batchID, 10)+"-part-00000.parquet")
	if err := os.WriteFile(expectedPart, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	basePart := filepath.Join(warehouse, "snapshots", "collection_month=2026-08", "payer_id=uhc", "output_id="+outputID, "rate_facts", outputID+"-part-00000.parquet")
	return activationFixture{
		target:    release.Target{PayerID: "uhc", CollectionMonth: "2026-08", OutputID: outputID, SnapshotID: snapshotID},
		warehouse: warehouse, expectedPart: expectedPart, basePart: basePart,
	}
}

func writeActivationWarehouse(t *testing.T, warehouse, payer, month, outputID string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(warehouse, "warehouse.json"), []byte(`{"warehouse_schema_version":"2.0.0","provider_catalog":{"schema_version":1,"release_month":"2026-08"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	datasets := []string{"rate_facts", "rate_provider_groups", "provider_groups", "provider_group_memberships", "ingestions", "network_names"}
	counts := map[string]map[string]int64{}
	final := filepath.Join(warehouse, "snapshots", "collection_month="+month, "payer_id="+payer, "output_id="+outputID)
	for _, dataset := range datasets {
		path := filepath.Join(final, dataset)
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, outputID+"-part-00000.parquet"), []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
		rowCount := int64(0)
		if dataset == "ingestions" {
			rowCount = 1
		}
		counts[dataset] = map[string]int64{"row_count": rowCount, "part_count": 1}
	}
	manifest := map[string]any{
		"manifest_schema_version": "2.0.0", "output_schema_version": "2.0.0",
		"output_id": outputID, "payer_id": payer, "collection_month": month,
		"provider_catalog": map[string]any{"schema_version": 1, "release_month": "2026-08"},
		"datasets":         counts,
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(final, "manifest.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationActivationRejectsDamagedInactivePublication(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, activationFixture)
	}{
		{name: "missing expected plan part", mutate: func(t *testing.T, f activationFixture) {
			if err := os.Remove(f.expectedPart); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unexpected plan part", mutate: func(t *testing.T, f activationFixture) {
			if err := os.WriteFile(filepath.Join(filepath.Dir(f.expectedPart), "plan-batch-999-part-00000.parquet"), []byte("fixture"), 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "damaged inactive base", mutate: func(t *testing.T, f activationFixture) {
			if err := os.Remove(f.basePart); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := activationTestDB(t)
			fixture := seedActivationFixture(t, pool, true)
			tc.mutate(t, fixture)
			month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
			_, err := release.Activate(context.Background(), pool, "uhc", month, func(targets []release.Target) error {
				return reconcile.ValidateActivationTargets(context.Background(), pool, fixture.warehouse, "uhc", month, targets)
			})
			if !jobs.IsFailure(err, jobs.FailureSealedReleaseInconsistent) {
				t.Fatalf("activation error %v", err)
			}
			var activeMonth string
			if err := pool.QueryRow(context.Background(), `
SELECT to_char(collection_month, 'YYYY-MM')
FROM mrfpipeline.monthly_releases
WHERE payer_id = 'uhc' AND status = 'active'`).Scan(&activeMonth); err != nil {
				t.Fatal(err)
			}
			if activeMonth != "2026-07" {
				t.Fatalf("failed reactivation changed active month to %s", activeMonth)
			}
		})
	}
}

func TestIntegrationFirstActivationRejectsDamagedPublication(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, activationFixture)
	}{
		{name: "missing expected plan part", mutate: func(t *testing.T, f activationFixture) {
			if err := os.Remove(f.expectedPart); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unexpected plan part", mutate: func(t *testing.T, f activationFixture) {
			if err := os.WriteFile(filepath.Join(filepath.Dir(f.expectedPart), "plan-batch-999-part-00000.parquet"), []byte("fixture"), 0600); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := activationTestDB(t)
			fixture := seedActivationFixture(t, pool, false)
			tc.mutate(t, fixture)
			month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
			_, err := release.Activate(context.Background(), pool, "uhc", month, func(targets []release.Target) error {
				return reconcile.ValidateActivationTargets(context.Background(), pool, fixture.warehouse, "uhc", month, targets)
			})
			if !jobs.IsFailure(err, jobs.FailureReleaseNotReady) {
				t.Fatalf("activation error %v", err)
			}
			var activeMonth string
			if err := pool.QueryRow(context.Background(), `
SELECT to_char(collection_month, 'YYYY-MM')
FROM mrfpipeline.monthly_releases
WHERE payer_id = 'uhc' AND status = 'active'`).Scan(&activeMonth); err != nil {
				t.Fatal(err)
			}
			if activeMonth != "2026-07" {
				t.Fatalf("failed first activation changed active month to %s", activeMonth)
			}
		})
	}
}

func TestIntegrationFirstActivationPublishesHealthyInventory(t *testing.T) {
	pool := activationTestDB(t)
	fixture := seedActivationFixture(t, pool, false)
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	preflight := func(targets []release.Target) error {
		return reconcile.ValidateActivationTargets(context.Background(), pool, fixture.warehouse, "uhc", month, targets)
	}
	first, err := release.Activate(context.Background(), pool, "uhc", month, preflight)
	if err != nil {
		t.Fatal(err)
	}
	if first.PreviousCollectionMonth == nil || *first.PreviousCollectionMonth != "2026-07" || first.OutputCount != 1 {
		t.Fatalf("unexpected first activation result: %+v", first)
	}
	outputs, err := release.ListActiveOutputs(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}
	if len(outputs) != 1 || outputs[0].OutputID != fixture.target.OutputID {
		t.Fatalf("unexpected active outputs: %+v", outputs)
	}
	second, err := release.Activate(context.Background(), pool, "uhc", month, preflight)
	if err != nil {
		t.Fatal(err)
	}
	if second.PreviousCollectionMonth != nil || second.OutputCount != 1 {
		t.Fatalf("unexpected converged activation result: %+v", second)
	}
}
