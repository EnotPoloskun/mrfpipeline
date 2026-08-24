package release

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5/pgxpool"
)

func releaseTestDB(t *testing.T) *pgxpool.Pool {
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
		t.Fatal("connect test database")
	}
	reset := func() {
		for _, schema := range []string{database.RiverSchema, database.ApplicationSchema, "mrfpipeline_test"} {
			_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		}
	}
	reset()
	t.Cleanup(func() {
		reset()
		pool.Close()
		database.TestDBMu.Unlock()
	})
	if _, err := database.Migrate(context.Background(), raw); err != nil {
		t.Fatal("migrate: ", err)
	}
	return pool
}

func seedReadyRelease(t *testing.T, pool *pgxpool.Pool, payer, month, suffix string) int64 {
	t.Helper()
	ctx := context.Background()
	monthDate, err := time.Parse("2006-01-02", month+"-01")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ($1, $2)
ON CONFLICT (payer_id, collection_month) DO NOTHING`, payer, monthDate); err != nil {
		t.Fatal("release: ", err)
	}
	var runID, sourceID, snapshotID, planID, batchID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, status, completed_at)
VALUES ($1, $2, 'succeeded', transaction_timestamp()) RETURNING id`, payer, monthDate).Scan(&runID); err != nil {
		t.Fatal("discovery: ", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id,
    download_status, parse_status, import_status
) VALUES ($1, $2, $3, $4, 'succeeded', 'succeeded', 'succeeded')`, payer, monthDate, "https://example.invalid/toc/"+suffix, runID); err != nil {
		t.Fatal("toc: ", err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ($1, $2, 'succeeded', 'succeeded') RETURNING id`, "https://example.invalid/mrf/"+suffix, monthDate).Scan(&sourceID); err != nil {
		t.Fatal("source: ", err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, $2, $3, 'succeeded') RETURNING id`, sourceID, payer, monthDate).Scan(&snapshotID); err != nil {
		t.Fatal("snapshot: ", err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_plans (
    mrf_snapshot_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type
) VALUES ($1, 'plan-' || $2, 'issuer', 'hios', $2, 'group') RETURNING id`, snapshotID, suffix).Scan(&planID); err != nil {
		t.Fatal("plan: ", err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batches (
    mrf_snapshot_id, status, requested_plan_count, added_plan_count, completed_at
) VALUES ($1, 'succeeded', 1, 1, transaction_timestamp()) RETURNING id`, snapshotID).Scan(&batchID); err != nil {
		t.Fatal("batch: ", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batch_items (plan_attachment_batch_id, mrf_plan_id)
VALUES ($1, $2)`, batchID, planID); err != nil {
		t.Fatal("batch item: ", err)
	}
	return snapshotID
}

func TestIntegrationActivationAndSealedSnapshotGate(t *testing.T) {
	pool := releaseTestDB(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('uhc', DATE '2026-08-01')`); err != nil {
		t.Fatal(err)
	}
	incomplete, err := Readiness(ctx, pool, "uhc", time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC))
	if err != nil || incomplete.DatabaseReady || len(incomplete.Blockers) != 3 || incomplete.Blockers[0] != "discovery_missing" || incomplete.Blockers[1] != "snapshot_missing" || incomplete.Blockers[2] != "toc_missing" {
		t.Fatalf("incomplete readiness %+v, %v", incomplete, err)
	}
	aug := seedReadyRelease(t, pool, "uhc", "2026-08", "uhc-aug")
	sep := seedReadyRelease(t, pool, "uhc", "2026-09", "uhc-sep")
	aetna := seedReadyRelease(t, pool, "aetna", "2026-08", "aetna-aug")
	augDate := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	sepDate := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)

	ready, err := Readiness(ctx, pool, "uhc", augDate)
	if err != nil || !ready.DatabaseReady || len(ready.Blockers) != 0 {
		t.Fatalf("readiness %+v, %v", ready, err)
	}
	first, err := Activate(ctx, pool, "uhc", augDate, nil)
	if err != nil || first.OutputCount != 1 || first.PreviousCollectionMonth != nil {
		t.Fatalf("first activation %+v, %v", first, err)
	}
	second, err := Activate(ctx, pool, "uhc", sepDate, nil)
	if err != nil || second.OutputCount != 1 || second.PreviousCollectionMonth == nil || *second.PreviousCollectionMonth != "2026-08" {
		t.Fatalf("second activation %+v, %v", second, err)
	}
	outputs, err := ListActiveOutputs(ctx, pool)
	if err != nil || len(outputs) != 1 || outputs[0].SnapshotID != sep {
		t.Fatalf("active outputs %+v, %v", outputs, err)
	}
	rollback, err := Activate(ctx, pool, "uhc", augDate, nil)
	if err != nil || rollback.PreviousCollectionMonth == nil || *rollback.PreviousCollectionMonth != "2026-09" {
		t.Fatalf("rollback %+v, %v", rollback, err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline.monthly_releases SET status = 'active'
WHERE payer_id = 'uhc' AND collection_month = DATE '2026-09-01'`); err == nil {
		t.Fatal("database must enforce one active release")
	}
	if _, err := Activate(ctx, pool, "aetna", augDate, nil); err != nil {
		t.Fatal("aetna activation: ", err)
	}
	outputs, err = ListActiveOutputs(ctx, pool)
	if err != nil || len(outputs) != 2 || outputs[0].PayerID != "aetna" || outputs[0].SnapshotID != aetna || outputs[1].PayerID != "uhc" || outputs[1].SnapshotID != aug {
		t.Fatalf("multi-payer outputs %+v, %v", outputs, err)
	}
	var before, after time.Time
	if err := pool.QueryRow(ctx, `
SELECT last_activated_at FROM mrfpipeline.monthly_releases
WHERE payer_id = 'aetna' AND collection_month = DATE '2026-08-01'`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	idempotent, err := Activate(ctx, pool, "aetna", augDate, nil)
	if err != nil || idempotent.PreviousCollectionMonth != nil || idempotent.OutputCount != 1 {
		t.Fatalf("idempotent activation %+v, %v", idempotent, err)
	}
	if err := pool.QueryRow(ctx, `
SELECT last_activated_at FROM mrfpipeline.monthly_releases
WHERE payer_id = 'aetna' AND collection_month = DATE '2026-08-01'`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !before.Equal(after) {
		t.Fatalf("idempotent activation changed timestamp: %v -> %v", before, after)
	}
	seedReadyRelease(t, pool, "uhc", "2026-10", "uhc-oct")
	if _, err := Activate(ctx, pool, "uhc", time.Date(2026, time.October, 1, 0, 0, 0, 0, time.UTC), func([]Target) error {
		return errors.New("preflight rejected")
	}); err == nil {
		t.Fatal("failed activation unexpectedly succeeded")
	}
	var activeMonth string
	if err := pool.QueryRow(ctx, `
SELECT to_char(collection_month, 'YYYY-MM') FROM mrfpipeline.monthly_releases
WHERE payer_id = 'uhc' AND status = 'active'`).Scan(&activeMonth); err != nil || activeMonth != "2026-08" {
		t.Fatalf("failed activation changed active month: %s %v", activeMonth, err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = RequireBuildingForSnapshot(ctx, tx, aug)
	_ = tx.Rollback(ctx)
	if !jobs.IsFailure(err, jobs.FailureSealedReleaseInconsistent) {
		t.Fatalf("sealed gate error %v", err)
	}
}

func TestIntegrationReadinessRiverJobIdentity(t *testing.T) {
	pool := releaseTestDB(t)
	ctx := context.Background()
	month := time.Date(2026, time.November, 1, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('uhc', $1)`, month); err != nil {
		t.Fatal(err)
	}
	var runID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, status)
VALUES ('uhc', $1, 'pending') RETURNING id`, month).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	client, err := jobs.NewInsertClient(ctx, pool, jobs.NewLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	jobID, err := jobs.InsertTx(ctx, client, tx, &jobs.DiscoveryRunArgs{DiscoveryRunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
UPDATE mrfpipeline.discovery_runs SET river_job_id = $2 WHERE id = $1`, runID, jobID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	hasBlocker := func(report StatusReport, want string) bool {
		for _, blocker := range report.Blockers {
			if blocker == want {
				return true
			}
		}
		return false
	}
	valid, err := Readiness(ctx, pool, "uhc", month)
	if err != nil || hasBlocker(valid, "job_inconsistent") {
		t.Fatalf("valid current job readiness %+v, %v", valid, err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline_river.river_job
SET args = '{"discovery_run_id":999999}'::jsonb
WHERE id = $1`, jobID); err != nil {
		t.Fatal(err)
	}
	wrongArgs, err := Readiness(ctx, pool, "uhc", month)
	if err != nil || !hasBlocker(wrongArgs, "job_inconsistent") {
		t.Fatalf("wrong args readiness %+v, %v", wrongArgs, err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline_river.river_job
SET args = jsonb_build_object('discovery_run_id', $2::bigint), state = 'completed', finalized_at = transaction_timestamp()
WHERE id = $1`, jobID, runID); err != nil {
		t.Fatal(err)
	}
	terminal, err := Readiness(ctx, pool, "uhc", month)
	if err != nil || !hasBlocker(terminal, "job_inconsistent") {
		t.Fatalf("terminal job readiness %+v, %v", terminal, err)
	}
}

func TestIntegrationAllProductionGatesRespectSharedSealedReleases(t *testing.T) {
	pool := releaseTestDB(t)
	ctx := context.Background()
	uhcSnapshot := seedReadyRelease(t, pool, "uhc", "2026-08", "shared-uhc")
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('aetna', DATE '2026-08-01')`); err != nil {
		t.Fatal(err)
	}
	var sourceID, runID, tocID, batchID int64
	if err := pool.QueryRow(ctx, `SELECT mrf_source_id FROM mrfpipeline.mrf_snapshots WHERE id = $1`, uhcSnapshot).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, 'aetna', DATE '2026-08-01', 'succeeded') RETURNING id`, sourceID).Scan(new(int64)); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT id FROM mrfpipeline.discovery_runs WHERE payer_id = 'uhc'`).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT id FROM mrfpipeline.toc_files WHERE payer_id = 'uhc'`).Scan(&tocID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT id FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT id FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id = $1`, uhcSnapshot).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	ids := map[string]int64{
		jobs.KindDiscoveryRun: runID, jobs.KindTOCDownload: tocID, jobs.KindTOCParse: tocID,
		jobs.KindTOCImport: tocID, jobs.KindMRFDownload: sourceID, jobs.KindMRFParse: sourceID,
		jobs.KindConsumerIngest: uhcSnapshot, jobs.KindConsumerAttachPlans: batchID,
	}
	gate := func(kind string) error {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		return RequireBuildingForStage(ctx, tx, kind, ids[kind])
	}
	for _, kind := range jobs.ProductionKinds() {
		if err := gate(kind); err != nil {
			t.Fatalf("building gate %s: %v", kind, err)
		}
	}
	if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline.monthly_releases
SET status = 'active', sealed_at = transaction_timestamp(), last_activated_at = transaction_timestamp()
WHERE payer_id = 'aetna' AND collection_month = DATE '2026-08-01'`); err != nil {
		t.Fatal(err)
	}
	if err := gate(jobs.KindMRFDownload); !jobs.IsFailure(err, jobs.FailureSealedReleaseInconsistent) {
		t.Fatalf("shared source gate: %v", err)
	}
	for _, status := range []string{"active", "inactive"} {
		if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline.monthly_releases
SET status = $1, sealed_at = transaction_timestamp(), last_activated_at = transaction_timestamp()
WHERE payer_id = 'aetna'`, status); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline.monthly_releases
SET status = $1, sealed_at = transaction_timestamp(), last_activated_at = transaction_timestamp()
WHERE payer_id = 'uhc'`, status); err != nil {
			t.Fatal(err)
		}
		for _, kind := range jobs.ProductionKinds() {
			if err := gate(kind); !jobs.IsFailure(err, jobs.FailureSealedReleaseInconsistent) {
				t.Fatalf("%s gate for %s release: %v", kind, status, err)
			}
		}
	}
}
