package filtercatalog

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5/pgxpool"
)

func filterCatalogTestDB(t *testing.T) *pgxpool.Pool {
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
		for _, schema := range []string{"mrfweb", database.RiverSchema, database.ApplicationSchema} {
			_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		}
	}
	reset()
	if _, err := database.Migrate(context.Background(), raw); err != nil {
		pool.Close()
		database.TestDBMu.Unlock()
		t.Fatal(err)
	}
	t.Cleanup(func() { reset(); pool.Close(); database.TestDBMu.Unlock() })
	return pool
}

func seedFilterCatalogBuild(t *testing.T, pool *pgxpool.Pool) (int64, release.CatalogCandidate, warehouseIdentity) {
	t.Helper()
	ctx := context.Background()
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.monthly_releases(payer_id,collection_month,mrf_source_target_kind) VALUES ('uhc',$1,'all')`, month); err != nil {
		t.Fatal(err)
	}
	var runID, source, snap, plan, batch, cat int64
	if err := pool.QueryRow(ctx, `INSERT INTO mrfpipeline.discovery_runs(payer_id,collection_month,status,completed_at) VALUES ('uhc',$1,'succeeded',transaction_timestamp()) RETURNING id`, month).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.toc_files(payer_id,collection_month,source_url,first_discovery_run_id,download_status,parse_status,import_status) VALUES ('uhc',$1,'https://example.invalid/toc',$2,'succeeded','succeeded','succeeded')`, month, runID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO mrfpipeline.mrf_sources(source_url,collection_month,download_status,parse_status) VALUES ('https://example.invalid/mrf',$1,'succeeded','succeeded') RETURNING id`, month).Scan(&source); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.monthly_release_mrf_sources(payer_id,collection_month,mrf_source_id) VALUES ('uhc',$1,$2)`, month, source); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO mrfpipeline.mrf_snapshots(mrf_source_id,payer_id,collection_month,consume_status) VALUES ($1,'uhc',$2,'succeeded') RETURNING id`, source, month).Scan(&snap); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO mrfpipeline.mrf_plans(mrf_snapshot_id,plan_name,issuer_name,plan_id_type,plan_id,plan_market_type) VALUES ($1,'Plan','Issuer','hios','id','group') RETURNING id`, snap).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO mrfpipeline.plan_attachment_batches(mrf_snapshot_id,status,requested_plan_count,added_plan_count,completed_at) VALUES ($1,'succeeded',1,1,transaction_timestamp()) RETURNING id`, snap).Scan(&batch); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.plan_attachment_batch_items(plan_attachment_batch_id,mrf_plan_id) VALUES ($1,$2)`, batch, plan); err != nil {
		t.Fatal(err)
	}
	candidate, err := release.SelectCatalogCandidate(ctx, pool, "uhc", month)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO mrfweb.release_catalogs(payer_id,collection_month,publication_generation,output_fingerprint,output_count) VALUES ('uhc',$1,1,$2,1) RETURNING id`, month, candidate.OutputFingerprint).Scan(&cat); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfweb.release_outputs(catalog_id,mrf_snapshot_id,output_id) VALUES ($1,$2,$3)`, cat, candidate.Targets[0].SnapshotID, candidate.Targets[0].OutputID); err != nil {
		t.Fatal(err)
	}
	return cat, candidate, warehouseIdentity{SchemaVersion: 1, ReleaseMonth: month}
}

func TestIntegrationPopulateCatalogAtomicRows(t *testing.T) {
	pool := filterCatalogTestDB(t)
	cat, candidate, identity := seedFilterCatalogBuild(t, pool)
	conn, _ := pool.Acquire(context.Background())
	defer conn.Release()
	projection := planProjection{}
	_, err := populateCatalog(context.Background(), conn, cat, candidate, identity, Result{WarehouseSchemaVersion: "2.0.0", ProviderCatalogSchemaVersion: 1, ProviderCatalogReleaseMonth: identity.ReleaseMonth, OutputFingerprint: candidate.OutputFingerprint, StandardFactCount: 1, BillingCodes: []BillingCode{{BillingCodeType: "CPT", BillingCode: "1", ObservationCount: 1, UnmodifiedObservationCount: 1}}}, projection, nil)
	if err == nil || !jobs.IsFailure(err, jobs.FailureFilterCatalogPopulationFailed) {
		t.Fatalf("invalid extraction error=%v", err)
	}
	var status string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM mrfweb.release_catalogs WHERE id=$1`, cat).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "building" {
		t.Fatalf("status=%s", status)
	}
}
func TestIntegrationPopulateCatalogSuccessAndRollback(t *testing.T) {
	pool := filterCatalogTestDB(t)
	cat, candidate, identity := seedFilterCatalogBuild(t, pool)
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	plan := planIdentity{PlanName: "Plan", IssuerName: "Issuer", PlanIDType: "hios", PlanID: "id", PlanMarketType: "group"}
	projection := planProjection{Plans: []canonicalPlan{{planIdentity: plan, Outputs: []string{candidate.Targets[0].OutputID}}}, PlanOutputs: []planOutput{{Plan: plan, Output: candidate.Targets[0].OutputID}}}
	extracted := Result{WarehouseSchemaVersion: "2.0.0", ProviderCatalogSchemaVersion: 1, ProviderCatalogReleaseMonth: identity.ReleaseMonth, OutputFingerprint: candidate.OutputFingerprint, StandardFactCount: 1, BillingCodes: []BillingCode{{BillingCodeType: "CPT", BillingCode: "1", ObservationCount: 1, UnmodifiedObservationCount: 1}}}
	got, err := populateCatalog(context.Background(), conn, cat, candidate, identity, extracted, projection, nil)
	if err != nil || got.CatalogStatus != "ready" || got.PlanCount != 1 || got.PlanOutputCount != 1 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	var status string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM mrfweb.release_catalogs WHERE id=$1`, cat).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "ready" {
		t.Fatal(status)
	}
}

func TestIntegrationPopulateCatalogRollsBackMidTransaction(t *testing.T) {
	pool := filterCatalogTestDB(t)
	cat, candidate, identity := seedFilterCatalogBuild(t, pool)
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	plan := planIdentity{PlanName: "Plan", IssuerName: "Issuer", PlanIDType: "hios", PlanID: "id", PlanMarketType: "group"}
	projection := planProjection{Plans: []canonicalPlan{{planIdentity: plan, Outputs: []string{candidate.Targets[0].OutputID}}}, PlanOutputs: []planOutput{{Plan: plan, Output: candidate.Targets[0].OutputID}}}
	extracted := Result{WarehouseSchemaVersion: "2.0.0", ProviderCatalogSchemaVersion: 1, ProviderCatalogReleaseMonth: identity.ReleaseMonth, OutputFingerprint: candidate.OutputFingerprint, StandardFactCount: 2, BillingCodes: []BillingCode{{BillingCodeType: "CPT", BillingCode: "1", ObservationCount: 1, UnmodifiedObservationCount: 1}, {BillingCodeType: "CPT", BillingCode: "1", ObservationCount: 1, UnmodifiedObservationCount: 1}}}
	if _, err := populateCatalog(context.Background(), conn, cat, candidate, identity, extracted, projection, nil); err == nil {
		t.Fatal("duplicate billing key unexpectedly succeeded")
	}
	for _, table := range []string{"release_billing_codes", "release_code_filter_values", "release_plans", "release_plan_outputs", "release_output_code_networks", "release_provider_filter_values"} {
		var count int
		if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM mrfweb."+table+" WHERE catalog_id=$1", cat).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s rows=%d", table, count)
		}
	}
}

func TestIntegrationExistingCatalogValidation(t *testing.T) {
	pool := filterCatalogTestDB(t)
	cat, candidate, identity := seedFilterCatalogBuild(t, pool)
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	plan := planIdentity{PlanName: "Plan", IssuerName: "Issuer", PlanIDType: "hios", PlanID: "id", PlanMarketType: "group"}
	projection := planProjection{Plans: []canonicalPlan{{planIdentity: plan, Outputs: []string{candidate.Targets[0].OutputID}}}, PlanOutputs: []planOutput{{Plan: plan, Output: candidate.Targets[0].OutputID}}}
	extracted := Result{WarehouseSchemaVersion: "2.0.0", ProviderCatalogSchemaVersion: 1, ProviderCatalogReleaseMonth: identity.ReleaseMonth, OutputFingerprint: candidate.OutputFingerprint, StandardFactCount: 1, BillingCodes: []BillingCode{{BillingCodeType: "CPT", BillingCode: "1", ObservationCount: 1, UnmodifiedObservationCount: 1}}}
	if _, err := populateCatalog(context.Background(), conn, cat, candidate, identity, extracted, projection, nil); err != nil {
		t.Fatal(err)
	}
	snapshot := planSnapshot{Rows: []planSnapshotRow{{OutputID: candidate.Targets[0].OutputID, planIdentity: plan}}, Facts: []planReadinessFact{{OutputID: candidate.Targets[0].OutputID, PlanCount: 1, PositiveBatch: true}}}
	inspection, found, err := inspectExistingCatalog(context.Background(), conn, candidate, snapshot, identity)
	if err != nil || !found || !catalogInternallyValid(inspection) || !catalogMatchesCandidate(inspection, candidate, snapshot, identity) {
		t.Fatalf("inspection=%+v found=%v err=%v", inspection, found, err)
	}
	if got, err := resultFromHeader(inspection.Header, true); err != nil || !got.Unchanged || got.BillingCodeCount != 1 || got.PlanCount != 1 {
		t.Fatalf("result=%+v err=%v", got, err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE mrfweb.release_catalogs SET plan_count=99 WHERE id=$1`, cat); err != nil {
		t.Fatal(err)
	}
	bad, _, err := inspectExistingCatalog(context.Background(), conn, candidate, snapshot, identity)
	if err != nil || catalogInternallyValid(bad) {
		t.Fatalf("corrupt inspection=%+v err=%v", bad, err)
	}
}
func TestIntegrationPublishedCatalogIsImmutableAndCorruptionIsInconsistent(t *testing.T) {
	pool := filterCatalogTestDB(t)
	cat, candidate, identity := seedFilterCatalogBuild(t, pool)
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	plan := planIdentity{PlanName: "Plan", IssuerName: "Issuer", PlanIDType: "hios", PlanID: "id", PlanMarketType: "group"}
	projection := planProjection{Plans: []canonicalPlan{{planIdentity: plan, Outputs: []string{candidate.Targets[0].OutputID}}}, PlanOutputs: []planOutput{{Plan: plan, Output: candidate.Targets[0].OutputID}}}
	extracted := Result{WarehouseSchemaVersion: "2.0.0", ProviderCatalogSchemaVersion: 1, ProviderCatalogReleaseMonth: identity.ReleaseMonth, OutputFingerprint: candidate.OutputFingerprint, StandardFactCount: 1, BillingCodes: []BillingCode{{BillingCodeType: "CPT", BillingCode: "1", ObservationCount: 1, UnmodifiedObservationCount: 1}}}
	if _, err := populateCatalog(context.Background(), conn, cat, candidate, identity, extracted, projection, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE mrfweb.release_catalogs SET status='published', published_at=transaction_timestamp() WHERE id=$1`, cat); err != nil {
		t.Fatal(err)
	}
	snapshot := planSnapshot{Rows: []planSnapshotRow{{OutputID: candidate.Targets[0].OutputID, planIdentity: plan}}, Facts: []planReadinessFact{{OutputID: candidate.Targets[0].OutputID, PlanCount: 1, PositiveBatch: true}}}
	inspection, found, err := inspectExistingCatalog(context.Background(), conn, candidate, snapshot, identity)
	if err != nil || !found || !catalogInternallyValid(inspection) {
		t.Fatalf("published=%+v err=%v", inspection, err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE mrfweb.release_catalogs SET plan_count=99 WHERE id=$1`, cat); err != nil {
		t.Fatal(err)
	}
	bad, _, err := inspectExistingCatalog(context.Background(), conn, candidate, snapshot, identity)
	if err != nil || catalogInternallyValid(bad) {
		t.Fatalf("corrupt=%+v err=%v", bad, err)
	}
	if _, err := createBuildingCatalog(context.Background(), conn, candidate, true, cat); err == nil {
		t.Fatal("published replacement succeeded")
	}
	var status string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM mrfweb.release_catalogs WHERE id=$1`, cat).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "published" {
		t.Fatalf("status=%s", status)
	}
}

func TestIntegrationCatalogAdvisoryLockContention(t *testing.T) {
	pool := filterCatalogTestDB(t)
	ctx := context.Background()
	first, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := pool.Acquire(ctx)
	if err != nil {
		first.Release()
		t.Fatal(err)
	}
	key := release.FilterCatalogLockKey("uhc", "2026-08")
	defer func() {
		_, _ = first.Exec(context.Background(), "SELECT pg_advisory_unlock($1::bigint)", key)
		_, _ = second.Exec(context.Background(), "SELECT pg_advisory_unlock($1::bigint)", key)
		first.Release()
		second.Release()
	}()
	locked, err := tryCatalogLock(ctx, first, key)
	if err != nil || !locked {
		t.Fatalf("first lock=%v err=%v", locked, err)
	}
	locked, err = tryCatalogLock(ctx, second, key)
	if err != nil || locked {
		t.Fatalf("second lock=%v err=%v", locked, err)
	}
	_, _ = first.Exec(context.Background(), "SELECT pg_advisory_unlock($1::bigint)", key)
	locked, err = tryCatalogLock(ctx, second, key)
	if err != nil || !locked {
		t.Fatalf("retry lock=%v err=%v", locked, err)
	}
}
