package reconcile

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/consumeringest"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
)

func TestIntegrationStory31AuditActiveExactCatalog(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	m := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	ws := workspace(t)
	svc := filepath.Join(t.TempDir(), "services.csv")
	if err := os.WriteFile(svc, []byte("billing_code_type,billing_code\nCPT,1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.monthly_releases(payer_id,collection_month,status,sealed_at,last_activated_at,publication_generation) VALUES('uhc',$1,'active',transaction_timestamp(),transaction_timestamp(),1)`, m); err != nil {
		t.Fatal(err)
	}
	var source, snap, plan, batch, cat int64
	if err := pool.QueryRow(ctx, `INSERT INTO mrfpipeline.mrf_sources(source_url,collection_month,download_status,parse_status) VALUES('https://example.invalid/exact',$1,'succeeded','succeeded') RETURNING id`, m).Scan(&source); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO mrfpipeline.mrf_snapshots(mrf_source_id,payer_id,collection_month,consume_status) VALUES($1,'uhc',$2,'succeeded') RETURNING id`, source, m).Scan(&snap); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO mrfpipeline.mrf_plans(mrf_snapshot_id,plan_name,issuer_name,plan_id_type,plan_id,plan_market_type) VALUES($1,'Plan','Issuer','hios','id','group') RETURNING id`, snap).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO mrfpipeline.plan_attachment_batches(mrf_snapshot_id,status,requested_plan_count,added_plan_count,completed_at) VALUES($1,'succeeded',1,1,transaction_timestamp()) RETURNING id`, snap).Scan(&batch); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.plan_attachment_batch_items(plan_attachment_batch_id,mrf_plan_id) VALUES($1,$2)`, batch, plan); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.monthly_release_outputs(payer_id,collection_month,mrf_snapshot_id,published_generation) VALUES('uhc',$1,$2,1)`, m, snap); err != nil {
		t.Fatal(err)
	}
	target := release.Target{PayerID: "uhc", CollectionMonth: "2026-08", OutputID: "mrf-" + fmt.Sprint(snap), SnapshotID: snap}
	fp := release.OutputFingerprint([]release.Target{target})
	if err := pool.QueryRow(ctx, `INSERT INTO mrfweb.release_catalogs(payer_id,collection_month,publication_generation,status,output_fingerprint,output_count,standard_fact_count,provider_catalog_schema_version,provider_catalog_release_month,billing_code_count,plan_count,plan_output_count,code_filter_value_count,output_code_network_count,provider_filter_value_count,completed_at,published_at) VALUES('uhc',$1,1,'published',$2,1,1,1,$1,1,1,1,0,0,0,transaction_timestamp(),transaction_timestamp()) RETURNING id`, m, fp).Scan(&cat); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfweb.release_outputs(catalog_id,mrf_snapshot_id,output_id) VALUES($1,$2,$3)`, cat, snap, target.OutputID); err != nil {
		t.Fatal(err)
	}
	var pid int64
	if err := pool.QueryRow(ctx, `INSERT INTO mrfweb.release_plans(catalog_id,plan_name,issuer_name,plan_id_type,plan_id,plan_market_type,search_text) VALUES($1,'Plan','Issuer','hios','id','group','plan issuer hios id group') RETURNING id`, cat).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfweb.release_billing_codes(catalog_id,billing_code_type,billing_code,observation_count,unmodified_observation_count) VALUES($1,'CPT','1',1,1)`, cat); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfweb.release_plan_outputs(catalog_id,plan_id,output_id) VALUES($1,$2,$3)`, cat, pid, target.OutputID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE mrfpipeline.monthly_releases SET sealed_at=transaction_timestamp(),last_activated_at=transaction_timestamp() WHERE payer_id='uhc' AND collection_month=$1`, m); err != nil {
		t.Fatal(err)
	}
	catalogPath := filepath.Join(t.TempDir(), "provider_catalog")
	if err := os.MkdirAll(catalogPath, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(catalogPath, "manifest.json"), []byte(`{"schema_version":1,"release_month":"2026-08","providers":{"path":"providers","rows":0},"provider_taxonomies":{"path":"provider_taxonomies","rows":0}}`), 0600); err != nil {
		t.Fatal(err)
	}
	writeRecognizedSnapshot(t, ws.Root, "uhc", "2026-08", target.OutputID)
	audit := filepath.Join(ws.Root, "plan_associations", "output_id="+target.OutputID)
	if err := os.MkdirAll(audit, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(audit, fmt.Sprintf("plan-batch-%d-part-00000.parquet", batch)), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := release.AuditPublishedCatalog(ctx, pool, "uhc", m, 1, []release.Target{target}); err != nil {
		t.Fatal(err)
	}
	warehouse, err := consumeringest.InspectWarehouse(ws.Root)
	if err != nil {
		t.Fatal(err)
	}
	configured, err := consumeringest.InspectCatalog(catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	schema, releaseMonth, err := consumeringest.InspectCatalogIdentity(configured.Path)
	if err != nil || schema != 1 || releaseMonth != "2026-08" {
		t.Fatalf("catalog identity %d/%s/%v", schema, releaseMonth, err)
	}
	if err := consumeringest.CheckWarehouseCatalog(warehouse, configured, ws.Root, svc); err != nil {
		t.Fatal(err)
	}
	if err := ValidateActivationTargets(ctx, pool, ws.Root, "uhc", m, []release.Target{target}); err != nil {
		t.Fatal(err)
	}
	r := Report{}
	if err := auditFilterCatalogs(ctx, Params{Pool: pool, Workspace: ws, WarehousePath: ws.Root, ProviderCatalogPath: catalogPath, ServicesPath: svc}, &r); err != nil {
		t.Fatal(err)
	}
	if r.SealedReleaseInconsistencyCount != 0 {
		t.Fatalf("report=%+v", r)
	}
	full, err := Run(ctx, Params{Pool: pool, Workspace: ws, WarehousePath: ws.Root, ProviderCatalogPath: catalogPath, ServicesPath: svc, Logger: jobs.NewLogger(io.Discard)})
	if err != nil || full.SealedReleaseInconsistencyCount != 0 || full.FilterCatalogBackfillRequiredCount != 0 {
		t.Fatalf("full report=%+v err=%v", full, err)
	}
	emptyReport, err := Run(ctx, Params{Pool: pool, Workspace: ws, WarehousePath: t.TempDir(), ProviderCatalogPath: catalogPath, ServicesPath: svc, Logger: jobs.NewLogger(io.Discard)})
	if err != nil || emptyReport.SealedReleaseInconsistencyCount == 0 {
		t.Fatalf("empty report=%+v err=%v", emptyReport, err)
	}
	if err := os.WriteFile(filepath.Join(catalogPath, "manifest.json"), []byte(`{"schema_version":2,"release_month":"2026-09","providers":{"path":"providers","rows":0},"provider_taxonomies":{"path":"provider_taxonomies","rows":0}}`), 0600); err != nil {
		t.Fatal(err)
	}
	mismatchReport, err := Run(ctx, Params{Pool: pool, Workspace: ws, WarehousePath: ws.Root, ProviderCatalogPath: catalogPath, ServicesPath: svc, Logger: jobs.NewLogger(io.Discard)})
	if err != nil || mismatchReport.SealedReleaseInconsistencyCount == 0 {
		t.Fatalf("mismatch report=%+v err=%v", mismatchReport, err)
	}
	if err := os.WriteFile(filepath.Join(catalogPath, "manifest.json"), []byte(`{"schema_version":1,"release_month":"2026-08","providers":{"path":"providers","rows":0},"provider_taxonomies":{"path":"provider_taxonomies","rows":0}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE mrfweb.release_catalogs SET plan_count=plan_count+1 WHERE id=$1`, cat); err != nil {
		t.Fatal(err)
	}
	r = Report{}
	if err := auditFilterCatalogs(ctx, Params{Pool: pool, Workspace: ws, WarehousePath: ws.Root, ProviderCatalogPath: catalogPath, ServicesPath: svc}, &r); err != nil {
		t.Fatal(err)
	}
	if r.SealedReleaseInconsistencyCount != 1 || r.FilterCatalogBackfillRequiredCount != 0 {
		t.Fatalf("corrupt=%+v", r)
	}
	if _, err := pool.Exec(ctx, `UPDATE mrfweb.release_catalogs SET plan_count=plan_count-1 WHERE id=$1`, cat); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE mrfpipeline.monthly_releases SET status='inactive' WHERE payer_id='uhc' AND collection_month=$1`, m); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE mrfweb.release_catalogs SET plan_count=plan_count+1 WHERE id=$1`, cat); err != nil {
		t.Fatal(err)
	}
	r = Report{}
	if err := auditFilterCatalogs(ctx, Params{Pool: pool, Workspace: ws, WarehousePath: ws.Root, ProviderCatalogPath: catalogPath, ServicesPath: svc}, &r); err != nil {
		t.Fatal(err)
	}
	if r.SealedReleaseInconsistencyCount != 1 || r.FilterCatalogBackfillRequiredCount != 0 {
		t.Fatalf("inactive corrupt=%+v", r)
	}
}
