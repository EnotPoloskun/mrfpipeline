package reconcile

import (
	"context"
	"testing"
	"time"
)

func TestIntegrationStory31AuditCatalogMissingClassification(t *testing.T) {
	for _, tc := range []struct {
		status           string
		backfill, sealed int
	}{{"inactive", 1, 0}, {"active", 0, 1}} {
		t.Run(tc.status, func(t *testing.T) {
			pool := testDB(t)
			ctx := context.Background()
			month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
			ws := workspace(t)
			if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.monthly_releases(payer_id,collection_month,status,sealed_at,last_activated_at,publication_generation) VALUES('uhc',$1,$2,transaction_timestamp(),transaction_timestamp(),1)`, month, tc.status); err != nil {
				t.Fatal(err)
			}
			report := Report{}
			if err := auditFilterCatalogs(ctx, Params{Pool: pool, Workspace: ws, WarehousePath: t.TempDir(), ProviderCatalogPath: t.TempDir()}, &report); err != nil {
				t.Fatal(err)
			}
			if report.FilterCatalogBackfillRequiredCount != tc.backfill || report.SealedReleaseInconsistencyCount != tc.sealed {
				t.Fatalf("report=%+v", report)
			}
		})
	}
}
func TestIntegrationStory31InactiveReadyCatalogBackfill(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	m := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.monthly_releases(payer_id,collection_month,status,sealed_at,last_activated_at,publication_generation) VALUES('uhc',$1,'inactive',transaction_timestamp(),transaction_timestamp(),1)`, m); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfweb.release_catalogs(payer_id,collection_month,publication_generation,status,output_fingerprint,output_count,standard_fact_count,provider_catalog_schema_version,provider_catalog_release_month,billing_code_count,code_filter_value_count,plan_count,plan_output_count,output_code_network_count,provider_filter_value_count,completed_at) VALUES('uhc',$1,1,'ready',repeat('0',64),1,1,1,$1,1,0,1,1,0,0,transaction_timestamp())`, m); err != nil {
		t.Fatal(err)
	}
	r := Report{}
	if err := auditFilterCatalogs(ctx, Params{Pool: pool, Workspace: workspace(t), WarehousePath: t.TempDir(), ProviderCatalogPath: t.TempDir()}, &r); err != nil {
		t.Fatal(err)
	}
	if r.FilterCatalogBackfillRequiredCount != 1 || r.SealedReleaseInconsistencyCount != 0 {
		t.Fatalf("report=%+v", r)
	}
}
