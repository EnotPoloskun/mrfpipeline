package filtercatalog

import (
	"context"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5/pgxpool"
	"path/filepath"
	"testing"
	"time"
)

func setupInactiveStatus(t *testing.T) (*pgxpool.Pool, int64, release.CatalogCandidate, warehouseIdentity) {
	t.Helper()
	pool := filterCatalogTestDB(t)
	cat, candidate, identity := seedFilterCatalogBuild(t, pool)
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(context.Background(), `UPDATE mrfpipeline.monthly_releases SET status='inactive', sealed_at=transaction_timestamp(), last_activated_at=transaction_timestamp(), publication_generation=1 WHERE payer_id='uhc' AND collection_month=$1`, month); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `INSERT INTO mrfpipeline.monthly_release_outputs(payer_id,collection_month,mrf_snapshot_id,published_generation) VALUES ('uhc',$1,$2,1)`, month, candidate.Targets[0].SnapshotID); err != nil {
		t.Fatal(err)
	}
	return pool, cat, candidate, identity
}

func TestIntegrationStatusIsDatabaseOnlyAndNullable(t *testing.T) {
	pool, cat, _, _ := setupInactiveStatus(t)
	if _, err := pool.Exec(context.Background(), `DELETE FROM mrfweb.release_catalogs WHERE id=$1`, cat); err != nil {
		t.Fatal(err)
	}
	got, err := Status(context.Background(), pool, "uhc", time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	if err != nil || !got.ProspectiveCandidateValid || got.CurrentPublicationGeneration != 1 || got.ProspectivePublicationGeneration == nil || *got.ProspectivePublicationGeneration != 1 || got.CurrentCatalogID != nil || got.ProspectiveCatalogID != nil {
		t.Fatalf("status=%+v err=%v", got, err)
	}
}
func populateStatusFixture(t *testing.T, pool *pgxpool.Pool, cat int64, candidate release.CatalogCandidate, identity warehouseIdentity) {
	t.Helper()
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	plan := planIdentity{PlanName: "Plan", IssuerName: "Issuer", PlanIDType: "hios", PlanID: "id", PlanMarketType: "group"}
	projection := planProjection{Plans: []canonicalPlan{{planIdentity: plan, Outputs: []string{candidate.Targets[0].OutputID}}}, PlanOutputs: []planOutput{{Plan: plan, Output: candidate.Targets[0].OutputID}}}
	extracted := Result{WarehouseSchemaVersion: "2.0.0", ProviderCatalogSchemaVersion: 1, ProviderCatalogReleaseMonth: identity.ReleaseMonth, OutputFingerprint: candidate.OutputFingerprint, StandardFactCount: 1, BillingCodes: []BillingCode{{BillingCodeType: "CPT", BillingCode: "1", ObservationCount: 1, UnmodifiedObservationCount: 1}}}
	if _, err := populateCatalog(context.Background(), conn, cat, candidate, identity, extracted, projection); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationStatusReadyAndInvalidProspective(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "ready", true: "invalid"}[invalid], func(t *testing.T) {
			pool, cat, candidate, identity := setupInactiveStatus(t)
			populateStatusFixture(t, pool, cat, candidate, identity)
			if invalid {
				if _, err := pool.Exec(context.Background(), `UPDATE mrfpipeline.mrf_snapshots SET consume_status='failed', failure_code='test' WHERE id=$1`, candidate.Targets[0].SnapshotID); err != nil {
					t.Fatal(err)
				}
			}
			got, err := Status(context.Background(), pool, "uhc", identity.ReleaseMonth)
			if err != nil {
				t.Fatal(err)
			}
			if got.CurrentCatalogID == nil || got.CurrentCatalogStatus == nil || *got.CurrentCatalogStatus != "ready" || got.CurrentCatalogOutputCount == nil || *got.CurrentCatalogOutputCount != 1 {
				t.Fatalf("current=%+v", got)
			}
			if invalid {
				if got.ProspectiveCandidateValid || got.ProspectivePublicationGeneration != nil || got.ProspectiveCatalogID != nil || got.ProspectiveCatalogStatus != nil || got.ProspectiveCatalogOutputCount != nil {
					t.Fatalf("prospective=%+v", got)
				}
			} else if !got.ProspectiveCandidateValid || got.ProspectiveCatalogID == nil || *got.ProspectiveCatalogID != *got.CurrentCatalogID {
				t.Fatalf("prospective=%+v", got)
			}
		})
	}
}

func TestIntegrationBuildExactReadySkipsDuckDB(t *testing.T) {
	pool, cat, candidate, identity := setupInactiveStatus(t)
	populateStatusFixture(t, pool, cat, candidate, identity)
	warehouse := writeExtractionWarehouse(t, filepath.Join(t.TempDir(), "warehouse"))
	t.Setenv("PATH", t.TempDir())
	got, err := Build(context.Background(), BuildParams{Pool: pool, PayerID: "uhc", CollectionMonth: identity.ReleaseMonth, WarehousePath: warehouse, ProviderCatalogPath: filepath.Join(warehouse, "provider_catalog")})
	if err != nil || !got.Unchanged || got.CatalogID != cat || got.CatalogStatus != "ready" || got.OutputCount != 1 {
		t.Fatalf("build=%+v err=%v", got, err)
	}
}

func TestIntegrationUnpublishedCatalogReplacement(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "building", true: "failed"}[failed], func(t *testing.T) {
			pool, cat, candidate, _ := setupInactiveStatus(t)
			if failed {
				if _, err := pool.Exec(context.Background(), `UPDATE mrfweb.release_catalogs SET status='failed', failure_code='test', completed_at=transaction_timestamp() WHERE id=$1`, cat); err != nil {
					t.Fatal(err)
				}
			}
			conn, err := pool.Acquire(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Release()
			newID, err := createBuildingCatalog(context.Background(), conn, candidate, true, cat)
			if err != nil || newID == cat {
				t.Fatalf("new=%d err=%v", newID, err)
			}
			var old int
			if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfweb.release_catalogs WHERE id=$1`, cat).Scan(&old); err != nil {
				t.Fatal(err)
			}
			if old != 0 {
				t.Fatal("old catalog remains")
			}
			var count int
			if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfweb.release_outputs WHERE catalog_id=$1`, newID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("outputs=%d", count)
			}
		})
	}
}
