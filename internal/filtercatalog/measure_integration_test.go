package filtercatalog

import (
	"context"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

func TestIntegrationMeasureRefusesExistingCatalog(t *testing.T) {
	pool := filterCatalogTestDB(t)
	_, candidate, _ := seedFilterCatalogBuild(t, pool)
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	before := buildFailureCatalogState(t, pool)
	_, err := Measure(context.Background(), BuildParams{
		Pool: pool, PayerID: candidate.PayerID, CollectionMonth: month,
		WarehousePath: t.TempDir(), ProviderCatalogPath: t.TempDir(),
	})
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogMeasurePrecondition) {
		t.Fatalf("existing catalog measure error=%v", err)
	}
	if got := buildFailureCatalogState(t, pool); got != before {
		t.Fatal("measure precondition mutated catalog state")
	}
	var releaseStatus string
	if err := pool.QueryRow(context.Background(), `
SELECT status FROM mrfpipeline.monthly_releases
WHERE payer_id=$1 AND collection_month=$2`, candidate.PayerID, month).Scan(&releaseStatus); err != nil {
		t.Fatal(err)
	}
	if releaseStatus == "active" {
		t.Fatal("measure activated the release")
	}
}
