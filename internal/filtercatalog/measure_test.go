package filtercatalog

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestMeasurementJSONAllowlist(t *testing.T) {
	data, err := json.Marshal(Measurement{
		WarehouseSchemaVersion:       "2.0.0",
		ProviderCatalogSchemaVersion: 1,
		ProviderCatalogReleaseMonth:  "2026-08",
		PublicationGeneration:        1,
		CandidateOutputCount:         2,
		StandardFactCount:            3,
		BillingCodeCount:             4,
		CodeFilterValueCount:         5,
		PlanCount:                    6,
		PlanOutputCount:              7,
		OutputCodeNetworkCount:       8,
		ProviderFilterValueCount:     9,
		DuckDBWallTimeMS:             10,
		PostgresPopulationWallTimeMS: 11,
		TotalWallTimeMS:              12,
		PeakRSSBytes:                 13,
		CatalogDatabaseBytes:         14,
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"warehouse_schema_version", "provider_catalog_schema_version", "provider_catalog_release_month",
		"publication_generation", "candidate_output_count", "standard_fact_count", "billing_code_count",
		"code_filter_value_count", "plan_count", "plan_output_count", "output_code_network_count",
		"provider_filter_value_count", "duckdb_wall_time_ms", "postgres_population_wall_time_ms",
		"total_wall_time_ms", "peak_rss_bytes", "catalog_database_bytes",
	}
	keys := make([]string, 0, len(got))
	for key := range got {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	sort.Strings(want)
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("measurement fields=%v want=%v", keys, want)
	}
}

func TestElapsedMillisecondsIsMonotonicAndIntegral(t *testing.T) {
	start := time.Unix(0, 0)
	if got := elapsedMilliseconds(start.Add(1250*time.Millisecond), start); got != 1250 {
		t.Fatalf("elapsed=%d", got)
	}
	if got := elapsedMilliseconds(start.Add(-time.Nanosecond), start); got != 0 {
		t.Fatalf("negative elapsed=%d", got)
	}
}

func TestSameBuildCountsIgnoresUnchangedMarker(t *testing.T) {
	left := BuildResult{PayerID: "payer-a", CollectionMonth: "2026-08", PublicationGeneration: 1, CatalogID: 3, CatalogStatus: "ready", OutputCount: 1, StandardFactCount: 2, BillingCodeCount: 3, CodeFilterValueCount: 4, PlanCount: 5, PlanOutputCount: 6, OutputCodeNetworkCount: 7, ProviderFilterValueCount: 8}
	right := left
	right.Unchanged = true
	if !sameBuildCounts(left, right) {
		t.Fatal("equivalent build counts did not match")
	}
}

func TestMeasurementJSONLeavesPeakRSSForWrapper(t *testing.T) {
	data, err := json.Marshal(Measurement{
		WarehouseSchemaVersion:      "2.0.0",
		ProviderCatalogReleaseMonth: "2026-08",
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 17 {
		t.Fatalf("fields=%d", len(got))
	}
	if got["peak_rss_bytes"] != float64(0) {
		t.Fatalf("peak_rss_bytes=%v", got["peak_rss_bytes"])
	}
}
