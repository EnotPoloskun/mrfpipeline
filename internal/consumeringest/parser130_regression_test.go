package consumeringest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/EnotPoloskun/mrfparser"
	"github.com/enotpoloskun/mrfconsumer"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/mrfparse"
	"github.com/parquet-go/parquet-go"
)

const priceFilterPriceFields = `"billing_class":"professional","negotiated_rate":100,"expiration_date":"2027-01-01","setting":"outpatient"`

func priceFilterMRFJSON() string {
	return `{
  "reporting_entity_name": "Parser 1.3.0 price-filter regression",
  "reporting_entity_type": "issuer",
  "last_updated_on": "2026-07-27",
  "version": "2.2.1",
  "provider_references": [
    {
      "provider_group_id": 1,
      "provider_groups": [
        {"npi": [1111111111], "tin": {"type": "ein", "value": "11-1111111"}}
      ]
    },
    {
      "provider_group_id": 2,
      "provider_groups": [
        {"npi": [1111111111], "tin": {"type": "ein", "value": "22-2222222"}}
      ]
    }
  ],
  "in_network": [
    {
      "billing_code_type": "CPT",
      "billing_code": "99213",
      "negotiated_rates": [
        {
          "provider_references": [1],
          "negotiated_prices": [
            {"negotiated_type": "negotiated",` + priceFilterPriceFields + `},
            {"negotiated_type": "derived",` + priceFilterPriceFields + `},
            {"negotiated_type": "negotiated",` + priceFilterPriceFields + `}
          ]
        },
        {
          "provider_references": [1],
          "negotiated_prices": [
            {"negotiated_type": "derived",` + priceFilterPriceFields + `}
          ]
        }
      ]
    },
    {
      "billing_code_type": "CPT",
      "billing_code": "99214",
      "negotiated_rates": [
        {
          "provider_references": [2],
          "negotiated_prices": [
            {"negotiated_type": "percentage",` + priceFilterPriceFields + `},
            {"negotiated_type": "derived",` + priceFilterPriceFields + `}
          ]
        }
      ]
    }
  ]
}`
}

func writePriceFilterServices(t *testing.T, rows string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "services.csv")
	if err := os.WriteFile(path, []byte("billing_code_type,billing_code\n"+rows), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

type parserNegotiatedPriceRow struct {
	ServiceID      int64   `parquet:"service_id"`
	RateGroupID    int64   `parquet:"rate_group_id"`
	PriceID        int64   `parquet:"price_id"`
	NegotiatedType *string `parquet:"negotiated_type,optional"`
}

type parserRateProviderGroupRow struct {
	RateGroupID     int64  `parquet:"rate_group_id"`
	ProviderGroupID string `parquet:"provider_group_id"`
}

type snapshotRateFactRow struct {
	RateGroupID    int64   `parquet:"rate_group_id"`
	PriceID        int64   `parquet:"price_id"`
	NegotiatedType *string `parquet:"negotiated_type,optional"`
}

type snapshotRateProviderGroupRow struct {
	RateGroupID     int64  `parquet:"rate_group_id"`
	ProviderGroupID string `parquet:"provider_group_id"`
}

func parseMRFIntoWorkspace(t *testing.T, ws *artifact.Workspace, sourceID int64, mrf []byte, services string) (parsed, sourceURI string) {
	t.Helper()
	t.Setenv("TMPDIR", ws.StagingDir())
	data, err := ws.DownloadDataPath(artifact.KindMRF, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err = ws.ParsedDir(artifact.KindMRF, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(data), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(data, mrf, 0600); err != nil {
		t.Fatal(err)
	}
	cfg := mrfparser.DefaultConfig()
	cfg.Input = data
	cfg.Output = parsed
	cfg.Services = services
	cfg.TempDir = ws.StagingDir()
	if err := mrfparser.Parse(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	sourceURI, err = normalizePath(data)
	if err != nil {
		t.Fatal(err)
	}
	if resolved, rerr := filepath.EvalSymlinks(sourceURI); rerr == nil {
		sourceURI = resolved
	}
	if err := mrfparse.ValidateCompletedOutput(parsed, sourceURI, services); err != nil {
		t.Fatal(err)
	}
	return parsed, sourceURI
}

func readParquetRows[T any](t *testing.T, path string) []T {
	t.Helper()
	rows, err := parquet.ReadFile[T](path)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func firstParquetPart(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".parquet" {
			return filepath.Join(dir, e.Name())
		}
	}
	t.Fatalf("no parquet in %s", dir)
	return ""
}

func readParserManifestCounts(t *testing.T, parsed string) map[string]int64 {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(parsed, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var man struct {
		ManifestSchemaVersion string `json:"manifest_schema_version"`
		OutputSchemaVersion   string `json:"output_schema_version"`
		PriceSelection        struct {
			Mode            string   `json:"mode"`
			NegotiatedTypes []string `json:"negotiated_types"`
		} `json:"price_selection"`
		Counts map[string]int64 `json:"counts"`
	}
	if err := json.Unmarshal(raw, &man); err != nil {
		t.Fatal(err)
	}
	if man.ManifestSchemaVersion != "1.3.0" || man.OutputSchemaVersion != "1.3.0" {
		t.Fatalf("schema versions %#v", man)
	}
	if man.PriceSelection.Mode != "negotiated_types" || !reflect.DeepEqual(man.PriceSelection.NegotiatedTypes, []string{"negotiated"}) {
		t.Fatalf("price_selection %#v", man.PriceSelection)
	}
	return man.Counts
}

func downgradeParsedManifestTo120(t *testing.T, parsed string) {
	t.Helper()
	path := filepath.Join(parsed, "manifest.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var man map[string]any
	if err := json.Unmarshal(raw, &man); err != nil {
		t.Fatal(err)
	}
	man["manifest_schema_version"] = "1.2.0"
	man["output_schema_version"] = "1.2.0"
	delete(man, "price_selection")
	counts, ok := man["counts"].(map[string]any)
	if !ok {
		t.Fatal("counts")
	}
	delete(counts, "filtered_price_count")
	out, err := json.Marshal(man)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, out, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestParser130PriceFilterParsePipelineConsumer(t *testing.T) {
	ws := mustWorkspace(t)
	services := writePriceFilterServices(t, "CPT,99213\nCPT,99214\n")
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	const sourceID int64 = 1301
	parsed, _ := parseMRFIntoWorkspace(t, ws, sourceID, []byte(priceFilterMRFJSON()), services)

	counts := readParserManifestCounts(t, parsed)
	wantCounts := map[string]int64{
		"services": 2, "rate_groups": 3, "negotiated_prices": 2,
		"rate_provider_groups": 3, "provider_groups": 2, "providers": 2,
		"filtered_price_count": 4,
	}
	for key, want := range wantCounts {
		if counts[key] != want {
			t.Fatalf("%s = %d want %d (counts=%v)", key, counts[key], want, counts)
		}
	}

	prices := readParquetRows[parserNegotiatedPriceRow](t, firstParquetPart(t, filepath.Join(parsed, "negotiated_prices")))
	if len(prices) != 2 {
		t.Fatalf("negotiated_prices rows %d", len(prices))
	}
	sort.Slice(prices, func(i, j int) bool { return prices[i].PriceID < prices[j].PriceID })
	if prices[0].PriceID != 0 || prices[1].PriceID != 2 {
		t.Fatalf("gapped price_ids %+v", prices)
	}
	for _, row := range prices {
		if row.NegotiatedType == nil || *row.NegotiatedType != "negotiated" {
			t.Fatalf("negotiated_type %+v", row)
		}
	}

	links := readParquetRows[parserRateProviderGroupRow](t, firstParquetPart(t, filepath.Join(parsed, "rate_provider_groups")))
	if len(links) != 3 {
		t.Fatalf("rate_provider_groups rows %d", len(links))
	}

	const snapID = 1301
	w := &Worker{Workspace: ws, WarehousePath: warehouse, ServicesPath: services, Catalog: cat}
	if err := w.ingest(context.Background(), ingestJob(1, snapID), claimIdentity{
		SnapshotID: snapID, SourceID: sourceID, PayerID: "uhc",
		Month: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), MonthText: "2026-08", ConsumeJobID: 1,
	}); err != nil {
		t.Fatal(err)
	}

	outputID := formatSnapshotOutputID(snapID)
	final, err := expectedFinalPath(warehouse, "uhc", "2026-08", outputID)
	if err != nil {
		t.Fatal(err)
	}
	if err := inspectCompletedSnapshot(warehouse, "uhc", "2026-08", outputID, catalogIdentity{
		SchemaVersion: 1, ReleaseMonth: testCatalogMonth,
	}); err != nil {
		t.Fatal(err)
	}

	snapCounts, err := inspectCompletedSnapshotCounts(warehouse, "uhc", "2026-08", outputID, catalogIdentity{
		SchemaVersion: 1, ReleaseMonth: testCatalogMonth,
	})
	if err != nil {
		t.Fatal(err)
	}
	if snapCounts["rate_facts"].RowCount != 2 || snapCounts["rate_provider_groups"].RowCount != 3 {
		t.Fatalf("snapshot counts %+v", snapCounts)
	}

	facts := readParquetRows[snapshotRateFactRow](t, firstParquetPart(t, filepath.Join(final, "rate_facts")))
	if len(facts) != 2 {
		t.Fatalf("rate_facts %d", len(facts))
	}
	sort.Slice(facts, func(i, j int) bool { return facts[i].PriceID < facts[j].PriceID })
	if facts[0].PriceID != 0 || facts[1].PriceID != 2 {
		t.Fatalf("snapshot price_ids %+v", facts)
	}
	for _, row := range facts {
		if row.NegotiatedType == nil || *row.NegotiatedType != "negotiated" {
			t.Fatalf("fact type %+v", row)
		}
	}

	snapLinks := readParquetRows[snapshotRateProviderGroupRow](t, firstParquetPart(t, filepath.Join(final, "rate_provider_groups")))
	if len(snapLinks) != 3 {
		t.Fatalf("snapshot rate_provider_groups %d", len(snapLinks))
	}
}

func TestParser130AllPricesFilteredZeroFactsRetainsLinks(t *testing.T) {
	ws := mustWorkspace(t)
	services := writePriceFilterServices(t, "CPT,99214\n")
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	const sourceID int64 = 1302
	parsed, _ := parseMRFIntoWorkspace(t, ws, sourceID, []byte(priceFilterMRFJSON()), services)

	counts := readParserManifestCounts(t, parsed)
	if counts["negotiated_prices"] != 0 || counts["filtered_price_count"] != 2 || counts["rate_provider_groups"] != 1 {
		t.Fatalf("counts %v", counts)
	}
	links := readParquetRows[parserRateProviderGroupRow](t, firstParquetPart(t, filepath.Join(parsed, "rate_provider_groups")))
	if len(links) != 1 || links[0].ProviderGroupID != "2" {
		t.Fatalf("parser links %+v", links)
	}

	const snapID = 1302
	w := &Worker{Workspace: ws, WarehousePath: warehouse, ServicesPath: services, Catalog: cat}
	if err := w.ingest(context.Background(), ingestJob(1, snapID), claimIdentity{
		SnapshotID: snapID, SourceID: sourceID, PayerID: "uhc",
		Month: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), MonthText: "2026-08", ConsumeJobID: 1,
	}); err != nil {
		t.Fatal(err)
	}

	outputID := formatSnapshotOutputID(snapID)
	final, err := expectedFinalPath(warehouse, "uhc", "2026-08", outputID)
	if err != nil {
		t.Fatal(err)
	}
	snapCounts, err := inspectCompletedSnapshotCounts(warehouse, "uhc", "2026-08", outputID, catalogIdentity{
		SchemaVersion: 1, ReleaseMonth: testCatalogMonth,
	})
	if err != nil {
		t.Fatal(err)
	}
	if snapCounts["rate_facts"].RowCount != 0 || snapCounts["rate_provider_groups"].RowCount != 1 {
		t.Fatalf("snapshot counts %+v", snapCounts)
	}
	facts := readParquetRows[snapshotRateFactRow](t, firstParquetPart(t, filepath.Join(final, "rate_facts")))
	if len(facts) != 0 {
		t.Fatalf("rate_facts %+v", facts)
	}
	snapLinks := readParquetRows[snapshotRateProviderGroupRow](t, firstParquetPart(t, filepath.Join(final, "rate_provider_groups")))
	if len(snapLinks) != 1 || snapLinks[0].ProviderGroupID != "2" {
		t.Fatalf("snapshot links %+v", snapLinks)
	}
}

func TestWorkerRejectsReusedParser120OutputWithoutConsumer(t *testing.T) {
	ws := mustWorkspace(t)
	services := writePriceFilterServices(t, "CPT,99213\n")
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	const sourceID int64 = 1303
	parsed, _ := parseMRFIntoWorkspace(t, ws, sourceID, []byte(priceFilterMRFJSON()), services)
	pricePart := firstParquetPart(t, filepath.Join(parsed, "negotiated_prices"))
	before, err := os.ReadFile(pricePart)
	if err != nil {
		t.Fatal(err)
	}
	downgradeParsedManifestTo120(t, parsed)

	called := false
	w := &Worker{
		Workspace: ws, WarehousePath: warehouse, ServicesPath: services, Catalog: cat,
		Ingest: func(context.Context, mrfconsumer.Config) (mrfconsumer.Report, error) {
			called = true
			return mrfconsumer.Report{}, errors.New("consumer must not run")
		},
	}
	err = w.ingest(context.Background(), ingestJob(1, sourceID), claimIdentity{
		SnapshotID: sourceID, SourceID: sourceID, PayerID: "uhc",
		Month: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), MonthText: "2026-08", ConsumeJobID: 1,
	})
	if !jobs.IsFailure(err, jobs.FailureConsumerIngestInputInvalid) {
		t.Fatalf("got %v", err)
	}
	if called {
		t.Fatal("consumer invoked on legacy parser output")
	}
	after, err := os.ReadFile(pricePart)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("parsed parquet changed after rejected ingest")
	}
	if _, err := os.Stat(filepath.Join(parsed, "manifest.json")); err != nil {
		t.Fatal(err)
	}
}
