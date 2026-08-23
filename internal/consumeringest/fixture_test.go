package consumeringest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/EnotPoloskun/mrfparser"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/mrfparse"
	"github.com/parquet-go/parquet-go"
)

const testCatalogMonth = "2026-08"

type catalogProviderRow struct {
	NPI        string  `parquet:"npi"`
	State      *string `parquet:"state,optional"`
	City       *string `parquet:"city,optional"`
	PostalCode *string `parquet:"postal_code,optional"`
}

type catalogTaxonomyRow struct {
	NPI          string `parquet:"npi"`
	TaxonomyCode string `parquet:"taxonomy_code"`
}

func writeCatalogFixture(t testing.TB, root string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "providers"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "provider_taxonomies"), 0700); err != nil {
		t.Fatal(err)
	}
	state, city, zip := "fl", "miami", "33101"
	writeParquet(t, filepath.Join(root, "providers", "part-00000.parquet"), []catalogProviderRow{{
		NPI: "1111111111", State: &state, City: &city, PostalCode: &zip,
	}})
	writeParquet(t, filepath.Join(root, "provider_taxonomies", "part-00000.parquet"), []catalogTaxonomyRow{{
		NPI: "1111111111", TaxonomyCode: "207Q00000X",
	}})
	man := map[string]any{
		"schema_version":      1,
		"release_month":       testCatalogMonth,
		"providers":           map[string]any{"path": "providers", "rows": 1},
		"provider_taxonomies": map[string]any{"path": "provider_taxonomies", "rows": 1},
	}
	data, err := json.Marshal(man)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, fileManifest), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func writeParquet[T any](t testing.TB, path string, rows []T) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[T](f)
	if _, err := w.Write(rows); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func mustCatalog(t *testing.T) CatalogID {
	t.Helper()
	root := filepath.Join(t.TempDir(), "catalog")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	writeCatalogFixture(t, root)
	id, err := InspectCatalog(root)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustWorkspace(t *testing.T) *artifact.Workspace {
	t.Helper()
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

func mustServices(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "services.csv")
	if err := os.WriteFile(path, testdata(t, "services.csv"), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func testdata(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "mrfparse", "testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func writeValidParsed(t *testing.T, ws *artifact.Workspace, sourceID int64, servicesPath string) string {
	t.Helper()
	input, err := ws.DownloadDataPath(artifact.KindMRF, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	output, err := ws.ParsedDir(artifact.KindMRF, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	input, err = normalizePath(input)
	if err != nil {
		t.Fatal(err)
	}
	output, err = normalizePath(output)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"mrf_files", "services", "service_relations", "rate_groups",
		"negotiated_prices", "rate_provider_groups", "provider_groups", "providers",
	} {
		p := filepath.Join(output, name)
		if err := os.MkdirAll(p, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(p, "part-00000.parquet"), []byte("p"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	sel := servicesPath
	if resolved, err := filepath.EvalSymlinks(servicesPath); err == nil {
		sel = resolved
	}
	if err := os.WriteFile(filepath.Join(output, "manifest.json"), validParserManifest(mrfparse.ExpectedSourceURI(input), sel), 0600); err != nil {
		t.Fatal(err)
	}
	st := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)
	stats, _ := json.Marshal(map[string]any{
		"stored_bytes": 1, "decoded_bytes": 1,
		"started_at": st.Format(time.RFC3339Nano), "finished_at": st.Add(time.Second).Format(time.RFC3339Nano),
		"wall_seconds": 1, "decoded_mib_per_second": 1,
	})
	if err := os.WriteFile(filepath.Join(output, "processing_stats.json"), stats, 0600); err != nil {
		t.Fatal(err)
	}
	if err := mrfparse.ValidateCompletedOutput(output, mrfparse.ExpectedSourceURI(input), servicesPath); err != nil {
		t.Fatal(err)
	}
	return output
}

func validParserManifest(sourceURI, selectorURI string) []byte {
	m := map[string]any{
		"manifest_schema_version": "1.1.0",
		"output_schema_version":   "1.1.0",
		"status":                  "complete",
		"source":                  map[string]any{"uri": sourceURI, "kind": "local"},
		"selection": map[string]any{
			"mode": "service_csv", "selector_uri": selectorURI,
			"requested_unique_pair_count": 1, "matched_unique_pair_count": 0, "unmatched_unique_pair_count": 1,
		},
		"counts": map[string]any{
			"source_service_count": 0, "retained_service_count": 0, "filtered_service_count": 0,
			"mrf_files": 1, "services": 0, "service_relations": 0, "rate_groups": 0,
			"negotiated_prices": 0, "rate_provider_groups": 0, "provider_groups": 0, "providers": 0,
		},
		"warnings": map[string]any{
			"totals": map[string]any{
				"unknown_field": 0, "missing_required": 0, "invalid_value": 0, "source_schema": 0,
				"unresolved_provider_reference": 0, "duplicate_provider_definition": 0,
				"unused_provider_definition": 0, "duplicate_selector_entry": 0,
			},
			"examples": []any{}, "examples_truncated": false, "examples_omitted_count": 0,
		},
	}
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

func writePublishedSnapshot(t *testing.T, warehouse, payer, feedID, month, outputID string) {
	t.Helper()
	if err := os.MkdirAll(warehouse, 0700); err != nil {
		t.Fatal(err)
	}
	ident := catalogIdentity{SchemaVersion: 1, ReleaseMonth: testCatalogMonth}
	writeWarehouseJSON(t, warehouse, ident)
	final, err := expectedFinalPath(warehouse, payer, month, outputID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(final, 0700); err != nil {
		t.Fatal(err)
	}
	counts := map[string]datasetCount{}
	for _, name := range snapshotDatasets {
		rows := int64(0)
		if name == "ingestions" {
			rows = 1
		}
		counts[name] = datasetCount{RowCount: rows, PartCount: 1}
		p := filepath.Join(final, name)
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
		part := fmt.Sprintf("%s-part-%05d.parquet", outputID, 0)
		if err := os.WriteFile(filepath.Join(p, part), []byte("p"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	man := snapshotManifest{
		OutputID: outputID, PayerID: payer, FeedID: feedID, CollectionMonth: month,
		Catalog: ident, Datasets: counts,
	}
	if err := os.WriteFile(filepath.Join(final, fileManifest), mustSnapshotJSON(man), 0600); err != nil {
		t.Fatal(err)
	}
}

func writeWarehouseJSON(t *testing.T, warehouse string, ident catalogIdentity) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"warehouse_schema_version": warehouseVersion,
		"provider_catalog":         map[string]any{"schema_version": ident.SchemaVersion, "release_month": ident.ReleaseMonth},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(warehouse, fileWarehouse), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func mustSnapshotJSON(m snapshotManifest) []byte {
	datasets := map[string]any{}
	for name, c := range m.Datasets {
		datasets[name] = map[string]any{"row_count": c.RowCount, "part_count": c.PartCount}
	}
	raw, err := json.Marshal(map[string]any{
		"manifest_schema_version": warehouseVersion,
		"output_schema_version":   warehouseVersion,
		"output_id":               m.OutputID,
		"payer_id":                m.PayerID,
		"feed_id":                 m.FeedID,
		"collection_month":        m.CollectionMonth,
		"provider_catalog":        map[string]any{"schema_version": m.Catalog.SchemaVersion, "release_month": m.Catalog.ReleaseMonth},
		"datasets":                datasets,
	})
	if err != nil {
		panic(err)
	}
	return raw
}

func mustRealParsed(t *testing.T) (parsed, services string) {
	t.Helper()
	t.Setenv("TMPDIR", t.TempDir())
	services = mustServices(t)
	dir := t.TempDir()
	in := filepath.Join(dir, "input.json")
	if err := os.WriteFile(in, testdata(t, "mrf.json"), 0600); err != nil {
		t.Fatal(err)
	}
	parsed = filepath.Join(dir, "parsed")
	cfg := mrfparser.DefaultConfig()
	cfg.Input = in
	cfg.Output = parsed
	cfg.Services = services
	cfg.TempDir = filepath.Join(dir, "tmp")
	if err := os.Mkdir(cfg.TempDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := mrfparser.Parse(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	source, err := normalizePath(in)
	if err != nil {
		t.Fatal(err)
	}
	if resolved, rerr := filepath.EvalSymlinks(source); rerr == nil {
		source = resolved
	}
	if err := mrfparse.ValidateCompletedOutput(parsed, source, services); err != nil {
		t.Fatal(err)
	}
	return parsed, services
}
