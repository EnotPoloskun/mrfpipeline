package filtercatalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/parquet-go/parquet-go"
)

type testRateFact struct {
	OutputID               string   `parquet:"output_id"`
	PayerID                string   `parquet:"payer_id"`
	CollectionMonth        string   `parquet:"collection_month"`
	ServiceID              int64    `parquet:"service_id"`
	RateGroupID            int64    `parquet:"rate_group_id"`
	PriceID                int64    `parquet:"price_id"`
	NegotiationArrangement *string  `parquet:"negotiation_arrangement,optional"`
	ServiceName            *string  `parquet:"service_name,optional"`
	BillingCodeType        *string  `parquet:"billing_code_type,optional"`
	BillingCodeTypeVersion *string  `parquet:"billing_code_type_version,optional"`
	BillingCode            *string  `parquet:"billing_code,optional"`
	ServiceDescription     *string  `parquet:"service_description,optional"`
	ServiceCodes           []string `parquet:"service_codes,optional"`
	BillingClass           *string  `parquet:"billing_class,optional"`
	Setting                *string  `parquet:"setting,optional"`
	NegotiatedType         *string  `parquet:"negotiated_type,optional"`
	BillingCodeModifiers   []string `parquet:"billing_code_modifiers,optional"`
	NetworkNames           []string `parquet:"network_names,list"`
	NegotiatedRate         []byte   `parquet:"negotiated_rate,optional,decimal(9:38)"`
	ExpirationDate         *int32   `parquet:"expiration_date,date"`
}

type testRateProviderGroup struct {
	OutputID        string `parquet:"output_id"`
	RateGroupID     int64  `parquet:"rate_group_id"`
	ProviderGroupID string `parquet:"provider_group_id"`
}

type testMembership struct {
	OutputID        string `parquet:"output_id"`
	ProviderGroupID string `parquet:"provider_group_id"`
	NPI             string `parquet:"npi"`
}

type testProvider struct {
	NPI        string  `parquet:"npi"`
	State      *string `parquet:"state,optional"`
	City       *string `parquet:"city,optional"`
	PostalCode *string `parquet:"postal_code,optional"`
}

type testTaxonomy struct {
	NPI          string `parquet:"npi"`
	TaxonomyCode string `parquet:"taxonomy_code"`
}

type testUnusedRow struct {
	Value string `parquet:"value"`
}

type testFileState struct {
	Mode    os.FileMode
	Size    int64
	ModTime int64
	SHA256  string
}

func stringPtr(value string) *string {
	return &value
}

func TestExtractionSyntheticWarehouse(t *testing.T) {
	binaryPath := os.Getenv("MRFPIPELINE_DUCKDB_BIN")
	if binaryPath == "" {
		t.Skip("MRFPIPELINE_DUCKDB_BIN is not set")
	}
	warehouse := writeExtractionWarehouse(t, filepath.Join(t.TempDir(), "ware'house*?[]"))
	before := extractionFileState(t, warehouse)
	outputs := []Output{
		{PayerID: "payer", CollectionMonth: "2026-08", OutputID: "mrf-20", SnapshotID: 20},
		{PayerID: "payer", CollectionMonth: "2026-08", OutputID: "mrf-3", SnapshotID: 3},
	}
	sorted := append([]Output(nil), outputs...)
	sortOutputs(sorted)
	fingerprint := outputFingerprint(sorted)
	result, err := Extract(context.Background(), DuckDB{path: binaryPath}, Params{WarehousePath: warehouse, OutputFingerprint: fingerprint, Outputs: outputs})
	if err != nil {
		t.Fatal(err)
	}
	repeat, err := Extract(context.Background(), DuckDB{path: binaryPath}, Params{WarehousePath: warehouse, OutputFingerprint: fingerprint, Outputs: outputs})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result, repeat) {
		t.Fatalf("repeat extraction differs:\nfirst=%+v\nrepeat=%+v", result, repeat)
	}
	if result.WarehouseSchemaVersion != "2.0.0" || result.ProviderCatalogSchemaVersion != 1 || !result.ProviderCatalogReleaseMonth.Equal(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("identity=%+v", result)
	}
	if result.StandardFactCount != 3 || len(result.BillingCodes) != 1 {
		t.Fatalf("summary/codes=%d/%#v", result.StandardFactCount, result.BillingCodes)
	}
	code := result.BillingCodes[0]
	wantCode := BillingCode{
		BillingCodeType:             "CPT",
		BillingCode:                 "99214",
		BillingCodeTypeVersion:      stringPtr("2025"),
		WarehouseServiceName:        stringPtr("Alpha"),
		WarehouseServiceDescription: stringPtr("Alpha description"),
		ObservationCount:            3,
		UnmodifiedObservationCount:  2,
	}
	if !reflect.DeepEqual(code, wantCode) {
		t.Fatalf("billing code=%+v want %+v", code, wantCode)
	}
	wantCodeFilters := []CodeFilterValue{
		{BillingCodeType: "CPT", BillingCode: "99214", FilterKind: "billing_class", FilterValue: "professional", DisplayLabel: "professional", ObservationCount: 2},
		{BillingCodeType: "CPT", BillingCode: "99214", FilterKind: "modifier", FilterValue: "95", DisplayLabel: "95", ObservationCount: 1},
		{BillingCodeType: "CPT", BillingCode: "99214", FilterKind: "negotiation_arrangement", FilterValue: "ffs", DisplayLabel: "ffs", ObservationCount: 2},
		{BillingCodeType: "CPT", BillingCode: "99214", FilterKind: "place_of_service", FilterValue: "11", DisplayLabel: "11", ObservationCount: 3},
		{BillingCodeType: "CPT", BillingCode: "99214", FilterKind: "place_of_service", FilterValue: "CSTM-00", DisplayLabel: "Broad or unspecified place of service", ObservationCount: 1},
		{BillingCodeType: "CPT", BillingCode: "99214", FilterKind: "setting", FilterValue: "outpatient", DisplayLabel: "outpatient", ObservationCount: 2},
	}
	if !reflect.DeepEqual(result.CodeFilterValues, wantCodeFilters) {
		t.Fatalf("code filters=%#v want %#v", result.CodeFilterValues, wantCodeFilters)
	}
	wantNetworks := []OutputCodeNetwork{
		{OutputID: "mrf-20", BillingCodeType: "CPT", BillingCode: "99214", NetworkName: "Network A", ObservationCount: 1},
		{OutputID: "mrf-3", BillingCodeType: "CPT", BillingCode: "99214", NetworkName: "Network A", ObservationCount: 1},
		{OutputID: "mrf-3", BillingCodeType: "CPT", BillingCode: "99214", NetworkName: "Network B", ObservationCount: 1},
	}
	if !reflect.DeepEqual(result.OutputCodeNetworks, wantNetworks) {
		t.Fatalf("networks=%#v want %#v", result.OutputCodeNetworks, wantNetworks)
	}
	wantProviders := []ProviderFilterValue{
		{FilterKind: "city", ParentValue: "ca", FilterValue: "los angeles", ProviderCount: 1},
		{FilterKind: "city", ParentValue: "ny", FilterValue: "new york", ProviderCount: 1},
		{FilterKind: "state", FilterValue: "ca", ProviderCount: 1},
		{FilterKind: "state", FilterValue: "ny", ProviderCount: 1},
		{FilterKind: "taxonomy", FilterValue: "tax-a", ProviderCount: 1},
		{FilterKind: "taxonomy", FilterValue: "tax-b", ProviderCount: 1},
	}
	if !reflect.DeepEqual(result.ProviderFilterValues, wantProviders) {
		t.Fatalf("provider filters=%#v want %#v", result.ProviderFilterValues, wantProviders)
	}
	for _, network := range result.OutputCodeNetworks {
		if network.OutputID == "mrf-99" {
			t.Fatalf("unselected output emitted: %#v", network)
		}
	}
	if after := extractionFileState(t, warehouse); !reflect.DeepEqual(before, after) {
		t.Fatalf("warehouse changed before=%#v after=%#v", before, after)
	}
}
func TestExtractionRejectsMixedCaseCityWithoutState(t *testing.T) {
	binaryPath := os.Getenv("MRFPIPELINE_DUCKDB_BIN")
	if binaryPath == "" {
		t.Skip("MRFPIPELINE_DUCKDB_BIN is not set")
	}
	warehouse := writeMixedCaseMissingStateWarehouse(t, filepath.Join(t.TempDir(), "warehouse"))
	outputs := []Output{{PayerID: "payer", CollectionMonth: "2026-08", OutputID: "mrf-20", SnapshotID: 20}}
	fingerprint := outputFingerprint(outputs)
	_, err := Extract(context.Background(), DuckDB{path: binaryPath}, Params{
		WarehousePath:     warehouse,
		OutputFingerprint: fingerprint,
		Outputs:           outputs,
	})
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogValueInvalid) {
		t.Fatalf("error=%v", err)
	}
}
func TestExtractionRejectsProviderCatalogManifestMismatch(t *testing.T) {
	warehouse := writeExtractionWarehouse(t, filepath.Join(t.TempDir(), "warehouse"))
	manifestPath := filepath.Join(warehouse, "provider_catalog", "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.Replace(data, []byte(`"release_month":"2026-08"`), []byte(`"release_month":"2026-09"`), 1)
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	outputs := []Output{{PayerID: "payer", CollectionMonth: "2026-08", OutputID: "mrf-20", SnapshotID: 20}}
	_, err = Extract(context.Background(), DuckDB{path: "duckdb"}, Params{
		WarehousePath:     warehouse,
		OutputFingerprint: outputFingerprint(outputs),
		Outputs:           outputs,
	})
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogWarehouseInvalid) {
		t.Fatalf("error=%v", err)
	}
}

func writeMixedCaseMissingStateWarehouse(t *testing.T, warehouse string) string {
	t.Helper()
	writeExtractionWarehouse(t, warehouse)
	catalog := filepath.Join(warehouse, "provider_catalog")
	ca, caCity, caZip := "ca", "los angeles", "90001"
	ny, nyCity, nyZip := "ny", "new york", "10001"
	badCity := "New York"
	providers := []testProvider{
		{NPI: "111", State: &ca, City: &caCity, PostalCode: &caZip},
		{NPI: "222", State: &ny, City: &nyCity, PostalCode: &nyZip},
		{NPI: "333", City: &badCity},
	}
	taxonomies := []testTaxonomy{
		{NPI: "111", TaxonomyCode: "tax-a"},
		{NPI: "222", TaxonomyCode: "tax-b"},
		{NPI: "333", TaxonomyCode: "tax-c"},
	}
	providersPath := filepath.Join(catalog, "providers", "part-00000.parquet")
	taxonomiesPath := filepath.Join(catalog, "provider_taxonomies", "part-00000.parquet")
	if err := os.Remove(providersPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(taxonomiesPath); err != nil {
		t.Fatal(err)
	}
	writeTestParquet(t, providersPath, providers)
	writeTestParquet(t, taxonomiesPath, taxonomies)
	manifestPath := filepath.Join(catalog, "manifest.json")
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest["providers"].(map[string]any)["rows"] = 3
	manifest["provider_taxonomies"].(map[string]any)["rows"] = 3
	manifestData, err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, append(manifestData, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}

	snapshot := filepath.Join(warehouse, "snapshots", "collection_month=2026-08", "payer_id=payer", "output_id=mrf-20")
	membershipsPath := filepath.Join(snapshot, "provider_group_memberships", "mrf-20-part-00000.parquet")
	if err := os.Remove(membershipsPath); err != nil {
		t.Fatal(err)
	}
	writeTestParquet(t, membershipsPath, []testMembership{
		{OutputID: "mrf-20", ProviderGroupID: "g1", NPI: "111"},
		{OutputID: "mrf-20", ProviderGroupID: "g1", NPI: "333"},
	})
	setSnapshotDatasetRowCount(t, snapshot, "provider_group_memberships", 2)
	return warehouse
}

func setSnapshotDatasetRowCount(t *testing.T, snapshot, dataset string, count int) {
	t.Helper()
	path := filepath.Join(snapshot, "manifest.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest["datasets"].(map[string]any)[dataset].(map[string]any)["row_count"] = count
	data, err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeExtractionWarehouse(t *testing.T, warehouse string) string {
	t.Helper()
	if err := os.MkdirAll(warehouse, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(warehouse, "warehouse.json"), []byte(`{"warehouse_schema_version":"2.0.0","provider_catalog":{"schema_version":1,"release_month":"2026-08"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	catalog := filepath.Join(warehouse, "provider_catalog")
	if err := os.MkdirAll(filepath.Join(catalog, "providers"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(catalog, "provider_taxonomies"), 0o700); err != nil {
		t.Fatal(err)
	}
	ca, caCity, caZip := "ca", "los angeles", "90001"
	ny, nyCity, nyZip := "ny", "new york", "10001"
	providers := []testProvider{{NPI: "111", State: &ca, City: &caCity, PostalCode: &caZip}, {NPI: "222", State: &ny, City: &nyCity, PostalCode: &nyZip}}
	taxonomies := []testTaxonomy{{NPI: "111", TaxonomyCode: "tax-a"}, {NPI: "222", TaxonomyCode: "tax-b"}}
	writeTestParquet(t, filepath.Join(catalog, "providers", "part-00000.parquet"), providers)
	writeTestParquet(t, filepath.Join(catalog, "provider_taxonomies", "part-00000.parquet"), taxonomies)
	manifest, err := json.Marshal(map[string]any{"schema_version": 1, "release_month": "2026-08", "providers": map[string]any{"path": "providers", "rows": 2}, "provider_taxonomies": map[string]any{"path": "provider_taxonomies", "rows": 2}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(catalog, "manifest.json"), append(manifest, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}

	alpha, arrangement, codeType, version, code, description, class, setting, negotiated := "Alpha", "ffs", "CPT", "2025", "99214", "Zulu description", "professional", "outpatient", "negotiated"
	oldDate := int32(1)
	standard20 := testRateFact{OutputID: "mrf-20", PayerID: "payer", CollectionMonth: "2026-08", ServiceID: 1, RateGroupID: 1, PriceID: 1, NegotiationArrangement: &arrangement, ServiceName: &alpha, BillingCodeType: &codeType, BillingCodeTypeVersion: &version, BillingCode: &code, ServiceDescription: &description, ServiceCodes: []string{"11", "11", "CSTM-00"}, BillingClass: &class, Setting: &setting, NegotiatedType: &negotiated, BillingCodeModifiers: []string{"95", "95"}, NetworkNames: []string{"Network A", "Network A"}, NegotiatedRate: testDecimal(10), ExpirationDate: &oldDate}
	alpha2, description2 := "Alpha", "Alpha description"
	standard3 := testRateFact{OutputID: "mrf-3", PayerID: "payer", CollectionMonth: "2026-08", ServiceID: 1, RateGroupID: 1, PriceID: 1, NegotiationArrangement: &arrangement, ServiceName: &alpha2, BillingCodeType: &codeType, BillingCodeTypeVersion: &version, BillingCode: &code, ServiceDescription: &description2, ServiceCodes: []string{"11"}, BillingClass: &class, Setting: &setting, NegotiatedType: &negotiated, BillingCodeModifiers: nil, NetworkNames: []string{"Network A"}, NegotiatedRate: testDecimal(20), ExpirationDate: &oldDate}
	standard3b := standard3
	standard3b.PriceID = 2
	standard3b.NetworkNames = []string{"Network B"}
	empty := ""
	standard3b.BillingCodeTypeVersion = &empty
	standard3b.ServiceName = nil
	standard3b.ServiceDescription = nil
	standard3b.BillingClass = &empty
	standard3b.Setting = nil
	standard3b.NegotiationArrangement = &empty
	nonstandardType, nonstandardCode := "HCPCS", "NOT-COUNTED"
	nonstandard := standard3
	nonstandard.PriceID = 3
	nonstandard.BillingCodeType = &nonstandardType
	nonstandard.BillingCode = &nonstandardCode
	nonstandard.NegotiatedType = nil
	percentageType, derivedType := "percentage", "derived"
	percentage := standard3
	percentage.PriceID = 4
	percentage.NegotiatedType = &percentageType
	derived := standard3
	derived.PriceID = 5
	derived.NegotiatedType = &derivedType
	nullRate := standard3
	nullRate.PriceID = 6
	nullRate.NegotiatedRate = nil
	extraType, extraCode := "CPT", "EXTRA"
	extra := standard3
	extra.OutputID = "mrf-99"
	extra.BillingCode = &extraCode
	extra.BillingCodeType = &extraType
	extra.PriceID = 1
	writeExtractionSnapshot(t, warehouse, "payer", "2026-08", "mrf-20", []testRateFact{standard20}, []testRateProviderGroup{{OutputID: "mrf-20", RateGroupID: 1, ProviderGroupID: "g1"}}, []testMembership{{OutputID: "mrf-20", ProviderGroupID: "g1", NPI: "111"}})
	writeExtractionSnapshot(t, warehouse, "payer", "2026-08", "mrf-3", []testRateFact{standard3, standard3b, nonstandard, percentage, derived, nullRate}, []testRateProviderGroup{{OutputID: "mrf-3", RateGroupID: 1, ProviderGroupID: "g1"}}, []testMembership{{OutputID: "mrf-3", ProviderGroupID: "g1", NPI: "222"}})
	writeExtractionSnapshot(t, warehouse, "payer", "2026-08", "mrf-99", []testRateFact{extra}, []testRateProviderGroup{{OutputID: "mrf-99", RateGroupID: 1, ProviderGroupID: "g1"}}, []testMembership{{OutputID: "mrf-99", ProviderGroupID: "g1", NPI: "111"}})
	return warehouse
}

func writeExtractionSnapshot(t *testing.T, warehouse, payer, month, outputID string, facts []testRateFact, links []testRateProviderGroup, memberships []testMembership) {
	t.Helper()
	root := filepath.Join(warehouse, "snapshots", "collection_month="+month, "payer_id="+payer, "output_id="+outputID)
	for _, dataset := range []string{"rate_facts", "rate_provider_groups", "provider_groups", "provider_group_memberships", "ingestions", "network_names"} {
		if err := os.MkdirAll(filepath.Join(root, dataset), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	writeTestParquet(t, filepath.Join(root, "rate_facts", outputID+"-part-00000.parquet"), facts)
	writeTestParquet(t, filepath.Join(root, "rate_provider_groups", outputID+"-part-00000.parquet"), links)
	writeTestParquet(t, filepath.Join(root, "provider_group_memberships", outputID+"-part-00000.parquet"), memberships)
	unused := []testUnusedRow{{Value: "unused"}}
	for _, dataset := range []string{"provider_groups", "ingestions", "network_names"} {
		writeTestParquet(t, filepath.Join(root, dataset, outputID+"-part-00000.parquet"), unused)
	}
	counts := map[string]any{
		"rate_facts":                 map[string]any{"row_count": len(facts), "part_count": 1},
		"rate_provider_groups":       map[string]any{"row_count": len(links), "part_count": 1},
		"provider_groups":            map[string]any{"row_count": 1, "part_count": 1},
		"provider_group_memberships": map[string]any{"row_count": len(memberships), "part_count": 1},
		"ingestions":                 map[string]any{"row_count": 1, "part_count": 1},
		"network_names":              map[string]any{"row_count": 1, "part_count": 1},
	}
	manifest := map[string]any{"manifest_schema_version": "2.0.0", "output_schema_version": "2.0.0", "output_id": outputID, "payer_id": payer, "collection_month": month, "provider_catalog": map[string]any{"schema_version": 1, "release_month": "2026-08"}, "datasets": counts}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeTestParquet[T any](t testing.TB, path string, rows []T) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	writer := parquet.NewGenericWriter[T](file)
	if _, err := writer.Write(rows); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func testDecimal(value int64) []byte {
	result := make([]byte, 16)
	binary.BigEndian.PutUint64(result[8:], uint64(value)*1_000_000_000)
	return result
}

func extractionFileState(t *testing.T, root string) map[string]testFileState {
	t.Helper()
	result := map[string]testFileState{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		hash := sha256.Sum256(data)
		result[relative] = testFileState{Mode: info.Mode(), Size: info.Size(), ModTime: info.ModTime().UnixNano(), SHA256: hex.EncodeToString(hash[:])}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
