package filtercatalog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

func TestValidateParamsCopiesAndSortsOutputs(t *testing.T) {
	outputs := []Output{
		{PayerID: "payer", CollectionMonth: "2026-08", OutputID: "mrf-20", SnapshotID: 20},
		{PayerID: "payer", CollectionMonth: "2026-08", OutputID: "mrf-3", SnapshotID: 3},
	}
	wantInput := append([]Output(nil), outputs...)
	sorted := append([]Output(nil), outputs...)
	sortOutputs(sorted)
	fingerprint := outputFingerprint(sorted)
	got, gotFingerprint, err := validateParams(Params{Outputs: outputs, OutputFingerprint: fingerprint})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(outputs, wantInput) {
		t.Fatalf("input outputs mutated: %#v", outputs)
	}
	if !reflect.DeepEqual(got, sorted) || gotFingerprint != fingerprint {
		t.Fatalf("got outputs=%#v fingerprint=%q", got, gotFingerprint)
	}
}

func TestRenderExtractionSQLEscapesWarehouseRootAndCandidateValues(t *testing.T) {
	root := filepath.Join(t.TempDir(), "ware'house*?[]__SELECTED_OUTPUT_VALUES____WAREHOUSE_ROOT__")
	outputs := []Output{{PayerID: "payer", CollectionMonth: "2026-08", OutputID: "mrf-1", SnapshotID: 1}}
	rendered, err := renderExtractionSQL(root, outputs)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"ware''house", "[*]", "[?]", "[[]", "[]]", "hive_partitioning = false", "union_by_name = false", ".mode jsonlines", "CREATE TEMPORARY TABLE", "CREATE TEMPORARY TABLE selected_facts AS", "CREATE TEMPORARY TABLE selected_links AS", "CREATE TEMPORARY TABLE selected_memberships AS", "selected(payer_id, collection_month, output_id, mrf_snapshot_id)", "FROM read_parquet(", "GROUP BY output_id, price_id", "SELECT DISTINCT output_id, price_id", "provider.city <> lower(provider.city)"} {
		if !strings.Contains(rendered, fragment) {
			t.Fatalf("rendered SQL missing %q", fragment)
		}
	}
	for _, projection := range []string{
		"SELECT\n    fact.output_id,\n    fact.payer_id,\n    fact.collection_month,\n    fact.rate_group_id,\n    fact.price_id,\n    fact.negotiation_arrangement,\n    fact.service_name,\n    fact.billing_code_type,\n    fact.billing_code_type_version,\n    fact.billing_code,\n    fact.service_description,\n    fact.service_codes,\n    fact.billing_class,\n    fact.setting,\n    fact.negotiated_type,\n    fact.billing_code_modifiers,\n    fact.network_names,\n    fact.negotiated_rate\nFROM read_parquet(",
		"SELECT\n    link.output_id,\n    link.rate_group_id,\n    link.provider_group_id\nFROM read_parquet(",
		"SELECT\n    membership.output_id,\n    membership.provider_group_id,\n    membership.npi\nFROM read_parquet(",
	} {
		if !strings.Contains(rendered, projection) {
			t.Fatalf("rendered SQL missing explicit projection %q", projection)
		}
	}
	for _, wildcard := range []string{"fact.*", "link.*", "membership.*"} {
		if strings.Contains(rendered, wildcard) {
			t.Fatalf("rendered SQL contains wildcard projection %q", wildcard)
		}
	}
	if strings.Count(rendered, warehouseRootToken) != 5 || strings.Count(rendered, selectedOutputsToken) != 5 {
		t.Fatal("rendered SQL unexpectedly replaced literal token text in the warehouse path")
	}
	if strings.Count(rendered, "('payer', '2026-08', 'mrf-1', 1)") != 1 {
		t.Fatal("rendered SQL spliced selected values into the warehouse path")
	}
	forbidden := []string{"COPY ", "INSERT ", "UPDATE ", "DELETE ", "ATTACH ", "INSTALL ", "LOAD ", "CREATE SECRET", "CREATE TABLE "}
	for _, operation := range forbidden {
		if strings.Contains(strings.ToUpper(rendered), operation) {
			t.Fatalf("rendered SQL contains forbidden operation %q", operation)
		}
	}
	for _, source := range []string{"rate_facts_source", "rate_provider_groups_source", "provider_group_memberships_source"} {
		if strings.Contains(rendered, source) {
			t.Fatalf("rendered SQL retained full-warehouse source table %q", source)
		}
	}
}

func TestValidateRowsRejectsNonLowercaseCity(t *testing.T) {
	rows := rowSet{
		summary: &summaryRow{standardFactCount: 1},
		billingCodes: []BillingCode{{
			BillingCodeType:            "CPT",
			BillingCode:                "99214",
			ObservationCount:           1,
			UnmodifiedObservationCount: 1,
		}},
		providerFilterValues: []ProviderFilterValue{{
			FilterKind:    "city",
			ParentValue:   "ca",
			FilterValue:   "Los Angeles",
			ProviderCount: 1,
		}},
	}
	_, err := validateRows(rows, []Output{{OutputID: "mrf-1"}}, strings.Repeat("0", 64))
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogValueInvalid) {
		t.Fatalf("error=%v", err)
	}
}

func TestValidateRowsRejectsMismatchedDisplayLabels(t *testing.T) {
	cases := []CodeFilterValue{
		{
			BillingCodeType:  "CPT",
			BillingCode:      "99214",
			FilterKind:       "modifier",
			FilterValue:      "CSTM-00",
			DisplayLabel:     "Broad or unspecified place of service",
			ObservationCount: 1,
		},
		{
			BillingCodeType:  "CPT",
			BillingCode:      "99214",
			FilterKind:       "place_of_service",
			FilterValue:      "11",
			DisplayLabel:     "Broad or unspecified place of service",
			ObservationCount: 1,
		},
	}
	for _, value := range cases {
		rows := rowSet{
			summary: &summaryRow{standardFactCount: 1},
			billingCodes: []BillingCode{{
				BillingCodeType:            "CPT",
				BillingCode:                "99214",
				ObservationCount:           1,
				UnmodifiedObservationCount: 1,
			}},
			codeFilterValues: []CodeFilterValue{value},
		}
		_, err := validateRows(rows, []Output{{OutputID: "mrf-1"}}, strings.Repeat("0", 64))
		if !jobs.IsFailure(err, jobs.FailureFilterCatalogProtocolInvalid) {
			t.Fatalf("value=%+v error=%v", value, err)
		}
	}
}

func TestValidateRowsRejectsPerCodeCounts(t *testing.T) {
	billingCodes := []BillingCode{
		{BillingCodeType: "CPT", BillingCode: "99214", ObservationCount: 1, UnmodifiedObservationCount: 1},
		{BillingCodeType: "CPT", BillingCode: "99215", ObservationCount: 2, UnmodifiedObservationCount: 2},
	}
	tests := []rowSet{
		{
			summary:      &summaryRow{standardFactCount: 3},
			billingCodes: billingCodes,
			codeFilterValues: []CodeFilterValue{{
				BillingCodeType: "CPT", BillingCode: "99214", FilterKind: "modifier",
				FilterValue: "x", DisplayLabel: "x", ObservationCount: 2,
			}},
		},
		{
			summary:      &summaryRow{standardFactCount: 3},
			billingCodes: billingCodes,
			outputCodeNetworks: []OutputCodeNetwork{{
				OutputID: "mrf-1", BillingCodeType: "CPT", BillingCode: "99214",
				NetworkName: "Network A", ObservationCount: 2,
			}},
		},
	}
	for _, rows := range tests {
		_, err := validateRows(rows, []Output{{OutputID: "mrf-1"}}, strings.Repeat("0", 64))
		if !jobs.IsFailure(err, jobs.FailureFilterCatalogProtocolInvalid) {
			t.Fatalf("rows=%+v error=%v", rows, err)
		}
	}
}

func TestExtractCancellationWhileWaitingForGate(t *testing.T) {
	extractionGate <- struct{}{}
	defer func() { <-extractionGate }()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := Extract(ctx, DuckDB{}, Params{})
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("extract returned before gate release: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		if !jobs.IsFailure(err, jobs.FailureFilterCatalogCancelled) {
			t.Fatalf("error=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("extract did not stop while waiting for gate")
	}
}

func TestProtocolRejectsMalformedRowsAndOrders(t *testing.T) {
	cases := []string{
		"\n",
		"{\"dataset\":\"summary\",\"standard_fact_count\":1}\n\n{\"dataset\":\"billing_codes\",\"billing_code_type\":\"CPT\",\"billing_code\":\"99214\",\"billing_code_type_version\":null,\"warehouse_service_name\":null,\"warehouse_service_description\":null,\"observation_count\":1,\"unmodified_observation_count\":1}\n",
		"{\"dataset\":\"\\ud800\",\"standard_fact_count\":1}\n",
		"{\"dataset\":\"billing_codes\"}\n",
		"{\"dataset\":\"unknown\"}\n",
		"{\"dataset\":\"summary\",\"standard_fact_count\":1}\n{\"dataset\":\"summary\",\"standard_fact_count\":1}\n",
		"{\"dataset\":\"summary\",\"standard_fact_count\":1}\n{\"dataset\":\"billing_codes\",\"billing_code_type\":\"CPT\",\"billing_code\":\"99214\",\"billing_code_type_version\":null,\"warehouse_service_name\":null,\"warehouse_service_description\":null,\"observation_count\":1,\"unmodified_observation_count\":1}\n{\"dataset\":\"billing_codes\",\"billing_code_type\":\"CPT\",\"billing_code\":\"99214\",\"billing_code_type_version\":null,\"warehouse_service_name\":null,\"warehouse_service_description\":null,\"observation_count\":1,\"unmodified_observation_count\":1}\n",
	}
	for _, input := range cases {
		result := readProtocol(context.Background(), strings.NewReader(input))
		if !jobs.IsFailure(result.err, jobs.FailureFilterCatalogProtocolInvalid) {
			t.Fatalf("input %q returned %v", input, result.err)
		}
	}
}

func TestRunExtractionRedactsProcessFailure(t *testing.T) {
	script := filepath.Join(t.TempDir(), "duckdb-fake")
	content := "#!/bin/sh\nprintf '%s\\n' 'hostile warehouse/provider secret' >&2\nexit 7\n"
	if err := os.WriteFile(script, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := runExtraction(context.Background(), DuckDB{path: script}, "SELECT 1;\n")
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogQueryFailed) {
		t.Fatalf("error=%v", err)
	}
	if strings.Contains(err.Error(), "hostile") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("unredacted error=%v", err)
	}
}
func TestRunExtractionClassifiesOnlyExactGuardDiagnostics(t *testing.T) {
	tests := []struct {
		name   string
		stderr string
		want   string
	}{
		{
			name:   "marker in unrelated text",
			stderr: "warehouse path contains filter_catalog_value_invalid but this is not a guard",
			want:   jobs.FailureFilterCatalogQueryFailed,
		},
		{
			name:   "exact value guard",
			stderr: "Invalid Input Error: filter_catalog_value_invalid",
			want:   jobs.FailureFilterCatalogValueInvalid,
		},
		{
			name:   "CLI wrapper value guard",
			stderr: "Error: Invalid Input Error: filter_catalog_value_invalid",
			want:   jobs.FailureFilterCatalogValueInvalid,
		},
		{
			name:   "exact warehouse guard",
			stderr: "Invalid Input Error: filter_catalog_warehouse_invalid",
			want:   jobs.FailureFilterCatalogWarehouseInvalid,
		},
		{
			name:   "CLI wrapper warehouse guard",
			stderr: "Error: Invalid Input Error: filter_catalog_warehouse_invalid",
			want:   jobs.FailureFilterCatalogWarehouseInvalid,
		},
		{
			name:   "warehouse takes precedence",
			stderr: "Invalid Input Error: filter_catalog_value_invalid\nInvalid Input Error: filter_catalog_warehouse_invalid",
			want:   jobs.FailureFilterCatalogWarehouseInvalid,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			script := filepath.Join(t.TempDir(), "duckdb-fake")
			content := "#!/bin/sh\ncat >&2 <<'EOF'\n" + test.stderr + "\nEOF\nexit 7\n"
			if err := os.WriteFile(script, []byte(content), 0o700); err != nil {
				t.Fatal(err)
			}
			_, err := runExtraction(context.Background(), DuckDB{path: script}, "SELECT 1;\n")
			if !jobs.IsFailure(err, test.want) {
				t.Fatalf("error=%v want %s", err, test.want)
			}
		})
	}
}
func TestRunExtractionStopsAfterProtocolError(t *testing.T) {
	script := filepath.Join(t.TempDir(), "duckdb-fake")
	content := `#!/bin/sh
printf '%s\n' 'not json'
sleep 30
`
	if err := os.WriteFile(script, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := runExtraction(ctx, DuckDB{path: script}, strings.Repeat("x", 1<<20))
		result <- err
	}()
	select {
	case err := <-result:
		if !jobs.IsFailure(err, jobs.FailureFilterCatalogProtocolInvalid) {
			t.Fatalf("error=%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("protocol error did not stop extraction")
	}
}
func TestRunExtractionProtocolErrorWinsProcessFailure(t *testing.T) {
	script := filepath.Join(t.TempDir(), "duckdb-fake")
	content := "#!/bin/sh\nprintf '%s\\n' 'not json'\nexit 7\n"
	if err := os.WriteFile(script, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := runExtraction(context.Background(), DuckDB{path: script}, "SELECT 1;\n")
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogProtocolInvalid) {
		t.Fatalf("error=%v", err)
	}
}

func TestResolveDuckDBVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "duckdb")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nprintf '%s\\n' 'v1.5.5 12345678'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	duck, err := ResolveDuckDB(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if duck.path != path {
		t.Fatalf("path=%q want %q", duck.path, path)
	}
}

func TestResolveDuckDBWrongVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "duckdb")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nprintf '%s\\n' 'v1.5.4 12345678'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	_, err := ResolveDuckDB(context.Background())
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogDuckDBVersionInvalid) {
		t.Fatalf("error=%v", err)
	}
}

func TestCancellationMapsToFixedFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ResolveDuckDB(ctx)
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogCancelled) {
		t.Fatalf("error=%v", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation leaked context error=%v", err)
	}
}

func TestRunExtractionStartCancellationMapsToCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := runExtraction(ctx, DuckDB{path: filepath.Join(t.TempDir(), "missing-duckdb")}, "SELECT 1;\n")
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogCancelled) {
		t.Fatalf("error=%v", err)
	}
}

func TestResolveDuckDBCancellationTerminatesProcessTree(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "duckdb")
	started := filepath.Join(dir, "started")
	childPath := filepath.Join(dir, "child")
	content := `#!/bin/sh
/bin/sleep 30 &
printf '%s' "$!" > "$MRFPIPELINE_TEST_CHILD"
printf started > "$MRFPIPELINE_TEST_STARTED"
wait
`
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv("MRFPIPELINE_TEST_STARTED", started)
	t.Setenv("MRFPIPELINE_TEST_CHILD", childPath)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := ResolveDuckDB(ctx)
		result <- err
	}()
	waitForTestFile(t, started)
	childBytes := waitForTestFile(t, childPath)
	childPID, err := strconv.Atoi(string(childBytes))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-result:
		if !jobs.IsFailure(err, jobs.FailureFilterCatalogCancelled) {
			t.Fatalf("error=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("version probe did not stop after cancellation")
	}
	waitForTestProcessExit(t, childPID)
}

func TestRunExtractionCancellationTerminatesProcessTree(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "duckdb")
	started := filepath.Join(dir, "started")
	childPath := filepath.Join(dir, "child")
	content := `#!/bin/sh
sleep 30 &
printf '%s' "$!" > "$MRFPIPELINE_TEST_CHILD"
printf started > "$MRFPIPELINE_TEST_STARTED"
cat >/dev/null
wait
`
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MRFPIPELINE_TEST_STARTED", started)
	t.Setenv("MRFPIPELINE_TEST_CHILD", childPath)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := runExtraction(ctx, DuckDB{path: path}, "SELECT 1;\n")
		result <- err
	}()
	waitForTestFile(t, started)
	childBytes := waitForTestFile(t, childPath)
	childPID, err := strconv.Atoi(string(childBytes))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-result:
		if !jobs.IsFailure(err, jobs.FailureFilterCatalogCancelled) {
			t.Fatalf("error=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("extraction did not stop after cancellation")
	}
	waitForTestProcessExit(t, childPID)
}

func waitForTestFile(t *testing.T, path string) []byte {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		data, err := os.ReadFile(path)
		if err == nil && len(data) > 0 {
			return data
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForTestProcessExit(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("process %d remains after cancellation", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func sortOutputs(outputs []Output) {
	for i := range outputs {
		for j := i + 1; j < len(outputs); j++ {
			if strings.Compare(outputs[j].OutputID, outputs[i].OutputID) < 0 {
				outputs[i], outputs[j] = outputs[j], outputs[i]
			}
		}
	}
}
