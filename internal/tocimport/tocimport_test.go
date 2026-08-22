package tocimport

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/EnotPoloskun/mrftocparser"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/parquet-go/parquet-go"
	"github.com/riverqueue/river"
)

func TestClassifyClaim(t *testing.T) {
	t.Parallel()
	job := int64(72)
	same := job
	other := int64(9)
	cases := []struct {
		name     string
		imp      string
		download string
		parse    string
		stored   *int64
		want     string
		err      string
	}{
		{"succeeded", jobs.StatusSucceeded, jobs.StatusSucceeded, jobs.StatusSucceeded, &same, jobs.ClaimNoop, ""},
		{"failed", jobs.StatusFailed, jobs.StatusSucceeded, jobs.StatusSucceeded, &same, jobs.ClaimNoop, ""},
		{"stale pending", jobs.StatusPending, jobs.StatusSucceeded, jobs.StatusSucceeded, &other, jobs.ClaimNoop, ""},
		{"nil job", jobs.StatusPending, jobs.StatusSucceeded, jobs.StatusSucceeded, nil, jobs.ClaimNoop, ""},
		{"pending ready", jobs.StatusPending, jobs.StatusSucceeded, jobs.StatusSucceeded, &same, jobs.ClaimWork, ""},
		{"running ready", jobs.StatusRunning, jobs.StatusSucceeded, jobs.StatusSucceeded, &same, jobs.ClaimWork, ""},
		{"pending parse not succeeded", jobs.StatusPending, jobs.StatusSucceeded, jobs.StatusPending, &same, "", jobs.FailureDomainInvariant},
		{"pending download not succeeded", jobs.StatusPending, jobs.StatusPending, jobs.StatusSucceeded, &same, "", jobs.FailureDomainInvariant},
		{"blocked import", jobs.StatusBlocked, jobs.StatusSucceeded, jobs.StatusSucceeded, &same, "", jobs.FailureDomainInvariant},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := classifyClaim(tc.imp, tc.download, tc.parse, tc.stored, job)
			if tc.err != "" {
				if !jobs.IsFailure(err, tc.err) {
					t.Fatalf("got %v", err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %s %v want %s", got, err, tc.want)
			}
		})
	}
}

func TestPhysicalSchemasMatchPublishedDescriptors(t *testing.T) {
	t.Parallel()
	checks := []struct {
		name string
		row  any
	}{
		{"toc_files", tocFileRow{}},
		{"mrf_plan_associations", assocRow{}},
	}
	for _, tc := range checks {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			schema := parquet.SchemaOf(tc.row)
			desc := loadDescriptor(t, tc.name)
			if desc.OutputSchemaVersion != "1.0.0" || desc.Dataset != tc.name || len(desc.Fields) != len(schema.Fields()) {
				t.Fatal("descriptor/schema mismatch")
			}
			for i, expected := range desc.Fields {
				field := schema.Fields()[i]
				if field.Name() != expected.Name {
					t.Fatalf("field %d name", i)
				}
				if field.Type().PhysicalType() == nil || field.Type().PhysicalType().String() != expected.PhysicalType {
					t.Fatalf("field %d physical", i)
				}
				logical := ""
				if field.Type().LogicalType() != nil {
					logical = field.Type().LogicalType().String()
				}
				if logical != expected.LogicalType {
					t.Fatalf("field %d logical %q", i, logical)
				}
				rep := "required"
				if field.Optional() {
					rep = "optional"
				} else if field.Repeated() {
					rep = "repeated"
				}
				if rep != expected.Repetition {
					t.Fatalf("field %d repetition", i)
				}
			}
		})
	}
}

func TestParserOutputMatchesLocalSchema(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	dir := t.TempDir()
	in := filepath.Join(dir, "toc.json")
	if err := os.WriteFile(in, []byte(sampleTOC(`https://example.test/a.json`, "plan", "issuer", "hios", "id", "group", "")), 0600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "parsed")
	if _, err := mrftocparser.Parse(context.Background(), mrftocparser.Config{
		InputPath: in, OutputPath: out, TOCOutputID: "toc-1", PayerID: "uhc", CollectionMonth: "2026-08",
	}); err != nil {
		t.Fatal(err)
	}
	if err := validatePartSchema(filepath.Join(out, datasetTOCFiles, "part-00000.parquet"), tocFileSchema); err != nil {
		t.Fatalf("toc schema: %v", err)
	}
	if err := validatePartSchema(filepath.Join(out, datasetAssociations, "part-00000.parquet"), assocSchema); err != nil {
		t.Fatalf("assoc schema: %v", err)
	}
}

func TestFilenameDerivation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		location string
		filename string
		ok       bool
	}{
		{"https://example.test/path/file.json?sig=x#frag", "file.json", true},
		{"https://example.test/path/File%20Name.json", "File Name.json", true},
		{"https://example.test/path/a%2Fb", "", false},
		{"https://example.test/path/", "", false},
		{"https://example.test/path/..", "", false},
		{"https://example.test/path/.", "", false},
		{"https://example.test/path/%5C", "", false},
		{"https://example.test/path/%252F", "%2F", true},
		{"https://example.test/path/%FF", "", false},
		{"https://example.test/path/%00", "", false},
		{"https://example.test/path/%7f", "", false},
	}
	for _, tc := range cases {
		got, ok := deriveFilename(tc.location)
		if got != tc.filename || ok != tc.ok {
			t.Fatalf("deriveFilename(%q) = %q %v want %q %v", tc.location, got, ok, tc.filename, tc.ok)
		}
	}
	for _, location := range []string{"HTTPS://example.test/file", "http://example.test/file", "https://", "https://example.test/a%", "https://example.test/a b"} {
		if _, ok := deriveFilename(location); ok {
			t.Fatalf("accepted %q", location)
		}
		if validHTTPSLocation(location) {
			t.Fatalf("https accepted %q", location)
		}
	}
}

func TestHTTPLocationRejected(t *testing.T) {
	t.Parallel()
	row := validAssoc("http://example.test/a.json", "plan", "issuer", nil, "hios", "id", "group")
	if err := validateAssocRow(row, "toc-1", "uhc", "2026-08"); err != errRow {
		t.Fatalf("got %v", err)
	}
}

func TestRowValidationContracts(t *testing.T) {
	t.Parallel()
	good := validAssoc("https://example.test/a.json", "plan", "issuer", nil, "hios", "id", "group")
	if err := validateAssocRow(good, "toc-1", "uhc", "2026-08"); err != nil {
		t.Fatal(err)
	}
	wrongFN := good
	other := "other.json"
	wrongFN.MRFFilename = &other
	if err := validateAssocRow(wrongFN, "toc-1", "uhc", "2026-08"); err != errRow {
		t.Fatal("filename")
	}
	emptyEIN := validAssoc("https://example.test/a.json", "plan", "issuer", strPtr(""), "ein", "12-3", "group")
	if err := validateAssocRow(emptyEIN, "toc-1", "uhc", "2026-08"); err != errRow {
		t.Fatal("empty ein sponsor")
	}
	emptyHIOS := good
	emptyHIOS.PlanSponsorName = strPtr("")
	if err := validateAssocRow(emptyHIOS, "toc-1", "uhc", "2026-08"); err != errRow {
		t.Fatal("empty hios sponsor")
	}
	badJSON := good
	badJSON.PlanAdditionalFieldsJSON = strPtr(`{}`)
	if err := validateAssocRow(badJSON, "toc-1", "uhc", "2026-08"); err != errRow {
		t.Fatal("empty json object")
	}
}

func TestFirstPassOrderAndCount(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	source := filepath.Join(dir, "download", "data")
	out := filepath.Join(dir, "parsed")
	a := validAssoc("https://example.test/a.json", "plan", "issuer", nil, "hios", "id", "group")
	b := validAssoc("https://example.test/b.json", "plan", "issuer", nil, "hios", "id", "group")
	writeCompleted(t, out, "toc-1", "uhc", "2026-08", source, sampleTOCRow("toc-1", "uhc", "2026-08", source), []assocRow{a, b})
	n, err := validateOutput(context.Background(), out, "toc-1", "uhc", "2026-08", source)
	if err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	writeCompleted(t, out, "toc-1", "uhc", "2026-08", source, sampleTOCRow("toc-1", "uhc", "2026-08", source), []assocRow{b, a})
	_, err = validateOutput(context.Background(), out, "toc-1", "uhc", "2026-08", source)
	if !jobs.IsFailure(err, jobs.FailureTOCImportOrderInvalid) {
		t.Fatalf("order %v", err)
	}
	writeCompleted(t, out, "toc-1", "uhc", "2026-08", source, sampleTOCRow("toc-1", "uhc", "2026-08", source), []assocRow{a, a})
	_, err = validateOutput(context.Background(), out, "toc-1", "uhc", "2026-08", source)
	if !jobs.IsFailure(err, jobs.FailureTOCImportOrderInvalid) {
		t.Fatalf("dup %v", err)
	}
}

func TestFeedFormatting(t *testing.T) {
	t.Parallel()
	if formatFeedID(1) != "mrf-source-1" || formatFeedID(9021) != "mrf-source-9021" {
		t.Fatal(formatFeedID(1), formatFeedID(9021))
	}
	if strings.Contains(formatFeedID(12), "012") {
		t.Fatal("padded")
	}
}

func TestDistinctLocationsSortByExactURL(t *testing.T) {
	t.Parallel()
	rows := []assocRow{
		validAssoc("https://b.example/a", "p", "i", nil, "hios", "1", "group"),
		validAssoc("https://a.example/a", "p", "i", nil, "hios", "1", "group"),
		validAssoc("https://a.example/A", "p", "i", nil, "hios", "1", "group"),
		validAssoc("https://a.example/a?x=1", "p", "i", nil, "hios", "1", "group"),
	}
	got := distinctLocations(rows)
	want := []string{"https://a.example/A", "https://a.example/a", "https://a.example/a?x=1", "https://b.example/a"}
	if len(got) != 4 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("%v", got)
	}
	ids := sourceIDsSorted([]int64{9, 2, 7})
	if ids[0] != 2 || ids[1] != 7 || ids[2] != 9 {
		t.Fatalf("%v", ids)
	}
}

func TestCanonicalSponsorProjection(t *testing.T) {
	t.Parallel()
	sponsor := "Acme"
	hios := validAssoc("https://example.test/a.json", "plan", "issuer", &sponsor, "hios", "id", "group")
	if canonicalSponsor(hios) != nil {
		t.Fatal("hios sponsor")
	}
	ein := validAssoc("https://example.test/a.json", "plan", "issuer", &sponsor, "ein", "12", "group")
	if canonicalSponsor(ein) == nil || *canonicalSponsor(ein) != sponsor {
		t.Fatal("ein sponsor")
	}
}

func TestNoAttachmentBatchPlanning(t *testing.T) {
	t.Parallel()
	src, err := os.ReadFile("import.go")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(src, []byte("plan_attachment")) || bytes.Contains(src, []byte("KindConsumerAttachPlans")) {
		t.Fatal("attachment hook")
	}
}

func TestErrorsRedactValues(t *testing.T) {
	t.Parallel()
	secret := "https://secret.example/path/file.json"
	row := validAssoc(secret, "SecretPlan", "SecretIssuer", nil, "hios", "hid", "group")
	err := validateAssocRow(row, "toc-99", "uhc", "2026-08")
	if err != nil {
		if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "SecretPlan") {
			t.Fatalf("leaked %v", err)
		}
	}
	mapped := mapValidateErr(errRow)
	if !jobs.IsFailure(mapped, jobs.FailureTOCImportRowInvalid) {
		t.Fatal(mapped)
	}
	if strings.Contains(mapped.Error(), secret) || strings.Contains(mapped.Error(), "/") {
		t.Fatalf("mapped leaked %v", mapped)
	}
}

func TestWorkerRegistration(t *testing.T) {
	t.Parallel()
	if (jobs.TOCImportArgs{}).Kind() != jobs.KindTOCImport {
		t.Fatal("kind")
	}
	if (jobs.TOCImportArgs{}).InsertOpts().Queue != jobs.QueueTOCImport {
		t.Fatal("queue")
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{})
}

func TestFormatMonth(t *testing.T) {
	t.Parallel()
	if formatMonth(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)) != "2026-08" {
		t.Fatal("month")
	}
}

type descriptor struct {
	OutputSchemaVersion string
	Dataset             string
	Fields              []descriptorField
}

type descriptorField struct {
	Name         string `json:"name"`
	PhysicalType string `json:"physical_type"`
	LogicalType  string `json:"logical_type"`
	Repetition   string `json:"repetition"`
}

func loadDescriptor(t *testing.T, dataset string) descriptor {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "parquet-schema", dataset+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var raw struct {
		OutputSchemaVersion string            `json:"output_schema_version"`
		Dataset             string            `json:"dataset"`
		Fields              []descriptorField `json:"fields"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	return descriptor{OutputSchemaVersion: raw.OutputSchemaVersion, Dataset: raw.Dataset, Fields: raw.Fields}
}

func strPtr(s string) *string { return &s }

func validAssoc(location, plan, issuer string, sponsor *string, idType, id, market string) assocRow {
	row := assocRow{
		TOCOutputID:     "toc-1",
		PayerID:         "uhc",
		CollectionMonth: "2026-08",
		MRFLocation:     location,
		PlanName:        plan,
		IssuerName:      issuer,
		PlanSponsorName: sponsor,
		PlanIDType:      idType,
		PlanID:          id,
		PlanMarketType:  market,
	}
	if name, ok := deriveFilename(location); ok {
		row.MRFFilename = &name
	}
	return row
}

func sampleTOCRow(id, payer, month, source string) tocFileRow {
	return tocFileRow{
		TOCOutputID:         id,
		PayerID:             payer,
		CollectionMonth:     month,
		SourceURI:           source,
		ReportingEntityName: "entity",
		ReportingEntityType: "issuer",
	}
}

func writeParquet[T any](t *testing.T, path string, rows []T) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[T](f, parquet.Compression(&parquet.Zstd))
	if len(rows) > 0 {
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeCompleted(t *testing.T, dir, tocOutputID, payer, month, sourceURI string, toc tocFileRow, assocs []assocRow) {
	t.Helper()
	writeParquet(t, filepath.Join(dir, datasetTOCFiles, "part-00000.parquet"), []tocFileRow{toc})
	writeParquet(t, filepath.Join(dir, datasetAssociations, "part-00000.parquet"), assocs)
	writeManifest(t, dir, tocOutputID, payer, month, sourceURI, int64(len(assocs)))
}

func writeManifest(t *testing.T, dir, tocOutputID, payer, month, sourceURI string, associations int64) {
	t.Helper()
	type man struct {
		ManifestSchemaVersion string `json:"manifest_schema_version"`
		OutputSchemaVersion   string `json:"output_schema_version"`
		TOCOutputID           string `json:"toc_output_id"`
		PayerID               string `json:"payer_id"`
		CollectionMonth       string `json:"collection_month"`
		Source                struct {
			URI      string `json:"uri"`
			Encoding string `json:"encoding"`
		} `json:"source"`
		TOC struct {
			ReportingEntityName string  `json:"reporting_entity_name"`
			ReportingEntityType string  `json:"reporting_entity_type"`
			LastUpdatedOn       *string `json:"last_updated_on"`
			LastUpdatedOnRaw    *string `json:"last_updated_on_raw"`
			SourceSchemaVersion *string `json:"source_schema_version"`
		} `json:"toc"`
		Counts   mrftocparser.Counts `json:"counts"`
		Warnings struct {
			Totals               mrftocparser.WarningTotals `json:"totals"`
			Examples             []struct{}                 `json:"examples"`
			ExamplesTruncated    bool                       `json:"examples_truncated"`
			ExamplesOmittedCount int64                      `json:"examples_omitted_count"`
		} `json:"warnings"`
	}
	var m man
	m.ManifestSchemaVersion = "1.0.0"
	m.OutputSchemaVersion = "1.0.0"
	m.TOCOutputID = tocOutputID
	m.PayerID = payer
	m.CollectionMonth = month
	m.Source.URI = sourceURI
	m.Source.Encoding = "json"
	m.TOC.ReportingEntityName = "entity"
	m.TOC.ReportingEntityType = "issuer"
	m.Counts.TOCFiles = 1
	m.Counts.MRFPlanAssociations = associations
	m.Counts.CandidateAssociationCount = associations
	m.Warnings.Examples = []struct{}{}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
}

func sampleTOC(location, plan, issuer, idType, id, market, sponsor string) string {
	sponsorJSON := ""
	if sponsor != "" {
		sponsorJSON = `,"plan_sponsor_name":"` + sponsor + `"`
	}
	return `{"reporting_entity_name":"entity","reporting_entity_type":"issuer","version":"2.2.1","reporting_structure":[{"reporting_plans":[{"plan_name":"` + plan + `","issuer_name":"` + issuer + `","plan_id_type":"` + idType + `","plan_id":"` + id + `","plan_market_type":"` + market + `"` + sponsorJSON + `}],"in_network_files":[{"description":"file","location":"` + location + `"}]}]}`
}

func mustWorkspace(t *testing.T) *artifact.Workspace {
	t.Helper()
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	return ws
}
