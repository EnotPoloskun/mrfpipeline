package tocparse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/EnotPoloskun/mrftocparser"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

func TestClassifyClaim(t *testing.T) {
	t.Parallel()
	job := int64(72)
	same := job
	other := int64(9)
	cases := []struct {
		name     string
		parse    string
		download string
		imp      string
		stored   *int64
		want     string
		err      string
	}{
		{"succeeded ignores import", jobs.StatusSucceeded, jobs.StatusSucceeded, jobs.StatusPending, &same, jobs.ClaimNoop, ""},
		{"failed", jobs.StatusFailed, jobs.StatusSucceeded, jobs.StatusBlocked, &same, jobs.ClaimNoop, ""},
		{"stale pending", jobs.StatusPending, jobs.StatusSucceeded, jobs.StatusBlocked, &other, jobs.ClaimNoop, ""},
		{"nil job", jobs.StatusPending, jobs.StatusSucceeded, jobs.StatusBlocked, nil, jobs.ClaimNoop, ""},
		{"pending ready", jobs.StatusPending, jobs.StatusSucceeded, jobs.StatusBlocked, &same, jobs.ClaimWork, ""},
		{"running ready", jobs.StatusRunning, jobs.StatusSucceeded, jobs.StatusBlocked, &same, jobs.ClaimWork, ""},
		{"pending download not succeeded", jobs.StatusPending, jobs.StatusPending, jobs.StatusBlocked, &same, "", jobs.FailureDomainInvariant},
		{"pending import not blocked", jobs.StatusPending, jobs.StatusSucceeded, jobs.StatusPending, &same, "", jobs.FailureDomainInvariant},
		{"blocked parse", jobs.StatusBlocked, jobs.StatusSucceeded, jobs.StatusBlocked, &same, "", jobs.FailureDomainInvariant},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := classifyClaim(tc.parse, tc.download, tc.imp, tc.stored, job)
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

func TestFormatConfig(t *testing.T) {
	t.Parallel()
	ws := mustWorkspace(t)
	id := int64(72)
	input, output, err := generatedPaths(ws, id)
	if err != nil {
		t.Fatal(err)
	}
	name, err := artifact.RecordDirName(artifact.KindTOC, id)
	if err != nil || name != "toc-72" {
		t.Fatalf("id %s %v", name, err)
	}
	if !strings.HasSuffix(input, filepath.Join("toc", "toc-72", "download", "data")) {
		t.Fatalf("input %s", input)
	}
	if !strings.HasSuffix(output, filepath.Join("toc", "toc-72", "parsed")) {
		t.Fatalf("output %s", output)
	}
	if formatMonth(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)) != "2026-08" {
		t.Fatal("month")
	}
}

func TestMapWorkError(t *testing.T) {
	t.Parallel()
	if err := mapWorkError(context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if err := mapWorkError(mrftocparser.ErrInvalidConfig); !jobs.IsFailure(err, jobs.FailureDomainInvariant) {
		t.Fatalf("config: %v", err)
	}
	if err := mapWorkError(mrftocparser.ErrInvalidInput); !jobs.IsFailure(err, jobs.FailureTOCParseInputInvalid) {
		t.Fatalf("input: %v", err)
	}
	if err := mapWorkError(mrftocparser.ErrResource); !jobs.IsFailure(err, jobs.FailureTOCParseResourceFailed) {
		t.Fatalf("resource: %v", err)
	}
	if err := mapWorkError(mrftocparser.ErrOutput); !jobs.IsFailure(err, jobs.FailureTOCParseOutputFailed) {
		t.Fatalf("output: %v", err)
	}
	secret := "/tmp/artifacts/toc/toc-72/download/data"
	err := mapWorkError(errors.Join(mrftocparser.ErrInvalidInput, errors.New(secret)))
	if !jobs.IsFailure(err, jobs.FailureTOCParseInputInvalid) {
		t.Fatalf("wrapped: %v", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "toc-72") {
		t.Fatalf("exposed: %v", err)
	}
	err = mapWorkError(errors.Join(artifact.ErrArtifact, errors.New(secret)))
	if !jobs.IsFailure(err, jobs.FailureDomainInvariant) {
		t.Fatalf("artifact: %v", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("exposed artifact: %v", err)
	}
}

func TestManifestAndLayoutValidation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	source := filepath.Join(dir, "download", "data")
	out := filepath.Join(dir, "parsed")
	writeValidParsed(t, out, "toc-72", "uhc", "2026-08", source, 0)
	m, err := validateCompletedOutput(out, "toc-72", "uhc", "2026-08", source)
	if err != nil || m.Counts.TOCFiles != 1 || m.SourceURI != source {
		t.Fatalf("%+v %v", m, err)
	}
	if err := os.WriteFile(filepath.Join(out, ".DS_Store"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := validateCompletedOutput(out, "toc-72", "uhc", "2026-08", source); err == nil {
		t.Fatal("extra root entry")
	}
	if err := os.Remove(filepath.Join(out, ".DS_Store")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, datasetAssociations, "part-00002.parquet"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := validateCompletedOutput(out, "toc-72", "uhc", "2026-08", source); err == nil {
		t.Fatal("gap")
	}
}

func TestStrictManifestDecoding(t *testing.T) {
	t.Parallel()
	good := validManifestJSON("/abs/download/data", "toc-72", "uhc", "2026-08", 0)
	if _, err := decodeManifest(good); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeManifest(append(append([]byte{}, good...), []byte(" {}")...)); err == nil {
		t.Fatal("trailing")
	}
	if _, err := decodeManifest([]byte(`{"manifest_schema_version":1}`)); err == nil {
		t.Fatal("wrong type")
	}
	bad := bytes.Replace(good, []byte(`"toc_files":1`), []byte(`"toc_files":1.0`), 1)
	if _, err := decodeManifest(bad); err == nil {
		t.Fatal("float count")
	}
	unknown := bytes.Replace(good, []byte(`"warnings"`), []byte(`"extra":1,"warnings"`), 1)
	if _, err := decodeManifest(unknown); err == nil {
		t.Fatal("unknown")
	}
}

func TestSourceURIAfterDownloadRemoved(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	source := filepath.Join(dir, "toc", "toc-5", "download", "data")
	out := filepath.Join(dir, "parsed")
	writeValidParsed(t, out, "toc-5", "uhc", "2026-08", source, 1)
	if _, err := os.Lstat(source); !os.IsNotExist(err) {
		t.Fatal("source should be absent")
	}
	m, err := validateCompletedOutput(out, "toc-5", "uhc", "2026-08", source)
	if err != nil || m.SourceURI != source {
		t.Fatalf("%+v %v", m, err)
	}
}

func TestManifestPresentSkipsParse(t *testing.T) {
	t.Parallel()
	ws := mustWorkspace(t)
	id := int64(4)
	body := []byte(`{"ok":true}`)
	writeDownload(t, ws, id, body)
	input, output, err := generatedPaths(ws, id)
	if err != nil {
		t.Fatal(err)
	}
	writeValidParsed(t, output, "toc-4", "uhc", "2026-08", input, 0)
	called := false
	w := &Worker{
		Workspace: ws,
		Parse: func(context.Context, mrftocparser.Config) (mrftocparser.Report, error) {
			called = true
			return mrftocparser.Report{}, errors.New("should not parse")
		},
	}
	if err := w.parse(context.Background(), parseJob(9, id), "uhc", "2026-08"); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("parsed manifest-present output")
	}
	if _, err := os.Lstat(filepath.Join(output, fileManifest)); err != nil {
		t.Fatal("removed output")
	}
	if _, err := os.Lstat(filepath.Join(ws.Root, "toc", "toc-4", "download")); !os.IsNotExist(err) {
		t.Fatal("download remained")
	}
}

func TestManifestPresentWithoutDownloadSucceeds(t *testing.T) {
	t.Parallel()
	ws := mustWorkspace(t)
	id := int64(9)
	input, output, err := generatedPaths(ws, id)
	if err != nil {
		t.Fatal(err)
	}
	writeValidParsed(t, output, "toc-9", "uhc", "2026-08", input, 0)
	if _, err := os.Lstat(filepath.Join(ws.Root, "toc", "toc-9", "download")); !os.IsNotExist(err) {
		t.Fatal("download should be absent")
	}
	called := false
	w := &Worker{
		Workspace: ws,
		Parse: func(context.Context, mrftocparser.Config) (mrftocparser.Report, error) {
			called = true
			return mrftocparser.Report{}, errors.New("should not parse")
		},
	}
	if err := w.parse(context.Background(), parseJob(11, id), "uhc", "2026-08"); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("parsed after download cleanup")
	}
	if _, err := os.Lstat(filepath.Join(output, fileManifest)); err != nil {
		t.Fatal("removed output")
	}
}

func TestIncompleteResetThenParse(t *testing.T) {
	t.Parallel()
	ws := mustWorkspace(t)
	id := int64(6)
	writeDownload(t, ws, id, []byte("x"))
	output, err := ws.ParsedDir(artifact.KindTOC, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(output, 0700); err != nil {
		t.Fatal(err)
	}
	partial := filepath.Join(output, "partial")
	if err := os.WriteFile(partial, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	called := false
	w := &Worker{
		Workspace: ws,
		Parse: func(ctx context.Context, cfg mrftocparser.Config) (mrftocparser.Report, error) {
			called = true
			if _, err := os.Lstat(partial); !os.IsNotExist(err) {
				t.Fatal("partial survived reset")
			}
			writeValidParsed(t, cfg.OutputPath, cfg.TOCOutputID, cfg.PayerID, cfg.CollectionMonth, cfg.InputPath, 0)
			return matchingReport(cfg, 0), nil
		},
	}
	if err := w.parse(context.Background(), parseJob(3, id), "uhc", "2026-08"); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("did not parse")
	}
}

func TestReportMismatchPreservesOutput(t *testing.T) {
	t.Parallel()
	ws := mustWorkspace(t)
	id := int64(8)
	writeDownload(t, ws, id, []byte("x"))
	w := &Worker{
		Workspace: ws,
		Parse: func(ctx context.Context, cfg mrftocparser.Config) (mrftocparser.Report, error) {
			writeValidParsed(t, cfg.OutputPath, cfg.TOCOutputID, cfg.PayerID, cfg.CollectionMonth, cfg.InputPath, 0)
			rep := matchingReport(cfg, 0)
			rep.Counts.MRFPlanAssociations = 99
			return rep, nil
		},
	}
	err := w.parse(context.Background(), parseJob(3, id), "uhc", "2026-08")
	if !jobs.IsFailure(err, jobs.FailureTOCParseOutputInvalid) {
		t.Fatalf("got %v", err)
	}
	output, _ := ws.ParsedDir(artifact.KindTOC, id)
	if _, err := os.Lstat(filepath.Join(output, fileManifest)); err != nil {
		t.Fatal("deleted mismatch output")
	}
}

func TestTMPDIRNotMutatedAroundParse(t *testing.T) {
	ws := mustWorkspace(t)
	id := int64(2)
	writeDownload(t, ws, id, []byte("x"))
	want := filepath.Join(t.TempDir(), "staging")
	t.Setenv("TMPDIR", want)
	w := &Worker{
		Workspace: ws,
		Parse: func(ctx context.Context, cfg mrftocparser.Config) (mrftocparser.Report, error) {
			if os.Getenv("TMPDIR") != want {
				t.Fatalf("TMPDIR mutated to %q", os.Getenv("TMPDIR"))
			}
			writeValidParsed(t, cfg.OutputPath, cfg.TOCOutputID, cfg.PayerID, cfg.CollectionMonth, cfg.InputPath, 0)
			return matchingReport(cfg, 0), nil
		},
	}
	if err := w.parse(context.Background(), parseJob(1, id), "uhc", "2026-08"); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("TMPDIR") != want {
		t.Fatal("TMPDIR changed after parse")
	}
}

func TestProgressPhasesWithoutPercent(t *testing.T) {
	ws := mustWorkspace(t)
	id := int64(11)
	writeDownload(t, ws, id, testdata(t, "toc.json"))
	t.Setenv("TMPDIR", t.TempDir())
	var buf bytes.Buffer
	progress := jobs.NewProgress(jobs.NewLogger(&buf))
	w := &Worker{Workspace: ws, Progress: progress}
	if err := w.parse(context.Background(), parseJob(15, id), "uhc", "2026-08"); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines %d %s", len(lines), buf.String())
	}
	want := []string{"toc_destination_prepared", "toc_input_parsed", "toc_parquet_closed"}
	for i, line := range lines {
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatal(err)
		}
		if obj["phase"] != want[i] {
			t.Fatalf("phase %v", obj)
		}
		if _, ok := obj["percent"]; ok {
			t.Fatalf("percent %v", obj)
		}
		if strings.Contains(line, "https://") || strings.Contains(line, "/toc-") || strings.Contains(line, ws.Root) {
			t.Fatalf("leaked %s", line)
		}
	}
}

func TestFixturesParseThroughPackage(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	for _, name := range []string{"toc.json", "toc_empty.json"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			in := filepath.Join(dir, "input.json")
			if err := os.WriteFile(in, testdata(t, name), 0600); err != nil {
				t.Fatal(err)
			}
			out := filepath.Join(dir, "parsed")
			report, err := DefaultParse(context.Background(), mrftocparser.Config{
				InputPath: in, OutputPath: out, TOCOutputID: "toc-1", PayerID: "uhc", CollectionMonth: "2026-08",
			})
			if err != nil {
				t.Fatal(err)
			}
			if name == "toc_empty.json" && report.Counts.MRFPlanAssociations != 0 {
				t.Fatalf("empty associations %d", report.Counts.MRFPlanAssociations)
			}
			if name == "toc.json" && report.Counts.MRFPlanAssociations != 1 {
				t.Fatalf("assoc %d", report.Counts.MRFPlanAssociations)
			}
			m, err := validateCompletedOutput(out, "toc-1", "uhc", "2026-08", mustAbs(t, in))
			if err != nil || m.Counts.TOCFiles != 1 {
				t.Fatalf("%+v %v", m, err)
			}
		})
	}
}

func mustAbs(t *testing.T, path string) string {
	t.Helper()
	p, err := normalizePath(path)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestWorkerRegistration(t *testing.T) {
	t.Parallel()
	if (jobs.TOCParseArgs{}).Kind() != jobs.KindTOCParse {
		t.Fatal("kind")
	}
	if (jobs.TOCParseArgs{}).InsertOpts().Queue != jobs.QueueTOCParse {
		t.Fatal("queue")
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{})
}

func parseJob(jobID, tocID int64) *river.Job[jobs.TOCParseArgs] {
	return &river.Job[jobs.TOCParseArgs]{
		JobRow: &rivertype.JobRow{ID: jobID},
		Args:   jobs.TOCParseArgs{TOCFileID: tocID},
	}
}

func mustWorkspace(t *testing.T) *artifact.Workspace {
	t.Helper()
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

func testdata(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func writeDownload(t *testing.T, ws *artifact.Workspace, id int64, body []byte) {
	t.Helper()
	name, err := artifact.RecordDirName(artifact.KindTOC, id)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(ws.Root, "toc", name, "download")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "data"), body, 0600); err != nil {
		t.Fatal(err)
	}
	man := []byte(`{"schema_version":"1.0.0","byte_count":` + strconv.Itoa(len(body)) + `}` + "\n")
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), man, 0600); err != nil {
		t.Fatal(err)
	}
}

func writeValidParsed(t *testing.T, dir, tocOutputID, payer, month, sourceURI string, associations int64) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, datasetTOCFiles), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, datasetAssociations), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, datasetTOCFiles, "part-00000.parquet"), []byte("p"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, datasetAssociations, "part-00000.parquet"), []byte("p"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, fileManifest), validManifestJSON(sourceURI, tocOutputID, payer, month, associations), 0600); err != nil {
		t.Fatal(err)
	}
}

func matchingReport(cfg mrftocparser.Config, associations int64) mrftocparser.Report {
	return mrftocparser.Report{
		TOCOutputID: cfg.TOCOutputID,
		FinalPath:   cfg.OutputPath,
		Counts: mrftocparser.Counts{
			TOCFiles:                 1,
			MRFPlanAssociations:      associations,
			CandidateAssociationCount: associations,
		},
	}
}

func validManifestJSON(sourceURI, tocOutputID, payer, month string, associations int64) []byte {
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
		Counts mrftocparser.Counts `json:"counts"`
		Warnings struct {
			Totals               mrftocparser.WarningTotals `json:"totals"`
			Examples             []struct{}                 `json:"examples"`
			ExamplesTruncated    bool                       `json:"examples_truncated"`
			ExamplesOmittedCount int64                      `json:"examples_omitted_count"`
		} `json:"warnings"`
	}
	var m man
	m.ManifestSchemaVersion = manifestSchemaVersion
	m.OutputSchemaVersion = outputSchemaVersion
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
		panic(err)
	}
	return b
}
