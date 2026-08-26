package mrfparse

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/EnotPoloskun/mrfparser"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

func TestClassifyClaim(t *testing.T) {
	t.Parallel()
	job := int64(151)
	same := job
	other := int64(9)
	cases := []struct {
		name     string
		parse    string
		download string
		stored   *int64
		want     string
		err      string
	}{
		{"succeeded", jobs.StatusSucceeded, jobs.StatusSucceeded, &same, jobs.ClaimNoop, ""},
		{"failed", jobs.StatusFailed, jobs.StatusSucceeded, &same, jobs.ClaimNoop, ""},
		{"stale pending", jobs.StatusPending, jobs.StatusSucceeded, &other, jobs.ClaimNoop, ""},
		{"nil job", jobs.StatusPending, jobs.StatusSucceeded, nil, jobs.ClaimNoop, ""},
		{"pending ready", jobs.StatusPending, jobs.StatusSucceeded, &same, jobs.ClaimWork, ""},
		{"running ready", jobs.StatusRunning, jobs.StatusSucceeded, &same, jobs.ClaimWork, ""},
		{"pending download not succeeded", jobs.StatusPending, jobs.StatusPending, &same, "", jobs.FailureDomainInvariant},
		{"blocked parse", jobs.StatusBlocked, jobs.StatusSucceeded, &same, "", jobs.FailureDomainInvariant},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := classifyClaim(tc.parse, tc.download, tc.stored, job)
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

func TestDefaultConfigOnlyAllowedFields(t *testing.T) {
	t.Parallel()
	cfg := mrfparser.DefaultConfig()
	if cfg.MemoryLimit != 1<<30 || cfg.TempDiskLimit != 10<<30 ||
		cfg.TargetFileSize != 256<<20 || cfg.RowGroupRows != 100_000 {
		t.Fatalf("defaults %+v", cfg)
	}
	typ := reflect.TypeOf(cfg)
	allowed := map[string]bool{
		"Input": true, "Output": true, "Services": true, "TempDir": true, "OnProgress": true,
		"MemoryLimit": true, "TempDiskLimit": true, "TargetFileSize": true, "RowGroupRows": true,
	}
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if !allowed[name] {
			t.Fatalf("unexpected field %s", name)
		}
		if name == "Plans" || name == "Payer" || name == "Feed" || name == "CollectionMonth" ||
			name == "Snapshot" || name == "TOC" {
			t.Fatal(name)
		}
	}
}

func TestMapWorkErrorRedactsParserText(t *testing.T) {
	t.Parallel()
	if err := mapWorkError(context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	secret := "https://secret.example.invalid/mrf-source-151?token=abc"
	err := mapWorkError(errors.Join(errors.New("parse boom"), errors.New(secret)))
	if !jobs.IsFailure(err, jobs.FailureMRFParseExecutionFailed) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "151") {
		t.Fatalf("exposed: %v", err)
	}
}

func TestManifestPresentWithoutDownloadSucceeds(t *testing.T) {
	t.Parallel()
	ws := mustWorkspace(t)
	svc := mustServices(t)
	id := int64(9)
	input, output, err := generatedPaths(ws, id)
	if err != nil {
		t.Fatal(err)
	}
	writeValidParsed(t, output, expectedSourceURI(input), svc.Path)
	if _, err := os.Lstat(filepath.Join(ws.Root, "mrf", "mrf-source-9", "download")); !os.IsNotExist(err) {
		t.Fatal("download should be absent")
	}
	called := false
	w := &Worker{
		Workspace: ws,
		Services:  svc,
		Parse: func(context.Context, mrfparser.Config) error {
			called = true
			return errors.New("should not parse")
		},
	}
	if err := w.parse(context.Background(), parseJob(11, id)); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("parsed after download cleanup")
	}
	if _, err := os.Lstat(filepath.Join(output, fileManifest)); err != nil {
		t.Fatal("removed output")
	}
}

func TestMissingDownloadWhenParseRequired(t *testing.T) {
	t.Parallel()
	ws := mustWorkspace(t)
	svc := mustServices(t)
	id := int64(7)
	called := false
	w := &Worker{
		Workspace: ws,
		Services:  svc,
		Parse: func(context.Context, mrfparser.Config) error {
			called = true
			return errors.New("should not parse")
		},
	}
	err := w.parse(context.Background(), parseJob(4, id))
	if !jobs.IsFailure(err, jobs.FailureDomainInvariant) {
		t.Fatalf("got %v", err)
	}
	if called {
		t.Fatal("parsed without download")
	}
}

func TestSelectorChangedFailsParse(t *testing.T) {
	t.Parallel()
	ws := mustWorkspace(t)
	svc := mustServices(t)
	id := int64(5)
	writeDownload(t, ws, id, []byte("x"))
	if err := os.WriteFile(svc.Path, append(testdata(t, "services.csv"), '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	called := false
	w := &Worker{
		Workspace: ws,
		Services:  svc,
		Parse: func(context.Context, mrfparser.Config) error {
			called = true
			return errors.New("should not parse")
		},
	}
	err := w.parse(context.Background(), parseJob(2, id))
	if !jobs.IsFailure(err, jobs.FailureMRFParseSelectorChanged) {
		t.Fatalf("got %v", err)
	}
	if called {
		t.Fatal("parsed after selector change")
	}
}

func TestIncompleteResetThenParse(t *testing.T) {
	t.Parallel()
	ws := mustWorkspace(t)
	svc := mustServices(t)
	id := int64(6)
	writeDownload(t, ws, id, []byte("x"))
	output, err := ws.ParsedDir(artifact.KindMRF, id)
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
		Services:  svc,
		Parse: func(ctx context.Context, cfg mrfparser.Config) error {
			called = true
			if _, err := os.Lstat(partial); !os.IsNotExist(err) {
				t.Fatal("partial survived reset")
			}
			wantTemp, _ := ws.MRFParserTempDir(id)
			if cfg.TempDir != wantTemp || cfg.Services != svc.Path {
				t.Fatalf("cfg %+v", cfg)
			}
			if cfg.MemoryLimit != 1<<30 || cfg.OnProgress == nil {
				t.Fatal("defaults or progress")
			}
			input, _, _ := generatedPaths(ws, id)
			writeValidParsed(t, cfg.Output, expectedSourceURI(input), svc.Path)
			return nil
		},
	}
	if err := w.parse(context.Background(), parseJob(3, id)); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("did not parse")
	}
}

func TestInvalidManifestPresentPreserved(t *testing.T) {
	t.Parallel()
	ws := mustWorkspace(t)
	svc := mustServices(t)
	id := int64(8)
	writeDownload(t, ws, id, []byte("x"))
	output, err := ws.ParsedDir(artifact.KindMRF, id)
	if err != nil {
		t.Fatal(err)
	}
	input, _, _ := generatedPaths(ws, id)
	writeValidParsed(t, output, expectedSourceURI(input), svc.Path)
	if err := os.WriteFile(filepath.Join(output, ".DS_Store"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	called := false
	w := &Worker{Workspace: ws, Services: svc, Parse: func(context.Context, mrfparser.Config) error {
		called = true
		return errors.New("should not parse")
	}}
	err = w.parse(context.Background(), parseJob(3, id))
	if !jobs.IsFailure(err, jobs.FailureMRFParseOutputInvalid) {
		t.Fatalf("got %v", err)
	}
	if called {
		t.Fatal("reparsed corrupt output")
	}
	if _, err := os.Lstat(filepath.Join(output, fileManifest)); err != nil {
		t.Fatal("deleted corrupt output")
	}
	if _, err := os.Lstat(filepath.Join(output, ".DS_Store")); err != nil {
		t.Fatal("deleted extra entry")
	}
}

func TestLayoutRejectsPlansAndDSStore(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	svc := mustServices(t)
	source := filepath.Join(dir, "download", "data")
	out := filepath.Join(dir, "parsed")
	writeValidParsed(t, out, source, svc.Path)
	if err := validateCompletedOutput(out, source, svc.Path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, ".DS_Store"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateCompletedOutput(out, source, svc.Path); err == nil {
		t.Fatal("extra root")
	}
	if err := os.Remove(filepath.Join(out, ".DS_Store")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(out, "plans"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := validateCompletedOutput(out, source, svc.Path); err == nil {
		t.Fatal("plans")
	}
}

func TestStrictManifestAndStats(t *testing.T) {
	t.Parallel()
	svc := mustServices(t)
	good := validManifestJSON("/abs/download/data", svc.Path, 0)
	if _, err := decodeManifest(good); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeManifest(append(append([]byte{}, good...), []byte(" {}")...)); err == nil {
		t.Fatal("trailing")
	}
	unknown := bytes.Replace(good, []byte(`"warnings"`), []byte(`"extra":1,"warnings"`), 1)
	if _, err := decodeManifest(unknown); err == nil {
		t.Fatal("unknown")
	}
	plans := bytes.Replace(good, []byte(`"mrf_files":1`), []byte(`"plans":0,"mrf_files":1`), 1)
	if _, err := decodeManifest(plans); err == nil {
		t.Fatal("plans count")
	}
	if err := decodeProcessingStats(validStatsJSON()); err != nil {
		t.Fatal(err)
	}
	if err := decodeProcessingStats([]byte(`{"stored_bytes":1}`)); err == nil {
		t.Fatal("short stats")
	}
}

func TestSelectorMetadataNoDigest(t *testing.T) {
	t.Parallel()
	a := mustServices(t)
	b, err := InspectServices(a.Path)
	if err != nil || !a.same(b) {
		t.Fatalf("%+v %+v %v", a, b, err)
	}
	raw, _ := json.Marshal(a)
	if strings.Contains(string(raw), "sha") || strings.Contains(string(raw), "digest") {
		t.Fatal(string(raw))
	}
	if err := os.WriteFile(a.Path, append(testdata(t, "services.csv"), '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := InspectServices(a.Path)
	if err != nil || a.same(c) {
		t.Fatal("mtime/size should change")
	}
}

func TestTMPDIRNotMutatedAroundParse(t *testing.T) {
	ws := mustWorkspace(t)
	svc := mustServices(t)
	id := int64(2)
	writeDownload(t, ws, id, []byte("x"))
	want := filepath.Join(t.TempDir(), "staging")
	t.Setenv("TMPDIR", want)
	w := &Worker{
		Workspace: ws,
		Services:  svc,
		Parse: func(ctx context.Context, cfg mrfparser.Config) error {
			if os.Getenv("TMPDIR") != want {
				t.Fatalf("TMPDIR mutated to %q", os.Getenv("TMPDIR"))
			}
			wantTemp, _ := ws.MRFParserTempDir(id)
			if cfg.TempDir != wantTemp {
				t.Fatalf("temp %s", cfg.TempDir)
			}
			input, _, _ := generatedPaths(ws, id)
			writeValidParsed(t, cfg.Output, expectedSourceURI(input), svc.Path)
			return nil
		},
	}
	if err := w.parse(context.Background(), parseJob(1, id)); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("TMPDIR") != want {
		t.Fatal("TMPDIR changed after parse")
	}
}

func TestProgressLiveVersusReuse(t *testing.T) {
	ws := mustWorkspace(t)
	svc := mustServices(t)
	id := int64(11)
	writeDownload(t, ws, id, []byte("x"))
	var live bytes.Buffer
	w := &Worker{
		Workspace: ws,
		Services:  svc,
		Progress:  jobs.NewProgress(jobs.NewLogger(&live)),
		Parse: func(ctx context.Context, cfg mrfparser.Config) error {
			if cfg.OnProgress == nil {
				t.Fatal("missing OnProgress")
			}
			if err := cfg.OnProgress(mrfparser.Progress{StoredBytes: 12, TotalBytes: 100, SizeKnown: true}); err != nil {
				t.Fatal(err)
			}
			input, _, _ := generatedPaths(ws, id)
			writeValidParsed(t, cfg.Output, expectedSourceURI(input), svc.Path)
			return nil
		},
	}
	if err := w.parse(context.Background(), parseJob(15, id)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(live.String(), `"phase":"mrf_parse"`) || !strings.Contains(live.String(), `"percent":`) {
		t.Fatalf("live %s", live.String())
	}
	if strings.Contains(live.String(), "mrf-source") || strings.Contains(live.String(), ws.Root) {
		t.Fatalf("leaked %s", live.String())
	}
	var reuse bytes.Buffer
	w.Progress = jobs.NewProgress(jobs.NewLogger(&reuse))
	w.Parse = func(context.Context, mrfparser.Config) error {
		t.Fatal("reuse parsed")
		return nil
	}
	if err := w.parse(context.Background(), parseJob(15, id)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(reuse.String(), "mrf_parse") {
		t.Fatalf("reuse progress %s", reuse.String())
	}
}

func TestParseHoldsProcessMutex(t *testing.T) {
	ws := mustWorkspace(t)
	svc := mustServices(t)
	id := int64(4)
	writeDownload(t, ws, id, []byte("x"))
	held := false
	w := &Worker{
		Workspace: ws,
		Services:  svc,
		Parse: func(ctx context.Context, cfg mrfparser.Config) error {
			if parseMu.TryLock() {
				parseMu.Unlock()
				t.Fatal("mutex not held")
			}
			held = true
			input, _, _ := generatedPaths(ws, id)
			writeValidParsed(t, cfg.Output, expectedSourceURI(input), svc.Path)
			return nil
		},
	}
	if err := w.parse(context.Background(), parseJob(1, id)); err != nil {
		t.Fatal(err)
	}
	if !held {
		t.Fatal("parse not called")
	}
}

func TestWorkerRegistration(t *testing.T) {
	t.Parallel()
	if (jobs.MRFParseArgs{}).Kind() != jobs.KindMRFParse {
		t.Fatal("kind")
	}
	if opts := (jobs.MRFParseArgs{}).InsertOpts(); opts.Queue != jobs.QueueMRFParse || opts.MaxAttempts != 4 {
		t.Fatalf("insert opts %+v", opts)
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{})
}

func TestFixturesParseThroughPackage(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	svc := mustServices(t)
	for _, name := range []string{"mrf.json", "mrf_no_plan.json"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			in := filepath.Join(dir, "input.json")
			if err := os.WriteFile(in, testdata(t, name), 0600); err != nil {
				t.Fatal(err)
			}
			out := filepath.Join(dir, "parsed")
			cfg := mrfparser.DefaultConfig()
			cfg.Input = in
			cfg.Output = out
			cfg.Services = svc.Path
			cfg.TempDir = filepath.Join(dir, "tmp")
			if err := os.Mkdir(cfg.TempDir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := DefaultParse(context.Background(), cfg); err != nil {
				t.Fatal(err)
			}
			source, err := normalizePath(in)
			if err != nil {
				t.Fatal(err)
			}
			if resolved, rerr := filepath.EvalSymlinks(source); rerr == nil {
				source = resolved
			}
			if err := validateCompletedOutput(out, source, svc.Path); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(out, "plans")); !os.IsNotExist(err) {
				t.Fatal("plans present")
			}
		})
	}
	t.Run("gzip", func(t *testing.T) {
		dir := t.TempDir()
		in := filepath.Join(dir, "input.json.gz")
		f, err := os.OpenFile(in, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		zw := gzip.NewWriter(f)
		if _, err := zw.Write(testdata(t, "mrf.json")); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		out := filepath.Join(dir, "parsed")
		cfg := mrfparser.DefaultConfig()
		cfg.Input = in
		cfg.Output = out
		cfg.Services = svc.Path
		cfg.TempDir = filepath.Join(dir, "tmp")
		if err := os.Mkdir(cfg.TempDir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := DefaultParse(context.Background(), cfg); err != nil {
			t.Fatal(err)
		}
		source, err := normalizePath(in)
		if err != nil {
			t.Fatal(err)
		}
		if resolved, rerr := filepath.EvalSymlinks(source); rerr == nil {
			source = resolved
		}
		if err := validateCompletedOutput(out, source, svc.Path); err != nil {
			t.Fatal(err)
		}
	})
}

func parseJob(jobID, sourceID int64) *river.Job[jobs.MRFParseArgs] {
	return &river.Job[jobs.MRFParseArgs]{
		JobRow: &rivertype.JobRow{ID: jobID},
		Args:   jobs.MRFParseArgs{MRFSourceID: sourceID},
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

func mustServices(t *testing.T) ServicesID {
	t.Helper()
	path := filepath.Join(t.TempDir(), "services.csv")
	if err := os.WriteFile(path, testdata(t, "services.csv"), 0600); err != nil {
		t.Fatal(err)
	}
	id, err := InspectServices(path)
	if err != nil {
		t.Fatal(err)
	}
	return id
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
	name, err := artifact.RecordDirName(artifact.KindMRF, id)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(ws.Root, "mrf", name, "download")
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

func writeValidParsed(t *testing.T, dir, sourceURI, servicesPath string) {
	t.Helper()
	for _, name := range datasetNames {
		p := filepath.Join(dir, name)
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
	if err := os.WriteFile(filepath.Join(dir, fileManifest), validManifestJSON(sourceURI, sel, 0), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, fileStats), validStatsJSON(), 0600); err != nil {
		t.Fatal(err)
	}
}

func validManifestJSON(sourceURI, selectorURI string, retained int64) []byte {
	type man struct {
		ManifestSchemaVersion string `json:"manifest_schema_version"`
		OutputSchemaVersion   string `json:"output_schema_version"`
		Status                string `json:"status"`
		Source                struct {
			URI  string `json:"uri"`
			Kind string `json:"kind"`
		} `json:"source"`
		Selection struct {
			Mode                     string `json:"mode"`
			SelectorURI              string `json:"selector_uri"`
			RequestedUniquePairCount int64  `json:"requested_unique_pair_count"`
			MatchedUniquePairCount   int64  `json:"matched_unique_pair_count"`
			UnmatchedUniquePairCount int64  `json:"unmatched_unique_pair_count"`
		} `json:"selection"`
		Counts struct {
			SourceServiceCount   int64 `json:"source_service_count"`
			RetainedServiceCount int64 `json:"retained_service_count"`
			FilteredServiceCount int64 `json:"filtered_service_count"`
			MRFFiles             int64 `json:"mrf_files"`
			Services             int64 `json:"services"`
			ServiceRelations     int64 `json:"service_relations"`
			RateGroups           int64 `json:"rate_groups"`
			NegotiatedPrices     int64 `json:"negotiated_prices"`
			RateProviderGroups   int64 `json:"rate_provider_groups"`
			ProviderGroups       int64 `json:"provider_groups"`
			Providers            int64 `json:"providers"`
		} `json:"counts"`
		Warnings struct {
			Totals struct {
				UnknownField                int64 `json:"unknown_field"`
				MissingRequired             int64 `json:"missing_required"`
				InvalidValue                int64 `json:"invalid_value"`
				SourceSchema                int64 `json:"source_schema"`
				UnresolvedProviderReference int64 `json:"unresolved_provider_reference"`
				DuplicateProviderDefinition int64 `json:"duplicate_provider_definition"`
				UnusedProviderDefinition    int64 `json:"unused_provider_definition"`
				DuplicateSelectorEntry      int64 `json:"duplicate_selector_entry"`
			} `json:"totals"`
			Examples             []struct{} `json:"examples"`
			ExamplesTruncated    bool       `json:"examples_truncated"`
			ExamplesOmittedCount int64      `json:"examples_omitted_count"`
		} `json:"warnings"`
	}
	var m man
	m.ManifestSchemaVersion = manifestSchemaVersion
	m.OutputSchemaVersion = outputSchemaVersion
	m.Status = statusComplete
	m.Source.URI = sourceURI
	m.Source.Kind = sourceKindLocal
	m.Selection.Mode = selectionServiceCSV
	m.Selection.SelectorURI = selectorURI
	m.Selection.RequestedUniquePairCount = 1
	m.Selection.UnmatchedUniquePairCount = 1
	m.Counts.MRFFiles = 1
	m.Counts.SourceServiceCount = retained
	m.Counts.RetainedServiceCount = retained
	m.Counts.Services = retained
	m.Warnings.Examples = []struct{}{}
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

func validStatsJSON() []byte {
	st := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)
	fn := st.Add(time.Second)
	type stats struct {
		StoredBytes         int64   `json:"stored_bytes"`
		DecodedBytes        int64   `json:"decoded_bytes"`
		StartedAt           string  `json:"started_at"`
		FinishedAt          string  `json:"finished_at"`
		WallSeconds         float64 `json:"wall_seconds"`
		DecodedMiBPerSecond float64 `json:"decoded_mib_per_second"`
	}
	b, err := json.Marshal(stats{
		StoredBytes: 1, DecodedBytes: 1,
		StartedAt: st.Format(time.RFC3339Nano), FinishedAt: fn.Format(time.RFC3339Nano),
		WallSeconds: 1, DecodedMiBPerSecond: 1,
	})
	if err != nil {
		panic(err)
	}
	return b
}
