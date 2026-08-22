package jobs

import (
	"encoding/json"
	"errors"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/riverqueue/river"
)

func TestProductionJobContracts(t *testing.T) {
	t.Parallel()
	type row struct {
		kind   string
		queue  string
		field  string
		kindFn func() string
		opts   river.InsertOpts
		args   any
	}
	cases := []row{
		{KindDiscoveryRun, QueueDiscovery, FieldDiscoveryRunID, DiscoveryRunArgs{}.Kind, DiscoveryRunArgs{}.InsertOpts(), DiscoveryRunArgs{DiscoveryRunID: 7}},
		{KindTOCDownload, QueueTOCDownload, FieldTOCFileID, TOCDownloadArgs{}.Kind, TOCDownloadArgs{}.InsertOpts(), TOCDownloadArgs{TOCFileID: 7}},
		{KindTOCParse, QueueTOCParse, FieldTOCFileID, TOCParseArgs{}.Kind, TOCParseArgs{}.InsertOpts(), TOCParseArgs{TOCFileID: 7}},
		{KindTOCImport, QueueTOCImport, FieldTOCFileID, TOCImportArgs{}.Kind, TOCImportArgs{}.InsertOpts(), TOCImportArgs{TOCFileID: 7}},
		{KindMRFDownload, QueueMRFDownload, FieldMRFSourceID, MRFDownloadArgs{}.Kind, MRFDownloadArgs{}.InsertOpts(), MRFDownloadArgs{MRFSourceID: 7}},
		{KindMRFParse, QueueMRFParse, FieldMRFSourceID, MRFParseArgs{}.Kind, MRFParseArgs{}.InsertOpts(), MRFParseArgs{MRFSourceID: 7}},
		{KindConsumerIngest, QueueConsumer, FieldMRFSnapshotID, ConsumerIngestArgs{}.Kind, ConsumerIngestArgs{}.InsertOpts(), ConsumerIngestArgs{MRFSnapshotID: 7}},
		{KindConsumerAttachPlans, QueueConsumer, FieldPlanAttachmentBatchID, ConsumerAttachPlansArgs{}.Kind, ConsumerAttachPlansArgs{}.InsertOpts(), ConsumerAttachPlansArgs{PlanAttachmentBatchID: 7}},
	}
	if len(cases) != len(ProductionKinds()) {
		t.Fatalf("catalog size %d", len(cases))
	}
	for i, tc := range cases {
		if tc.kindFn() != tc.kind || ProductionKinds()[i] != tc.kind {
			t.Fatalf("kind %s", tc.kind)
		}
		opts := tc.opts
		if opts.Queue != tc.queue {
			t.Fatalf("%s queue %q", tc.kind, opts.Queue)
		}
		if opts.UniqueOpts.ByArgs || opts.UniqueOpts.ByPeriod != 0 || opts.UniqueOpts.ByQueue || opts.UniqueOpts.ExcludeKind || len(opts.UniqueOpts.ByState) != 0 || len(opts.Tags) != 0 || len(opts.Metadata) != 0 {
			t.Fatalf("%s insert opts %+v", tc.kind, opts)
		}
		raw, err := json.Marshal(tc.args)
		if err != nil {
			t.Fatal(err)
		}
		var obj map[string]any
		if err := json.Unmarshal(raw, &obj); err != nil {
			t.Fatal(err)
		}
		if len(obj) != 1 {
			t.Fatalf("%s fields %v", tc.kind, obj)
		}
		if _, ok := obj[tc.field]; !ok {
			t.Fatalf("%s missing %s", tc.kind, tc.field)
		}
	}
}

func TestJobArgsRejectInvalidJSON(t *testing.T) {
	t.Parallel()
	payloads := []string{
		`{}`,
		`{"discovery_run_id":0}`,
		`{"discovery_run_id":-1}`,
		`{"discovery_run_id":1.5}`,
		`{"discovery_run_id":"1"}`,
		`{"discovery_run_id":null}`,
		`{"discovery_run_id":9223372036854775808}`,
		`{"discovery_run_id":1,"extra":2}`,
		`{"other_id":1}`,
		`{"discovery_run_id":1,"discovery_run_id":2}`,
		`[]`,
		`1`,
	}
	for _, raw := range payloads {
		var args DiscoveryRunArgs
		err := json.Unmarshal([]byte(raw), &args)
		if !errors.Is(err, ErrJob) {
			t.Fatalf("%s: %v", raw, err)
		}
		if !isFailure(err, FailureInvalidArguments) && !strings.Contains(err.Error(), FailureInvalidArguments) {
			t.Fatalf("%s: %v", raw, err)
		}
		if strings.Contains(err.Error(), "postgres://") || strings.Contains(err.Error(), "https://") {
			t.Fatalf("leaked: %v", err)
		}
	}
	var ok DiscoveryRunArgs
	if err := json.Unmarshal([]byte(`{"discovery_run_id":41}`), &ok); err != nil || ok.DiscoveryRunID != 41 {
		t.Fatalf("valid: %+v %v", ok, err)
	}
}

func TestParserMutexRecordedWithoutImport(t *testing.T) {
	t.Parallel()
	if !ParserMutexRequired {
		t.Fatal("Story 10 parser mutex must be recorded")
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	dir := filepath.Dir(file)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		src, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		af, err := parser.ParseFile(fset, entry.Name(), src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range af.Imports {
			path := strings.Trim(spec.Path.Value, `"`)
			if strings.Contains(path, "mrfparser") {
				t.Fatalf("%s imports mrfparser", entry.Name())
			}
		}
	}
	cfg := ProductionPolicy()
	if cfg.Queues[QueueMRFParse].MaxWorkers != 1 {
		t.Fatal("mrf_parse concurrency")
	}
}
