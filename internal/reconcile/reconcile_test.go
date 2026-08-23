package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/riverqueue/river/rivertype"
)

func TestClassifyRiverStates(t *testing.T) {
	t.Parallel()
	cases := []struct {
		found  bool
		state  string
		action string
		fail   bool
	}{
		{false, "", actionReplace, false},
		{true, string(rivertype.JobStateAvailable), actionKeep, false},
		{true, string(rivertype.JobStatePending), actionKeep, false},
		{true, string(rivertype.JobStateScheduled), actionKeep, false},
		{true, string(rivertype.JobStateRetryable), actionKeep, false},
		{true, string(rivertype.JobStateRunning), actionReplace, false},
		{true, string(rivertype.JobStateCompleted), actionReplace, false},
		{true, string(rivertype.JobStateCancelled), actionFail, false},
		{true, string(rivertype.JobStateDiscarded), actionFail, false},
		{true, "mystery", actionInvariant, true},
	}
	seen := map[string]bool{}
	for _, tc := range cases {
		got, err := classifyRiver(tc.found, tc.state)
		if tc.fail {
			if err == nil || !jobs.IsFailure(err, jobs.FailureDomainInvariant) {
				t.Fatalf("state %q: %v", tc.state, err)
			}
			continue
		}
		if err != nil || got != tc.action {
			t.Fatalf("state %q: %s %v", tc.state, got, err)
		}
		if tc.found {
			seen[tc.state] = true
		}
	}
	for _, st := range rivertype.JobStates() {
		if !seen[string(st)] {
			t.Fatalf("untested river state %s", st)
		}
	}
}

func TestFormatReportAndRetry(t *testing.T) {
	t.Parallel()
	text, err := FormatReport(Report{RepairedJobCount: 3, UnblockedStageCount: 2, ScheduledPlanBatchCount: 1, CleanedArtifactCount: 4})
	if err != nil {
		t.Fatal(err)
	}
	if text != `{"repaired_job_count":3,"unblocked_stage_count":2,"scheduled_plan_batch_count":1,"cleaned_artifact_count":4}`+"\n" {
		t.Fatalf("report %q", text)
	}
	if strings.Contains(text, "http") || strings.Contains(text, "toc-") {
		t.Fatal("report leaked identity")
	}
	retry, err := FormatRetry(RetryResult{Stage: jobs.KindConsumerAttachPlans, DomainID: 230, RiverJobID: 9012})
	if err != nil || retry != `{"stage":"consumer.attach_plans","domain_id":230,"river_job_id":9012}`+"\n" {
		t.Fatalf("retry %q %v", retry, err)
	}
}

func TestLeaseErrorsOmitConstants(t *testing.T) {
	t.Parallel()
	for _, code := range []string{
		jobs.FailureWorkerLeaseUnavailable,
		jobs.FailureWorkerLeaseLost,
		jobs.FailureReconciliationDatabaseFailed,
		jobs.FailureArtifactReconciliationFailed,
	} {
		err := jobs.Failure(code)
		if strings.Contains(err.Error(), "7319") || strings.Contains(err.Error(), "objid") {
			t.Fatalf("%v", err)
		}
	}
}

func TestProductionBindingsCoverKinds(t *testing.T) {
	t.Parallel()
	if len(jobs.ProductionBindings()) != 8 {
		t.Fatal("expected eight production stages")
	}
	seen := map[string]bool{}
	for _, b := range jobs.ProductionBindings() {
		if seen[b.Kind] || b.Spec.Table == "" || b.Spec.StatusColumn == "" || b.Spec.JobIDColumn == "" || b.ArgField == "" {
			t.Fatalf("binding %+v", b)
		}
		seen[b.Kind] = true
	}
}

func TestCleanupEligibility(t *testing.T) {
	t.Parallel()
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.RemoveParsed(artifact.KindTOC, 9); err != nil {
		t.Fatal(err)
	}
	if err := ws.RemovePlanBatch(4); err != nil {
		t.Fatal(err)
	}
	rec, err := ws.PlanBatchDir(4)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(rec, "keep"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rec, "plans.json"), []byte("[]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ws.RemovePlanBatch(4); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(rec); !os.IsNotExist(err) {
		t.Fatal("plan batch remained")
	}
	parsed := filepath.Join(ws.Root, "toc", "toc-3", "parsed")
	if err := os.MkdirAll(parsed, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parsed, "manifest.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ws.RemoveParsed(artifact.KindTOC, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(parsed); !os.IsNotExist(err) {
		t.Fatal("parsed remained")
	}
	staging := ws.StagingDir()
	young := filepath.Join(staging, "young")
	old := filepath.Join(staging, "olddir")
	if err := os.WriteFile(young, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(old, 0700); err != nil {
		t.Fatal(err)
	}
	oldTime := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(old, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	var report Report
	if err := cleanStaging(ws, &report); err != nil {
		t.Fatal(err)
	}
	if report.CleanedArtifactCount != 1 {
		t.Fatalf("cleaned %d", report.CleanedArtifactCount)
	}
	if _, err := os.Lstat(young); err != nil {
		t.Fatal("removed young staging")
	}
	if _, err := os.Lstat(old); !os.IsNotExist(err) {
		t.Fatal("old staging remained")
	}
	link := filepath.Join(staging, "link")
	if err := os.Symlink(young, link); err != nil {
		t.Fatal(err)
	}
	oldTime = time.Now().Add(-25 * time.Hour)
	_ = os.Chtimes(link, oldTime, oldTime)
	report = Report{}
	if err := cleanStaging(ws, &report); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(link); err != nil {
		t.Fatal("removed symlink")
	}
}

func TestAcceptanceGuards(t *testing.T) {
	t.Parallel()
	art := filepath.Join(t.TempDir(), "art")
	wh := filepath.Join(t.TempDir(), "wh")
	if err := os.Mkdir(art, 0700); err != nil || os.Mkdir(wh, 0700) != nil {
		t.Fatal("mkdir")
	}
	base := map[string]string{
		EnvRealAcceptance:                "1",
		EnvTestDatabase:                  "postgres://u@127.0.0.1:1/mrfpipeline_test_guard",
		"MRFPIPELINE_ARTIFACT_ROOT":      art,
		"MRFPIPELINE_WAREHOUSE_PATH":     wh,
		"MRFPIPELINE_PROVIDER_CATALOG_PATH": filepath.Join(t.TempDir(), "cat"),
		"MRFPIPELINE_SERVICES_PATH":      filepath.Join(t.TempDir(), "svc.csv"),
	}
	getenv := func(m map[string]string) func(string) string {
		return func(k string) string { return m[k] }
	}
	n, err := CheckAcceptanceGuards(getenv(base))
	if err != nil || n != 1 {
		t.Fatalf("default limit %d %v", n, err)
	}
	base[EnvRealTOCLimit] = "11"
	if _, err := CheckAcceptanceGuards(getenv(base)); err == nil {
		t.Fatal("limit 11")
	}
	base[EnvRealTOCLimit] = "2"
	if n, err = CheckAcceptanceGuards(getenv(base)); err != nil || n != 2 {
		t.Fatalf("limit 2 %d %v", n, err)
	}
	bad := copyMap(base)
	bad[EnvTestDatabase] = "postgres://u@127.0.0.1:1/production"
	if _, err := CheckAcceptanceGuards(getenv(bad)); err == nil {
		t.Fatal("production db")
	}
	overlap := copyMap(base)
	overlap["MRFPIPELINE_WAREHOUSE_PATH"] = art
	if _, err := CheckAcceptanceGuards(getenv(overlap)); err == nil {
		t.Fatal("overlap")
	}
	if _, err := CheckAcceptanceGuards(func(string) string { return "" }); err == nil {
		t.Fatal("opt-in")
	}

	unmarked := copyMap(base)
	if err := os.WriteFile(filepath.Join(art, "stray"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckAcceptanceGuards(getenv(unmarked)); err == nil {
		t.Fatal("nonempty unmarked artifact")
	}
	if err := os.WriteFile(filepath.Join(art, "workspace.json"), []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckAcceptanceGuards(getenv(unmarked)); err != nil {
		t.Fatalf("marked artifact: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wh, "other"), []byte("y"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckAcceptanceGuards(getenv(unmarked)); err == nil {
		t.Fatal("nonempty unmarked warehouse")
	}
}

func TestHealthLeaseLostCancels(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var lost atomic.Bool
	var called bool
	h := Health(nil, func() {
		called = true
		cancel()
	}, &lost)
	err := h(context.Background())
	if !jobs.IsFailure(err, jobs.FailureWorkerLeaseLost) {
		t.Fatalf("got %v", err)
	}
	if !lost.Load() || !called {
		t.Fatal("did not cancel process")
	}
	if ctx.Err() == nil {
		t.Fatal("work ctx not canceled")
	}
	if strings.Contains(err.Error(), "7319") || strings.Contains(err.Error(), "objid") {
		t.Fatalf("logged lease keys: %v", err)
	}
}

func TestHealthCanceledContextIsNotLeaseLost(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var lost atomic.Bool
	var called bool
	h := Health(nil, func() { called = true }, &lost)
	err := h(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if lost.Load() || called {
		t.Fatal("shutdown cancel treated as lease loss")
	}
}

func TestStatusSQLHasNoURLs(t *testing.T) {
	t.Parallel()
	for _, q := range []string{
		SQLDiscoveryCounts, SQLTOCStageCounts, SQLMRFSourceCounts, SQLSnapshotConsumeCounts,
		SQLPlanBatchCounts, SQLTerminalFailures, SQLUnassignedPlans, SQLPlanReadySnapshots, SQLSharedSources,
	} {
		if strings.Contains(q, "source_url") {
			t.Fatalf("default query includes url: %s", q)
		}
	}
	if !strings.Contains(SQLAuthorizedURLDebug, "source_url") {
		t.Fatal("debug query must return urls")
	}
}

func TestReportJSONAllowlist(t *testing.T) {
	t.Parallel()
	var obj map[string]any
	text, err := FormatReport(Report{})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(text), &obj); err != nil {
		t.Fatal(err)
	}
	if len(obj) != 4 {
		t.Fatalf("fields %v", obj)
	}
}

func copyMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
