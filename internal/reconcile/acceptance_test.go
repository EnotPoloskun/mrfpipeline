package reconcile

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
)

func TestRealAcceptanceGuardsWithoutOptIn(t *testing.T) {
	t.Parallel()
	if os.Getenv(EnvRealAcceptance) == "1" {
		t.Skip("live acceptance env is set")
	}
	if _, err := CheckAcceptanceGuards(os.Getenv); err == nil {
		t.Fatal("ordinary go test must not enable live acceptance")
	}
}

func TestBoundedReleaseStatusRequiresOnlyIntentionalPartialBlocker(t *testing.T) {
	good := release.StatusReport{
		Partial:            true,
		Blockers:           []string{"mrf_source_target_partial"},
		MRFSourcesSelected: 1,
		ConsumerSucceeded:  1,
	}
	if !boundedReleaseStatus(good) {
		t.Fatal("valid bounded status rejected")
	}
	bad := good
	bad.ConsumerFailed = 1
	if boundedReleaseStatus(bad) {
		t.Fatal("failed consumer accepted")
	}
	bad = good
	bad.Blockers = []string{"mrf_source_target_partial", "consume_incomplete"}
	if boundedReleaseStatus(bad) {
		t.Fatal("incomplete consumer blocker accepted")
	}
}

func TestWriteLiveReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	getenv := func(key string) string {
		if key == EnvRealReport {
			return path
		}
		return ""
	}
	if err := writeLiveReport(getenv, liveReport{TOCRows: 1, PeakResidentHeld: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func TestRealUHCAcceptance(t *testing.T) {
	limit, err := CheckAcceptanceGuards(os.Getenv)
	if err != nil {
		t.Skip(err.Error())
	}
	if limit < 1 {
		t.Fatal("limit")
	}
	database.TestDBMu.Lock()
	defer database.TestDBMu.Unlock()
	bin := buildAcceptanceBinary(t)
	ctx := context.Background()
	if deadline, ok := t.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	rep, err := runLiveAcceptance(ctx, os.Getenv, bin)
	if err != nil {
		t.Fatal(err)
	}
	if rep.TOCRows == 0 {
		t.Fatal("no admitted TOC")
	}
	if os.Getenv(EnvRealBounded) != "1" && (rep.RestartSources != rep.Sources || rep.RestartSnapshots != rep.SnapshotsSucceeded || rep.RestartWarehouse != rep.WarehouseFiles) {
		t.Fatalf("restart mutated domain or warehouse counts: %+v", rep)
	}
}

func buildAcceptanceBinary(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	out := filepath.Join(t.TempDir(), "mrfpipeline")
	cmd := exec.Command("go", "build", "-o", out, "./cmd/mrfpipeline")
	cmd.Dir = root
	if got, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, got)
	}
	return out
}
