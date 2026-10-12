package mrfparse

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/enotpoloskun/mrfpipeline/internal/admission"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

func TestIntegrationCLIExit4ReleasesSlot(t *testing.T) {
	t.Setenv("MRFPARSE_HELPER", "exit4")
	path, err := LookupParser(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	SetResolvedParser(path)
	t.Cleanup(func() { SetResolvedParser("") })
	pool := testDB(t)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	svc := mustServices(t)
	client := insertClient(t, pool)
	sourceID, parseJobID := insertParseJob(t, pool, client)
	insertBlockedSnapshot(t, pool, sourceID, "2026-08-01")
	var waitingID int64
	if err := pool.QueryRow(context.Background(), `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ($1, DATE '2026-08-01', 'blocked', 'blocked')
RETURNING id`, "https://files.test/mrf/waiting-cli-exit4-"+strconv.FormatInt(sourceURLSeq.Add(1), 10)).Scan(&waitingID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.monthly_release_mrf_sources
    (payer_id, collection_month, mrf_source_id)
VALUES ('uhc', DATE '2026-08-01', $1)`, waitingID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfpipeline.pipeline_runtime (artifact_root, resident_capacity)
VALUES ($1, 1)`, ws.Root); err != nil {
		t.Fatal(err)
	}
	writeDownload(t, ws, sourceID, []byte("x"))
	startParseRuntime(t, pool, ws, nil, svc, 8)
	waitParse(t, pool, sourceID, jobs.StatusFailed)
	waitDownload(t, pool, sourceID, jobs.StatusBlocked)
	waitRiverJob(t, pool, parseJobID, "cancelled")
	var fail string
	if err := pool.QueryRow(context.Background(), `SELECT failure_code FROM mrfpipeline.mrf_sources WHERE id = $1`, sourceID).Scan(&fail); err != nil {
		t.Fatal(err)
	}
	if fail != jobs.FailureMRFParseInputInvalid {
		t.Fatalf("failure code %s", fail)
	}
	var attempt, maxAttempts int
	var riverState string
	if err := pool.QueryRow(context.Background(), `
SELECT attempt, max_attempts, state FROM mrfpipeline_river.river_job WHERE id = $1`, parseJobID).Scan(&attempt, &maxAttempts, &riverState); err != nil {
		t.Fatal(err)
	}
	if attempt != 1 || maxAttempts != 4 || riverState != "cancelled" {
		t.Fatalf("river attempt=%d max_attempts=%d state=%s", attempt, maxAttempts, riverState)
	}
	raw, err := ws.InspectDownloadState(artifact.KindMRF, sourceID)
	if err != nil || raw != artifact.DownloadAbsent {
		t.Fatalf("raw state %s %v", raw, err)
	}
	parsed, err := ws.InspectParsed(artifact.KindMRF, sourceID)
	if err != nil || parsed != artifact.ParsedAbsent {
		t.Fatalf("parsed state %s %v", parsed, err)
	}
	var held int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.mrf_materialization_slots`).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != 0 {
		t.Fatalf("held slots %d", held)
	}
	if err := admission.ScheduleWaiting(context.Background(), pool, client); err != nil {
		t.Fatal(err)
	}
	var admitted bool
	if err := pool.QueryRow(context.Background(), `
SELECT EXISTS (
    SELECT 1 FROM mrfpipeline.mrf_materialization_slots WHERE mrf_source_id = $1
)`, waitingID).Scan(&admitted); err != nil {
		t.Fatal(err)
	}
	if !admitted {
		t.Fatal("waiting source was not admitted")
	}
}
