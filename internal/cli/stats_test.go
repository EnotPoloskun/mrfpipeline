package cli

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/config"
	"github.com/enotpoloskun/mrfpipeline/internal/stats"
)

func TestParseStatsCommand(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		args      []string
		wantPayer string
		wantMonth string
		wantJSON  bool
	}{
		{"all", []string{"stats"}, "", "", false},
		{"payer", []string{"stats", "--payer", "uhc"}, "uhc", "", false},
		{"month", []string{"stats", "--collection-month", "2026-08"}, "", "2026-08", false},
		{"both", []string{"stats", "--payer", "uhc", "--collection-month", "2026-08"}, "uhc", "2026-08", false},
		{"json first", []string{"stats", "--json", "--payer", "uhc"}, "uhc", "", true},
		{"json both", []string{"stats", "--json", "--payer", "uhc", "--collection-month", "2026-08"}, "uhc", "2026-08", true},
		{"equals payer", []string{"stats", "--payer=uhc", "--collection-month=2026-08"}, "uhc", "2026-08", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parse(tc.args)
			if err != nil {
				t.Fatal(err)
			}
			if got.command != cmdStats || got.payer != tc.wantPayer || got.month != tc.wantMonth || got.statsJSON != tc.wantJSON {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func TestParseStatsCommandRejectsInvalidForms(t *testing.T) {
	t.Parallel()
	cases := [][]string{
		{"stats", "--payer"},
		{"stats", "--collection-month"},
		{"stats", "extra"},
		{"stats", "--bogus"},
		{"stats", "--json", "true"},
		{"stats", "--json=true"},
		{"stats", "--json", "--json"},
		{"stats", "--payer", "uhc", "--payer", "aetna"},
		{"stats", "--collection-month", "2026-08", "--collection-month", "2026-09"},
		{"stats", "--help", "--payer", "uhc"},
		{"stats", "-payer", "uhc"},
	}
	for _, args := range cases {
		if _, err := parse(args); err == nil || !isUsage(err) {
			t.Fatalf("%v: got %v", args, err)
		}
	}
}

func TestStatsHelpIsInformational(t *testing.T) {
	t.Parallel()
	code, stdout, stderr := runCLI(context.Background(), []string{"stats", "--help"}, fatalEnv(t))
	if code != 0 || stderr != "" || !strings.Contains(stdout, "read-only count table") {
		t.Fatalf("exit %d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if !strings.Contains(rootHelp, "stats") {
		t.Fatal("root help missing stats")
	}
}

func TestIntegrationStatsCommand(t *testing.T) {
	env, pool := testDiscoverEnv(t)
	ctx := context.Background()
	aug := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	sep := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	for _, row := range []struct {
		payer string
		month time.Time
	}{
		{"uhc", aug},
		{"aetna", aug},
		{"uhc", sep},
	} {
		if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month) VALUES ($1, $2)`, row.payer, row.month); err != nil {
			t.Fatal(err)
		}
	}

	var runID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.discovery_runs (payer_id, collection_month, status, completed_at)
VALUES ('uhc', $1, 'succeeded', transaction_timestamp()) RETURNING id`, aug).Scan(&runID); err != nil {
		t.Fatal(err)
	}

	tocStatuses := []struct {
		download, parse, import_ string
	}{
		{"succeeded", "pending", "blocked"},
		{"running", "blocked", "blocked"},
		{"failed", "blocked", "blocked"},
	}
	for i, st := range tocStatuses {
		if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.toc_files (
    payer_id, collection_month, source_url, first_discovery_run_id,
    download_status, parse_status, import_status
) VALUES ('uhc', $1, $2, $3, $4, $5, $6)`,
			aug, "https://example.invalid/toc-"+strconv.Itoa(i), runID, st.download, st.parse, st.import_); err != nil {
			t.Fatal(err)
		}
	}

	var sharedSource, selectedSource, snapUHC, snapAetna int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ('https://example.invalid/shared-mrf', $1, 'succeeded', 'pending') RETURNING id`, aug).Scan(&sharedSource); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month, download_status, parse_status)
VALUES ('https://example.invalid/unselected-mrf', $1, 'pending', 'blocked') RETURNING id`, aug).Scan(&selectedSource); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_release_mrf_sources (payer_id, collection_month, mrf_source_id)
VALUES ('uhc', $1, $2)`, aug, sharedSource); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, 'uhc', $2, 'succeeded') RETURNING id`, sharedSource, aug).Scan(&snapUHC); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, 'aetna', $2, 'pending') RETURNING id`, sharedSource, aug).Scan(&snapAetna); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, 'uhc', $2, 'running')`, selectedSource, aug); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, 'uhc', $2, 'blocked')`, selectedSource, sep); err != nil {
		t.Fatal(err)
	}

	var batchID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batches (mrf_snapshot_id, status, requested_plan_count)
VALUES ($1, 'pending', 2) RETURNING id`, snapUHC).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	_ = batchID

	byStage := func(rows []stats.Row) map[string]stats.Row {
		out := make(map[string]stats.Row, len(rows))
		for _, row := range rows {
			if row.PayerID == "uhc" && row.CollectionMonth == "2026-08" {
				out[row.Stage] = row
			}
		}
		return out
	}

	code, stdout, stderr := runCLI(ctx, []string{"stats", "--payer", "uhc", "--collection-month", "2026-08"}, env)
	if code != 0 || stderr != "" {
		t.Fatalf("stats exit %d stderr=%q", code, stderr)
	}
	if !strings.HasPrefix(stdout, "stage ") || !strings.Contains(stdout, "toc.download") {
		t.Fatalf("stdout %q", stdout)
	}

	allRows, err := stats.Collect(ctx, pool, stats.Options{Payer: "uhc", Month: aug})
	if err != nil {
		t.Fatal(err)
	}
	got := byStage(allRows)

	if got["toc.download"].Total != 3 || got["toc.parse"].Total != 3 || got["toc.import"].Total != 3 {
		t.Fatalf("toc totals %+v", got)
	}
	if got["mrf.download"].Total != 2 || got["mrf.parse"].Total != 2 {
		t.Fatalf("mrf totals %+v", got)
	}
	if got["consumer.ingest"].Total != 2 {
		t.Fatalf("ingest total %+v", got)
	}
	if got["consumer.attach_plans"].Blocked != 0 || got["consumer.attach_plans"].Total != 1 {
		t.Fatalf("attach plans %+v", got["consumer.attach_plans"])
	}

	code, stdout, stderr = runCLI(ctx, []string{"stats", "--payer", "uhc", "--collection-month", "2026-08", "--json"}, env)
	if code != 0 || stderr != "" {
		t.Fatalf("json exit %d stderr=%q", code, stderr)
	}
	var decoded []stats.Row
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) == 0 {
		t.Fatal("empty json")
	}

	code, stdout, stderr = runCLI(ctx, []string{"stats", "--payer", "aetna", "--collection-month", "2026-08"}, env)
	if code != 0 || stderr != "" || strings.Contains(stdout, "uhc") {
		t.Fatalf("payer filter exit %d stdout=%q stderr=%q", code, stdout, stderr)
	}
	aetnaRows, err := stats.Collect(ctx, pool, stats.Options{Payer: "aetna", Month: aug})
	if err != nil {
		t.Fatal(err)
	}
	aetnaByStage := make(map[string]stats.Row, len(aetnaRows))
	for _, row := range aetnaRows {
		if row.PayerID == "aetna" && row.CollectionMonth == "2026-08" {
			aetnaByStage[row.Stage] = row
		}
	}
	if aetnaByStage["mrf.download"].Total != 1 || aetnaByStage["mrf.parse"].Total != 1 {
		t.Fatalf("aetna shared source mrf totals %+v", aetnaByStage)
	}

	code, stdout, stderr = runCLI(ctx, []string{"stats", "--payer", "uhc", "--collection-month", "2026-09"}, env)
	if code != 0 || stderr != "" || strings.Contains(stdout, "2026-08") {
		t.Fatalf("month filter exit %d stdout=%q stderr=%q", code, stdout, stderr)
	}

	code, stdout, stderr = runCLI(ctx, []string{"stats", "--payer", "missing", "--collection-month", "2026-08"}, env)
	if code != 0 || stderr != "" {
		t.Fatalf("unknown payer exit %d stderr=%q", code, stderr)
	}
	if stdout != "stage payer month blocked pending running succeeded failed total\n" {
		t.Fatalf("unknown payer stdout %q", stdout)
	}

	code, stdout, stderr = runCLI(ctx, []string{"stats", "--payer", "uhc", "--collection-month", "2020-01"}, env)
	if code != 0 || stderr != "" {
		t.Fatalf("unknown month exit %d stderr=%q", code, stderr)
	}
	if stdout != "stage payer month blocked pending running succeeded failed total\n" {
		t.Fatalf("unknown month stdout %q", stdout)
	}

	code, stdout, stderr = runCLI(ctx, []string{"stats", "--payer", "missing", "--collection-month", "2026-08", "--json"}, env)
	if code != 0 || stderr != "" {
		t.Fatalf("unknown payer json exit %d stderr=%q", code, stderr)
	}
	if stdout != "[]\n" {
		t.Fatalf("unknown payer json stdout %q", stdout)
	}

	if err := config.ValidatePayerIdentifier("aetna"); err != nil {
		t.Fatal(err)
	}
}
