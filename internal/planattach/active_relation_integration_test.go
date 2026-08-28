package planattach

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/enotpoloskun/mrfconsumer"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestIntegrationConsumerUsesPipelineActiveRelation(t *testing.T) {
	duckdb := os.Getenv("MRFCONSUMER_DUCKDB_BIN")
	if duckdb == "" {
		t.Skip("MRFCONSUMER_DUCKDB_BIN is not set")
	}
	if _, err := os.Stat(duckdb); err != nil {
		t.Fatal(err)
	}

	pool := testDB(t)
	ctx := context.Background()
	sourceID, snapshotID := insertConsumed(t, pool)
	if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline.monthly_releases
SET status = 'active', sealed_at = transaction_timestamp(), last_activated_at = transaction_timestamp()
WHERE payer_id = 'uhc' AND collection_month = DATE '2026-08-01'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_release_outputs
    (payer_id, collection_month, mrf_snapshot_id, published_generation)
VALUES ('uhc', DATE '2026-08-01', $1, 1)`, snapshotID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases
    (payer_id, collection_month, status, sealed_at, last_activated_at)
VALUES ('uhc', DATE '2026-07-01', 'inactive', transaction_timestamp(), transaction_timestamp())`); err != nil {
		t.Fatal(err)
	}
	inactiveSourceID, err := insertConsumerSource(ctx, pool, "2026-07")
	if err != nil {
		t.Fatal(err)
	}
	var inactiveSnapshotID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots
    (mrf_source_id, payer_id, collection_month, consume_status)
VALUES ($1, 'uhc', DATE '2026-07-01', 'succeeded')
RETURNING id`, inactiveSourceID).Scan(&inactiveSnapshotID); err != nil {
		t.Fatal(err)
	}
	inactiveID := "mrf-" + strconv.FormatInt(inactiveSnapshotID, 10)
	targets, err := release.ListActiveOutputs(ctx, pool)
	if err != nil || len(targets) != 1 || targets[0].SnapshotID != snapshotID {
		t.Fatalf("active relation: %#v %v", targets, err)
	}
	if targets[0].OutputID == inactiveID {
		t.Fatalf("inactive output selected: %#v", targets)
	}

	ws := mustWorkspace(t)
	services := mustServices(t)
	catalog := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "warehouse")
	writeRealParsed(t, ws, sourceID, services)
	parsed, err := ws.ParsedDir(artifact.KindMRF, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	activeID := targets[0].OutputID
	outputs := []struct {
		payer string
		month string
		id    string
	}{
		{payer: "uhc", month: "2026-08", id: activeID},
		{payer: "uhc", month: "2026-07", id: inactiveID},
		{payer: "uhc", month: "2026-08", id: "unrelated-output"},
	}
	plans := filepath.Join(t.TempDir(), "plans.json")
	canonical, err := encodePlans([]planRow{{PlanName: "A", IssuerName: "issuer", PlanIDType: "hios", PlanID: "1", PlanMarketType: "group"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plans, canonical, 0600); err != nil {
		t.Fatal(err)
	}
	for _, output := range outputs {
		if _, err := mrfconsumer.Ingest(ctx, mrfconsumer.Config{
			InputPath: parsed, ProviderCatalogPath: catalog, OutputPath: warehouse,
			PayerID: output.payer, CollectionMonth: output.month, OutputID: output.id,
		}); err != nil {
			t.Fatalf("ingest %s: %v", output.id, err)
		}
		if _, err := mrfconsumer.AttachPlans(ctx, mrfconsumer.AttachPlansConfig{
			PlansPath: plans, OutputPath: warehouse, OutputID: output.id,
			PlanBatchID: output.id + "-plans",
		}); err != nil {
			t.Fatalf("attach %s: %v", output.id, err)
		}
	}

	moduleDir, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/enotpoloskun/mrfconsumer").Output()
	if err != nil {
		t.Fatal(err)
	}
	template, err := os.ReadFile(filepath.Join(strings.TrimSpace(string(moduleDir)), "sql", "duckdb", "views.sql.tmpl"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs(warehouse)
	if err != nil {
		t.Fatal(err)
	}
	literal := strings.ReplaceAll(strings.ReplaceAll(root, "'", "''"), "\\", "/")
	views := strings.ReplaceAll(string(template), "__WAREHOUSE_ROOT__", literal)
	scope := "WITH active_outputs(payer_id, collection_month, output_id) AS (VALUES (" +
		duckString(targets[0].PayerID) + ", " + duckString(targets[0].CollectionMonth) + ", " + duckString(activeID) + "))\n"
	queries := []string{
		views + "\n" + scope + `SELECT ingestion.output_id
FROM active_outputs active
JOIN all_ingestions ingestion
  ON ingestion.output_id = active.output_id
 AND ingestion.payer_id = active.payer_id
 AND ingestion.collection_month = active.collection_month
ORDER BY ingestion.output_id;`,
		views + "\n" + scope + `SELECT plans.output_id
FROM active_outputs active
JOIN all_output_plans plans ON plans.output_id = active.output_id
ORDER BY plans.output_id;`,
	}
	results := runActiveRelationQueries(t, duckdb, queries)
	if len(results[0]) != 1 || string(results[0][0]["output_id"]) != strconv.Quote(activeID) {
		t.Fatalf("active ingestion selection: %#v", results[0])
	}
	if len(results[1]) != 1 || string(results[1][0]["output_id"]) != strconv.Quote(activeID) {
		t.Fatalf("active plan selection: %#v", results[1])
	}
}

func insertConsumerSource(ctx context.Context, pool *pgxpool.Pool, month string) (int64, error) {
	var sourceID int64
	err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources
    (source_url, collection_month, download_status, parse_status)
VALUES ($1, $2, 'succeeded', 'succeeded')
RETURNING id`, "https://files.test/active-relation/"+month, month+"-01").Scan(&sourceID)
	return sourceID, err
}

func duckString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func runActiveRelationQueries(t *testing.T, binary string, queries []string) [][]map[string]json.RawMessage {
	t.Helper()
	var input strings.Builder
	input.WriteString(".mode json\n")
	for _, query := range queries {
		input.WriteString(query)
		input.WriteByte('\n')
	}
	cmd := exec.Command(binary)
	cmd.Stdin = strings.NewReader(input.String())
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("DuckDB failed: %v: %s", err, stderr.String())
	}
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	results := make([][]map[string]json.RawMessage, 0, len(queries))
	for range queries {
		var rows []map[string]json.RawMessage
		if err := decoder.Decode(&rows); err != nil {
			t.Fatalf("decode DuckDB result: %v; stdout=%s", err, stdout.String())
		}
		results = append(results, rows)
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err == nil || !errors.Is(err, io.EOF) {
		t.Fatalf("unexpected DuckDB output: %s", stdout.String())
	}
	return results
}
