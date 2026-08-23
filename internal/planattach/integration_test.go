package planattach

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfconsumer"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/planbatch"
)

func TestIntegrationClaimAndSuccessLockOrder(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	_, snapID := insertConsumed(t, pool)
	insertPlan(t, pool, snapID, "A", "hios", "1", nil)
	batchID := scheduleBatch(t, pool, client, snapID)
	var jobID int64
	if err := pool.QueryRow(context.Background(), `SELECT river_job_id FROM mrfpipeline.plan_attachment_batches WHERE id = $1`, batchID).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	res, ident, err := claimAttach(context.Background(), pool, batchID, jobID)
	if err != nil || res.Action != jobs.ClaimWork {
		t.Fatalf("%+v %v", res, err)
	}
	if ident.SnapshotID != snapID || ident.RequestedCount != 1 || ident.PayerID != "uhc" {
		t.Fatalf("%+v", ident)
	}
	var started time.Time
	if err := pool.QueryRow(context.Background(), `SELECT started_at FROM mrfpipeline.plan_attachment_batches WHERE id = $1`, batchID).Scan(&started); err != nil || started.IsZero() {
		t.Fatal(started)
	}
	res, _, err = claimAttach(context.Background(), pool, batchID, jobID)
	if err != nil || res.Action != jobs.ClaimWork {
		t.Fatalf("reclaim %+v %v", res, err)
	}
	var started2 time.Time
	if err := pool.QueryRow(context.Background(), `SELECT started_at FROM mrfpipeline.plan_attachment_batches WHERE id = $1`, batchID).Scan(&started2); err != nil || !started2.Equal(started) {
		t.Fatal("started_at reset")
	}
	stale, _, err := claimAttach(context.Background(), pool, batchID, jobID+9)
	if err != nil || stale.Action != jobs.ClaimNoop {
		t.Fatalf("stale %+v %v", stale, err)
	}
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := confirmAttachSuccess(context.Background(), tx, client, ident, 1); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	done, _, err := claimAttach(context.Background(), pool, batchID, jobID)
	if err != nil || done.Action != jobs.ClaimNoop {
		t.Fatalf("succeeded %+v %v", done, err)
	}
}

func TestIntegrationPlansDuringRunningThenNextBatch(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	_, snapID := insertConsumed(t, pool)
	insertPlan(t, pool, snapID, "A", "hios", "1", nil)
	batchID := scheduleBatch(t, pool, client, snapID)
	if _, err := pool.Exec(context.Background(), `UPDATE mrfpipeline.plan_attachment_batches SET status = 'running', started_at = transaction_timestamp() WHERE id = $1`, batchID); err != nil {
		t.Fatal(err)
	}
	insertPlan(t, pool, snapID, "B", "hios", "2", nil)
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := planbatch.Schedule(context.Background(), tx, client, snapID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	var n, unassigned int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id = $1`, snapID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("batches %d", n)
	}
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline.mrf_plans p
WHERE p.mrf_snapshot_id = $1 AND NOT EXISTS (
    SELECT 1 FROM mrfpipeline.plan_attachment_batch_items i WHERE i.mrf_plan_id = p.id
)`, snapID).Scan(&unassigned); err != nil || unassigned != 1 {
		t.Fatalf("unassigned %d", unassigned)
	}
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.plan_attachment_batches
SET status = 'succeeded', added_plan_count = 1, completed_at = transaction_timestamp()
WHERE id = $1`, batchID); err != nil {
		t.Fatal(err)
	}
	tx, err = pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := planbatch.Schedule(context.Background(), tx, client, snapID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id = $1`, snapID).Scan(&n); err != nil || n != 2 {
		t.Fatalf("next %d", n)
	}
}

func TestIntegrationFailedBatchBlocksBypass(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	_, snapID := insertConsumed(t, pool)
	insertPlan(t, pool, snapID, "A", "hios", "1", nil)
	batchID := scheduleBatch(t, pool, client, snapID)
	if _, err := pool.Exec(context.Background(), `
UPDATE mrfpipeline.plan_attachment_batches
SET status = 'failed', failure_code = 'plan_attach_output_failed', completed_at = transaction_timestamp()
WHERE id = $1`, batchID); err != nil {
		t.Fatal(err)
	}
	insertPlan(t, pool, snapID, "B", "hios", "2", nil)
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := planbatch.Schedule(context.Background(), tx, client, snapID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	var n, items int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id = $1`, snapID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("bypass %d", n)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.plan_attachment_batch_items WHERE plan_attachment_batch_id = $1`, batchID).Scan(&items); err != nil || items != 1 {
		t.Fatalf("items %d", items)
	}
}

func TestIntegrationABThenBC(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	sourceID, snapID := insertConsumed(t, pool)
	sponsor := "Sponsor"
	insertPlan(t, pool, snapID, "A", "ein", "1", &sponsor)
	insertPlan(t, pool, snapID, "B", "hios", "2", nil)
	ingestWarehouse(t, sourceID, snapID, ws, cat, warehouse, svc)
	before := snapshotBytes(t, warehouse, snapID)
	batch1 := scheduleBatch(t, pool, client, snapID)
	w := &Worker{Pool: pool, Workspace: ws, WarehousePath: warehouse, Logger: jobs.NewLogger(nil)}
	startAttachRuntime(t, pool, w, 8)
	waitBatch(t, pool, batch1, jobs.StatusSucceeded)
	insertPlan(t, pool, snapID, "C", "hios", "3", nil)
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := planbatch.Schedule(context.Background(), tx, client, snapID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	var batch2 int64
	if err := pool.QueryRow(context.Background(), `
SELECT id FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id = $1 AND id <> $2`, snapID, batch1).Scan(&batch2); err != nil {
		t.Fatal(err)
	}
	waitBatch(t, pool, batch2, jobs.StatusSucceeded)
	after := snapshotBytes(t, warehouse, snapID)
	if before != after {
		t.Fatal("rate snapshot changed")
	}
	part1 := associationPart(warehouse, snapID, batch1)
	part2 := associationPart(warehouse, snapID, batch2)
	if _, err := os.Lstat(part1); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(part2); err != nil {
		t.Fatal(err)
	}
	var added2 int64
	if err := pool.QueryRow(context.Background(), `SELECT added_plan_count FROM mrfpipeline.plan_attachment_batches WHERE id = $1`, batch2).Scan(&added2); err != nil || added2 != 1 {
		t.Fatalf("added2 %d", added2)
	}
}

func TestIntegrationRepeatIdenticalZeroAdd(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	sourceID, snapID := insertConsumed(t, pool)
	insertPlan(t, pool, snapID, "A", "hios", "1", nil)
	ingestWarehouse(t, sourceID, snapID, ws, cat, warehouse, svc)
	batch1 := scheduleBatch(t, pool, client, snapID)
	startAttachRuntime(t, pool, &Worker{Pool: pool, Workspace: ws, WarehousePath: warehouse, Logger: jobs.NewLogger(nil)}, 8)
	waitBatch(t, pool, batch1, jobs.StatusSucceeded)
	var jobID int64
	if err := pool.QueryRow(context.Background(), `SELECT river_job_id FROM mrfpipeline.plan_attachment_batches WHERE id = $1`, batch1).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	res, ident, err := claimAttach(context.Background(), pool, batch1, jobID)
	if err != nil || res.Action != jobs.ClaimNoop {
		t.Fatalf("redelivery %+v %v", res, err)
	}
	_ = ident
	report, err := mrfconsumer.AttachPlans(context.Background(), mrfconsumer.AttachPlansConfig{
		PlansPath: mustPlansPath(t, ws, batch1), OutputPath: warehouse,
		OutputID: formatSnapshotID(snapID), PlanBatchID: formatBatchID(batch1),
	})
	if err != nil || report.AddedPlanCount != 0 {
		t.Fatalf("zero add %+v %v", report, err)
	}
}

func TestIntegrationKillBeforeRenameThenRetry(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	sourceID, snapID := insertConsumed(t, pool)
	insertPlan(t, pool, snapID, "A", "hios", "1", nil)
	ingestWarehouse(t, sourceID, snapID, ws, cat, warehouse, svc)
	batchID := scheduleBatch(t, pool, client, snapID)
	var calls atomic.Int64
	w := &Worker{Pool: pool, Workspace: ws, WarehousePath: warehouse, Logger: jobs.NewLogger(nil),
		Attach: func(ctx context.Context, cfg mrfconsumer.AttachPlansConfig) (mrfconsumer.AttachPlansReport, error) {
			if calls.Add(1) == 1 {
				return mrfconsumer.AttachPlansReport{}, context.Canceled
			}
			return mrfconsumer.AttachPlans(ctx, cfg)
		}}
	startAttachRuntime(t, pool, w, 8)
	waitBatch(t, pool, batchID, jobs.StatusSucceeded)
	part := associationPart(warehouse, snapID, batchID)
	if _, err := os.Lstat(part); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationKillAfterPublishBeforeCommit(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	sourceID, snapID := insertConsumed(t, pool)
	insertPlan(t, pool, snapID, "A", "hios", "1", nil)
	ingestWarehouse(t, sourceID, snapID, ws, cat, warehouse, svc)
	batchID := scheduleBatch(t, pool, client, snapID)
	var calls atomic.Int64
	w := &Worker{Pool: pool, Workspace: ws, WarehousePath: warehouse, Logger: jobs.NewLogger(nil),
		Attach: func(ctx context.Context, cfg mrfconsumer.AttachPlansConfig) (mrfconsumer.AttachPlansReport, error) {
			report, err := mrfconsumer.AttachPlans(ctx, cfg)
			if err != nil {
				return report, err
			}
			if calls.Add(1) == 1 {
				return report, errors.New("lost commit")
			}
			return report, nil
		}}
	startAttachRuntime(t, pool, w, 8)
	waitBatch(t, pool, batchID, jobs.StatusSucceeded)
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id = $1`, snapID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("batches %d", n)
	}
	var added int64
	if err := pool.QueryRow(context.Background(), `SELECT added_plan_count FROM mrfpipeline.plan_attachment_batches WHERE id = $1`, batchID).Scan(&added); err != nil || added < 0 {
		t.Fatal(added)
	}
}

func TestIntegrationReusedBatchIDNewPlansFailsClosed(t *testing.T) {
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	sourceID, snapID := insertConsumed(t, pool)
	insertPlan(t, pool, snapID, "A", "hios", "1", nil)
	ingestWarehouse(t, sourceID, snapID, ws, cat, warehouse, svc)
	batchID := scheduleBatch(t, pool, client, snapID)
	startAttachRuntime(t, pool, &Worker{Pool: pool, Workspace: ws, WarehousePath: warehouse, Logger: jobs.NewLogger(nil)}, 8)
	waitBatch(t, pool, batchID, jobs.StatusSucceeded)
	extra, err := encodePlans([]planRow{
		{PlanName: "A", IssuerName: "issuer", PlanIDType: "hios", PlanID: "1", PlanMarketType: "group"},
		{PlanName: "Z", IssuerName: "issuer", PlanIDType: "hios", PlanID: "9", PlanMarketType: "group"},
	})
	if err != nil {
		t.Fatal(err)
	}
	path := mustPlansPath(t, ws, batchID)
	if err := os.WriteFile(path, extra, 0600); err != nil {
		t.Fatal(err)
	}
	_, err = mrfconsumer.AttachPlans(context.Background(), mrfconsumer.AttachPlansConfig{
		PlansPath: path, OutputPath: warehouse,
		OutputID: formatSnapshotID(snapID), PlanBatchID: formatBatchID(batchID),
	})
	if !errors.Is(err, mrfconsumer.ErrOutput) {
		t.Fatalf("got %v", err)
	}
	if !jobs.IsFailure(mapWorkError(err), jobs.FailurePlanAttachOutputFailed) {
		t.Fatal(mapWorkError(err))
	}
}

func TestIntegrationHIOSEmptySponsorRejected(t *testing.T) {
	ws := mustWorkspace(t)
	canonical, err := encodePlans([]planRow{{PlanName: "A", IssuerName: "issuer", PlanIDType: "hios", PlanID: "1", PlanMarketType: "group"}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(canonical), `"plan_sponsor_name":""`) {
		t.Fatal(string(canonical))
	}
	injected := []byte(`[{"plan_name":"A","issuer_name":"issuer","plan_sponsor_name":"","plan_id_type":"hios","plan_id":"1","plan_market_type":"group"}]` + "\n")
	path, err := publishPlansJSON(ws, 88, canonical)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, injected, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := publishPlansJSON(ws, 88, canonical); !jobs.IsFailure(err, jobs.FailurePlanAttachInputInvalid) {
		t.Fatalf("accepted injected empty sponsor %v", err)
	}
	got, _ := os.ReadFile(path)
	if string(got) != string(injected) {
		t.Fatal("rewrote injected")
	}
}

func TestIntegrationIngestAndAttachSerialized(t *testing.T) {
	var inflight, overlap atomic.Int64
	gate := func() {
		if inflight.Add(1) > 1 {
			overlap.Add(1)
		}
		time.Sleep(20 * time.Millisecond)
		inflight.Add(-1)
	}
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	sourceID, snapID := insertConsumed(t, pool)
	insertPlan(t, pool, snapID, "A", "hios", "1", nil)
	ingestWarehouse(t, sourceID, snapID, ws, cat, warehouse, svc)
	batchID := scheduleBatch(t, pool, client, snapID)
	w := &Worker{Pool: pool, Workspace: ws, WarehousePath: warehouse, Logger: jobs.NewLogger(nil),
		Attach: func(ctx context.Context, cfg mrfconsumer.AttachPlansConfig) (mrfconsumer.AttachPlansReport, error) {
			gate()
			return mrfconsumer.AttachPlans(ctx, cfg)
		}}
	startAttachRuntime(t, pool, w, 8)
	waitBatch(t, pool, batchID, jobs.StatusSucceeded)
	if overlap.Load() != 0 {
		t.Fatal("overlap")
	}
}

func TestIntegrationDuckDBSeesNewAssociations(t *testing.T) {
	bin := os.Getenv("MRFCONSUMER_DUCKDB_BIN")
	if bin == "" {
		t.Skip("MRFCONSUMER_DUCKDB_BIN is not set")
	}
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/enotpoloskun/mrfconsumer").Output()
	if err != nil {
		t.Fatal(err)
	}
	tmpl := filepath.Join(strings.TrimSpace(string(out)), "sql", "duckdb", "views.sql.tmpl")
	if _, err := os.Stat(tmpl); err != nil {
		t.Fatal(err)
	}
	pool := testDB(t)
	client := insertClient(t, pool)
	ws := mustWorkspace(t)
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	sourceID, snapID := insertConsumed(t, pool)
	insertPlan(t, pool, snapID, "A", "hios", "1", nil)
	insertPlan(t, pool, snapID, "B", "hios", "2", nil)
	ingestWarehouse(t, sourceID, snapID, ws, cat, warehouse, svc)
	batch1 := scheduleBatch(t, pool, client, snapID)
	startAttachRuntime(t, pool, &Worker{Pool: pool, Workspace: ws, WarehousePath: warehouse, Logger: jobs.NewLogger(nil)}, 8)
	waitBatch(t, pool, batch1, jobs.StatusSucceeded)
	insertPlan(t, pool, snapID, "C", "hios", "3", nil)
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := planbatch.Schedule(context.Background(), tx, client, snapID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	var batch2 int64
	if err := pool.QueryRow(context.Background(), `SELECT id FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id = $1 AND id <> $2`, snapID, batch1).Scan(&batch2); err != nil {
		t.Fatal(err)
	}
	waitBatch(t, pool, batch2, jobs.StatusSucceeded)
	if _, err := os.Lstat(associationPart(warehouse, snapID, batch1)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(associationPart(warehouse, snapID, batch2)); err != nil {
		t.Fatal(err)
	}
}

func snapshotBytes(t *testing.T, warehouse string, snapID int64) string {
	t.Helper()
	final := filepath.Join(warehouse, "snapshots", "collection_month=2026-08", "payer_id=uhc", "output_id="+formatSnapshotID(snapID))
	var b strings.Builder
	_ = filepath.Walk(final, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(final, path)
		data, _ := os.ReadFile(path)
		b.WriteString(rel)
		b.WriteByte(':')
		b.Write(data)
		b.WriteByte('\n')
		return nil
	})
	return b.String()
}

func associationPart(warehouse string, snapID, batchID int64) string {
	return filepath.Join(warehouse, "plan_associations", "output_id="+formatSnapshotID(snapID), formatBatchID(batchID)+"-part-00000.parquet")
}

func mustPlansPath(t *testing.T, ws *artifact.Workspace, batchID int64) string {
	t.Helper()
	path, err := plansPath(ws, batchID)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func formatSnapshotID(id int64) string {
	return "mrf-" + strconv.FormatInt(id, 10)
}
