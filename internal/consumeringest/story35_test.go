package consumeringest

import (
	"context"
	"errors"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfconsumer"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

func attachTransformOverlapProbe(sharedInflight *atomic.Int64, overlap *atomic.Int64) func(mrfconsumer.IngestProgress) {
	var entered atomic.Bool
	return func(p mrfconsumer.IngestProgress) {
		if entered.Load() {
			return
		}
		switch p.Phase {
		case "provider_relationships", "rate_facts", "publishing":
			entered.Store(true)
			if sharedInflight.Add(1) > 1 {
				overlap.Add(1)
			}
			time.Sleep(50 * time.Millisecond)
			sharedInflight.Add(-1)
		}
	}
}

func startDualConsumerClients(t *testing.T, pool *pgxpool.Pool, w *Worker) {
	t.Helper()
	for i := 0; i < 2; i++ {
		workers := river.NewWorkers()
		river.AddWorker(workers, &Worker{
			Pool: w.Pool, Workspace: w.Workspace, WarehousePath: w.WarehousePath,
			ServicesPath: w.ServicesPath, Catalog: w.Catalog, Logger: w.Logger,
			Ingest: w.Ingest, Health: w.Health, Progress: w.Progress,
		})
		cfg := jobs.ClientConfig(workers, map[string]river.QueueConfig{jobs.QueueConsumer: {MaxWorkers: 1}}, nil, jobs.NewLogger(io.Discard))
		cfg.SkipUnknownJobCheck = true
		cfg.MaxAttempts = 8
		cfg.RetryPolicy = immediateRetry{}
		cfg.FetchCooldown = 50 * time.Millisecond
		cfg.FetchPollInterval = 50 * time.Millisecond
		client, err := river.NewClient(riverpgxv5.New(pool), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = jobs.Shutdown(context.Background(), client) })
	}
}

func TestIntegrationDistinctSnapshotsOverlapRealIngest(t *testing.T) {
	pool := testDB(t)
	ws := mustWorkspace(t)
	t.Setenv("TMPDIR", ws.StagingDir())
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	client := insertClient(t, pool)
	s1, snap1, _ := insertIngestJobForURL(t, pool, client, "2026-08-01", "https://files.test/mrf/a")
	s2, snap2, _ := insertIngestJobForURL(t, pool, client, "2026-09-01", "https://files.test/mrf/b")
	writeRealParsed(t, ws, s1, svc)
	writeRealParsed(t, ws, s2, svc)
	var sharedInflight atomic.Int64
	var overlap atomic.Int64
	ingest := func(ctx context.Context, cfg mrfconsumer.Config) (mrfconsumer.Report, error) {
		cfg.OnProgress = attachTransformOverlapProbe(&sharedInflight, &overlap)
		return mrfconsumer.Ingest(ctx, cfg)
	}
	startDualConsumerClients(t, pool, &Worker{
		Pool: pool, Workspace: ws, WarehousePath: warehouse, ServicesPath: svc, Catalog: cat,
		Logger: jobs.NewLogger(io.Discard), Ingest: ingest, Health: func(context.Context) error { return nil },
	})
	waitConsume(t, pool, snap1, jobs.StatusSucceeded)
	waitConsume(t, pool, snap2, jobs.StatusSucceeded)
	if overlap.Load() == 0 {
		t.Fatal("expected overlapping ingest across consumer clients")
	}
}

func TestWorkerNilHealthSucceeds(t *testing.T) {
	w := &Worker{Health: func(context.Context) error { return nil }}
	if err := w.Health(context.Background()); err != nil {
		t.Fatalf("worker health: %v", err)
	}
}

func writeContractPlansJSON(t *testing.T) string {
	t.Helper()
	data, err := json.Marshal([]map[string]string{
		{"plan_name": "Z", "issuer_name": "Issuer", "plan_id_type": "hios", "plan_id": "9", "plan_market_type": "group"},
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "plans.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func contractCatalogPath(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "catalog")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	writeCatalogFixture(t, root)
	return root
}

func contractIngest(t *testing.T, ws *artifact.Workspace, catalog, warehouse, services, month, outputID string, sourceID int64) {
	t.Helper()
	writeRealParsed(t, ws, sourceID, services)
	parsed, err := ws.ParsedDir(artifact.KindMRF, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mrfconsumer.Ingest(context.Background(), mrfconsumer.Config{
		InputPath: parsed, ProviderCatalogPath: catalog, OutputPath: warehouse,
		PayerID: "uhc", CollectionMonth: month, OutputID: outputID,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestContractIngestOverlapsAttachConsumedOutput(t *testing.T) {
	ws := mustWorkspace(t)
	t.Setenv("TMPDIR", ws.StagingDir())
	services := mustServices(t)
	catalog := contractCatalogPath(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	contractIngest(t, ws, catalog, warehouse, services, "2026-08", "mrf-consumed", 1)
	var attachErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		contractIngest(t, ws, catalog, warehouse, services, "2026-09", "mrf-new", 2)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, attachErr = mrfconsumer.AttachPlans(context.Background(), mrfconsumer.AttachPlansConfig{
			PlansPath: writeContractPlansJSON(t), OutputPath: warehouse, OutputID: "mrf-consumed", PlanBatchID: "job-overlap",
		})
	}()
	wg.Wait()
	if attachErr != nil {
		t.Fatalf("attach failed: %v", attachErr)
	}
	if _, err := os.Lstat(filepath.Join(warehouse, "snapshots", "collection_month=2026-09", "payer_id=uhc", "output_id=mrf-new")); err != nil {
		t.Fatalf("new ingest missing: %v", err)
	}
}

func TestContractSameSnapshotIngestAttachSerializes(t *testing.T) {
	ws := mustWorkspace(t)
	t.Setenv("TMPDIR", ws.StagingDir())
	services := mustServices(t)
	catalog := contractCatalogPath(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	writeRealParsed(t, ws, 1, services)
	parsed, err := ws.ParsedDir(artifact.KindMRF, 1)
	if err != nil {
		t.Fatal(err)
	}
	cfg := mrfconsumer.Config{
		InputPath: parsed, ProviderCatalogPath: catalog, OutputPath: warehouse,
		PayerID: "uhc", CollectionMonth: "2026-08", OutputID: "mrf-same",
	}
	var ingestErr, attachErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, ingestErr = mrfconsumer.Ingest(context.Background(), cfg)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, attachErr = mrfconsumer.AttachPlans(context.Background(), mrfconsumer.AttachPlansConfig{
			PlansPath: writeContractPlansJSON(t), OutputPath: warehouse, OutputID: "mrf-same", PlanBatchID: "job-same",
		})
	}()
	wg.Wait()
	if ingestErr != nil {
		t.Fatalf("ingest failed: %v", ingestErr)
	}
	final := filepath.Join(warehouse, "snapshots", "collection_month=2026-08", "payer_id=uhc", "output_id=mrf-same")
	if _, err := os.Lstat(final); err != nil {
		t.Fatalf("snapshot missing: %v", err)
	}
	if attachErr != nil && !errors.Is(attachErr, mrfconsumer.ErrOutput) {
		t.Fatalf("attach failed: %v", attachErr)
	}
}

func TestConcurrentConsumerIngestRace(t *testing.T) {
	if testing.Short() {
		t.Skip("race harness")
	}
	t.Parallel()
	TestIntegrationDistinctSnapshotsOverlapRealIngest(t)
}
