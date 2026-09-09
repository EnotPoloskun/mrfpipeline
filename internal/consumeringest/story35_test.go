package consumeringest

import (
	"context"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/enotpoloskun/mrfconsumer"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/reconcile"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

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
	var inflight atomic.Int64
	var overlap atomic.Int64
	health := reconcile.Health(nil, nil, nil)
	ingest := func(ctx context.Context, cfg mrfconsumer.Config) (mrfconsumer.Report, error) {
		if inflight.Add(1) > 1 {
			overlap.Add(1)
		}
		defer inflight.Add(-1)
		return mrfconsumer.Ingest(ctx, cfg)
	}
	startDualConsumerClients(t, pool, &Worker{
		Pool: pool, Workspace: ws, WarehousePath: warehouse, ServicesPath: svc, Catalog: cat,
		Logger: jobs.NewLogger(io.Discard), Ingest: ingest, Health: health,
	})
	waitConsume(t, pool, snap1, jobs.StatusSucceeded)
	waitConsume(t, pool, snap2, jobs.StatusSucceeded)
	if overlap.Load() == 0 {
		t.Fatal("expected overlapping ingest across consumer clients")
	}
}

func TestIntegrationConsumerHealthNilLease(t *testing.T) {
	health := reconcile.Health(nil, nil, nil)
	if err := health(context.Background()); err != nil {
		t.Fatalf("health: %v", err)
	}
	w := &Worker{Health: health}
	if err := w.Health(context.Background()); err != nil {
		t.Fatalf("worker health: %v", err)
	}
}

func TestConcurrentConsumerIngestRace(t *testing.T) {
	if testing.Short() {
		t.Skip("race harness")
	}
	t.Parallel()
	TestIntegrationDistinctSnapshotsOverlapRealIngest(t)
}
