package work

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/admission"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/consumeringest"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/discovery"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/mrfdownload"
	"github.com/enotpoloskun/mrfpipeline/internal/mrfparse"
	"github.com/enotpoloskun/mrfpipeline/internal/planattach"
	"github.com/enotpoloskun/mrfpipeline/internal/reconcile"
	"github.com/enotpoloskun/mrfpipeline/internal/tocdownload"
	"github.com/enotpoloskun/mrfpipeline/internal/tocimport"
	"github.com/enotpoloskun/mrfpipeline/internal/tocparse"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// Runtime is one explicit role-specific production River process.
type Runtime struct {
	Role             string
	ResidentCapacity int64
	// Lease is optional for callers that must acquire control before mutating
	// artifact initialization. When nil, Run acquires the role lease itself.
	Lease               *database.Lease
	Pool                *pgxpool.Pool
	Workspace           *artifact.Workspace
	Logger              *slog.Logger
	Discover            discovery.DiscoverFunc
	Downloader          *artifact.Downloader
	Parse               tocparse.ParseFunc
	ParseMRF            mrfparse.ParseFunc
	Ingest              consumeringest.IngestFunc
	Attach              planattach.AttachFunc
	ServicesPath        string
	WarehousePath       string
	ProviderCatalogPath string
}

// QueuesForRole returns the queues owned by one explicit long-lived process.
func QueuesForRole(role string) map[string]river.QueueConfig {
	switch role {
	case "control":
		return map[string]river.QueueConfig{
			jobs.QueueControl:     {MaxWorkers: 1},
			jobs.QueueDiscovery:   {MaxWorkers: 1},
			jobs.QueueTOCDownload: {MaxWorkers: 4},
			jobs.QueueTOCParse:    {MaxWorkers: 2},
			jobs.QueueTOCImport:   {MaxWorkers: 2},
		}
	case "mrf":
		return map[string]river.QueueConfig{
			jobs.QueueMRFDownload: {MaxWorkers: 2},
			jobs.QueueMRFParse:    {MaxWorkers: 1},
		}
	case "consumer":
		return map[string]river.QueueConfig{jobs.QueueConsumer: {MaxWorkers: 1}}
	default:
		return nil
	}
}

// Run starts one explicit role-specific River client, waits until ctx is
// canceled or the client stops, then shuts down. A requested shutdown after
// Start succeeds returns nil. MRF parse concurrency is one per process because
// every parser call holds the process mutex. Consumer concurrency is one while
// the pinned warehouse writer requires serialized writes.
func (r Runtime) Run(ctx context.Context) error {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.Pool == nil || r.Workspace == nil {
		return jobs.Failure("runtime")
	}
	role := r.Role
	if role != "control" && role != "mrf" && role != "consumer" {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	services, err := mrfparse.InspectServices(r.ServicesPath)
	if err != nil {
		return err
	}
	var catalog consumeringest.CatalogID
	var warehouse consumeringest.WarehouseState
	if role == "control" || role == "consumer" {
		catalog, err = consumeringest.InspectCatalog(r.ProviderCatalogPath)
		if err != nil {
			return err
		}
		warehouse, err = consumeringest.InspectWarehouse(r.WarehousePath)
		if err != nil {
			return err
		}
		if err := consumeringest.CheckWarehouseCatalog(warehouse, catalog, r.Workspace.Root, r.ServicesPath); err != nil {
			return err
		}
	}
	lease := r.Lease
	ownsLease := lease == nil
	if lease == nil && role == "control" {
		lease, err = database.AcquireControlLease(ctx, r.Pool)
	} else if lease == nil && role == "consumer" {
		lease, err = database.AcquireConsumerLease(ctx, r.Pool)
	}
	if err != nil {
		if database.IsLeaseUnavailable(err) {
			return jobs.Failure(jobs.FailureWorkerBusy)
		}
		return err
	}
	if lease != nil && ownsLease {
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
			_ = lease.Release(cleanupCtx)
			cleanupCancel()
		}()
	}
	if lease != nil {
		if err := lease.Check(ctx); err != nil {
			return jobs.Failure(jobs.FailureWorkerLeaseLost)
		}
	}
	logger := r.Logger
	if logger == nil {
		logger = jobs.NewLogger(nil)
	}
	logger = logger.With(slog.String("role", role))
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var lost atomic.Bool
	var watchDone chan struct{}
	if lease != nil {
		watchDone = make(chan struct{})
		go func() {
			defer close(watchDone)
			reconcile.WatchLease(workCtx, lease, cancel, &lost)
		}()
	}
	defer func() {
		cancel()
		if watchDone != nil {
			<-watchDone
		}
	}()
	if role == "control" {
		if r.ResidentCapacity <= 0 {
			return jobs.Failure(jobs.FailureInvalidArguments)
		}
		if err := admission.ValidateRuntimeRoot(workCtx, r.Pool, r.Workspace.Root); err != nil {
			return err
		}
		if err := admission.NormalizePending(workCtx, r.Pool); err != nil {
			return err
		}
		if err := admission.BackfillSlots(workCtx, r.Pool, r.Workspace); err != nil {
			return err
		}
		if _, err := admission.ConfigureRuntime(workCtx, r.Pool, r.Workspace.Root, r.ResidentCapacity); err != nil {
			if jobs.IsFailure(err, jobs.FailureCapacityBelowHeld) {
				var held int64
				if queryErr := r.Pool.QueryRow(workCtx, `SELECT count(*) FROM mrfpipeline.mrf_materialization_slots`).Scan(&held); queryErr != nil {
					held = -1
				}
				logger.LogAttrs(workCtx, slog.LevelError, "worker_start_failed",
					slog.String("failure", jobs.FailureCapacityBelowHeld),
					slog.Int64("held_slots", held),
					slog.Int64("requested_capacity", r.ResidentCapacity))
			}
			return err
		}
	}
	if role == "control" {
		if _, err := reconcile.Run(workCtx, reconcile.Params{
			Pool: r.Pool, Workspace: r.Workspace, WarehousePath: r.WarehousePath,
			ProviderCatalogPath: r.ProviderCatalogPath, ServicesPath: r.ServicesPath, Logger: logger,
		}); err != nil {
			if lost.Load() {
				return jobs.Failure(jobs.FailureWorkerLeaseLost)
			}
			return err
		}
	}
	health := reconcile.Health(lease, cancel, &lost)

	progress := jobs.NewProgress(logger)
	downloader := r.Downloader
	if downloader == nil {
		downloader = artifact.NewDownloader(r.Workspace, progress)
	}
	workers := river.NewWorkers()
	if role == "control" {
		river.AddWorker(workers, &admission.Worker{Pool: r.Pool, Logger: logger})
		river.AddWorker(workers, &discovery.Worker{Pool: r.Pool, Discover: r.Discover, Logger: logger})
		river.AddWorker(workers, &tocdownload.Worker{Pool: r.Pool, Downloader: downloader, Logger: logger})
		river.AddWorker(workers, &tocparse.Worker{Pool: r.Pool, Workspace: r.Workspace, Parse: r.Parse, Progress: progress, Logger: logger})
		river.AddWorker(workers, &tocimport.Worker{Pool: r.Pool, Workspace: r.Workspace, Logger: logger})
	}
	if role == "mrf" {
		river.AddWorker(workers, &mrfdownload.Worker{Pool: r.Pool, Downloader: downloader, Logger: logger})
		river.AddWorker(workers, &mrfparse.Worker{Pool: r.Pool, Workspace: r.Workspace, Parse: r.ParseMRF, Progress: progress, Logger: logger, Services: services})
	}
	if role == "consumer" {
		river.AddWorker(workers, &consumeringest.Worker{
			Pool: r.Pool, Workspace: r.Workspace, WarehousePath: r.WarehousePath,
			ServicesPath: services.Path, Catalog: catalog, Ingest: r.Ingest, Progress: progress, Logger: logger, Health: health,
		})
		river.AddWorker(workers, &planattach.Worker{
			Pool: r.Pool, Workspace: r.Workspace, WarehousePath: r.WarehousePath,
			Attach: r.Attach, Logger: logger, Health: health,
		})
	}
	handler := jobs.NewDomainErrorHandler(r.Pool, jobs.ProductionBindings(), logger)
	client, err := jobs.NewRuntime(workCtx, r.Pool, workers, QueuesForRole(role), handler, logger)
	if err != nil {
		return err
	}
	if role == "control" {
		if err := admission.ScheduleWaitingWithLogger(workCtx, r.Pool, client, logger); err != nil {
			return err
		}
	}
	if err := client.Start(context.Background()); err != nil {
		if workCtx.Err() != nil {
			if lost.Load() {
				return jobs.Failure(jobs.FailureWorkerLeaseLost)
			}
			return workCtx.Err()
		}
		return jobs.Failure("start")
	}
	logWorkerStarted(logger)
	select {
	case <-workCtx.Done():
		if err := jobs.Shutdown(context.Background(), client); err != nil {
			return err
		}
		if lost.Swap(false) {
			logWorkerLeaseLost(logger)
			return jobs.Failure(jobs.FailureWorkerLeaseLost)
		}
		if ctx.Err() != nil {
			return nil
		}
		return jobs.Failure(jobs.FailureWorkerLeaseLost)
	case <-client.Stopped():
		return jobs.Failure("runtime")
	}
}

func logWorkerStarted(logger *slog.Logger) {
	if logger == nil {
		return
	}
	logger.LogAttrs(context.Background(), slog.LevelInfo, "worker_started",
		slog.String("kind", "runtime"),
	)
}

func logWorkerLeaseLost(logger *slog.Logger) {
	if logger == nil {
		return
	}
	logger.LogAttrs(context.Background(), slog.LevelError, "worker_lease_lost",
		slog.String("kind", "runtime"),
		slog.String("failure", jobs.FailureWorkerLeaseLost),
	)
}
