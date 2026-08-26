package reconcile

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/admission"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/consumeringest"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/mrfparse"
	"github.com/enotpoloskun/mrfpipeline/internal/planbatch"
	"github.com/jackc/pgx/v5/pgxpool"
)

const LeaseHealthEvery = 5 * time.Second

// Params is the shared work/reconcile environment.
type Params struct {
	Pool                *pgxpool.Pool
	Workspace           *artifact.Workspace
	WarehousePath       string
	ProviderCatalogPath string
	ServicesPath        string
	Logger              *slog.Logger
}

// Command validates warehouse boundaries, acquires the lease, runs the pass,
// and releases the lease.
func Command(ctx context.Context, p Params) (Report, error) {
	if ctx == nil {
		panic("nil context")
	}
	var zero Report
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if err := inspectEnv(p); err != nil {
		return zero, err
	}
	lease, err := database.AcquireWorkerLease(ctx, p.Pool)
	if err != nil {
		if database.IsLeaseUnavailable(err) {
			return zero, jobs.Failure(jobs.FailureWorkerLeaseUnavailable)
		}
		return zero, err
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
		_ = lease.Release(cleanupCtx)
		cleanupCancel()
	}()
	if err := lease.Check(ctx); err != nil {
		return zero, jobs.Failure(jobs.FailureWorkerLeaseLost)
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	watchDone := make(chan struct{})
	var lost atomic.Bool
	go func() {
		defer close(watchDone)
		WatchLease(workCtx, lease, cancel, &lost)
	}()
	report, err := Run(workCtx, p)
	cancel()
	<-watchDone
	if lost.Load() {
		return zero, jobs.Failure(jobs.FailureWorkerLeaseLost)
	}
	return report, err
}

func inspectEnv(p Params) error {
	if p.Pool == nil || p.Workspace == nil {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	services, err := mrfparse.InspectServices(p.ServicesPath)
	if err != nil {
		return err
	}
	catalog, err := consumeringest.InspectCatalog(p.ProviderCatalogPath)
	if err != nil {
		return err
	}
	warehouse, err := consumeringest.InspectWarehouse(p.WarehousePath)
	if err != nil {
		return err
	}
	return consumeringest.CheckWarehouseCatalog(warehouse, catalog, p.Workspace.Root, services.Path)
}

// Run executes the safe reconciliation pass. The caller holds the worker lease.
func Run(ctx context.Context, p Params) (Report, error) {
	if ctx == nil {
		panic("nil context")
	}
	var report Report
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if p.Pool == nil || p.Workspace == nil {
		return report, jobs.Failure(jobs.FailureInvalidArguments)
	}
	logger := p.Logger
	if logger == nil {
		logger = jobs.NewLogger(nil)
	}
	report.logger = logger
	client, err := jobs.NewInsertClient(ctx, p.Pool, logger)
	if err != nil {
		return report, err
	}
	if err := admission.AuditSlots(ctx, p.Pool, p.Workspace); err != nil {
		return report, err
	}
	if err := admission.RestoreWakes(ctx, p.Pool, client); err != nil {
		return report, err
	}
	if err := restorePrerequisites(ctx, p.Pool, client, p.Workspace, &report); err != nil {
		return report, err
	}
	if err := unblockSuccessors(ctx, p.Pool, client, &report); err != nil {
		return report, err
	}
	if err := repairCurrentJobs(ctx, p.Pool, client, &report); err != nil {
		return report, err
	}
	if err := releaseTerminalParses(ctx, p.Pool, client, p.Workspace, p.ServicesPath, logger); err != nil {
		return report, err
	}
	if err := releaseEmptyTerminalDownloads(ctx, p.Pool, p.Workspace, logger); err != nil {
		return report, err
	}
	if err := admission.ScheduleWaitingWithLogger(ctx, p.Pool, client, logger); err != nil {
		return report, err
	}
	if err := scheduleParsedSources(ctx, p.Pool, client, &report); err != nil {
		return report, err
	}
	n, err := planbatch.SweepConsumedReleaseAware(ctx, p.Pool, client, func(snapshotID int64) {
		report.recordSealedSnapshot(logger, jobs.KindConsumerAttachPlans, snapshotID)
	})
	if err != nil {
		return report, err
	}
	report.ScheduledPlanBatchCount += n
	if err := auditSealedPlanSets(ctx, p.Pool, p.WarehousePath, &report); err != nil {
		return report, err
	}
	if err := cleanArtifacts(ctx, p.Pool, p.Workspace, p.ServicesPath, &report); err != nil {
		return report, err
	}
	// Staging cleanup can make a previously terminal, empty download eligible
	// for slot release. Re-run the repair after that cleanup so the refill wake
	// is published in this same reconciliation pass.
	if err := releaseTerminalParses(ctx, p.Pool, client, p.Workspace, p.ServicesPath, logger); err != nil {
		return report, err
	}
	if err := releaseEmptyTerminalDownloads(ctx, p.Pool, p.Workspace, logger); err != nil {
		return report, err
	}
	if err := admission.ScheduleWaitingWithLogger(ctx, p.Pool, client, logger); err != nil {
		return report, err
	}
	return report, nil
}

// WatchLease cancels workCtx when the dedicated lease connection fails a health check.
func WatchLease(ctx context.Context, lease *database.Lease, cancel context.CancelFunc, lost *atomic.Bool) {
	if lease == nil || cancel == nil {
		return
	}
	t := time.NewTicker(LeaseHealthEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := lease.Check(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				if lost != nil {
					lost.Store(true)
				}
				cancel()
				return
			}
		}
	}
}

// Health is called before consumer warehouse writes. A failed check cancels
// the worker process and must not be treated as a retryable domain error.
func Health(lease *database.Lease, cancel context.CancelFunc, lost *atomic.Bool) func(context.Context) error {
	return func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		fail := func() error {
			if lost != nil {
				lost.Store(true)
			}
			if cancel != nil {
				cancel()
			}
			return leaseLost()
		}
		if lease == nil {
			return fail()
		}
		if err := lease.Check(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fail()
		}
		return nil
	}
}
