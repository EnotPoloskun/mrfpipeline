package work

import (
	"context"
	"log/slog"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/discovery"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/tocdownload"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// Runtime is the production River process for Story 06.
type Runtime struct {
	Pool       *pgxpool.Pool
	Workspace  *artifact.Workspace
	Logger     *slog.Logger
	Discover   discovery.DiscoverFunc
	Downloader *artifact.Downloader
}

// Queues is the Story 06 worker map: discovery and toc_download only.
func Queues() map[string]river.QueueConfig {
	return map[string]river.QueueConfig{
		jobs.QueueDiscovery:   {MaxWorkers: 1},
		jobs.QueueTOCDownload: {MaxWorkers: 4},
	}
}

// Run starts a River client that consumes discovery.run and toc.download,
// waits until ctx is canceled or the client stops, then shuts down. A
// requested shutdown after Start succeeds returns nil.
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
	logger := r.Logger
	if logger == nil {
		logger = jobs.NewLogger(nil)
	}
	downloader := r.Downloader
	if downloader == nil {
		downloader = artifact.NewDownloader(r.Workspace, jobs.NewProgress(logger))
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, &discovery.Worker{Pool: r.Pool, Discover: r.Discover, Logger: logger})
	river.AddWorker(workers, &tocdownload.Worker{Pool: r.Pool, Downloader: downloader, Logger: logger})
	handler := jobs.NewDomainErrorHandler(r.Pool, []jobs.KindBinding{
		{Kind: jobs.KindDiscoveryRun, Spec: jobs.DiscoveryRunStage, ArgField: jobs.FieldDiscoveryRunID},
		{Kind: jobs.KindTOCDownload, Spec: jobs.TOCDownloadStage, ArgField: jobs.FieldTOCFileID},
	}, logger)
	client, err := jobs.NewRuntime(ctx, r.Pool, workers, Queues(), handler, logger)
	if err != nil {
		return err
	}
	if err := client.Start(context.Background()); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return jobs.Failure("start")
	}
	select {
	case <-ctx.Done():
		if err := jobs.Shutdown(context.Background(), client); err != nil {
			return err
		}
		return nil
	case <-client.Stopped():
		return jobs.Failure("runtime")
	}
}
