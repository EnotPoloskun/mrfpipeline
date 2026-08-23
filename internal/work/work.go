package work

import (
	"context"
	"log/slog"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/consumeringest"
	"github.com/enotpoloskun/mrfpipeline/internal/discovery"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/mrfdownload"
	"github.com/enotpoloskun/mrfpipeline/internal/mrfparse"
	"github.com/enotpoloskun/mrfpipeline/internal/tocdownload"
	"github.com/enotpoloskun/mrfpipeline/internal/tocimport"
	"github.com/enotpoloskun/mrfpipeline/internal/tocparse"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// Runtime is the production River process for Stories 06–11.
type Runtime struct {
	Pool                *pgxpool.Pool
	Workspace           *artifact.Workspace
	Logger              *slog.Logger
	Discover            discovery.DiscoverFunc
	Downloader          *artifact.Downloader
	Parse               tocparse.ParseFunc
	ParseMRF            mrfparse.ParseFunc
	Ingest              consumeringest.IngestFunc
	ServicesPath        string
	WarehousePath       string
	ProviderCatalogPath string
}

// Queues is the Story 11 worker map: discovery through mrf_parse plus consumer.
func Queues() map[string]river.QueueConfig {
	return map[string]river.QueueConfig{
		jobs.QueueDiscovery:   {MaxWorkers: 1},
		jobs.QueueTOCDownload: {MaxWorkers: 4},
		jobs.QueueTOCParse:    {MaxWorkers: 2},
		jobs.QueueTOCImport:   {MaxWorkers: 2},
		jobs.QueueMRFDownload: {MaxWorkers: 2},
		jobs.QueueMRFParse:    {MaxWorkers: 1},
		jobs.QueueConsumer:    {MaxWorkers: 1},
	}
}

// Run starts a River client that consumes discovery.run, toc.download,
// toc.parse, toc.import, mrf.download, mrf.parse, and consumer.ingest, waits
// until ctx is canceled or the client stops, then shuts down. A requested
// shutdown after Start succeeds returns nil. Queue concurrency 1 on mrf_parse
// is not the parser safety contract; every mrfparser.Parse holds the process
// mutex. consumer max 1 is the only warehouse writer.
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
	services, err := mrfparse.InspectServices(r.ServicesPath)
	if err != nil {
		return err
	}
	catalog, err := consumeringest.InspectCatalog(r.ProviderCatalogPath)
	if err != nil {
		return err
	}
	warehouse, err := consumeringest.InspectWarehouse(r.WarehousePath)
	if err != nil {
		return err
	}
	if err := consumeringest.CheckWarehouseCatalog(warehouse, catalog, r.Workspace.Root, r.ServicesPath); err != nil {
		return err
	}
	logger := r.Logger
	if logger == nil {
		logger = jobs.NewLogger(nil)
	}
	progress := jobs.NewProgress(logger)
	downloader := r.Downloader
	if downloader == nil {
		downloader = artifact.NewDownloader(r.Workspace, progress)
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, &discovery.Worker{Pool: r.Pool, Discover: r.Discover, Logger: logger})
	river.AddWorker(workers, &tocdownload.Worker{Pool: r.Pool, Downloader: downloader, Logger: logger})
	river.AddWorker(workers, &tocparse.Worker{Pool: r.Pool, Workspace: r.Workspace, Parse: r.Parse, Progress: progress, Logger: logger})
	river.AddWorker(workers, &tocimport.Worker{Pool: r.Pool, Workspace: r.Workspace, Logger: logger})
	river.AddWorker(workers, &mrfdownload.Worker{Pool: r.Pool, Downloader: downloader, Logger: logger})
	river.AddWorker(workers, &mrfparse.Worker{Pool: r.Pool, Workspace: r.Workspace, Parse: r.ParseMRF, Progress: progress, Logger: logger, Services: services})
	river.AddWorker(workers, &consumeringest.Worker{
		Pool: r.Pool, Workspace: r.Workspace, WarehousePath: r.WarehousePath,
		ServicesPath: services.Path, Catalog: catalog, Ingest: r.Ingest, Progress: progress, Logger: logger,
	})
	handler := jobs.NewDomainErrorHandler(r.Pool, []jobs.KindBinding{
		{Kind: jobs.KindDiscoveryRun, Spec: jobs.DiscoveryRunStage, ArgField: jobs.FieldDiscoveryRunID},
		{Kind: jobs.KindTOCDownload, Spec: jobs.TOCDownloadStage, ArgField: jobs.FieldTOCFileID},
		{Kind: jobs.KindTOCParse, Spec: jobs.TOCParseStage, ArgField: jobs.FieldTOCFileID},
		{Kind: jobs.KindTOCImport, Spec: jobs.TOCImportStage, ArgField: jobs.FieldTOCFileID},
		{Kind: jobs.KindMRFDownload, Spec: jobs.MRFDownloadStage, ArgField: jobs.FieldMRFSourceID},
		{Kind: jobs.KindMRFParse, Spec: jobs.MRFParseStage, ArgField: jobs.FieldMRFSourceID},
		{Kind: jobs.KindConsumerIngest, Spec: jobs.ConsumerIngestStage, ArgField: jobs.FieldMRFSnapshotID},
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
