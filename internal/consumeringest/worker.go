package consumeringest

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/enotpoloskun/mrfconsumer"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/mrfparse"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// Worker executes consumer.ingest and marks the snapshot consume stage succeeded.
type Worker struct {
	river.WorkerDefaults[jobs.ConsumerIngestArgs]
	Pool          *pgxpool.Pool
	Workspace     *artifact.Workspace
	WarehousePath string
	ServicesPath  string
	Catalog       CatalogID
	Ingest        IngestFunc
	Progress      *jobs.Progress
	Logger        *slog.Logger
}

func (w *Worker) Work(ctx context.Context, job *river.Job[jobs.ConsumerIngestArgs]) error {
	var ident claimIdentity
	client := river.ClientFromContext[pgx.Tx](ctx)
	return jobs.Run(ctx, jobs.RunParams{
		Pool:        w.Pool,
		Client:      client,
		Spec:        jobs.ConsumerIngestStage,
		DomainID:    job.Args.MRFSnapshotID,
		RiverJobID:  job.ID,
		Attempt:     job.Attempt,
		MaxAttempts: job.MaxAttempts,
		Claim: func(ctx context.Context) (jobs.ClaimResult, error) {
			res, claimed, err := claimIngest(ctx, w.Pool, job.Args.MRFSnapshotID, job.ID)
			ident = claimed
			return res, err
		},
		Work: func(ctx context.Context) error {
			return w.ingest(ctx, job, ident)
		},
		Confirm: func(ctx context.Context, tx pgx.Tx) error {
			return confirmIngestSuccess(ctx, tx, ident)
		},
	})
}

func (w *Worker) ingest(ctx context.Context, job *river.Job[jobs.ConsumerIngestArgs], ident claimIdentity) error {
	if w == nil || w.Workspace == nil || job == nil || ident.SnapshotID <= 0 {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	parsed, sourceURI, err := parsedInput(w.Workspace, ident.SourceID)
	if err != nil {
		return jobs.Failure(jobs.FailureConsumerIngestInputInvalid)
	}
	state, err := w.Workspace.InspectParsed(artifact.KindMRF, ident.SourceID)
	if err != nil || state != artifact.ParsedManifestPresent {
		return jobs.Failure(jobs.FailureConsumerIngestInputInvalid)
	}
	if err := mrfparse.ValidateCompletedOutput(parsed, sourceURI, w.ServicesPath); err != nil {
		return jobs.Failure(jobs.FailureConsumerIngestInputInvalid)
	}

	outputID := formatSnapshotOutputID(ident.SnapshotID)
	final, err := expectedFinalPath(w.WarehousePath, ident.PayerID, ident.MonthText, outputID)
	if err != nil {
		return jobs.Failure(jobs.FailureConsumerIngestOutputInvalid)
	}
	_, err = os.Lstat(final)
	if err == nil {
		return w.validatePublished(ident, outputID)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return jobs.Failure(jobs.FailureConsumerIngestOutputInvalid)
	}

	current, err := InspectCatalog(w.Catalog.Path)
	if err != nil || !w.Catalog.same(current) {
		return jobs.Failure(jobs.FailureConsumerIngestProviderChanged)
	}

	progress := w.Progress
	if progress == nil {
		progress = jobs.NewProgress(w.Logger)
	}
	cfg := mrfconsumer.Config{
		InputPath:           parsed,
		ProviderCatalogPath: w.Catalog.Path,
		OutputPath:          w.WarehousePath,
		PayerID:             ident.PayerID,
		FeedID:              ident.FeedID,
		CollectionMonth:     ident.MonthText,
		OutputID:            outputID,
		OnProgress: func(p mrfconsumer.IngestProgress) {
			pct := p.Percent
			_ = progress.Log(jobs.ProgressParams{
				JobID: job.ID, Kind: jobs.KindConsumerIngest, Queue: jobs.QueueConsumer,
				Phase: p.Phase, Percent: &pct,
			})
		},
	}
	report, err := resolveIngest(w.Ingest)(ctx, cfg)
	if err != nil {
		return mapWorkError(err)
	}
	cleaned, err := normalizePath(report.FinalPath)
	if err != nil {
		return jobs.Failure(jobs.FailureConsumerIngestOutputInvalid)
	}
	if report.OutputID != outputID || cleaned != final {
		return jobs.Failure(jobs.FailureConsumerIngestOutputInvalid)
	}
	if report.RateFactsRowCount < 0 || report.RateProviderGroupsRowCount < 0 ||
		report.ProviderGroupsRowCount < 0 || report.ProviderGroupMembershipsRowCount < 0 {
		return jobs.Failure(jobs.FailureConsumerIngestOutputInvalid)
	}
	return w.validatePublished(ident, outputID)
}

func (w *Worker) validatePublished(ident claimIdentity, outputID string) error {
	wh, err := InspectWarehouse(w.WarehousePath)
	if err != nil || wh.Kind != warehouseRecognized {
		return jobs.Failure(jobs.FailureConsumerIngestOutputInvalid)
	}
	if err := inspectCompletedSnapshot(w.WarehousePath, ident.PayerID, ident.FeedID, ident.MonthText, outputID, wh.Catalog); err != nil {
		return jobs.Failure(jobs.FailureConsumerIngestOutputInvalid)
	}
	return nil
}

func parsedInput(ws *artifact.Workspace, sourceID int64) (parsed, sourceURI string, err error) {
	data, err := ws.DownloadDataPath(artifact.KindMRF, sourceID)
	if err != nil {
		return "", "", err
	}
	parsed, err = ws.ParsedDir(artifact.KindMRF, sourceID)
	if err != nil {
		return "", "", err
	}
	data, err = normalizePath(data)
	if err != nil {
		return "", "", err
	}
	parsed, err = normalizePath(parsed)
	if err != nil {
		return "", "", err
	}
	return parsed, mrfparse.ExpectedSourceURI(data), nil
}
