package mrfdownload

import (
	"context"
	"errors"
	"log/slog"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// Worker executes mrf.download and publishes one mrf.parse successor.
type Worker struct {
	river.WorkerDefaults[jobs.MRFDownloadArgs]
	Pool       *pgxpool.Pool
	Downloader *artifact.Downloader
	Logger     *slog.Logger
}

func (w *Worker) Work(ctx context.Context, job *river.Job[jobs.MRFDownloadArgs]) error {
	var sourceURL string
	return jobs.Run(ctx, jobs.RunParams{
		Pool:        w.Pool,
		Client:      river.ClientFromContext[pgx.Tx](ctx),
		Spec:        jobs.MRFDownloadStage,
		DomainID:    job.Args.MRFSourceID,
		RiverJobID:  job.ID,
		Attempt:     job.Attempt,
		MaxAttempts: job.MaxAttempts,
		Claim: func(ctx context.Context) (jobs.ClaimResult, error) {
			res, url, err := claimDownload(ctx, w.Pool, job.Args.MRFSourceID, job.ID)
			sourceURL = url
			return res, err
		},
		Work: func(ctx context.Context) error {
			return w.download(ctx, job, sourceURL)
		},
		Successor: &jobs.Successor{
			Spec:     jobs.MRFParseStage,
			DomainID: job.Args.MRFSourceID,
			Args:     &jobs.MRFParseArgs{MRFSourceID: job.Args.MRFSourceID},
		},
	})
}

func (w *Worker) download(ctx context.Context, job *river.Job[jobs.MRFDownloadArgs], sourceURL string) error {
	if w == nil || w.Downloader == nil || job == nil {
		return jobs.Failure(jobs.FailureMRFDownload)
	}
	_, err := w.Downloader.Download(ctx, artifact.KindMRF, job.Args.MRFSourceID, sourceURL, artifact.ProgressID{
		JobID: job.ID,
		Kind:  jobs.KindMRFDownload,
		Queue: jobs.QueueMRFDownload,
	})
	if err != nil {
		return mapDownloadError(err)
	}
	ws := w.Downloader.Workspace()
	if ws == nil {
		return jobs.Failure(jobs.FailureMRFDownload)
	}
	if _, err := ws.InspectDownload(artifact.KindMRF, job.Args.MRFSourceID); err != nil {
		return mapDownloadError(err)
	}
	return nil
}

func mapDownloadError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return jobs.Failure(jobs.FailureMRFDownload)
}
