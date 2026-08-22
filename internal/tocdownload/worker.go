package tocdownload

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

// Worker executes toc.download and publishes one toc.parse successor.
type Worker struct {
	river.WorkerDefaults[jobs.TOCDownloadArgs]
	Pool       *pgxpool.Pool
	Downloader *artifact.Downloader
	Logger     *slog.Logger
}

func (w *Worker) Work(ctx context.Context, job *river.Job[jobs.TOCDownloadArgs]) error {
	var sourceURL string
	return jobs.Run(ctx, jobs.RunParams{
		Pool:        w.Pool,
		Client:      river.ClientFromContext[pgx.Tx](ctx),
		Spec:        jobs.TOCDownloadStage,
		DomainID:    job.Args.TOCFileID,
		RiverJobID:  job.ID,
		Attempt:     job.Attempt,
		MaxAttempts: job.MaxAttempts,
		Claim: func(ctx context.Context) (jobs.ClaimResult, error) {
			res, url, err := claimDownload(ctx, w.Pool, job.Args.TOCFileID, job.ID)
			sourceURL = url
			return res, err
		},
		Work: func(ctx context.Context) error {
			return w.download(ctx, job, sourceURL)
		},
		Successor: &jobs.Successor{
			Spec:     jobs.TOCParseStage,
			DomainID: job.Args.TOCFileID,
			Args:     &jobs.TOCParseArgs{TOCFileID: job.Args.TOCFileID},
		},
	})
}

func (w *Worker) download(ctx context.Context, job *river.Job[jobs.TOCDownloadArgs], sourceURL string) error {
	if w == nil || w.Downloader == nil || job == nil {
		return jobs.Failure(jobs.FailureTOCDownload)
	}
	_, err := w.Downloader.Download(ctx, artifact.KindTOC, job.Args.TOCFileID, sourceURL, artifact.ProgressID{
		JobID: job.ID,
		Kind:  jobs.KindTOCDownload,
		Queue: jobs.QueueTOCDownload,
	})
	if err != nil {
		return mapDownloadError(err)
	}
	ws := w.Downloader.Workspace()
	if ws == nil {
		return jobs.Failure(jobs.FailureTOCDownload)
	}
	if _, err := ws.InspectDownload(artifact.KindTOC, job.Args.TOCFileID); err != nil {
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
	return jobs.Failure(jobs.FailureTOCDownload)
}
