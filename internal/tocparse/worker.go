package tocparse

import (
	"context"
	"errors"
	"log/slog"

	"github.com/EnotPoloskun/mrftocparser"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// Worker executes toc.parse and publishes one toc.import successor.
type Worker struct {
	river.WorkerDefaults[jobs.TOCParseArgs]
	Pool           *pgxpool.Pool
	Workspace      *artifact.Workspace
	Parse          ParseFunc
	Progress       *jobs.Progress
	Logger         *slog.Logger
	removeDownload func(kind string, id int64) error
}

func (w *Worker) Work(ctx context.Context, job *river.Job[jobs.TOCParseArgs]) error {
	var payer, month string
	client := river.ClientFromContext[pgx.Tx](ctx)
	return jobs.Run(ctx, jobs.RunParams{
		Pool:        w.Pool,
		Client:      client,
		Spec:        jobs.TOCParseStage,
		DomainID:    job.Args.TOCFileID,
		RiverJobID:  job.ID,
		Attempt:     job.Attempt,
		MaxAttempts: job.MaxAttempts,
		Kind:        jobs.KindTOCParse,
		Queue:       jobs.QueueTOCParse,
		Logger:      w.Logger,
		Claim: func(ctx context.Context) (jobs.ClaimResult, error) {
			res, p, m, err := claimParse(ctx, w.Pool, job.Args.TOCFileID, job.ID)
			payer, month = p, m
			return res, err
		},
		Work: func(ctx context.Context) error {
			return w.parse(ctx, job, payer, month)
		},
		Successor: &jobs.Successor{
			Spec:     jobs.TOCImportStage,
			DomainID: job.Args.TOCFileID,
			Args:     &jobs.TOCImportArgs{TOCFileID: job.Args.TOCFileID},
		},
		Confirm: func(ctx context.Context, tx pgx.Tx) error {
			return confirmDownloadSucceeded(ctx, tx, job.Args.TOCFileID)
		},
	})
}

func (w *Worker) parse(ctx context.Context, job *river.Job[jobs.TOCParseArgs], payer, month string) error {
	if w == nil || w.Workspace == nil || job == nil {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	id := job.Args.TOCFileID
	input, output, err := generatedPaths(w.Workspace, id)
	if err != nil {
		return mapWorkError(err)
	}
	tocOutputID, err := artifact.RecordDirName(artifact.KindTOC, id)
	if err != nil {
		return mapWorkError(err)
	}
	state, err := w.Workspace.InspectParsed(artifact.KindTOC, id)
	if err != nil {
		return mapWorkError(err)
	}
	runParse := true
	switch state {
	case artifact.ParsedAbsent, artifact.ParsedEmpty, artifact.ParsedIncomplete:
		if _, err := w.Workspace.InspectDownload(artifact.KindTOC, id); err != nil {
			return mapWorkError(err)
		}
		if err := w.Workspace.ResetParsed(artifact.KindTOC, id); err != nil {
			return mapWorkError(err)
		}
	case artifact.ParsedManifestPresent:
		runParse = false
	default:
		return jobs.Failure(jobs.FailureDomainInvariant)
	}

	var report mrftocparser.Report
	var haveReport bool
	if runParse {
		progress := w.Progress
		if progress == nil {
			progress = jobs.NewProgress(w.Logger)
		}
		pctx := mrftocparser.WithProgress(ctx, func(stage mrftocparser.Stage) error {
			phase, ok := progressPhase(stage)
			if !ok {
				return nil
			}
			return progress.Log(jobs.ProgressParams{
				JobID: job.ID, Kind: jobs.KindTOCParse, Queue: jobs.QueueTOCParse, Phase: phase,
			})
		})
		report, err = resolveParse(w.Parse)(pctx, mrftocparser.Config{
			InputPath:       input,
			OutputPath:      output,
			TOCOutputID:     tocOutputID,
			PayerID:         payer,
			CollectionMonth: month,
		})
		if err != nil {
			return mapWorkError(err)
		}
		haveReport = true
	}

	manifest, err := validateCompletedOutput(output, tocOutputID, payer, month, input)
	if err != nil {
		return jobs.Failure(jobs.FailureTOCParseOutputInvalid)
	}
	if haveReport {
		if err := reportMatches(report, manifest, tocOutputID, output); err != nil {
			return jobs.Failure(jobs.FailureTOCParseOutputInvalid)
		}
	}

	remove := w.removeDownload
	if remove == nil {
		remove = w.Workspace.RemoveDownload
	}
	if err := remove(artifact.KindTOC, id); err != nil {
		if errors.Is(err, context.Canceled) {
			return context.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return context.DeadlineExceeded
		}
		return jobs.Failure(jobs.FailureTOCParseCleanupFailed)
	}
	return nil
}
