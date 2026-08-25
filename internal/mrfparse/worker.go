package mrfparse

import (
	"context"
	"errors"
	"log/slog"

	"github.com/EnotPoloskun/mrfparser"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// Worker executes mrf.parse and unblocks waiting consumer snapshots.
type Worker struct {
	river.WorkerDefaults[jobs.MRFParseArgs]
	Pool           *pgxpool.Pool
	Workspace      *artifact.Workspace
	Parse          ParseFunc
	Progress       *jobs.Progress
	Logger         *slog.Logger
	Services       ServicesID
	removeDownload func(kind string, id int64) error
}

func (w *Worker) Work(ctx context.Context, job *river.Job[jobs.MRFParseArgs]) error {
	client := river.ClientFromContext[pgx.Tx](ctx)
	released := false
	err := jobs.RunWithExecutionLock(ctx, jobs.RunParams{
		Pool:        w.Pool,
		Client:      client,
		Spec:        jobs.MRFParseStage,
		DomainID:    job.Args.MRFSourceID,
		RiverJobID:  job.ID,
		Attempt:     job.Attempt,
		MaxAttempts: job.MaxAttempts,
		Kind:        jobs.KindMRFParse,
		Queue:       jobs.QueueMRFParse,
		Logger:      w.Logger,
		Claim: func(ctx context.Context) (jobs.ClaimResult, error) {
			return claimParse(ctx, w.Pool, job.Args.MRFSourceID, job.ID)
		},
		Work: func(ctx context.Context) error {
			return w.parse(ctx, job)
		},
		Confirm: func(ctx context.Context, tx pgx.Tx) error {
			err := confirmParseSuccess(ctx, tx, client, job.Args.MRFSourceID)
			if err == nil {
				released = true
			}
			return err
		},
		PreLock: func(ctx context.Context, tx pgx.Tx) error {
			return release.RequireBuildingForStage(ctx, tx, jobs.KindMRFParse, job.Args.MRFSourceID)
		},
	}, jobs.LockNamespaceMRF, job.Args.MRFSourceID)
	if err == nil && released && w.Logger != nil {
		w.Logger.LogAttrs(ctx, slog.LevelInfo, "mrf_slot_released", slog.Int64("source_id", job.Args.MRFSourceID))
	}
	return err
}

func (w *Worker) parse(ctx context.Context, job *river.Job[jobs.MRFParseArgs]) error {
	if w == nil || w.Workspace == nil || job == nil {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	id := job.Args.MRFSourceID
	input, output, err := generatedPaths(w.Workspace, id)
	if err != nil {
		return mapWorkError(err)
	}
	sourceURI := expectedSourceURI(input)
	state, err := w.Workspace.InspectParsed(artifact.KindMRF, id)
	if err != nil {
		return mapWorkError(err)
	}
	runParse := true
	switch state {
	case artifact.ParsedAbsent, artifact.ParsedEmpty, artifact.ParsedIncomplete:
		if _, err := w.Workspace.InspectDownload(artifact.KindMRF, id); err != nil {
			return jobs.Failure(jobs.FailureDomainInvariant)
		}
		if err := w.Workspace.ResetParsed(artifact.KindMRF, id); err != nil {
			return mapWorkError(err)
		}
	case artifact.ParsedManifestPresent:
		runParse = false
	default:
		return jobs.Failure(jobs.FailureDomainInvariant)
	}

	if runParse {
		current, err := InspectServices(w.Services.Path)
		if err != nil || !w.Services.same(current) {
			return jobs.Failure(jobs.FailureMRFParseSelectorChanged)
		}
		progress := w.Progress
		if progress == nil {
			progress = jobs.NewProgress(w.Logger)
		}
		cfg := mrfparser.DefaultConfig()
		cfg.Input = input
		cfg.Output = output
		cfg.Services = w.Services.Path
		cfg.TempDir, err = w.Workspace.PrepareMRFParserTemp(id)
		if err != nil {
			return jobs.Failure(jobs.FailureMRFParseExecutionFailed)
		}
		cfg.OnProgress = func(p mrfparser.Progress) error {
			params := jobs.ProgressParams{
				JobID: job.ID, Kind: jobs.KindMRFParse, Queue: jobs.QueueMRFParse, Phase: "mrf_parse",
				CopiedBytes: p.StoredBytes,
			}
			if p.SizeKnown && p.TotalBytes > 0 {
				params.TotalBytes = p.TotalBytes
			}
			_ = progress.Log(params)
			return nil
		}
		if err := callParse(w.Parse, ctx, cfg); err != nil {
			return mapWorkError(err)
		}
	}

	if err := validateCompletedOutput(output, sourceURI, w.Services.Path); err != nil {
		return jobs.Failure(jobs.FailureMRFParseOutputInvalid)
	}

	remove := w.removeDownload
	if remove == nil {
		remove = w.Workspace.RemoveDownload
	}
	if err := remove(artifact.KindMRF, id); err != nil {
		if errors.Is(err, context.Canceled) {
			return context.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return context.DeadlineExceeded
		}
		return jobs.Failure(jobs.FailureMRFParseCleanupFailed)
	}
	if err := w.Workspace.RemoveMRFParserTemp(id); err != nil {
		return jobs.Failure(jobs.FailureMRFParseCleanupFailed)
	}
	state, err = w.Workspace.InspectDownloadState(artifact.KindMRF, id)
	if err != nil || state != artifact.DownloadAbsent {
		return jobs.Failure(jobs.FailureMRFParseCleanupFailed)
	}
	staging, err := w.Workspace.HasDownloadStaging(artifact.KindMRF, id)
	if err != nil || staging {
		return jobs.Failure(jobs.FailureMRFParseCleanupFailed)
	}
	return nil
}
