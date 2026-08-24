package tocimport

import (
	"context"
	"log/slog"
	"path/filepath"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// Worker executes toc.import.
type Worker struct {
	river.WorkerDefaults[jobs.TOCImportArgs]
	Pool       *pgxpool.Pool
	Workspace  *artifact.Workspace
	Logger     *slog.Logger
	afterBatch func(int) error
	hold       func()
}

func (w *Worker) Work(ctx context.Context, job *river.Job[jobs.TOCImportArgs]) error {
	var info claimInfo
	return jobs.Run(ctx, jobs.RunParams{
		Pool:        w.Pool,
		Client:      river.ClientFromContext[pgx.Tx](ctx),
		Spec:        jobs.TOCImportStage,
		DomainID:    job.Args.TOCFileID,
		RiverJobID:  job.ID,
		Attempt:     job.Attempt,
		MaxAttempts: job.MaxAttempts,
		Claim: func(ctx context.Context) (jobs.ClaimResult, error) {
			res, claimed, err := claimImport(ctx, w.Pool, job.Args.TOCFileID, job.ID)
			info = claimed
			return res, err
		},
		Work: func(ctx context.Context) error {
			return w.importTOC(ctx, river.ClientFromContext[pgx.Tx](ctx), job.Args.TOCFileID, info)
		},
		Confirm: func(ctx context.Context, tx pgx.Tx) error {
			return confirmImportSuccess(ctx, tx, river.ClientFromContext[pgx.Tx](ctx), job.Args.TOCFileID)
		},
		PreLock: func(ctx context.Context, tx pgx.Tx) error {
			return release.RequireBuildingForTOC(ctx, tx, job.Args.TOCFileID)
		},
	})
}

func (w *Worker) importTOC(ctx context.Context, client *river.Client[pgx.Tx], tocID int64, info claimInfo) error {
	if w == nil || w.Workspace == nil {
		return jobs.Failure(jobs.FailureTOCImportInvariant)
	}
	if w.hold != nil {
		w.hold()
	}
	tocOutputID, err := artifact.RecordDirName(artifact.KindTOC, tocID)
	if err != nil {
		return jobs.Failure(jobs.FailureTOCImportInvariant)
	}
	sourceURI, output, err := generatedPaths(w.Workspace, tocID)
	if err != nil {
		return jobs.Failure(jobs.FailureTOCImportInvariant)
	}
	state, err := w.Workspace.InspectParsed(artifact.KindTOC, tocID)
	if err != nil || state != artifact.ParsedManifestPresent {
		return jobs.Failure(jobs.FailureTOCImportManifestInvalid)
	}
	n, err := validateOutput(ctx, output, tocOutputID, info.payer, info.month, sourceURI)
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	return importAssociations(ctx, w.Pool, client, output, importMeta{
		tocID: tocID, payer: info.payer, month: info.month, monthDate: info.monthDate,
	}, w.afterBatch)
}

func generatedPaths(ws *artifact.Workspace, id int64) (sourceURI, output string, err error) {
	data, err := ws.DownloadDataPath(artifact.KindTOC, id)
	if err != nil {
		return "", "", err
	}
	parsed, err := ws.ParsedDir(artifact.KindTOC, id)
	if err != nil {
		return "", "", err
	}
	sourceURI, err = normalizePath(data)
	if err != nil {
		return "", "", err
	}
	output, err = normalizePath(parsed)
	if err != nil {
		return "", "", err
	}
	return sourceURI, output, nil
}

func normalizePath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}
