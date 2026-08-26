package mrfparse

import (
	"context"
	"errors"
	"log/slog"

	"github.com/enotpoloskun/mrfpipeline/internal/admission"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// TerminalCleanupResult reports whether terminal parse artifacts are fully
// absent and whether a valid completed output was found. Valid completed output
// is retained because it is outside the terminal parse cleanup contract.
type TerminalCleanupResult struct {
	Ready       bool
	ValidOutput bool
}

// CleanupUnpublishedParsed removes only incomplete or invalid parsed output.
// A valid completed output is retained. Callers must hold the source
// execution lock while invoking it.
func CleanupUnpublishedParsed(ctx context.Context, ws *artifact.Workspace, sourceID int64, servicesPath string) (TerminalCleanupResult, error) {
	if ctx == nil {
		panic("nil context")
	}
	var zero TerminalCleanupResult
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if ws == nil || sourceID <= 0 {
		return zero, jobs.Failure(jobs.FailureInvalidArguments)
	}
	parsed, err := ws.ParsedDir(artifact.KindMRF, sourceID)
	if err != nil {
		return zero, terminalCleanupError(err)
	}
	state, err := ws.InspectParsed(artifact.KindMRF, sourceID)
	if err != nil {
		return zero, terminalCleanupError(err)
	}
	switch state {
	case artifact.ParsedManifestPresent:
		sourceURI, err := expectedSourceURIForWorkspace(ws, sourceID)
		if err != nil {
			return zero, terminalCleanupError(err)
		}
		if err := ValidateCompletedOutput(parsed, sourceURI, servicesPath); err == nil {
			return TerminalCleanupResult{ValidOutput: true}, nil
		} else if !errors.Is(err, errOutputInvalid) {
			return zero, terminalCleanupError(err)
		}
	case artifact.ParsedAbsent, artifact.ParsedEmpty, artifact.ParsedIncomplete:
	default:
		return zero, jobs.Failure(jobs.FailureMRFParseCleanupFailed)
	}
	if err := ws.RemoveParsed(artifact.KindMRF, sourceID); err != nil {
		return zero, terminalCleanupError(err)
	}
	return zero, nil
}

// CleanupTerminalArtifacts removes unpublished parse output, parser staging,
// and the exact raw download. It does not mutate database state. Callers must
// hold the source execution lock while invoking it.
func CleanupTerminalArtifacts(ctx context.Context, ws *artifact.Workspace, sourceID int64, servicesPath string) (TerminalCleanupResult, error) {
	result, err := CleanupUnpublishedParsed(ctx, ws, sourceID, servicesPath)
	if err != nil || result.ValidOutput {
		return result, err
	}
	if err := ws.RemoveMRFParserTemp(sourceID); err != nil {
		return TerminalCleanupResult{}, terminalCleanupError(err)
	}
	if err := ws.RemoveDownload(artifact.KindMRF, sourceID); err != nil {
		return TerminalCleanupResult{}, terminalCleanupError(err)
	}
	state, err := ws.InspectDownloadState(artifact.KindMRF, sourceID)
	if err != nil || state != artifact.DownloadAbsent {
		if err != nil {
			return TerminalCleanupResult{}, terminalCleanupError(err)
		}
		return TerminalCleanupResult{}, jobs.Failure(jobs.FailureMRFParseCleanupFailed)
	}
	staging, err := ws.HasDownloadStaging(artifact.KindMRF, sourceID)
	if err != nil {
		return TerminalCleanupResult{}, terminalCleanupError(err)
	}
	parserStaging, err := ws.HasMRFParserTemp(sourceID)
	if err != nil {
		return TerminalCleanupResult{}, terminalCleanupError(err)
	}
	if staging || parserStaging {
		return TerminalCleanupResult{}, jobs.Failure(jobs.FailureMRFParseCleanupFailed)
	}
	return TerminalCleanupResult{Ready: true}, nil
}

// expectedSourceURIForWorkspace returns the generated source URI used by a
// parser output manifest for one MRF source.
func expectedSourceURIForWorkspace(ws *artifact.Workspace, sourceID int64) (string, error) {
	if ws == nil || sourceID <= 0 {
		return "", jobs.Failure(jobs.FailureInvalidArguments)
	}
	data, err := ws.DownloadDataPath(artifact.KindMRF, sourceID)
	if err != nil {
		return "", err
	}
	return ExpectedSourceURI(data), nil
}

// CleanupTerminal performs terminal parse artifact cleanup and, after absence
// is confirmed, atomically blocks download, releases the slot, and wakes the
// control scheduler. Callers must hold the source execution lock.
func CleanupTerminal(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], ws *artifact.Workspace, sourceID int64, servicesPath string, loggers ...*slog.Logger) error {
	if ctx == nil {
		panic("nil context")
	}
	if pool == nil || client == nil || ws == nil || sourceID <= 0 {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	result, err := CleanupTerminalArtifacts(ctx, ws, sourceID, servicesPath)
	if err != nil || result.ValidOutput || !result.Ready {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return terminalCleanupError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var parse string
	if err := tx.QueryRow(ctx, `
SELECT parse_status
FROM mrfpipeline.mrf_sources
WHERE id = $1
FOR UPDATE`, sourceID).Scan(&parse); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return jobs.Failure(jobs.FailureMissingRecord)
		}
		return terminalCleanupError(err)
	}
	if parse != jobs.StatusFailed {
		if err := tx.Commit(ctx); err != nil {
			return terminalCleanupError(err)
		}
		return nil
	}
	tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.mrf_sources
SET download_status = 'blocked',
    download_river_job_id = NULL,
    updated_at = transaction_timestamp()
WHERE id = $1 AND parse_status = 'failed'`, sourceID)
	if err != nil || tag.RowsAffected() != 1 {
		return terminalCleanupError(err)
	}
	if err := admission.ReleaseSlotTx(ctx, tx, sourceID); err != nil {
		return err
	}
	if err := admission.WakeTx(ctx, tx, client); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return terminalCleanupError(err)
	}
	if len(loggers) > 0 && loggers[0] != nil {
		loggers[0].LogAttrs(ctx, slog.LevelInfo, "mrf_slot_released", slog.Int64("source_id", sourceID))
	}
	return nil
}

func terminalCleanupError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return jobs.Failure(jobs.FailureMRFParseCleanupFailed)
}
