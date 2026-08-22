package tocparse

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/EnotPoloskun/mrftocparser"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

// ParseFunc matches mrftocparser.Parse.
type ParseFunc func(ctx context.Context, cfg mrftocparser.Config) (mrftocparser.Report, error)

// DefaultParse calls the pinned public parser.
func DefaultParse(ctx context.Context, cfg mrftocparser.Config) (mrftocparser.Report, error) {
	return mrftocparser.Parse(ctx, cfg)
}

func resolveParse(fn ParseFunc) ParseFunc {
	if fn != nil {
		return fn
	}
	return DefaultParse
}

func generatedPaths(ws *artifact.Workspace, id int64) (input, output string, err error) {
	data, err := ws.DownloadDataPath(artifact.KindTOC, id)
	if err != nil {
		return "", "", err
	}
	parsed, err := ws.ParsedDir(artifact.KindTOC, id)
	if err != nil {
		return "", "", err
	}
	input, err = normalizePath(data)
	if err != nil {
		return "", "", err
	}
	output, err = normalizePath(parsed)
	if err != nil {
		return "", "", err
	}
	return input, output, nil
}

func normalizePath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}

func mapWorkError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	for _, code := range []string{
		jobs.FailureDomainInvariant,
		jobs.FailureInvalidArguments,
		jobs.FailureMissingRecord,
		jobs.FailureTOCParseInputInvalid,
		jobs.FailureTOCParseResourceFailed,
		jobs.FailureTOCParseOutputFailed,
		jobs.FailureTOCParseOutputInvalid,
		jobs.FailureTOCParseCleanupFailed,
	} {
		if jobs.IsFailure(err, code) {
			return err
		}
	}
	if errors.Is(err, mrftocparser.ErrInvalidConfig) {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	if errors.Is(err, mrftocparser.ErrInvalidInput) {
		return jobs.Failure(jobs.FailureTOCParseInputInvalid)
	}
	if errors.Is(err, mrftocparser.ErrResource) {
		return jobs.Failure(jobs.FailureTOCParseResourceFailed)
	}
	if errors.Is(err, mrftocparser.ErrOutput) {
		return jobs.Failure(jobs.FailureTOCParseOutputFailed)
	}
	if errors.Is(err, artifact.ErrArtifact) {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	return jobs.Failure(jobs.FailureTOCParseOutputFailed)
}

func progressPhase(stage mrftocparser.Stage) (string, bool) {
	switch stage {
	case mrftocparser.StageDestinationPrepared:
		return "toc_destination_prepared", true
	case mrftocparser.StageInputParsed:
		return "toc_input_parsed", true
	case mrftocparser.StageParquetClosed:
		return "toc_parquet_closed", true
	default:
		return "", false
	}
}
