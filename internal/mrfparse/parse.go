package mrfparse

import (
	"context"
	"errors"
	"path/filepath"
	"sync"

	"github.com/EnotPoloskun/mrfparser"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

// ParseFunc matches mrfparser.Parse.
type ParseFunc func(ctx context.Context, cfg mrfparser.Config) error

// parseMu serializes every mrfparser.Parse call in this process, including
// injected test fakes. Queue concurrency 1 is not that safety contract:
// River 0.39 rescue does not terminate an in-flight Go invocation.
var parseMu sync.Mutex

// DefaultParse calls the pinned public parser.
func DefaultParse(ctx context.Context, cfg mrfparser.Config) error {
	return mrfparser.Parse(ctx, cfg)
}

func resolveParse(fn ParseFunc) ParseFunc {
	if fn != nil {
		return fn
	}
	return DefaultParse
}

func callParse(fn ParseFunc, ctx context.Context, cfg mrfparser.Config) error {
	parseMu.Lock()
	defer parseMu.Unlock()
	return resolveParse(fn)(ctx, cfg)
}

func generatedPaths(ws *artifact.Workspace, id int64) (input, output string, err error) {
	data, err := ws.DownloadDataPath(artifact.KindMRF, id)
	if err != nil {
		return "", "", err
	}
	parsed, err := ws.ParsedDir(artifact.KindMRF, id)
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

// ExpectedSourceURI is the Story 10 generated download URI: EvalSymlinks of
// the record directory plus download/data, so the deleted leaf is not required.
func ExpectedSourceURI(generatedDownloadDataPath string) string {
	return expectedSourceURI(generatedDownloadDataPath)
}

func expectedSourceURI(generated string) string {
	rec := filepath.Dir(filepath.Dir(generated))
	if resolved, err := filepath.EvalSymlinks(rec); err == nil {
		return filepath.Join(resolved, filepath.Base(filepath.Dir(generated)), filepath.Base(generated))
	}
	return generated
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
		jobs.FailureMRFParseExecutionFailed,
		jobs.FailureMRFParseOutputInvalid,
		jobs.FailureMRFParseSelectorChanged,
		jobs.FailureMRFParseCleanupFailed,
		jobs.FailureMRFParseDatabaseFailed,
	} {
		if jobs.IsFailure(err, code) {
			return err
		}
	}
	if errors.Is(err, artifact.ErrArtifact) {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	return jobs.Failure(jobs.FailureMRFParseExecutionFailed)
}
