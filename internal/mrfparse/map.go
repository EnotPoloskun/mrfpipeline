package mrfparse

import (
	"context"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

func mapParserExit(ctx context.Context, code int) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	switch code {
	case 0:
		return nil
	case 4:
		return jobs.Failure(jobs.FailureMRFParseInputInvalid)
	default:
		return jobs.Failure(jobs.FailureMRFParseExecutionFailed)
	}
}
