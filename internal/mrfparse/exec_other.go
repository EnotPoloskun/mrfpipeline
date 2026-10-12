//go:build !unix

package mrfparse

import (
	"context"

	"github.com/EnotPoloskun/mrfparser"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

func runParserCLI(ctx context.Context, cfg mrfparser.Config) error {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return jobs.Failure(jobs.FailureMRFParseExecutionFailed)
}
