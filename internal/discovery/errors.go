package discovery

import (
	"context"
	"errors"
	"fmt"

	"github.com/EnotPoloskun/mrfdiscoverer"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

func mapDiscoverError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, mrfdiscoverer.ErrInvalidConfig) {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	return jobs.Failure(jobs.FailureDiscoveryListing)
}

func dbFail(ctx context.Context, op string, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("%w: %s", database.ErrDatabase, op)
}
