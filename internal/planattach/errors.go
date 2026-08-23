package planattach

import (
	"context"
	"errors"

	"github.com/enotpoloskun/mrfconsumer"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

var errInvariant = jobs.Failure(jobs.FailurePlanAttachInvariant)

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
		jobs.FailurePlanAttachConfigInvalid,
		jobs.FailurePlanAttachInputInvalid,
		jobs.FailurePlanAttachOutputFailed,
		jobs.FailurePlanAttachOutputInvalid,
		jobs.FailurePlanAttachDatabaseFailed,
		jobs.FailurePlanAttachInvariant,
	} {
		if jobs.IsFailure(err, code) {
			return err
		}
	}
	if errors.Is(err, mrfconsumer.ErrInvalidConfig) {
		return jobs.Failure(jobs.FailurePlanAttachConfigInvalid)
	}
	if errors.Is(err, mrfconsumer.ErrInvalidInput) {
		return jobs.Failure(jobs.FailurePlanAttachInputInvalid)
	}
	if errors.Is(err, mrfconsumer.ErrOutput) {
		return jobs.Failure(jobs.FailurePlanAttachOutputFailed)
	}
	return jobs.Failure(jobs.FailurePlanAttachOutputFailed)
}

func classifyDB(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return jobs.Failure(jobs.FailurePlanAttachDatabaseFailed)
}
