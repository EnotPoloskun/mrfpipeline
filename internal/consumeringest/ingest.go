package consumeringest

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/enotpoloskun/mrfconsumer"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

// IngestFunc matches mrfconsumer.Ingest.
type IngestFunc func(ctx context.Context, cfg mrfconsumer.Config) (mrfconsumer.Report, error)

// DefaultIngest calls the pinned public consumer.
func DefaultIngest(ctx context.Context, cfg mrfconsumer.Config) (mrfconsumer.Report, error) {
	return mrfconsumer.Ingest(ctx, cfg)
}

func resolveIngest(fn IngestFunc) IngestFunc {
	if fn != nil {
		return fn
	}
	return DefaultIngest
}

func formatMonth(d time.Time) string {
	return fmt.Sprintf("%04d-%02d", d.Year(), int(d.Month()))
}

func formatSnapshotOutputID(id int64) string {
	return "mrf-" + strconv.FormatInt(id, 10)
}

func expectedFinalPath(warehouse, payer, feedMonth, outputID string) (string, error) {
	root, err := normalizePath(warehouse)
	if err != nil {
		return "", err
	}
	return filepath.Join(root, "snapshots", "collection_month="+feedMonth, "payer_id="+payer, "output_id="+outputID), nil
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
		jobs.FailureConsumerIngestConfigInvalid,
		jobs.FailureConsumerIngestInputInvalid,
		jobs.FailureConsumerIngestOutputFailed,
		jobs.FailureConsumerIngestOutputInvalid,
		jobs.FailureConsumerIngestProviderChanged,
		jobs.FailureConsumerIngestDatabaseFailed,
	} {
		if jobs.IsFailure(err, code) {
			return err
		}
	}
	if errors.Is(err, mrfconsumer.ErrInvalidConfig) {
		return jobs.Failure(jobs.FailureConsumerIngestConfigInvalid)
	}
	if errors.Is(err, mrfconsumer.ErrInvalidInput) {
		return jobs.Failure(jobs.FailureConsumerIngestInputInvalid)
	}
	if errors.Is(err, mrfconsumer.ErrOutput) {
		return jobs.Failure(jobs.FailureConsumerIngestOutputFailed)
	}
	return jobs.Failure(jobs.FailureConsumerIngestOutputFailed)
}
