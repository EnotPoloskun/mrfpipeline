package reconcile

import (
	"context"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/consumeringest"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ValidateActivationTargets checks the shallow base and plan-part inventory
// before a release changes its serving state. It does not inspect Parquet
// rows or mutate the database or warehouse.
func ValidateActivationTargets(ctx context.Context, pool *pgxpool.Pool, warehouse, payer string, month time.Time, targets []release.Target) error {
	sealedAt, err := release.ReleaseSealedAt(ctx, pool, payer, month)
	if err != nil {
		return err
	}
	for _, target := range targets {
		if err := consumeringest.InspectCompletedSnapshot(warehouse, target.PayerID, target.CollectionMonth, target.OutputID); err != nil {
			if consumeringest.IsPublicationUnreadable(err) {
				return jobs.Failure(jobs.FailureArtifactReconciliationFailed)
			}
			if sealedAt != nil {
				return jobs.Failure(jobs.FailureSealedReleaseInconsistent)
			}
			return jobs.Failure(jobs.FailureReleaseNotReady)
		}
		batches, err := release.ListPlanBatchIDs(ctx, pool, target.SnapshotID)
		if err != nil {
			return err
		}
		if sealedAt != nil {
			changed, err := release.HasPlanRowsAfterSeal(ctx, pool, target.SnapshotID, *sealedAt)
			if err != nil {
				return err
			}
			if changed {
				return jobs.Failure(jobs.FailureSealedReleaseInconsistent)
			}
		}
		if err := consumeringest.InspectPlanAssociations(warehouse, target.OutputID, batches); err != nil {
			if consumeringest.IsPublicationUnreadable(err) {
				return jobs.Failure(jobs.FailureArtifactReconciliationFailed)
			}
			if sealedAt != nil {
				return jobs.Failure(jobs.FailureSealedReleaseInconsistent)
			}
			return jobs.Failure(jobs.FailureReleaseNotReady)
		}
	}
	return nil
}
