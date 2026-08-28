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
	for _, target := range targets {
		publishedAt, err := release.OutputPublishedAt(ctx, pool, payer, month, target.SnapshotID)
		if err != nil {
			return err
		}
		if err := consumeringest.InspectCompletedSnapshot(warehouse, target.PayerID, target.CollectionMonth, target.OutputID); err != nil {
			if consumeringest.IsPublicationUnreadable(err) {
				return jobs.Failure(jobs.FailureArtifactReconciliationFailed)
			}
			if publishedAt != nil {
				return jobs.Failure(jobs.FailureSealedReleaseInconsistent)
			}
			return jobs.Failure(jobs.FailureReleaseNotReady)
		}
		batches, err := release.ListPlanBatchIDs(ctx, pool, target.SnapshotID)
		if err != nil {
			return err
		}
		if publishedAt != nil {
			changed, err := release.HasPlanRowsAfterSeal(ctx, pool, target.SnapshotID, *publishedAt)
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
			if publishedAt != nil {
				return jobs.Failure(jobs.FailureSealedReleaseInconsistent)
			}
			return jobs.Failure(jobs.FailureReleaseNotReady)
		}
	}
	return nil
}
