package reconcile

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/enotpoloskun/mrfpipeline/internal/consumeringest"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5/pgxpool"
)

// auditSealedPlanSets checks durable timestamps, shallow plan-part inventory,
// and active base publication metadata/layout. It does not inspect Parquet
// row/data contents.
func auditSealedPlanSets(ctx context.Context, pool *pgxpool.Pool, warehouse string, report *Report) error {
	if pool == nil || report == nil {
		return nil
	}
	if warehouse == "" {
		return artFail()
	}
	rows, err := pool.Query(ctx, `
SELECT 'mrf_plans', p.id
FROM mrfpipeline.mrf_plans p
JOIN mrfpipeline.mrf_snapshots s ON s.id = p.mrf_snapshot_id
JOIN mrfpipeline.monthly_release_outputs o ON o.mrf_snapshot_id = s.id
JOIN mrfpipeline.monthly_releases r
  ON r.payer_id = o.payer_id AND r.collection_month = o.collection_month
WHERE r.status IN ('active', 'inactive') AND p.created_at > o.published_at
UNION ALL
SELECT 'plan_attachment_batches', b.id
FROM mrfpipeline.plan_attachment_batches b
JOIN mrfpipeline.mrf_snapshots s ON s.id = b.mrf_snapshot_id
JOIN mrfpipeline.monthly_release_outputs o ON o.mrf_snapshot_id = s.id
JOIN mrfpipeline.monthly_releases r
  ON r.payer_id = o.payer_id AND r.collection_month = o.collection_month
WHERE r.status IN ('active', 'inactive') AND b.created_at > o.published_at
UNION ALL
SELECT 'plan_attachment_batch_items', i.mrf_plan_id
FROM mrfpipeline.plan_attachment_batch_items i
JOIN mrfpipeline.plan_attachment_batches b ON b.id = i.plan_attachment_batch_id
JOIN mrfpipeline.mrf_snapshots s ON s.id = b.mrf_snapshot_id
JOIN mrfpipeline.monthly_release_outputs o ON o.mrf_snapshot_id = s.id
JOIN mrfpipeline.monthly_releases r
  ON r.payer_id = o.payer_id AND r.collection_month = o.collection_month
WHERE r.status IN ('active', 'inactive') AND i.created_at > o.published_at
ORDER BY 1, 2`)
	if err != nil {
		return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	for rows.Next() {
		var table string
		var domainID int64
		if err := rows.Scan(&table, &domainID); err != nil {
			return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
		}
		report.recordSealedDomain(report.logger, table, jobs.KindConsumerAttachPlans, domainID)
	}
	if err := rows.Err(); err != nil {
		return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	rows.Close()

	rows, err = pool.Query(ctx, `
SELECT s.id
FROM mrfpipeline.monthly_release_outputs o
JOIN mrfpipeline.mrf_snapshots s ON s.id = o.mrf_snapshot_id
JOIN mrfpipeline.monthly_releases r
  ON r.payer_id = o.payer_id AND r.collection_month = o.collection_month
WHERE r.status IN ('active', 'inactive')
ORDER BY s.id`)
	if err != nil {
		return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	sealed := make([]int64, 0)
	for rows.Next() {
		var snapshotID int64
		if err := rows.Scan(&snapshotID); err != nil {
			return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
		}
		sealed = append(sealed, snapshotID)
	}
	if err := rows.Err(); err != nil {
		return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	rows.Close()

	expectedRows, err := pool.Query(ctx, `
SELECT b.mrf_snapshot_id, b.id
FROM mrfpipeline.plan_attachment_batches b
JOIN mrfpipeline.monthly_release_outputs o ON o.mrf_snapshot_id = b.mrf_snapshot_id
JOIN mrfpipeline.monthly_releases r
  ON r.payer_id = o.payer_id AND r.collection_month = o.collection_month
WHERE r.status IN ('active', 'inactive')
  AND b.status = 'succeeded'
  AND b.added_plan_count > 0
ORDER BY b.mrf_snapshot_id, b.id`)
	if err != nil {
		return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	expected := make(map[int64]map[string]int64)
	for expectedRows.Next() {
		var snapshotID, batchID int64
		if err := expectedRows.Scan(&snapshotID, &batchID); err != nil {
			expectedRows.Close()
			return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
		}
		if expected[snapshotID] == nil {
			expected[snapshotID] = make(map[string]int64)
		}
		expected[snapshotID][fmt.Sprintf("plan-batch-%d-part-00000.parquet", batchID)] = batchID
	}
	if err := expectedRows.Err(); err != nil {
		expectedRows.Close()
		return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	expectedRows.Close()

	for _, snapshot := range sealed {
		outputDir := filepath.Join(warehouse, "plan_associations", "output_id=mrf-"+strconv.FormatInt(snapshot, 10))
		batchIDs := make([]int64, 0, len(expected[snapshot]))
		for _, batchID := range expected[snapshot] {
			batchIDs = append(batchIDs, batchID)
		}
		inventoryErr := consumeringest.InspectPlanAssociations(warehouse, "mrf-"+strconv.FormatInt(snapshot, 10), batchIDs)
		if inventoryErr != nil && consumeringest.IsPublicationUnreadable(inventoryErr) {
			return artFail()
		}
		scanAffected := false
		entries, err := os.ReadDir(outputDir)
		if errors.Is(err, os.ErrNotExist) {
			for _, batchID := range expected[snapshot] {
				scanAffected = true
				report.recordSealedDomain(report.logger, "plan_attachment_batches", jobs.KindConsumerAttachPlans, batchID)
			}
			if inventoryErr != nil && !scanAffected {
				report.recordSealedSnapshot(report.logger, jobs.KindConsumerAttachPlans, snapshot)
			}
			continue
		}
		if err != nil {
			return artFail()
		}
		seen := make(map[string]struct{}, len(entries))
		for _, entry := range entries {
			seen[entry.Name()] = struct{}{}
			info, statErr := os.Lstat(filepath.Join(outputDir, entry.Name()))
			if statErr != nil {
				return artFail()
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				scanAffected = true
				report.recordSealedSnapshot(report.logger, jobs.KindConsumerAttachPlans, snapshot)
				continue
			}
			if _, ok := expected[snapshot][entry.Name()]; ok {
				continue
			}
			scanAffected = true
			report.recordSealedSnapshot(report.logger, jobs.KindConsumerAttachPlans, snapshot)
		}
		for name, batchID := range expected[snapshot] {
			if _, ok := seen[name]; !ok {
				scanAffected = true
				report.recordSealedDomain(report.logger, "plan_attachment_batches", jobs.KindConsumerAttachPlans, batchID)
			}
		}
		if inventoryErr != nil && !scanAffected {
			report.recordSealedSnapshot(report.logger, jobs.KindConsumerAttachPlans, snapshot)
		}
	}

	activeRows, err := pool.Query(ctx, `
SELECT s.id, s.payer_id, to_char(s.collection_month, 'YYYY-MM')
FROM mrfpipeline.monthly_release_outputs o
JOIN mrfpipeline.mrf_snapshots s ON s.id = o.mrf_snapshot_id
JOIN mrfpipeline.monthly_releases r
  ON r.payer_id = o.payer_id AND r.collection_month = o.collection_month
WHERE r.status = 'active'
ORDER BY s.id`)
	if err != nil {
		return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	for activeRows.Next() {
		var snapshotID int64
		var payer, month string
		if err := activeRows.Scan(&snapshotID, &payer, &month); err != nil {
			activeRows.Close()
			return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
		}
		outputID := "mrf-" + strconv.FormatInt(snapshotID, 10)
		if err := consumeringest.InspectCompletedSnapshot(warehouse, payer, month, outputID); err != nil {
			if consumeringest.IsPublicationUnreadable(err) {
				activeRows.Close()
				return artFail()
			}
			report.recordSealedSnapshot(report.logger, jobs.KindConsumerIngest, snapshotID)
		}
	}
	if err := activeRows.Err(); err != nil {
		activeRows.Close()
		return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	activeRows.Close()
	return nil
}
