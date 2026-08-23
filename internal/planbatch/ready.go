package planbatch

import (
	"context"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
)

// IsPlanReady reports the Story 12 readiness rule: consume succeeded, at
// least one succeeded batch, no unassigned plan, and no unresolved batch.
// A planless snapshot is not plan-ready.
func IsPlanReady(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, snapshotID int64) (bool, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if q == nil || snapshotID <= 0 {
		return false, jobs.Failure(jobs.FailureInvalidArguments)
	}
	var ready bool
	err := q.QueryRow(ctx, `
SELECT
    n.consume_status = 'succeeded'
    AND EXISTS (
        SELECT 1 FROM mrfpipeline.plan_attachment_batches b
        WHERE b.mrf_snapshot_id = n.id AND b.status = 'succeeded'
    )
    AND NOT EXISTS (
        SELECT 1 FROM mrfpipeline.mrf_plans p
        WHERE p.mrf_snapshot_id = n.id
          AND NOT EXISTS (
              SELECT 1 FROM mrfpipeline.plan_attachment_batch_items i
              WHERE i.mrf_plan_id = p.id
          )
    )
    AND NOT EXISTS (
        SELECT 1 FROM mrfpipeline.plan_attachment_batches b
        WHERE b.mrf_snapshot_id = n.id AND b.status IN ('pending', 'running', 'failed')
    )
FROM mrfpipeline.mrf_snapshots n
WHERE n.id = $1`, snapshotID).Scan(&ready)
	if err != nil {
		return false, classifyDB(ctx, err)
	}
	return ready, nil
}
