package planattach

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/enotpoloskun/mrfconsumer"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/consumeringest"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/planbatch"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// Worker executes consumer.attach_plans and marks the batch succeeded.
type Worker struct {
	river.WorkerDefaults[jobs.ConsumerAttachPlansArgs]
	Pool          *pgxpool.Pool
	Workspace     *artifact.Workspace
	WarehousePath string
	Attach        AttachFunc
	Logger        *slog.Logger
	Health        func(context.Context) error
}

func (w *Worker) Work(ctx context.Context, job *river.Job[jobs.ConsumerAttachPlansArgs]) error {
	var ident claimIdentity
	var added int64
	client := river.ClientFromContext[pgx.Tx](ctx)
	return jobs.Run(ctx, jobs.RunParams{
		Pool:        w.Pool,
		Client:      client,
		Spec:        jobs.ConsumerAttachPlansStage,
		DomainID:    job.Args.PlanAttachmentBatchID,
		RiverJobID:  job.ID,
		Attempt:     job.Attempt,
		MaxAttempts: job.MaxAttempts,
		Claim: func(ctx context.Context) (jobs.ClaimResult, error) {
			res, claimed, err := claimAttach(ctx, w.Pool, job.Args.PlanAttachmentBatchID, job.ID)
			ident = claimed
			return res, err
		},
		Work: func(ctx context.Context) error {
			n, err := w.attach(ctx, ident)
			added = n
			return err
		},
		Confirm: func(ctx context.Context, tx pgx.Tx) error {
			return confirmAttachSuccess(ctx, tx, client, ident, added)
		},
		PreLock: func(ctx context.Context, tx pgx.Tx) error {
			return lockSnapshotForSucceed(ctx, tx, ident.SnapshotID)
		},
	})
}

func (w *Worker) attach(ctx context.Context, ident claimIdentity) (int64, error) {
	if w == nil || w.Workspace == nil || ident.BatchID <= 0 || ident.SnapshotID <= 0 {
		return 0, jobs.Failure(jobs.FailureDomainInvariant)
	}
	rows, err := loadFrozenPlans(ctx, w.Pool, ident)
	if err != nil {
		return 0, err
	}
	canonical, err := encodePlans(rows)
	if err != nil {
		return 0, err
	}
	if int64(len(rows)) != ident.RequestedCount {
		return 0, jobs.Failure(jobs.FailurePlanAttachInvariant)
	}
	plansFile, err := publishPlansJSON(w.Workspace, ident.BatchID, canonical)
	if err != nil {
		return 0, err
	}

	if err := consumeringest.InspectCompletedSnapshot(w.WarehousePath, ident.PayerID, ident.MonthText, consumeringest.FormatSnapshotOutputID(ident.SnapshotID)); err != nil {
		return 0, jobs.Failure(jobs.FailurePlanAttachOutputInvalid)
	}

	outputID := consumeringest.FormatSnapshotOutputID(ident.SnapshotID)
	batchName := formatBatchID(ident.BatchID)
	if w.Health != nil {
		if err := w.Health(ctx); err != nil {
			return 0, err
		}
	}
	cfg := mrfconsumer.AttachPlansConfig{
		PlansPath:   plansFile,
		OutputPath:  w.WarehousePath,
		OutputID:    outputID,
		PlanBatchID: batchName,
	}
	report, err := resolveAttach(w.Attach)(ctx, cfg)
	if err != nil {
		return 0, mapWorkError(err)
	}
	if report.OutputID != outputID || report.PlanBatchID != batchName {
		return 0, jobs.Failure(jobs.FailurePlanAttachOutputInvalid)
	}
	if report.AddedPlanCount < 0 || report.AddedPlanCount > ident.RequestedCount {
		return 0, jobs.Failure(jobs.FailurePlanAttachOutputInvalid)
	}
	if report.AddedPlanCount > 0 {
		part := filepath.Join(w.WarehousePath, "plan_associations", "output_id="+outputID, batchName+"-part-00000.parquet")
		info, err := os.Lstat(part)
		if err != nil || isSymlink(info) || !info.Mode().IsRegular() {
			return 0, jobs.Failure(jobs.FailurePlanAttachOutputInvalid)
		}
	}
	return report.AddedPlanCount, nil
}

func isSymlink(info os.FileInfo) bool {
	return info.Mode()&os.ModeSymlink != 0
}

func loadFrozenPlans(ctx context.Context, pool *pgxpool.Pool, ident claimIdentity) ([]planRow, error) {
	if pool == nil {
		return nil, jobs.Failure(jobs.FailurePlanAttachDatabaseFailed)
	}
	rows, err := pool.Query(ctx, `
SELECT p.plan_name, p.issuer_name, p.plan_sponsor_name, p.plan_id_type, p.plan_id, p.plan_market_type
FROM mrfpipeline.plan_attachment_batch_items i
JOIN mrfpipeline.mrf_plans p ON p.id = i.mrf_plan_id
WHERE i.plan_attachment_batch_id = $1
ORDER BY p.plan_name, p.issuer_name, p.plan_id_type, p.plan_id, p.plan_market_type`, ident.BatchID)
	if err != nil {
		return nil, classifyDB(ctx, err)
	}
	defer rows.Close()
	var out []planRow
	for rows.Next() {
		var row planRow
		if err := rows.Scan(&row.PlanName, &row.IssuerName, &row.PlanSponsorName, &row.PlanIDType, &row.PlanID, &row.PlanMarketType); err != nil {
			return nil, classifyDB(ctx, err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, classifyDB(ctx, err)
	}
	return out, nil
}

func confirmAttachSuccess(ctx context.Context, tx pgx.Tx, client *river.Client[pgx.Tx], ident claimIdentity, added int64) error {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if tx == nil || ident.BatchID <= 0 || ident.SnapshotID <= 0 {
		return jobs.Failure(jobs.FailurePlanAttachDatabaseFailed)
	}
	if added < 0 {
		return jobs.Failure(jobs.FailurePlanAttachInvariant)
	}

	locked, err := lockSnapshotThenBatch(ctx, tx, ident.SnapshotID, ident.BatchID)
	if err != nil {
		return err
	}
	if locked.PayerID != ident.PayerID || locked.MonthText != ident.MonthText {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}

	var status string
	var stored *int64
	var requested int64
	if err := tx.QueryRow(ctx, `
SELECT status, river_job_id, requested_plan_count
FROM mrfpipeline.plan_attachment_batches
WHERE id = $1`, ident.BatchID).Scan(&status, &stored, &requested); err != nil {
		return classifyDB(ctx, err)
	}
	if stored == nil || *stored != ident.RiverJobID {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	if status == jobs.StatusSucceeded {
		return nil
	}
	if status != jobs.StatusRunning {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	if requested != ident.RequestedCount {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	ident.RequestedCount = requested
	if locked.Consume != jobs.StatusSucceeded {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	if err := requireFrozenItems(ctx, tx, ident); err != nil {
		return err
	}

	tag, err := tx.Exec(ctx, `
UPDATE mrfpipeline.plan_attachment_batches
SET status = $2,
    added_plan_count = $3,
    failure_code = NULL,
    completed_at = transaction_timestamp(),
    updated_at = transaction_timestamp()
WHERE id = $1`, ident.BatchID, jobs.StatusSucceeded, added)
	if err != nil || tag.RowsAffected() != 1 {
		return classifyDB(ctx, err)
	}
	if _, err := planbatch.Schedule(ctx, tx, client, ident.SnapshotID); err != nil {
		return err
	}
	return nil
}
