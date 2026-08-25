package jobs

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
)

// InsertTx inserts a typed River job on a caller-owned transaction. It never
// opens or commits a transaction. Insert options are taken only from the job
// args (queue). UniqueOpts, tags, and metadata are never set.
func InsertTx(ctx context.Context, client *river.Client[pgx.Tx], tx pgx.Tx, args river.JobArgs) (int64, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if client == nil || tx == nil || args == nil {
		return 0, jobErr("insert")
	}
	res, err := client.InsertTx(ctx, tx, args, nil)
	if err != nil {
		return 0, classifyJob(ctx, "insert", err)
	}
	if res == nil || res.Job == nil || res.Job.ID <= 0 {
		return 0, jobErr("insert")
	}
	if res.UniqueSkippedAsDuplicate {
		return 0, jobErr("insert")
	}
	return res.Job.ID, nil
}

// InsertCoalescedTx publishes a unique wake-up and treats an existing
// equivalent River job as success.
func InsertCoalescedTx(ctx context.Context, client *river.Client[pgx.Tx], tx pgx.Tx, args river.JobArgs) error {
	if ctx == nil {
		panic("nil context")
	}
	if client == nil || tx == nil || args == nil {
		return jobErr("insert")
	}
	res, err := client.InsertTx(ctx, tx, args, nil)
	if err != nil {
		return classifyJob(ctx, "insert", err)
	}
	if res == nil {
		return jobErr("insert")
	}
	if res.UniqueSkippedAsDuplicate {
		return nil
	}
	if res.Job == nil || res.Job.ID <= 0 {
		return jobErr("insert")
	}
	return nil
}
