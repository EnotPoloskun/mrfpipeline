package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/admission"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

const riverControlTopic = "river_control"

type riverControlPayload struct {
	Action string `json:"action"`
	JobID  int64  `json:"job_id"`
	Queue  string `json:"queue"`
}

// RepairCancelledMaterialization fails selected mrf.download / mrf.parse /
// toc.download stages whose current River job is already cancelled or
// discarded, then runs the existing terminal occupancy cleanup. It does not
// replace running jobs: those stay interruptions unless the worker observed a
// remote cancel.
func RepairCancelledMaterialization(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], ws *artifact.Workspace, servicesPath string, loggers ...*slog.Logger) error {
	if ctx == nil {
		panic("nil context")
	}
	if pool == nil || client == nil || ws == nil {
		return jobs.Failure(jobs.FailureInvalidArguments)
	}
	logger := loggerOrDiscard(loggers)
	for _, b := range []jobs.KindBinding{
		{Kind: jobs.KindMRFDownload, Spec: jobs.MRFDownloadStage, ArgField: jobs.FieldMRFSourceID},
		{Kind: jobs.KindMRFParse, Spec: jobs.MRFParseStage, ArgField: jobs.FieldMRFSourceID},
		{Kind: jobs.KindTOCDownload, Spec: jobs.TOCDownloadStage, ArgField: jobs.FieldTOCFileID},
	} {
		if err := repairCancelledKind(ctx, pool, client, b); err != nil {
			return err
		}
	}
	if err := releaseTerminalParses(ctx, pool, client, ws, servicesPath, logger); err != nil {
		return err
	}
	if err := removeFailedDownloadArtifacts(ctx, pool, ws); err != nil {
		return err
	}
	return releaseEmptyTerminalDownloads(ctx, pool, ws, logger)
}

func loggerOrDiscard(loggers []*slog.Logger) *slog.Logger {
	if len(loggers) > 0 && loggers[0] != nil {
		return loggers[0]
	}
	return jobs.NewLogger(nil)
}

func repairCancelledKind(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], b jobs.KindBinding) error {
	var after int64
	for {
		ids, err := pageIDs(ctx, pool, `
SELECT `+b.Spec.IDColumn+` FROM `+b.Spec.Table+`
WHERE `+b.Spec.StatusColumn+` IN ('pending', 'running') AND `+b.Spec.IDColumn+` > $1
ORDER BY `+b.Spec.IDColumn+`
LIMIT $2`, after, pageSize)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			if _, err := repairOneCancelled(ctx, pool, client, b, id); err != nil {
				if jobs.IsFailure(err, jobs.FailureSealedReleaseInconsistent) {
					after = id
					continue
				}
				return err
			}
			after = id
		}
	}
}

func repairOneCancelled(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], b jobs.KindBinding, domainID int64) (int, error) {
	if b.Kind == jobs.KindTOCDownload {
		return repairOneCancelledUnlocked(ctx, pool, client, b, domainID)
	}
	inserted := 0
	busy, err := jobs.WithExecutionLock(ctx, pool, jobs.LockNamespaceMRF, domainID, func(ctx context.Context) error {
		n, err := repairOneCancelledUnlocked(ctx, pool, client, b, domainID)
		if err == nil {
			inserted = n
		}
		return err
	})
	if err != nil {
		return 0, err
	}
	if busy {
		return 0, nil
	}
	return inserted, nil
}

func removeFailedDownloadArtifacts(ctx context.Context, pool *pgxpool.Pool, ws *artifact.Workspace) error {
	mrfQuery := `
SELECT s.id
FROM mrfpipeline.mrf_sources s
JOIN mrfpipeline.mrf_materialization_slots x ON x.mrf_source_id = s.id
WHERE s.download_status = 'failed'
  AND EXISTS (SELECT 1 FROM mrfpipeline.monthly_release_mrf_sources a WHERE a.mrf_source_id = s.id)
  AND s.id > $1
ORDER BY s.id LIMIT $2`
	var after int64
	for {
		ids, err := pageIDs(ctx, pool, mrfQuery, after, pageSize)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			busy, err := jobs.WithExecutionLock(ctx, pool, jobs.LockNamespaceMRF, id, func(ctx context.Context) error {
				return ws.RemoveUnpublishedDownload(artifact.KindMRF, id)
			})
			if err != nil {
				return err
			}
			_ = busy
			after = id
		}
	}

	tocQuery := `
SELECT id
FROM mrfpipeline.toc_files
WHERE download_status = 'failed'
  AND id > $1
ORDER BY id LIMIT $2`
	after = 0
	for {
		ids, err := pageIDs(ctx, pool, tocQuery, after, pageSize)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			if err := ws.RemoveUnpublishedDownload(artifact.KindTOC, id); err != nil {
				return err
			}
			after = id
		}
	}
}

func repairOneCancelledUnlocked(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], b jobs.KindBinding, domainID int64) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, dbFail(ctx.Err())
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := release.RequireBuildingForStage(ctx, tx, b.Kind, domainID); err != nil {
		return 0, err
	}
	row, err := lockStage(ctx, tx, b.Spec, domainID)
	if err != nil {
		return 0, err
	}
	if row.status != jobs.StatusPending && row.status != jobs.StatusRunning {
		if err := tx.Commit(ctx); err != nil {
			return 0, dbFail(ctx.Err())
		}
		return 0, nil
	}
	if row.jobID == nil {
		if err := tx.Commit(ctx); err != nil {
			return 0, dbFail(ctx.Err())
		}
		return 0, nil
	}
	job, jerr := client.JobGetTx(ctx, tx, *row.jobID)
	if jerr != nil && !errors.Is(jerr, rivertype.ErrNotFound) {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	if jerr != nil {
		if err := tx.Commit(ctx); err != nil {
			return 0, dbFail(ctx.Err())
		}
		return 0, nil
	}
	if job.Kind != b.Kind {
		return 0, jobs.Failure(jobs.FailureDomainInvariant)
	}
	got, aerr := jobs.DomainIDFromEncodedArgs(b.Kind, job.EncodedArgs)
	if aerr != nil || got != domainID {
		return 0, jobs.Failure(jobs.FailureDomainInvariant)
	}
	switch job.State {
	case rivertype.JobStateCancelled, rivertype.JobStateDiscarded:
		if err := markRiverTerminal(ctx, tx, b.Spec, domainID); err != nil {
			return 0, err
		}
	default:
		if err := tx.Commit(ctx); err != nil {
			return 0, dbFail(ctx.Err())
		}
		return 0, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, dbFail(ctx.Err())
	}
	return 1, nil
}

func materializationCancelQueue(queue string) bool {
	return queue == jobs.QueueMRFDownload || queue == jobs.QueueMRFParse || queue == jobs.QueueTOCDownload
}

func controlNotifyIsMRFCancel(payload string) bool {
	var decoded riverControlPayload
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		return false
	}
	return decoded.Action == "cancel" && materializationCancelQueue(decoded.Queue)
}

// WatchMaterializationCancels listens for River cancel notifications on MRF
// and TOC download queues and wakes control so queued cancels are failed and
// slots released without waiting for an unrelated refill.
func WatchMaterializationCancels(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) {
	if ctx == nil || pool == nil {
		return
	}
	if logger == nil {
		logger = jobs.NewLogger(nil)
	}
	for {
		if ctx.Err() != nil {
			return
		}
		if err := listenMaterializationCancels(ctx, pool, logger); err != nil && ctx.Err() != nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		logger.LogAttrs(ctx, slog.LevelInfo, "mrf_cancel_watch_retry",
			slog.String("failure", jobs.FailureReconciliationDatabaseFailed))
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func listenMaterializationCancels(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	channel := `"` + jobs.RiverSchema + `.` + riverControlTopic + `"`
	if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		_, _ = conn.Exec(cleanup, "UNLISTEN "+channel)
		cancel()
	}()
	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if n == nil || !controlNotifyIsMRFCancel(n.Payload) {
			continue
		}
		if err := admission.Wake(context.WithoutCancel(ctx), pool); err != nil && logger != nil {
			logger.LogAttrs(ctx, slog.LevelInfo, "mrf_cancel_wake_failed",
				slog.String("failure", jobs.FailureReconciliationDatabaseFailed))
			continue
		}
		if logger != nil {
			var decoded riverControlPayload
			_ = json.Unmarshal([]byte(n.Payload), &decoded)
			logger.LogAttrs(ctx, slog.LevelInfo, "mrf_cancel_wake",
				slog.String("queue", decoded.Queue),
				slog.Int64("job_id", decoded.JobID))
		}
	}
}
