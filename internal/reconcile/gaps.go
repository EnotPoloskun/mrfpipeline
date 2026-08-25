package reconcile

import (
	"context"
	"fmt"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

const pageSize = 100

func restorePrerequisites(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], ws *artifact.Workspace, report *Report) error {
	return restoreTOCDownloads(ctx, pool, client, ws, report)
}

func restoreTOCDownloads(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], ws *artifact.Workspace, report *Report) error {
	var after int64
	for {
		ids, err := pageIDs(ctx, pool, `
SELECT id FROM mrfpipeline.toc_files
WHERE download_status = 'succeeded'
  AND parse_status IN ('blocked', 'pending', 'running')
  AND id > $1
ORDER BY id
LIMIT $2`, after, pageSize)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			n, err := restoreOneDownload(ctx, pool, client, ws, artifact.KindTOC, jobs.TOCDownloadStage, jobs.TOCParseStage, jobs.KindTOCDownload, id)
			if err != nil {
				if jobs.IsFailure(err, jobs.FailureSealedReleaseInconsistent) {
					report.recordSealed(report.logger, jobs.KindTOCDownload, id)
					after = id
					continue
				}
				return err
			}
			report.RepairedJobCount += n
			after = id
		}
	}
}

func restoreOneDownload(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], ws *artifact.Workspace, kind string, downloadSpec, parseSpec jobs.StageSpec, downloadKind string, id int64) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, dbFail(ctx.Err())
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := release.RequireBuildingForStage(ctx, tx, downloadKind, id); err != nil {
		return 0, err
	}
	parsed, err := ws.InspectParsed(kind, id)
	if err != nil {
		return 0, artFail()
	}
	if parsed == artifact.ParsedManifestPresent {
		return 0, nil
	}
	dl, err := ws.InspectDownloadState(kind, id)
	if err != nil {
		return 0, artFail()
	}
	if dl != artifact.DownloadAbsent && dl != artifact.DownloadIncomplete {
		if err := tx.Commit(ctx); err != nil {
			return 0, dbFail(ctx.Err())
		}
		return 0, nil
	}
	drow, err := lockStage(ctx, tx, downloadSpec, id)
	if err != nil {
		return 0, err
	}
	prow, err := lockStage(ctx, tx, parseSpec, id)
	if err != nil {
		return 0, err
	}
	if drow.status != jobs.StatusSucceeded {
		if err := tx.Commit(ctx); err != nil {
			return 0, dbFail(ctx.Err())
		}
		return 0, nil
	}
	if prow.status == jobs.StatusFailed || prow.status == jobs.StatusSucceeded {
		if err := tx.Commit(ctx); err != nil {
			return 0, dbFail(ctx.Err())
		}
		return 0, nil
	}

	if err := clearParseJob(ctx, tx, parseSpec, id); err != nil {
		return 0, err
	}
	args, err := jobs.ArgsFor(downloadKind, id)
	if err != nil {
		return 0, err
	}
	jobID, err := jobs.InsertTx(ctx, client, tx, args)
	if err != nil {
		return 0, err
	}
	if err := setStatus(ctx, tx, downloadSpec, id, jobs.StatusPending, &jobID); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, dbFail(ctx.Err())
	}
	return 1, nil
}

func clearParseJob(ctx context.Context, tx pgx.Tx, spec jobs.StageSpec, domainID int64) error {
	q := fmt.Sprintf(
		`UPDATE %s SET %s = $2, %s = NULL, %s = transaction_timestamp() WHERE %s = $1`,
		spec.Table, spec.StatusColumn, spec.JobIDColumn, spec.UpdatedAtColumn, spec.IDColumn,
	)
	tag, err := tx.Exec(ctx, q, domainID, jobs.StatusBlocked)
	if err != nil || tag.RowsAffected() != 1 {
		return dbFail(ctx.Err())
	}
	return nil
}

func unblockSuccessors(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], report *Report) error {
	type gap struct {
		query string
		spec  jobs.StageSpec
		kind  string
	}
	gaps := []gap{
		{`
SELECT id FROM mrfpipeline.toc_files
WHERE download_status = 'succeeded' AND parse_status = 'blocked' AND parse_river_job_id IS NULL AND id > $1
ORDER BY id LIMIT $2`, jobs.TOCParseStage, jobs.KindTOCParse},
		{`
SELECT id FROM mrfpipeline.toc_files
WHERE parse_status = 'succeeded' AND import_status = 'blocked' AND import_river_job_id IS NULL AND id > $1
ORDER BY id LIMIT $2`, jobs.TOCImportStage, jobs.KindTOCImport},
	}
	for _, g := range gaps {
		if err := unblockGap(ctx, pool, client, g.query, g.spec, g.kind, report); err != nil {
			return err
		}
	}
	return nil
}

func unblockGap(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], query string, spec jobs.StageSpec, kind string, report *Report) error {
	var after int64
	for {
		ids, err := pageIDs(ctx, pool, query, after, pageSize)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			ok, err := unblockOne(ctx, pool, client, spec, kind, id)
			if err != nil {
				if jobs.IsFailure(err, jobs.FailureSealedReleaseInconsistent) {
					report.recordSealed(report.logger, kind, id)
					after = id
					continue
				}
				return err
			}
			if ok {
				report.UnblockedStageCount++
				report.RepairedJobCount++
			}
			after = id
		}
	}
}

func unblockOne(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], spec jobs.StageSpec, kind string, id int64) (bool, error) {
	if kind == jobs.KindMRFParse {
		var changed bool
		busy, err := jobs.WithExecutionLock(ctx, pool, jobs.LockNamespaceMRF, id, func(ctx context.Context) error {
			var err error
			changed, err = unblockOneUnlocked(ctx, pool, client, spec, kind, id)
			return err
		})
		if err != nil {
			return false, err
		}
		if busy {
			return false, nil
		}
		return changed, nil
	}
	return unblockOneUnlocked(ctx, pool, client, spec, kind, id)
}

func unblockOneUnlocked(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], spec jobs.StageSpec, kind string, id int64) (bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, dbFail(ctx.Err())
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := release.RequireBuildingForStage(ctx, tx, kind, id); err != nil {
		return false, err
	}
	row, err := lockStage(ctx, tx, spec, id)
	if err != nil {
		return false, err
	}
	if row.status != jobs.StatusBlocked || row.jobID != nil {
		if err := tx.Commit(ctx); err != nil {
			return false, dbFail(ctx.Err())
		}
		return false, nil
	}
	if _, err := insertAndStore(ctx, tx, client, spec, kind, id); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, dbFail(ctx.Err())
	}
	return true, nil
}

func scheduleParsedSources(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], report *Report) error {
	var after int64
	for {
		ids, err := pageIDs(ctx, pool, `
SELECT id FROM mrfpipeline.mrf_sources
WHERE parse_status = 'succeeded' AND id > $1
ORDER BY id
LIMIT $2`, after, pageSize)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			var needs bool
			if err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM mrfpipeline.mrf_snapshots
    WHERE mrf_source_id = $1
      AND (
          consume_status = 'blocked'
          OR (consume_status IN ('pending', 'running') AND consume_river_job_id IS NULL)
      )
)`, id).Scan(&needs); err != nil {
				return err
			}
			if !needs {
				after = id
				continue
			}
			n, err := scheduleSourceSnapshots(ctx, pool, client, id)
			if err != nil {
				if jobs.IsFailure(err, jobs.FailureSealedReleaseInconsistent) {
					report.recordSealed(report.logger, jobs.KindMRFParse, id)
					after = id
					continue
				}
				return err
			}
			report.RepairedJobCount += n
			report.UnblockedStageCount += n
			after = id
		}
	}
}

func scheduleSourceSnapshots(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], sourceID int64) (int, error) {
	inserted := 0
	busy, err := jobs.WithExecutionLock(ctx, pool, jobs.LockNamespaceMRF, sourceID, func(ctx context.Context) error {
		n, err := scheduleSourceSnapshotsUnlocked(ctx, pool, client, sourceID)
		inserted = n
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

func scheduleSourceSnapshotsUnlocked(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], sourceID int64) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, dbFail(ctx.Err())
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := release.RequireBuildingForStage(ctx, tx, jobs.KindMRFParse, sourceID); err != nil {
		return 0, err
	}
	var parse string
	if err := tx.QueryRow(ctx, `
SELECT parse_status FROM mrfpipeline.mrf_sources WHERE id = $1 FOR UPDATE`, sourceID).Scan(&parse); err != nil {
		return 0, dbFail(ctx.Err())
	}
	if parse != jobs.StatusSucceeded {
		if err := tx.Commit(ctx); err != nil {
			return 0, dbFail(ctx.Err())
		}
		return 0, nil
	}
	rows, err := tx.Query(ctx, `
SELECT id, consume_status, consume_river_job_id
FROM mrfpipeline.mrf_snapshots
WHERE mrf_source_id = $1
  AND EXISTS (
      SELECT 1 FROM mrfpipeline.monthly_release_mrf_sources a
      WHERE a.payer_id = mrf_snapshots.payer_id
        AND a.collection_month = mrf_snapshots.collection_month
        AND a.mrf_source_id = mrf_snapshots.mrf_source_id
  )
ORDER BY id
FOR UPDATE`, sourceID)
	if err != nil {
		return 0, dbFail(ctx.Err())
	}
	type snap struct {
		id     int64
		status string
		jobID  *int64
	}
	var list []snap
	for rows.Next() {
		var s snap
		if err := rows.Scan(&s.id, &s.status, &s.jobID); err != nil {
			rows.Close()
			return 0, dbFail(ctx.Err())
		}
		list = append(list, s)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, dbFail(ctx.Err())
	}
	inserted := 0
	for _, s := range list {
		switch s.status {
		case jobs.StatusBlocked:
			if s.jobID != nil {
				return 0, jobs.Failure(jobs.FailureDomainInvariant)
			}
			if _, err := insertAndStore(ctx, tx, client, jobs.ConsumerIngestStage, jobs.KindConsumerIngest, s.id); err != nil {
				return 0, err
			}
			inserted++
		case jobs.StatusPending, jobs.StatusRunning, jobs.StatusSucceeded, jobs.StatusFailed:
			if s.jobID == nil {
				return 0, jobs.Failure(jobs.FailureDomainInvariant)
			}
		default:
			return 0, jobs.Failure(jobs.FailureDomainInvariant)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, dbFail(ctx.Err())
	}
	return inserted, nil
}
