package reconcile

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/admission"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/mrfparse"
	"github.com/enotpoloskun/mrfpipeline/internal/tocparse"
	"github.com/jackc/pgx/v5/pgxpool"
)

const stagingAge = 24 * time.Hour

func cleanArtifacts(ctx context.Context, pool *pgxpool.Pool, ws *artifact.Workspace, servicesPath string, report *Report) error {
	if err := cleanTOCDownloads(ctx, pool, ws, report); err != nil {
		return err
	}
	if err := cleanTOCParsed(ctx, pool, ws, report); err != nil {
		return err
	}
	if err := cleanPlanBatches(ctx, pool, ws, report); err != nil {
		return err
	}
	if err := cleanStaging(ctx, pool, ws, report); err != nil {
		return err
	}
	return cleanMRFDownloads(ctx, pool, ws, servicesPath, report)
}

func cleanTOCDownloads(ctx context.Context, pool *pgxpool.Pool, ws *artifact.Workspace, report *Report) error {
	var after int64
	for {
		ids, err := pageIDs(ctx, pool, `
SELECT id FROM mrfpipeline.toc_files
WHERE parse_status = 'succeeded' AND id > $1
ORDER BY id LIMIT $2`, after, pageSize)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			n, err := cleanOneTOCDownload(ctx, pool, ws, id)
			if err != nil {
				return err
			}
			report.CleanedArtifactCount += n
			after = id
		}
	}
}

func cleanOneTOCDownload(ctx context.Context, pool *pgxpool.Pool, ws *artifact.Workspace, id int64) (int, error) {
	state, err := ws.InspectDownloadState(artifact.KindTOC, id)
	if err != nil {
		return 0, artFail()
	}
	if state == artifact.DownloadAbsent {
		return 0, nil
	}
	var payer, parse string
	var month time.Time
	err = pool.QueryRow(ctx, `
SELECT payer_id, collection_month, parse_status
FROM mrfpipeline.toc_files WHERE id = $1`, id).Scan(&payer, &month, &parse)
	if err != nil {
		return 0, dbFail(ctx.Err())
	}
	if parse != jobs.StatusSucceeded {
		return 0, nil
	}
	parsed, err := ws.ParsedDir(artifact.KindTOC, id)
	if err != nil {
		return 0, artFail()
	}
	data, err := ws.DownloadDataPath(artifact.KindTOC, id)
	if err != nil {
		return 0, artFail()
	}
	tocID, err := artifact.RecordDirName(artifact.KindTOC, id)
	if err != nil {
		return 0, artFail()
	}
	if _, err := tocparse.ValidateCompletedOutput(parsed, tocID, payer, formatMonth(month), data); err != nil {
		return 0, artFail()
	}
	if err := ws.RemoveDownload(artifact.KindTOC, id); err != nil {
		return 0, artFail()
	}
	return 1, nil
}

func cleanMRFDownloads(ctx context.Context, pool *pgxpool.Pool, ws *artifact.Workspace, servicesPath string, report *Report) error {
	var after int64
	for {
		ids, err := pageIDs(ctx, pool, `
SELECT id FROM mrfpipeline.mrf_sources
WHERE parse_status = 'succeeded' AND id > $1
ORDER BY id LIMIT $2`, after, pageSize)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			n, err := cleanOneMRFDownload(ctx, pool, ws, servicesPath, id, report.logger)
			if err != nil {
				return err
			}
			report.CleanedArtifactCount += n
			after = id
		}
	}
}

func cleanOneMRFDownload(ctx context.Context, pool *pgxpool.Pool, ws *artifact.Workspace, servicesPath string, id int64, loggers ...*slog.Logger) (int, error) {
	cleaned := 0
	busy, err := jobs.WithExecutionLock(ctx, pool, jobs.LockNamespaceMRF, id, func(ctx context.Context) error {
		n, err := cleanOneMRFDownloadUnlocked(ctx, pool, ws, servicesPath, id, loggers...)
		cleaned = n
		return err
	})
	if err != nil {
		return 0, err
	}
	if busy {
		return 0, nil
	}
	return cleaned, nil
}

func cleanOneMRFDownloadUnlocked(ctx context.Context, pool *pgxpool.Pool, ws *artifact.Workspace, servicesPath string, id int64, loggers ...*slog.Logger) (int, error) {
	state, err := ws.InspectDownloadState(artifact.KindMRF, id)
	if err != nil {
		return 0, artFail()
	}
	downloadStaging, err := ws.HasDownloadStaging(artifact.KindMRF, id)
	if err != nil {
		return 0, artFail()
	}
	parserStaging, err := ws.HasMRFParserTemp(id)
	if err != nil {
		return 0, artFail()
	}
	if state == artifact.DownloadAbsent && !downloadStaging && !parserStaging {
		return 0, nil
	}
	var parse string
	if err := pool.QueryRow(ctx, `
SELECT parse_status FROM mrfpipeline.mrf_sources WHERE id = $1`, id).Scan(&parse); err != nil {
		return 0, dbFail(ctx.Err())
	}
	if parse != jobs.StatusSucceeded {
		return 0, nil
	}
	parsed, err := ws.ParsedDir(artifact.KindMRF, id)
	if err != nil {
		return 0, artFail()
	}
	data, err := ws.DownloadDataPath(artifact.KindMRF, id)
	if err != nil {
		return 0, artFail()
	}
	if err := mrfparse.ValidateCompletedOutput(parsed, mrfparse.ExpectedSourceURI(data), servicesPath); err != nil {
		return 0, artFail()
	}
	if state != artifact.DownloadAbsent {
		if err := ws.RemoveDownload(artifact.KindMRF, id); err != nil {
			return 0, artFail()
		}
	}
	if parserStaging {
		if err := ws.RemoveMRFParserTemp(id); err != nil {
			return 0, artFail()
		}
	}
	// Download staging is owned by the downloader and is removed by the
	// source-locked staging sweep. Keep the slot until that cleanup converges.
	downloadStaging, err = ws.HasDownloadStaging(artifact.KindMRF, id)
	if err != nil {
		return 0, artFail()
	}
	parserStaging, err = ws.HasMRFParserTemp(id)
	if err != nil {
		return 0, artFail()
	}
	if downloadStaging || parserStaging {
		return 0, nil
	}
	if err := admission.ReleaseSlotAndWake(ctx, pool, id, loggers...); err != nil {
		return 0, err
	}
	return 1, nil
}

func cleanTOCParsed(ctx context.Context, pool *pgxpool.Pool, ws *artifact.Workspace, report *Report) error {
	var after int64
	for {
		ids, err := pageIDs(ctx, pool, `
SELECT id FROM mrfpipeline.toc_files
WHERE import_status = 'succeeded' AND id > $1
ORDER BY id LIMIT $2`, after, pageSize)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			state, err := ws.InspectParsed(artifact.KindTOC, id)
			if err != nil {
				return artFail()
			}
			if state == artifact.ParsedAbsent {
				after = id
				continue
			}
			if err := ws.RemoveParsed(artifact.KindTOC, id); err != nil {
				return artFail()
			}
			report.CleanedArtifactCount++
			after = id
		}
	}
}

func cleanPlanBatches(ctx context.Context, pool *pgxpool.Pool, ws *artifact.Workspace, report *Report) error {
	var after int64
	for {
		ids, err := pageIDs(ctx, pool, `
SELECT id FROM mrfpipeline.plan_attachment_batches
WHERE status = 'succeeded' AND id > $1
ORDER BY id LIMIT $2`, after, pageSize)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			n, err := cleanOnePlanBatch(ctx, pool, ws, id)
			if err != nil {
				return err
			}
			report.CleanedArtifactCount += n
			after = id
		}
	}
}

func cleanOnePlanBatch(ctx context.Context, pool *pgxpool.Pool, ws *artifact.Workspace, id int64) (int, error) {
	dir, err := ws.PlanBatchDir(id)
	if err != nil {
		return 0, artFail()
	}
	if _, err := os.Lstat(dir); os.IsNotExist(err) {
		return 0, nil
	} else if err != nil {
		return 0, artFail()
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, dbFail(ctx.Err())
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var status string
	var got int64
	err = tx.QueryRow(ctx, `
SELECT id, status FROM mrfpipeline.plan_attachment_batches WHERE id = $1 FOR UPDATE`, id).Scan(&got, &status)
	if err != nil {
		return 0, dbFail(ctx.Err())
	}
	if got != id || status != jobs.StatusSucceeded {
		if err := tx.Commit(ctx); err != nil {
			return 0, dbFail(ctx.Err())
		}
		return 0, nil
	}
	if err := ws.RemovePlanBatch(id); err != nil {
		return 0, artFail()
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, dbFail(ctx.Err())
	}
	return 1, nil
}

func cleanStaging(ctx context.Context, pool *pgxpool.Pool, ws *artifact.Workspace, report *Report) error {
	dir := ws.StagingDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return artFail()
	}
	cutoff := time.Now().Add(-stagingAge)
	for _, e := range entries {
		// Parser temp parents are source-owned. Their anonymous children must
		// never be removed by the generic age-based orphan sweep; the parser
		// worker removes its own parent after completion.
		if strings.HasPrefix(e.Name(), "mrf-source-") {
			continue
		}
		full := dir + string(os.PathSeparator) + e.Name()
		st, err := os.Lstat(full)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return artFail()
		}
		if st.Mode()&os.ModeSymlink != 0 {
			continue
		}
		if !st.Mode().IsRegular() && !st.IsDir() {
			continue
		}
		if st.ModTime().After(cutoff) {
			continue
		}
		remove := func(context.Context) error {
			if err := ws.RemoveStagingEntry(e.Name(), st); err != nil {
				return artFail()
			}
			return nil
		}
		kind, id, owned := artifact.ParseStagingName(e.Name())
		if owned && kind == artifact.KindMRF {
			if pool == nil {
				return jobs.Failure(jobs.FailureInvalidArguments)
			}
			busy, err := jobs.WithExecutionLock(ctx, pool, jobs.LockNamespaceMRF, id, remove)
			if err != nil {
				return err
			}
			if busy {
				continue
			}
		} else if err := remove(ctx); err != nil {
			return err
		}
		report.CleanedArtifactCount++
	}
	return nil
}

func formatMonth(d time.Time) string {
	return fmt.Sprintf("%04d-%02d", d.Year(), int(d.Month()))
}
