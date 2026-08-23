package reconcile

import (
	"context"
	"fmt"
	"os"
	"time"

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
	if err := cleanMRFDownloads(ctx, pool, ws, servicesPath, report); err != nil {
		return err
	}
	if err := cleanTOCParsed(ctx, pool, ws, report); err != nil {
		return err
	}
	if err := cleanPlanBatches(ctx, pool, ws, report); err != nil {
		return err
	}
	return cleanStaging(ws, report)
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
			n, err := cleanOneMRFDownload(ctx, pool, ws, servicesPath, id)
			if err != nil {
				return err
			}
			report.CleanedArtifactCount += n
			after = id
		}
	}
}

func cleanOneMRFDownload(ctx context.Context, pool *pgxpool.Pool, ws *artifact.Workspace, servicesPath string, id int64) (int, error) {
	state, err := ws.InspectDownloadState(artifact.KindMRF, id)
	if err != nil {
		return 0, artFail()
	}
	if state == artifact.DownloadAbsent {
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
	if err := ws.RemoveDownload(artifact.KindMRF, id); err != nil {
		return 0, artFail()
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

func cleanStaging(ws *artifact.Workspace, report *Report) error {
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
		info, err := e.Info()
		if err != nil {
			return artFail()
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
		_ = info
		if err := ws.RemoveStagingEntry(e.Name(), st); err != nil {
			return artFail()
		}
		report.CleanedArtifactCount++
	}
	return nil
}

func formatMonth(d time.Time) string {
	return fmt.Sprintf("%04d-%02d", d.Year(), int(d.Month()))
}
