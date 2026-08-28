package reconcile

import (
	"context"
	"log/slog"

	"github.com/enotpoloskun/mrfpipeline/internal/admission"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/mrfparse"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// releaseEmptyTerminalDownloads closes the small crash window in which a
// terminal download was durably marked failed before its terminal hook could
// release an empty slot. It deliberately only releases a slot after both
// physical artifact checks pass, and serializes the repair by source lock.
func releaseEmptyTerminalDownloads(ctx context.Context, pool *pgxpool.Pool, ws *artifact.Workspace, loggers ...*slog.Logger) error {
	query := `
SELECT s.id
FROM mrfpipeline.mrf_sources s
JOIN mrfpipeline.mrf_materialization_slots x ON x.mrf_source_id = s.id
WHERE s.download_status = 'failed'
  AND EXISTS (SELECT 1 FROM mrfpipeline.monthly_release_mrf_sources a WHERE a.mrf_source_id = s.id)
  AND s.id > $1
	ORDER BY s.id LIMIT $2`
	var after int64
	for {
		ids, err := pageIDs(ctx, pool, query, after, 100)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			busy, err := jobs.WithExecutionLock(ctx, pool, jobs.LockNamespaceMRF, id, func(ctx context.Context) error {
				raw, err := ws.InspectDownloadState(artifact.KindMRF, id)
				if err != nil {
					return artFail()
				}
				staging, err := ws.HasDownloadStaging(artifact.KindMRF, id)
				if err != nil {
					return artFail()
				}
				parserStaging, err := ws.HasMRFParserTemp(id)
				if err != nil {
					return artFail()
				}
				if raw != artifact.DownloadAbsent || staging || parserStaging {
					return nil
				}
				return admission.ReleaseSlotAndWake(ctx, pool, id, loggers...)
			})
			if err != nil {
				return err
			}
			_ = busy // A concurrent delivery will leave the source for the next pass.
			after = id
		}
	}
	return nil
}

// releaseTerminalParses converges selected terminal parses to an empty
// occupancy. It also removes invalid unpublished parsed output when no slot
// remains, but never reopens a failed parse.
func releaseTerminalParses(ctx context.Context, pool *pgxpool.Pool, client *river.Client[pgx.Tx], ws *artifact.Workspace, servicesPath string, loggers ...*slog.Logger) error {
	query := `
SELECT DISTINCT s.id
FROM mrfpipeline.mrf_sources s
JOIN mrfpipeline.monthly_release_mrf_sources a ON a.mrf_source_id = s.id
JOIN mrfpipeline.monthly_releases r
  ON r.payer_id = a.payer_id AND r.collection_month = a.collection_month
WHERE s.parse_status = 'failed'
  AND r.status IN ('building', 'active')
  AND s.id > $1
ORDER BY s.id LIMIT $2`
	var after int64
	for {
		ids, err := pageIDs(ctx, pool, query, after, 100)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			busy, err := jobs.WithExecutionLock(ctx, pool, jobs.LockNamespaceMRF, id, func(ctx context.Context) error {
				var held bool
				if err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM mrfpipeline.mrf_materialization_slots
    WHERE mrf_source_id = $1
)`, id).Scan(&held); err != nil {
					return dbFail(ctx.Err())
				}
				if !held {
					_, err := mrfparse.CleanupUnpublishedParsed(ctx, ws, id, servicesPath)
					return err
				}
				return mrfparse.CleanupTerminal(ctx, pool, client, ws, id, servicesPath, loggers...)
			})
			if err != nil {
				return err
			}
			_ = busy // A concurrent delivery will leave the source for the next pass.
			after = id
		}
	}
	return nil
}
