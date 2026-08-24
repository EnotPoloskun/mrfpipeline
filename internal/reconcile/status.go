package reconcile

// Redacted status queries. They return validated payer/month values, derived
// output IDs, blocker codes, numeric IDs, and aggregate counts only.
const (
	SQLReleaseCounts = `
SELECT status, count(*) AS n
FROM mrfpipeline.monthly_releases
GROUP BY status
ORDER BY status`

	SQLActiveOutputs = `
SELECT r.payer_id, r.collection_month, 'mrf-' || s.id AS output_id
FROM mrfpipeline.monthly_releases r
JOIN mrfpipeline.mrf_snapshots s
  ON s.payer_id = r.payer_id AND s.collection_month = r.collection_month
WHERE r.status = 'active'
ORDER BY r.payer_id, r.collection_month, s.id`

	SQLBuildingReleaseBlockers = `
WITH releases AS (
    SELECT payer_id, collection_month
    FROM mrfpipeline.monthly_releases
    WHERE status = 'building'
)
SELECT payer_id, collection_month, blocker
FROM (
    SELECT r.payer_id, r.collection_month, 'discovery_missing' AS blocker
    FROM releases r
    WHERE NOT EXISTS (
        SELECT 1 FROM mrfpipeline.discovery_runs d
        WHERE d.payer_id = r.payer_id AND d.collection_month = r.collection_month
    )
    UNION ALL
    SELECT r.payer_id, r.collection_month, 'discovery_incomplete'
    FROM releases r
    WHERE EXISTS (
        SELECT 1 FROM mrfpipeline.discovery_runs d
        WHERE d.payer_id = r.payer_id AND d.collection_month = r.collection_month
          AND d.status <> 'succeeded'
    )
    UNION ALL
    SELECT r.payer_id, r.collection_month, 'toc_missing'
    FROM releases r
    WHERE NOT EXISTS (
        SELECT 1 FROM mrfpipeline.toc_files t
        WHERE t.payer_id = r.payer_id AND t.collection_month = r.collection_month
    )
    UNION ALL
    SELECT r.payer_id, r.collection_month, 'toc_incomplete'
    FROM releases r
    WHERE EXISTS (
        SELECT 1 FROM mrfpipeline.toc_files t
        WHERE t.payer_id = r.payer_id AND t.collection_month = r.collection_month
          AND (t.download_status <> 'succeeded' OR t.parse_status <> 'succeeded' OR t.import_status <> 'succeeded')
    )
    UNION ALL
    SELECT r.payer_id, r.collection_month, 'source_incomplete'
    FROM releases r
    WHERE EXISTS (
        SELECT 1 FROM mrfpipeline.mrf_sources m
        WHERE (m.download_status <> 'succeeded' OR m.parse_status <> 'succeeded')
          AND EXISTS (
              SELECT 1 FROM mrfpipeline.mrf_snapshots s
              WHERE s.mrf_source_id = m.id
                AND s.payer_id = r.payer_id AND s.collection_month = r.collection_month
          )
    )
    UNION ALL
    SELECT r.payer_id, r.collection_month, 'snapshot_missing'
    FROM releases r
    WHERE NOT EXISTS (
        SELECT 1 FROM mrfpipeline.mrf_snapshots s
        WHERE s.payer_id = r.payer_id AND s.collection_month = r.collection_month
    )
    UNION ALL
    SELECT r.payer_id, r.collection_month, 'consume_incomplete'
    FROM releases r
    WHERE EXISTS (
        SELECT 1 FROM mrfpipeline.mrf_snapshots s
        WHERE s.payer_id = r.payer_id AND s.collection_month = r.collection_month
          AND s.consume_status <> 'succeeded'
    )
    UNION ALL
    SELECT r.payer_id, r.collection_month, 'attachment_missing'
    FROM releases r
    WHERE EXISTS (
        SELECT 1 FROM mrfpipeline.mrf_snapshots s
        WHERE s.payer_id = r.payer_id AND s.collection_month = r.collection_month
          AND NOT EXISTS (
              SELECT 1 FROM mrfpipeline.plan_attachment_batches b
              WHERE b.mrf_snapshot_id = s.id AND b.status = 'succeeded'
          )
    )
    UNION ALL
    SELECT r.payer_id, r.collection_month, 'attachment_incomplete'
    FROM releases r
    WHERE EXISTS (
        SELECT 1 FROM mrfpipeline.plan_attachment_batches b
        JOIN mrfpipeline.mrf_snapshots s ON s.id = b.mrf_snapshot_id
        WHERE s.payer_id = r.payer_id AND s.collection_month = r.collection_month
          AND b.status IN ('pending', 'running', 'failed')
    )
    UNION ALL
    SELECT r.payer_id, r.collection_month, 'plan_unassigned'
    FROM releases r
    WHERE EXISTS (
        SELECT 1
        FROM mrfpipeline.mrf_plans p
        JOIN mrfpipeline.mrf_snapshots s ON s.id = p.mrf_snapshot_id
        WHERE s.payer_id = r.payer_id AND s.collection_month = r.collection_month
          AND NOT EXISTS (
              SELECT 1
              FROM mrfpipeline.plan_attachment_batch_items i
              JOIN mrfpipeline.plan_attachment_batches b
                ON b.id = i.plan_attachment_batch_id
              WHERE i.mrf_plan_id = p.id
                AND b.mrf_snapshot_id = p.mrf_snapshot_id
                AND b.status = 'succeeded'
          )
    )
    UNION ALL
    SELECT r.payer_id, r.collection_month, 'job_inconsistent'
    FROM releases r
    WHERE EXISTS (
        SELECT 1
        FROM (
            SELECT d.payer_id, d.collection_month, d.river_job_id AS job_id,
                   'discovery.run' AS kind, 'discovery_run_id' AS arg_key,
                   d.id AS domain_id, d.status IN ('pending', 'running') AS nonterminal
            FROM mrfpipeline.discovery_runs d
            UNION ALL
            SELECT t.payer_id, t.collection_month, t.download_river_job_id,
                   'toc.download', 'toc_file_id', t.id,
                   t.download_status IN ('pending', 'running')
            FROM mrfpipeline.toc_files t
            UNION ALL
            SELECT t.payer_id, t.collection_month, t.parse_river_job_id,
                   'toc.parse', 'toc_file_id', t.id,
                   t.parse_status IN ('pending', 'running')
            FROM mrfpipeline.toc_files t
            UNION ALL
            SELECT t.payer_id, t.collection_month, t.import_river_job_id,
                   'toc.import', 'toc_file_id', t.id,
                   t.import_status IN ('pending', 'running')
            FROM mrfpipeline.toc_files t
            UNION ALL
            SELECT x.payer_id, x.collection_month, m.download_river_job_id,
                   'mrf.download', 'mrf_source_id', m.id,
                   m.download_status IN ('pending', 'running')
            FROM mrfpipeline.mrf_sources m
            JOIN (
                SELECT DISTINCT mrf_source_id, payer_id, collection_month
                FROM mrfpipeline.mrf_snapshots
            ) x ON x.mrf_source_id = m.id
            UNION ALL
            SELECT x.payer_id, x.collection_month, m.parse_river_job_id,
                   'mrf.parse', 'mrf_source_id', m.id,
                   m.parse_status IN ('pending', 'running')
            FROM mrfpipeline.mrf_sources m
            JOIN (
                SELECT DISTINCT mrf_source_id, payer_id, collection_month
                FROM mrfpipeline.mrf_snapshots
            ) x ON x.mrf_source_id = m.id
            UNION ALL
            SELECT s.payer_id, s.collection_month, s.consume_river_job_id,
                   'consumer.ingest', 'mrf_snapshot_id', s.id,
                   s.consume_status IN ('pending', 'running')
            FROM mrfpipeline.mrf_snapshots s
            UNION ALL
            SELECT s.payer_id, s.collection_month, b.river_job_id,
                   'consumer.attach_plans', 'plan_attachment_batch_id', b.id,
                   b.status IN ('pending', 'running')
            FROM mrfpipeline.plan_attachment_batches b
            JOIN mrfpipeline.mrf_snapshots s ON s.id = b.mrf_snapshot_id
        ) current_jobs
        WHERE current_jobs.payer_id = r.payer_id
          AND current_jobs.collection_month = r.collection_month
          AND current_jobs.nonterminal
          AND (current_jobs.job_id IS NULL OR NOT EXISTS (
              SELECT 1
              FROM mrfpipeline_river.river_job j
              WHERE j.id = current_jobs.job_id
                AND j.kind = current_jobs.kind
                AND j.state IN ('available', 'pending', 'scheduled', 'retryable', 'running')
                AND jsonb_typeof(j.args) = 'object'
                AND jsonb_object_length(j.args) = 1
                AND j.args ? current_jobs.arg_key
                AND jsonb_typeof(j.args -> current_jobs.arg_key) = 'number'
                AND j.args ->> current_jobs.arg_key = current_jobs.domain_id::text
          ))
    )
) blockers
ORDER BY payer_id, collection_month, blocker`

	SQLDiscoveryCounts = `
SELECT status, count(*) AS n
FROM mrfpipeline.discovery_runs
GROUP BY status
ORDER BY status`

	SQLTOCStageCounts = `
SELECT payer_id, collection_month, download_status, parse_status, import_status, count(*) AS n
FROM mrfpipeline.toc_files
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5`

	SQLMRFSourceCounts = `
SELECT collection_month, download_status, parse_status, count(*) AS n
FROM mrfpipeline.mrf_sources
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3`

	SQLSnapshotConsumeCounts = `
SELECT payer_id, collection_month, consume_status, count(*) AS n
FROM mrfpipeline.mrf_snapshots
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3`

	SQLPlanBatchCounts = `
SELECT status, count(*) AS n,
       coalesce(sum(requested_plan_count), 0) AS requested_total,
       coalesce(sum(added_plan_count), 0) AS added_total
FROM mrfpipeline.plan_attachment_batches
GROUP BY status
ORDER BY status`

	SQLTerminalFailures = `
SELECT 'discovery.run' AS stage, id, failure_code
FROM mrfpipeline.discovery_runs WHERE status = 'failed'
UNION ALL
SELECT 'toc.download', id, failure_code FROM mrfpipeline.toc_files WHERE download_status = 'failed'
UNION ALL
SELECT 'toc.parse', id, failure_code FROM mrfpipeline.toc_files WHERE parse_status = 'failed'
UNION ALL
SELECT 'toc.import', id, failure_code FROM mrfpipeline.toc_files WHERE import_status = 'failed'
UNION ALL
SELECT 'mrf.download', id, failure_code FROM mrfpipeline.mrf_sources WHERE download_status = 'failed'
UNION ALL
SELECT 'mrf.parse', id, failure_code FROM mrfpipeline.mrf_sources WHERE parse_status = 'failed'
UNION ALL
SELECT 'consumer.ingest', id, failure_code FROM mrfpipeline.mrf_snapshots WHERE consume_status = 'failed'
UNION ALL
SELECT 'consumer.attach_plans', id, failure_code FROM mrfpipeline.plan_attachment_batches WHERE status = 'failed'
ORDER BY 1, 2`

	SQLUnassignedPlans = `
SELECT n.id AS snapshot_id, count(*) AS unassigned_plan_count
FROM mrfpipeline.mrf_snapshots n
JOIN mrfpipeline.mrf_plans p ON p.mrf_snapshot_id = n.id
WHERE n.consume_status = 'succeeded'
  AND NOT EXISTS (
      SELECT 1 FROM mrfpipeline.plan_attachment_batch_items i WHERE i.mrf_plan_id = p.id
  )
GROUP BY n.id
ORDER BY n.id`

	SQLPlanReadySnapshots = `
SELECT n.id
FROM mrfpipeline.mrf_snapshots n
WHERE n.consume_status = 'succeeded'
  AND EXISTS (
      SELECT 1 FROM mrfpipeline.plan_attachment_batches b
      WHERE b.mrf_snapshot_id = n.id AND b.status = 'succeeded'
  )
  AND NOT EXISTS (
      SELECT 1 FROM mrfpipeline.mrf_plans p
      WHERE p.mrf_snapshot_id = n.id
        AND NOT EXISTS (
            SELECT 1 FROM mrfpipeline.plan_attachment_batch_items i WHERE i.mrf_plan_id = p.id
        )
  )
  AND NOT EXISTS (
      SELECT 1 FROM mrfpipeline.plan_attachment_batches b
      WHERE b.mrf_snapshot_id = n.id AND b.status IN ('pending', 'running', 'failed')
  )
ORDER BY n.id`

	SQLSharedSources = `
SELECT mrf_source_id, count(DISTINCT toc_file_id) AS toc_count
FROM mrfpipeline.toc_mrf_plan_associations a
JOIN mrfpipeline.mrf_snapshots n ON n.id = a.mrf_snapshot_id
GROUP BY mrf_source_id
HAVING count(DISTINCT toc_file_id) > 1
ORDER BY mrf_source_id`

	// SQLAuthorizedURLDebug is documentation-only. It is not used by default
	// status examples or worker logs. Replace :toc_id or :source_id on an
	// authorized connection.
	SQLAuthorizedURLDebug = `
SELECT 'toc' AS kind, id, source_url
FROM mrfpipeline.toc_files
WHERE id = :toc_id
UNION ALL
SELECT 'mrf', id, source_url
FROM mrfpipeline.mrf_sources
WHERE id = :source_id`
)
