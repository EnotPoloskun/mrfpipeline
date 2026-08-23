package reconcile

// Redacted status queries. They return numeric IDs and counts only.
const (
	SQLDiscoveryCounts = `
SELECT status, count(*) AS n
FROM mrfpipeline.discovery_runs
GROUP BY status
ORDER BY status`

	SQLTOCStageCounts = `
SELECT download_status, parse_status, import_status, count(*) AS n
FROM mrfpipeline.toc_files
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3`

	SQLMRFSourceCounts = `
SELECT download_status, parse_status, count(*) AS n
FROM mrfpipeline.mrf_sources
GROUP BY 1, 2
ORDER BY 1, 2`

	SQLSnapshotConsumeCounts = `
SELECT consume_status, count(*) AS n
FROM mrfpipeline.mrf_snapshots
GROUP BY consume_status
ORDER BY consume_status`

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
