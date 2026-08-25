-- Redacted diagnostic query. Change stale_after to a positive interval when
-- the default 30-minute threshold is not appropriate.
\set stale_after '30 minutes'

WITH stages(stage, domain_id, status, river_job_id, updated_at, started_at, arg_key) AS (
    SELECT 'discovery.run', d.id, d.status, d.river_job_id, d.updated_at, d.started_at, 'discovery_run_id'
    FROM mrfpipeline.discovery_runs d
    UNION ALL
    SELECT 'toc.download', t.id, t.download_status, t.download_river_job_id, t.updated_at, NULL::timestamptz, 'toc_file_id'
    FROM mrfpipeline.toc_files t
    UNION ALL
    SELECT 'toc.parse', t.id, t.parse_status, t.parse_river_job_id, t.updated_at, NULL::timestamptz, 'toc_file_id'
    FROM mrfpipeline.toc_files t
    UNION ALL
    SELECT 'toc.import', t.id, t.import_status, t.import_river_job_id, t.updated_at, NULL::timestamptz, 'toc_file_id'
    FROM mrfpipeline.toc_files t
    UNION ALL
    SELECT 'mrf.download', s.id, s.download_status, s.download_river_job_id, s.updated_at, NULL::timestamptz, 'mrf_source_id'
    FROM mrfpipeline.mrf_sources s
    UNION ALL
    SELECT 'mrf.parse', s.id, s.parse_status, s.parse_river_job_id, s.updated_at, NULL::timestamptz, 'mrf_source_id'
    FROM mrfpipeline.mrf_sources s
    UNION ALL
    SELECT 'consumer.ingest', s.id, s.consume_status, s.consume_river_job_id, s.updated_at, NULL::timestamptz, 'mrf_snapshot_id'
    FROM mrfpipeline.mrf_snapshots s
    UNION ALL
    SELECT 'consumer.attach_plans', b.id, b.status, b.river_job_id, b.updated_at, b.started_at, 'plan_attachment_batch_id'
    FROM mrfpipeline.plan_attachment_batches b
), old AS (
    SELECT stage, domain_id, status, river_job_id, arg_key,
           CASE WHEN status = 'running' THEN COALESCE(started_at, updated_at) ELSE updated_at END AS age_from
    FROM stages
    WHERE status IN ('pending', 'running')
)
SELECT o.stage,
       o.domain_id,
       o.status,
       o.river_job_id,
       j.attempt,
       GREATEST(0, floor(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - o.age_from))))::bigint AS age_seconds
FROM old o
LEFT JOIN mrfpipeline_river.river_job j
  ON j.id = o.river_job_id
 AND j.kind = o.stage
 AND j.state IN ('available', 'pending', 'scheduled', 'retryable', 'running')
 AND j.args = jsonb_build_object(o.arg_key, to_jsonb(o.domain_id))
WHERE o.age_from <= CURRENT_TIMESTAMP - :'stale_after'::interval
ORDER BY age_seconds DESC, o.stage, o.domain_id;
