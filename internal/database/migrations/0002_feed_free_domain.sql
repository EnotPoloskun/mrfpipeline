DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM mrfpipeline.discovery_runs)
        OR EXISTS (SELECT 1 FROM mrfpipeline.toc_files)
        OR EXISTS (SELECT 1 FROM mrfpipeline.discovery_run_toc_files)
        OR EXISTS (SELECT 1 FROM mrfpipeline.mrf_feeds)
        OR EXISTS (SELECT 1 FROM mrfpipeline.mrf_sources)
        OR EXISTS (SELECT 1 FROM mrfpipeline.mrf_snapshots)
        OR EXISTS (SELECT 1 FROM mrfpipeline.toc_mrf_plan_associations)
        OR EXISTS (SELECT 1 FROM mrfpipeline.mrf_plans)
        OR EXISTS (SELECT 1 FROM mrfpipeline.plan_attachment_batches)
        OR EXISTS (SELECT 1 FROM mrfpipeline.plan_attachment_batch_items)
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'feed-free migration requires a new pipeline database and warehouse';
    END IF;
END
$$;

ALTER TABLE mrfpipeline.discovery_runs
    DROP CONSTRAINT discovery_runs_payer_id_check,
    ADD CONSTRAINT discovery_runs_payer_id_format_check
        CHECK (payer_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$');

ALTER TABLE mrfpipeline.toc_files
    DROP CONSTRAINT toc_files_payer_id_check,
    ADD CONSTRAINT toc_files_payer_id_format_check
        CHECK (payer_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$');

ALTER TABLE mrfpipeline.mrf_snapshots
    DROP CONSTRAINT mrf_snapshots_mrf_source_id_fkey,
    DROP CONSTRAINT mrf_snapshots_mrf_feed_id_fkey,
    DROP CONSTRAINT mrf_snapshots_source_feed_month_key,
    DROP COLUMN mrf_feed_id,
    ADD COLUMN payer_id text NOT NULL,
    ADD CONSTRAINT mrf_snapshots_payer_id_format_check
        CHECK (payer_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    ADD CONSTRAINT mrf_snapshots_source_payer_month_key
        UNIQUE (mrf_source_id, payer_id, collection_month);

ALTER TABLE mrfpipeline.mrf_sources
    DROP CONSTRAINT mrf_sources_source_url_key,
    ADD COLUMN collection_month date NOT NULL,
    ADD CONSTRAINT mrf_sources_collection_month_check
        CHECK (collection_month = date_trunc('month', collection_month)::date),
    ADD CONSTRAINT mrf_sources_source_url_collection_month_key
        UNIQUE (source_url, collection_month),
    ADD CONSTRAINT mrf_sources_id_collection_month_key
        UNIQUE (id, collection_month);

ALTER TABLE mrfpipeline.mrf_snapshots
    ADD CONSTRAINT mrf_snapshots_source_collection_month_fkey
        FOREIGN KEY (mrf_source_id, collection_month)
        REFERENCES mrfpipeline.mrf_sources (id, collection_month)
        ON DELETE RESTRICT;

DROP TABLE mrfpipeline.mrf_feeds;
