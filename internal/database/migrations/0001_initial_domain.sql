CREATE TABLE mrfpipeline.discovery_runs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payer_id text NOT NULL,
    collection_month date NOT NULL,
    toc_limit integer,
    status text NOT NULL DEFAULT 'pending',
    discovered_count bigint NOT NULL DEFAULT 0,
    existing_count bigint NOT NULL DEFAULT 0,
    admitted_count bigint NOT NULL DEFAULT 0,
    overflow_count bigint NOT NULL DEFAULT 0,
    river_job_id bigint,
    failure_code text,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT discovery_runs_payer_id_check
        CHECK (payer_id = 'uhc'),
    CONSTRAINT discovery_runs_collection_month_check
        CHECK (collection_month = date_trunc('month', collection_month)::date),
    CONSTRAINT discovery_runs_toc_limit_check
        CHECK (toc_limit IS NULL OR toc_limit > 0),
    CONSTRAINT discovery_runs_status_check
        CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    CONSTRAINT discovery_runs_discovered_count_check
        CHECK (discovered_count >= 0),
    CONSTRAINT discovery_runs_existing_count_check
        CHECK (existing_count >= 0),
    CONSTRAINT discovery_runs_admitted_count_check
        CHECK (admitted_count >= 0),
    CONSTRAINT discovery_runs_overflow_count_check
        CHECK (overflow_count >= 0),
    CONSTRAINT discovery_runs_existing_le_discovered_check
        CHECK (existing_count <= discovered_count),
    CONSTRAINT discovery_runs_admitted_overflow_check
        CHECK (admitted_count + overflow_count <= discovered_count - existing_count),
    CONSTRAINT discovery_runs_count_sum_check
        CHECK (existing_count + admitted_count + overflow_count <= discovered_count),
    CONSTRAINT discovery_runs_admitted_le_limit_check
        CHECK (toc_limit IS NULL OR admitted_count <= toc_limit),
    CONSTRAINT discovery_runs_river_job_id_check
        CHECK (river_job_id IS NULL OR river_job_id > 0),
    CONSTRAINT discovery_runs_terminal_completed_check
        CHECK (
            (status IN ('succeeded', 'failed') AND completed_at IS NOT NULL)
            OR (status NOT IN ('succeeded', 'failed') AND completed_at IS NULL)
        ),
    CONSTRAINT discovery_runs_success_failure_code_check
        CHECK (status <> 'succeeded' OR failure_code IS NULL),
    CONSTRAINT discovery_runs_failure_code_check
        CHECK (failure_code IS NULL OR failure_code <> '')
);

CREATE TABLE mrfpipeline.toc_files (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payer_id text NOT NULL,
    collection_month date NOT NULL,
    source_url text NOT NULL,
    first_discovery_run_id bigint NOT NULL REFERENCES mrfpipeline.discovery_runs (id) ON DELETE RESTRICT,
    download_status text NOT NULL DEFAULT 'pending',
    parse_status text NOT NULL DEFAULT 'blocked',
    import_status text NOT NULL DEFAULT 'blocked',
    download_river_job_id bigint,
    parse_river_job_id bigint,
    import_river_job_id bigint,
    failure_code text,
    first_seen_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT toc_files_payer_id_check
        CHECK (payer_id = 'uhc'),
    CONSTRAINT toc_files_collection_month_check
        CHECK (collection_month = date_trunc('month', collection_month)::date),
    CONSTRAINT toc_files_source_url_check
        CHECK (source_url <> '' AND source_url !~ E'[\r\n]'),
    CONSTRAINT toc_files_download_status_check
        CHECK (download_status IN ('blocked', 'pending', 'running', 'succeeded', 'failed')),
    CONSTRAINT toc_files_parse_status_check
        CHECK (parse_status IN ('blocked', 'pending', 'running', 'succeeded', 'failed')),
    CONSTRAINT toc_files_import_status_check
        CHECK (import_status IN ('blocked', 'pending', 'running', 'succeeded', 'failed')),
    CONSTRAINT toc_files_parse_blocked_check
        CHECK (download_status = 'succeeded' OR parse_status = 'blocked'),
    CONSTRAINT toc_files_import_blocked_check
        CHECK (parse_status = 'succeeded' OR import_status = 'blocked'),
    CONSTRAINT toc_files_download_river_job_id_check
        CHECK (download_river_job_id IS NULL OR download_river_job_id > 0),
    CONSTRAINT toc_files_parse_river_job_id_check
        CHECK (parse_river_job_id IS NULL OR parse_river_job_id > 0),
    CONSTRAINT toc_files_import_river_job_id_check
        CHECK (import_river_job_id IS NULL OR import_river_job_id > 0),
    CONSTRAINT toc_files_failure_code_check
        CHECK (
            (failure_code IS NULL)
            = (
                download_status <> 'failed'
                AND parse_status <> 'failed'
                AND import_status <> 'failed'
            )
        ),
    CONSTRAINT toc_files_failure_code_nonempty_check
        CHECK (failure_code IS NULL OR failure_code <> ''),
    CONSTRAINT toc_files_payer_source_url_key UNIQUE (payer_id, source_url)
);

CREATE TABLE mrfpipeline.discovery_run_toc_files (
    discovery_run_id bigint NOT NULL REFERENCES mrfpipeline.discovery_runs (id) ON DELETE CASCADE,
    toc_file_id bigint NOT NULL REFERENCES mrfpipeline.toc_files (id) ON DELETE CASCADE,
    listing_ordinal bigint NOT NULL,
    was_new boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (discovery_run_id, toc_file_id),
    CONSTRAINT discovery_run_toc_files_listing_ordinal_check
        CHECK (listing_ordinal >= 0),
    CONSTRAINT discovery_run_toc_files_listing_ordinal_key UNIQUE (discovery_run_id, listing_ordinal)
);

CREATE TABLE mrfpipeline.mrf_feeds (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payer_id text NOT NULL,
    feed_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT mrf_feeds_payer_id_uhc_check
        CHECK (payer_id = 'uhc'),
    CONSTRAINT mrf_feeds_payer_id_format_check
        CHECK (payer_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    CONSTRAINT mrf_feeds_feed_id_format_check
        CHECK (feed_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    CONSTRAINT mrf_feeds_payer_feed_key UNIQUE (payer_id, feed_id)
);

CREATE TABLE mrfpipeline.mrf_sources (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_url text NOT NULL,
    first_mrf_filename text,
    download_status text NOT NULL DEFAULT 'pending',
    parse_status text NOT NULL DEFAULT 'blocked',
    download_river_job_id bigint,
    parse_river_job_id bigint,
    failure_code text,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT mrf_sources_source_url_check
        CHECK (source_url <> '' AND source_url !~ E'[\r\n]'),
    CONSTRAINT mrf_sources_source_url_key UNIQUE (source_url),
    CONSTRAINT mrf_sources_first_mrf_filename_check
        CHECK (
            first_mrf_filename IS NULL
            OR (first_mrf_filename <> '' AND first_mrf_filename !~ E'[\r\n]')
        ),
    CONSTRAINT mrf_sources_download_status_check
        CHECK (download_status IN ('blocked', 'pending', 'running', 'succeeded', 'failed')),
    CONSTRAINT mrf_sources_parse_status_check
        CHECK (parse_status IN ('blocked', 'pending', 'running', 'succeeded', 'failed')),
    CONSTRAINT mrf_sources_parse_blocked_check
        CHECK (download_status = 'succeeded' OR parse_status = 'blocked'),
    CONSTRAINT mrf_sources_download_river_job_id_check
        CHECK (download_river_job_id IS NULL OR download_river_job_id > 0),
    CONSTRAINT mrf_sources_parse_river_job_id_check
        CHECK (parse_river_job_id IS NULL OR parse_river_job_id > 0),
    CONSTRAINT mrf_sources_failure_code_check
        CHECK (
            (failure_code IS NULL)
            = (download_status <> 'failed' AND parse_status <> 'failed')
        ),
    CONSTRAINT mrf_sources_failure_code_nonempty_check
        CHECK (failure_code IS NULL OR failure_code <> '')
);

CREATE TABLE mrfpipeline.mrf_snapshots (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mrf_source_id bigint NOT NULL REFERENCES mrfpipeline.mrf_sources (id) ON DELETE RESTRICT,
    mrf_feed_id bigint NOT NULL REFERENCES mrfpipeline.mrf_feeds (id) ON DELETE RESTRICT,
    collection_month date NOT NULL,
    consume_status text NOT NULL DEFAULT 'blocked',
    consume_river_job_id bigint,
    failure_code text,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT mrf_snapshots_collection_month_check
        CHECK (collection_month = date_trunc('month', collection_month)::date),
    CONSTRAINT mrf_snapshots_consume_status_check
        CHECK (consume_status IN ('blocked', 'pending', 'running', 'succeeded', 'failed')),
    CONSTRAINT mrf_snapshots_consume_river_job_id_check
        CHECK (consume_river_job_id IS NULL OR consume_river_job_id > 0),
    CONSTRAINT mrf_snapshots_source_feed_month_key UNIQUE (mrf_source_id, mrf_feed_id, collection_month),
    CONSTRAINT mrf_snapshots_failure_code_check
        CHECK ((failure_code IS NULL) = (consume_status <> 'failed')),
    CONSTRAINT mrf_snapshots_failure_code_nonempty_check
        CHECK (failure_code IS NULL OR failure_code <> '')
);

CREATE TABLE mrfpipeline.toc_mrf_plan_associations (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    toc_file_id bigint NOT NULL REFERENCES mrfpipeline.toc_files (id) ON DELETE RESTRICT,
    mrf_snapshot_id bigint NOT NULL REFERENCES mrfpipeline.mrf_snapshots (id) ON DELETE RESTRICT,
    mrf_location text NOT NULL,
    mrf_filename text,
    plan_name text NOT NULL,
    issuer_name text NOT NULL,
    plan_sponsor_name text,
    plan_id_type text NOT NULL,
    plan_id text NOT NULL,
    plan_market_type text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT toc_mrf_plan_associations_mrf_location_check
        CHECK (mrf_location <> '' AND mrf_location !~ E'[\r\n]'),
    CONSTRAINT toc_mrf_plan_associations_mrf_filename_check
        CHECK (mrf_filename IS NULL OR mrf_filename <> ''),
    CONSTRAINT toc_mrf_plan_associations_plan_name_check
        CHECK (plan_name <> ''),
    CONSTRAINT toc_mrf_plan_associations_issuer_name_check
        CHECK (issuer_name <> ''),
    CONSTRAINT toc_mrf_plan_associations_plan_id_check
        CHECK (plan_id <> ''),
    CONSTRAINT toc_mrf_plan_associations_plan_id_type_check
        CHECK (plan_id_type IN ('ein', 'hios')),
    CONSTRAINT toc_mrf_plan_associations_plan_market_type_check
        CHECK (plan_market_type IN ('group', 'individual')),
    CONSTRAINT toc_mrf_plan_associations_sponsor_check
        CHECK (
            (plan_id_type = 'ein' AND plan_sponsor_name IS NOT NULL AND plan_sponsor_name <> '')
            OR (plan_id_type = 'hios' AND (plan_sponsor_name IS NULL OR plan_sponsor_name <> ''))
        ),
    CONSTRAINT toc_mrf_plan_associations_unique_key
        UNIQUE NULLS NOT DISTINCT (
            toc_file_id,
            mrf_snapshot_id,
            mrf_location,
            mrf_filename,
            plan_name,
            issuer_name,
            plan_sponsor_name,
            plan_id_type,
            plan_id,
            plan_market_type
        )
);

CREATE TABLE mrfpipeline.mrf_plans (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mrf_snapshot_id bigint NOT NULL REFERENCES mrfpipeline.mrf_snapshots (id) ON DELETE RESTRICT,
    plan_name text NOT NULL,
    issuer_name text NOT NULL,
    plan_sponsor_name text,
    plan_id_type text NOT NULL,
    plan_id text NOT NULL,
    plan_market_type text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT mrf_plans_plan_name_check
        CHECK (plan_name <> ''),
    CONSTRAINT mrf_plans_issuer_name_check
        CHECK (issuer_name <> ''),
    CONSTRAINT mrf_plans_plan_id_check
        CHECK (plan_id <> ''),
    CONSTRAINT mrf_plans_plan_id_type_check
        CHECK (plan_id_type IN ('ein', 'hios')),
    CONSTRAINT mrf_plans_plan_market_type_check
        CHECK (plan_market_type IN ('group', 'individual')),
    CONSTRAINT mrf_plans_sponsor_check
        CHECK (
            (plan_id_type = 'ein' AND plan_sponsor_name IS NOT NULL AND plan_sponsor_name <> '')
            OR (plan_id_type = 'hios' AND plan_sponsor_name IS NULL)
        ),
    CONSTRAINT mrf_plans_identity_key UNIQUE (
        mrf_snapshot_id,
        plan_name,
        issuer_name,
        plan_id_type,
        plan_id,
        plan_market_type
    )
);

CREATE TABLE mrfpipeline.plan_attachment_batches (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mrf_snapshot_id bigint NOT NULL REFERENCES mrfpipeline.mrf_snapshots (id) ON DELETE RESTRICT,
    status text NOT NULL DEFAULT 'pending',
    river_job_id bigint,
    requested_plan_count bigint NOT NULL DEFAULT 0,
    added_plan_count bigint,
    failure_code text,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT plan_attachment_batches_status_check
        CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    CONSTRAINT plan_attachment_batches_river_job_id_check
        CHECK (river_job_id IS NULL OR river_job_id > 0),
    CONSTRAINT plan_attachment_batches_requested_plan_count_check
        CHECK (requested_plan_count >= 0),
    CONSTRAINT plan_attachment_batches_added_plan_count_check
        CHECK (
            (status = 'succeeded' AND added_plan_count IS NOT NULL AND added_plan_count >= 0)
            OR (status <> 'succeeded' AND added_plan_count IS NULL)
        ),
    CONSTRAINT plan_attachment_batches_terminal_completed_check
        CHECK (
            (status IN ('succeeded', 'failed') AND completed_at IS NOT NULL)
            OR (status NOT IN ('succeeded', 'failed') AND completed_at IS NULL)
        ),
    CONSTRAINT plan_attachment_batches_success_failure_code_check
        CHECK (status <> 'succeeded' OR failure_code IS NULL),
    CONSTRAINT plan_attachment_batches_failure_code_check
        CHECK (failure_code IS NULL OR failure_code <> '')
);

CREATE TABLE mrfpipeline.plan_attachment_batch_items (
    plan_attachment_batch_id bigint NOT NULL REFERENCES mrfpipeline.plan_attachment_batches (id) ON DELETE CASCADE,
    mrf_plan_id bigint NOT NULL REFERENCES mrfpipeline.mrf_plans (id) ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (plan_attachment_batch_id, mrf_plan_id),
    CONSTRAINT plan_attachment_batch_items_mrf_plan_id_key UNIQUE (mrf_plan_id)
);

CREATE INDEX discovery_runs_status_idx
    ON mrfpipeline.discovery_runs (status);
CREATE INDEX toc_files_download_status_idx
    ON mrfpipeline.toc_files (download_status);
CREATE INDEX toc_files_parse_status_idx
    ON mrfpipeline.toc_files (parse_status);
CREATE INDEX toc_files_import_status_idx
    ON mrfpipeline.toc_files (import_status);
CREATE INDEX mrf_sources_download_status_idx
    ON mrfpipeline.mrf_sources (download_status);
CREATE INDEX mrf_sources_parse_status_idx
    ON mrfpipeline.mrf_sources (parse_status);
CREATE INDEX mrf_snapshots_mrf_source_id_idx
    ON mrfpipeline.mrf_snapshots (mrf_source_id);
CREATE INDEX mrf_snapshots_consume_status_idx
    ON mrfpipeline.mrf_snapshots (consume_status);
CREATE INDEX plan_attachment_batches_mrf_snapshot_id_idx
    ON mrfpipeline.plan_attachment_batches (mrf_snapshot_id);
CREATE INDEX plan_attachment_batches_status_idx
    ON mrfpipeline.plan_attachment_batches (status);
CREATE INDEX mrf_plans_mrf_snapshot_id_idx
    ON mrfpipeline.mrf_plans (mrf_snapshot_id);
