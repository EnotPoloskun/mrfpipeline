CREATE TABLE mrfpipeline.monthly_releases (
    payer_id text NOT NULL,
    collection_month date NOT NULL,
    status text NOT NULL DEFAULT 'building',
    sealed_at timestamptz,
    last_activated_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT monthly_releases_pkey PRIMARY KEY (payer_id, collection_month),
    CONSTRAINT monthly_releases_payer_id_format_check
        CHECK (payer_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    CONSTRAINT monthly_releases_collection_month_check
        CHECK (collection_month = date_trunc('month', collection_month)::date),
    CONSTRAINT monthly_releases_status_check
        CHECK (status IN ('building', 'active', 'inactive')),
    CONSTRAINT monthly_releases_status_timestamps_check
        CHECK (
            (status = 'building' AND sealed_at IS NULL AND last_activated_at IS NULL)
            OR (status IN ('active', 'inactive') AND sealed_at IS NOT NULL AND last_activated_at IS NOT NULL)
        ),
    CONSTRAINT monthly_releases_sealed_before_last_activation_check
        CHECK (sealed_at IS NULL OR last_activated_at IS NULL OR sealed_at <= last_activated_at)
);

INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
SELECT payer_id, collection_month
FROM (
    SELECT payer_id, collection_month FROM mrfpipeline.discovery_runs
    UNION
    SELECT payer_id, collection_month FROM mrfpipeline.toc_files
    UNION
    SELECT payer_id, collection_month FROM mrfpipeline.mrf_snapshots
) AS pairs;

CREATE UNIQUE INDEX monthly_releases_one_active_per_payer_idx
    ON mrfpipeline.monthly_releases (payer_id)
    WHERE status = 'active';

ALTER TABLE mrfpipeline.discovery_runs
    ADD CONSTRAINT discovery_runs_monthly_release_fkey
        FOREIGN KEY (payer_id, collection_month)
        REFERENCES mrfpipeline.monthly_releases (payer_id, collection_month)
        ON DELETE RESTRICT;

ALTER TABLE mrfpipeline.toc_files
    ADD CONSTRAINT toc_files_monthly_release_fkey
        FOREIGN KEY (payer_id, collection_month)
        REFERENCES mrfpipeline.monthly_releases (payer_id, collection_month)
        ON DELETE RESTRICT;

ALTER TABLE mrfpipeline.mrf_snapshots
    ADD CONSTRAINT mrf_snapshots_monthly_release_fkey
        FOREIGN KEY (payer_id, collection_month)
        REFERENCES mrfpipeline.monthly_releases (payer_id, collection_month)
        ON DELETE RESTRICT;
