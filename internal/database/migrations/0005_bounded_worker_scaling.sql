ALTER TABLE mrfpipeline.monthly_releases
    ADD COLUMN mrf_source_target_kind text NOT NULL DEFAULT 'unset',
    ADD COLUMN mrf_source_target_count bigint,
    ADD CONSTRAINT monthly_releases_mrf_source_target_kind_check
        CHECK (mrf_source_target_kind IN ('unset', 'numeric', 'all')),
    ADD CONSTRAINT monthly_releases_mrf_source_target_count_check
        CHECK (
            (mrf_source_target_kind = 'numeric' AND mrf_source_target_count > 0)
            OR (mrf_source_target_kind <> 'numeric' AND mrf_source_target_count IS NULL)
        );

UPDATE mrfpipeline.monthly_releases
SET mrf_source_target_kind = 'all'
WHERE status IN ('active', 'inactive');

CREATE TABLE mrfpipeline.monthly_release_mrf_sources (
    payer_id text NOT NULL,
    collection_month date NOT NULL,
    mrf_source_id bigint NOT NULL,
    selected_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (payer_id, collection_month, mrf_source_id),
    CONSTRAINT monthly_release_mrf_sources_release_fkey
        FOREIGN KEY (payer_id, collection_month)
        REFERENCES mrfpipeline.monthly_releases (payer_id, collection_month)
        ON DELETE RESTRICT,
    CONSTRAINT monthly_release_mrf_sources_source_fkey
        FOREIGN KEY (mrf_source_id, collection_month)
        REFERENCES mrfpipeline.mrf_sources (id, collection_month)
        ON DELETE RESTRICT
);

CREATE INDEX monthly_release_mrf_sources_source_idx
    ON mrfpipeline.monthly_release_mrf_sources (mrf_source_id);

CREATE TABLE mrfpipeline.mrf_materialization_slots (
    mrf_source_id bigint PRIMARY KEY
        REFERENCES mrfpipeline.mrf_sources (id) ON DELETE RESTRICT,
    acquired_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE mrfpipeline.pipeline_runtime (
    id boolean PRIMARY KEY DEFAULT true,
    artifact_root text NOT NULL,
    resident_capacity integer NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT pipeline_runtime_id_check CHECK (id),
    CONSTRAINT pipeline_runtime_artifact_root_check CHECK (artifact_root <> ''),
    CONSTRAINT pipeline_runtime_resident_capacity_check CHECK (resident_capacity > 0)
);

-- A wake is a durable control event rather than an empty, globally unique
-- River payload.  A distinct event identity means a wake committed while a
-- scheduler is already running cannot be hidden by River uniqueness.
CREATE TABLE mrfpipeline.control_schedule_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    consumed_at timestamptz
);

INSERT INTO mrfpipeline.monthly_release_mrf_sources
    (payer_id, collection_month, mrf_source_id)
SELECT DISTINCT s.payer_id, s.collection_month, s.mrf_source_id
FROM mrfpipeline.mrf_snapshots s
JOIN mrfpipeline.mrf_sources m ON m.id = s.mrf_source_id
WHERE m.download_status <> 'pending'
   OR m.parse_status <> 'blocked';
