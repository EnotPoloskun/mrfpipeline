ALTER TABLE mrfpipeline.monthly_releases
    ADD COLUMN publication_generation bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT monthly_releases_publication_generation_check
        CHECK (publication_generation >= 0);

CREATE UNIQUE INDEX mrf_snapshots_release_identity_idx
    ON mrfpipeline.mrf_snapshots (payer_id, collection_month, id);

CREATE TABLE mrfpipeline.monthly_release_outputs (
    payer_id text NOT NULL,
    collection_month date NOT NULL,
    mrf_snapshot_id bigint NOT NULL,
    published_generation bigint NOT NULL,
    published_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (payer_id, collection_month, mrf_snapshot_id),
    CONSTRAINT monthly_release_outputs_generation_check
        CHECK (published_generation > 0),
    CONSTRAINT monthly_release_outputs_release_fkey
        FOREIGN KEY (payer_id, collection_month)
        REFERENCES mrfpipeline.monthly_releases (payer_id, collection_month)
        ON DELETE RESTRICT,
    CONSTRAINT monthly_release_outputs_snapshot_fkey
        FOREIGN KEY (payer_id, collection_month, mrf_snapshot_id)
        REFERENCES mrfpipeline.mrf_snapshots (payer_id, collection_month, id)
        ON DELETE RESTRICT
);

CREATE INDEX monthly_release_outputs_snapshot_idx
    ON mrfpipeline.monthly_release_outputs (mrf_snapshot_id);

UPDATE mrfpipeline.monthly_releases
SET publication_generation = 1
WHERE status IN ('active', 'inactive');

INSERT INTO mrfpipeline.monthly_release_outputs
    (payer_id, collection_month, mrf_snapshot_id, published_generation, published_at)
SELECT s.payer_id, s.collection_month, s.id, 1, r.sealed_at
FROM mrfpipeline.monthly_releases r
JOIN mrfpipeline.mrf_snapshots s
  ON s.payer_id = r.payer_id AND s.collection_month = r.collection_month
WHERE r.status IN ('active', 'inactive');
