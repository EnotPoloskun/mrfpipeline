CREATE SCHEMA mrfweb;

CREATE TABLE mrfweb.release_catalogs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payer_id text NOT NULL,
    collection_month date NOT NULL,
    publication_generation bigint NOT NULL,
    status text NOT NULL DEFAULT 'building',
    output_fingerprint text NOT NULL,
    output_count bigint NOT NULL,
    standard_fact_count bigint,
    provider_catalog_schema_version bigint,
    provider_catalog_release_month date,
    billing_code_count bigint,
    code_filter_value_count bigint,
    plan_count bigint,
    plan_output_count bigint,
    output_code_network_count bigint,
    provider_filter_value_count bigint,
    failure_code text,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    completed_at timestamptz,
    published_at timestamptz,
    CONSTRAINT release_catalogs_payer_id_format_check
        CHECK (payer_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    CONSTRAINT release_catalogs_collection_month_check
        CHECK (collection_month = date_trunc('month', collection_month)::date),
    CONSTRAINT release_catalogs_publication_generation_check
        CHECK (publication_generation > 0),
    CONSTRAINT release_catalogs_status_check
        CHECK (status IN ('building', 'ready', 'failed', 'published')),
    CONSTRAINT release_catalogs_output_fingerprint_check
        CHECK (output_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT release_catalogs_output_count_check
        CHECK (output_count > 0),
    CONSTRAINT release_catalogs_standard_fact_count_check
        CHECK (standard_fact_count IS NULL OR standard_fact_count > 0),
    CONSTRAINT release_catalogs_provider_schema_version_check
        CHECK (provider_catalog_schema_version IS NULL OR provider_catalog_schema_version > 0),
    CONSTRAINT release_catalogs_provider_release_month_check
        CHECK (
            provider_catalog_release_month IS NULL
            OR provider_catalog_release_month = date_trunc('month', provider_catalog_release_month)::date
        ),
    CONSTRAINT release_catalogs_optional_count_check
        CHECK (
            (billing_code_count IS NULL OR billing_code_count >= 0)
            AND (code_filter_value_count IS NULL OR code_filter_value_count >= 0)
            AND (plan_count IS NULL OR plan_count >= 0)
            AND (plan_output_count IS NULL OR plan_output_count >= 0)
            AND (output_code_network_count IS NULL OR output_code_network_count >= 0)
            AND (provider_filter_value_count IS NULL OR provider_filter_value_count >= 0)
        ),
    CONSTRAINT release_catalogs_building_state_check
        CHECK (
            status <> 'building'
            OR (failure_code IS NULL AND completed_at IS NULL AND published_at IS NULL)
        ),
    CONSTRAINT release_catalogs_ready_state_check
        CHECK (
            status <> 'ready'
            OR (
                failure_code IS NULL
                AND completed_at IS NOT NULL
                AND published_at IS NULL
                AND provider_catalog_schema_version IS NOT NULL
                AND provider_catalog_release_month IS NOT NULL
                AND standard_fact_count IS NOT NULL
                AND billing_code_count IS NOT NULL
                AND code_filter_value_count IS NOT NULL
                AND plan_count IS NOT NULL
                AND plan_output_count IS NOT NULL
                AND output_code_network_count IS NOT NULL
                AND provider_filter_value_count IS NOT NULL
            )
        ),
    CONSTRAINT release_catalogs_failed_state_check
        CHECK (
            status <> 'failed'
            OR (failure_code IS NOT NULL AND failure_code <> '' AND completed_at IS NOT NULL AND published_at IS NULL)
        ),
    CONSTRAINT release_catalogs_published_state_check
        CHECK (
            status <> 'published'
            OR (
                failure_code IS NULL
                AND completed_at IS NOT NULL
                AND published_at IS NOT NULL
                AND provider_catalog_schema_version IS NOT NULL
                AND provider_catalog_release_month IS NOT NULL
                AND standard_fact_count IS NOT NULL
                AND billing_code_count IS NOT NULL
                AND code_filter_value_count IS NOT NULL
                AND plan_count IS NOT NULL
                AND plan_output_count IS NOT NULL
                AND output_code_network_count IS NOT NULL
                AND provider_filter_value_count IS NOT NULL
            )
        ),
    CONSTRAINT release_catalogs_failure_code_check
        CHECK (failure_code IS NULL OR failure_code <> ''),
    CONSTRAINT release_catalogs_completed_at_check
        CHECK (completed_at IS NULL OR completed_at >= created_at),
    CONSTRAINT release_catalogs_published_at_check
        CHECK (
            published_at IS NULL
            OR (completed_at IS NOT NULL AND published_at >= completed_at)
        ),
    CONSTRAINT release_catalogs_release_generation_key
        UNIQUE (payer_id, collection_month, publication_generation),
    CONSTRAINT release_catalogs_release_fkey
        FOREIGN KEY (payer_id, collection_month)
        REFERENCES mrfpipeline.monthly_releases (payer_id, collection_month)
        ON DELETE RESTRICT
);

CREATE TABLE mrfweb.release_outputs (
    catalog_id bigint NOT NULL,
    mrf_snapshot_id bigint NOT NULL,
    output_id text NOT NULL,
    CONSTRAINT release_outputs_pkey PRIMARY KEY (catalog_id, output_id),
    CONSTRAINT release_outputs_catalog_snapshot_key UNIQUE (catalog_id, mrf_snapshot_id),
    CONSTRAINT release_outputs_catalog_fkey
        FOREIGN KEY (catalog_id)
        REFERENCES mrfweb.release_catalogs (id)
        ON DELETE CASCADE,
    CONSTRAINT release_outputs_snapshot_fkey
        FOREIGN KEY (mrf_snapshot_id)
        REFERENCES mrfpipeline.mrf_snapshots (id)
        ON DELETE RESTRICT,
    CONSTRAINT release_outputs_output_id_check
        CHECK (output_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$')
);

CREATE TABLE mrfweb.release_billing_codes (
    catalog_id bigint NOT NULL,
    billing_code_type text NOT NULL,
    billing_code text NOT NULL,
    billing_code_type_version text,
    warehouse_service_name text,
    warehouse_service_description text,
    observation_count bigint NOT NULL,
    unmodified_observation_count bigint NOT NULL,
    CONSTRAINT release_billing_codes_pkey PRIMARY KEY (catalog_id, billing_code_type, billing_code),
    CONSTRAINT release_billing_codes_catalog_fkey
        FOREIGN KEY (catalog_id)
        REFERENCES mrfweb.release_catalogs (id)
        ON DELETE CASCADE,
    CONSTRAINT release_billing_codes_identity_check
        CHECK (
            billing_code_type <> '' AND billing_code_type !~ E'[\r\n]'
            AND billing_code <> '' AND billing_code !~ E'[\r\n]'
        ),
    CONSTRAINT release_billing_codes_optional_text_check
        CHECK (
            (billing_code_type_version IS NULL OR (billing_code_type_version <> '' AND billing_code_type_version !~ E'[\r\n]'))
            AND (warehouse_service_name IS NULL OR (warehouse_service_name <> '' AND warehouse_service_name !~ E'[\r\n]'))
            AND (warehouse_service_description IS NULL OR (warehouse_service_description <> '' AND warehouse_service_description !~ E'[\r\n]'))
        ),
    CONSTRAINT release_billing_codes_observation_count_check
        CHECK (observation_count > 0),
    CONSTRAINT release_billing_codes_unmodified_count_check
        CHECK (unmodified_observation_count >= 0 AND unmodified_observation_count <= observation_count)
);

CREATE TABLE mrfweb.release_code_filter_values (
    catalog_id bigint NOT NULL,
    billing_code_type text NOT NULL,
    billing_code text NOT NULL,
    filter_kind text NOT NULL,
    filter_value text NOT NULL,
    display_label text NOT NULL,
    observation_count bigint NOT NULL,
    CONSTRAINT release_code_filter_values_pkey
        PRIMARY KEY (catalog_id, billing_code_type, billing_code, filter_kind, filter_value),
    CONSTRAINT release_code_filter_values_code_fkey
        FOREIGN KEY (catalog_id, billing_code_type, billing_code)
        REFERENCES mrfweb.release_billing_codes (catalog_id, billing_code_type, billing_code)
        ON DELETE CASCADE,
    CONSTRAINT release_code_filter_values_kind_check
        CHECK (filter_kind IN ('modifier', 'place_of_service', 'billing_class', 'setting', 'negotiation_arrangement')),
    CONSTRAINT release_code_filter_values_text_check
        CHECK (
            filter_value <> '' AND filter_value !~ E'[\r\n]'
            AND display_label <> '' AND display_label !~ E'[\r\n]'
        ),
    CONSTRAINT release_code_filter_values_observation_count_check
        CHECK (observation_count > 0)
);

CREATE TABLE mrfweb.release_plans (
    id bigint GENERATED ALWAYS AS IDENTITY,
    catalog_id bigint NOT NULL,
    plan_name text NOT NULL,
    issuer_name text NOT NULL,
    plan_id_type text NOT NULL,
    plan_id text NOT NULL,
    plan_market_type text NOT NULL,
    search_text text NOT NULL,
    CONSTRAINT release_plans_pkey PRIMARY KEY (catalog_id, id),
    CONSTRAINT release_plans_catalog_fkey
        FOREIGN KEY (catalog_id)
        REFERENCES mrfweb.release_catalogs (id)
        ON DELETE CASCADE,
    CONSTRAINT release_plans_canonical_key
        UNIQUE (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type),
    CONSTRAINT release_plans_text_check
        CHECK (
            plan_name <> '' AND plan_name !~ E'[\r\n]'
            AND issuer_name <> '' AND issuer_name !~ E'[\r\n]'
            AND plan_id <> '' AND plan_id !~ E'[\r\n]'
            AND search_text <> '' AND search_text !~ E'[\r\n]'
        ),
    CONSTRAINT release_plans_id_type_check
        CHECK (plan_id_type IN ('ein', 'hios')),
    CONSTRAINT release_plans_market_type_check
        CHECK (plan_market_type IN ('group', 'individual'))
);

CREATE INDEX release_plans_catalog_identity_idx
    ON mrfweb.release_plans (
        catalog_id,
        plan_name COLLATE "C",
        issuer_name COLLATE "C",
        plan_id_type COLLATE "C",
        plan_id COLLATE "C",
        plan_market_type COLLATE "C"
    );

CREATE INDEX release_plans_search_idx
    ON mrfweb.release_plans (catalog_id, search_text COLLATE "C" text_pattern_ops, id);

CREATE TABLE mrfweb.release_plan_outputs (
    catalog_id bigint NOT NULL,
    plan_id bigint NOT NULL,
    output_id text NOT NULL,
    CONSTRAINT release_plan_outputs_pkey PRIMARY KEY (catalog_id, plan_id, output_id),
    CONSTRAINT release_plan_outputs_plan_fkey
        FOREIGN KEY (catalog_id, plan_id)
        REFERENCES mrfweb.release_plans (catalog_id, id)
        ON DELETE CASCADE,
    CONSTRAINT release_plan_outputs_output_fkey
        FOREIGN KEY (catalog_id, output_id)
        REFERENCES mrfweb.release_outputs (catalog_id, output_id)
        ON DELETE CASCADE
);

CREATE TABLE mrfweb.release_output_code_networks (
    catalog_id bigint NOT NULL,
    output_id text NOT NULL,
    billing_code_type text NOT NULL,
    billing_code text NOT NULL,
    network_name text NOT NULL,
    observation_count bigint NOT NULL,
    CONSTRAINT release_output_code_networks_pkey
        PRIMARY KEY (catalog_id, output_id, billing_code_type, billing_code, network_name),
    CONSTRAINT release_output_code_networks_output_fkey
        FOREIGN KEY (catalog_id, output_id)
        REFERENCES mrfweb.release_outputs (catalog_id, output_id)
        ON DELETE CASCADE,
    CONSTRAINT release_output_code_networks_code_fkey
        FOREIGN KEY (catalog_id, billing_code_type, billing_code)
        REFERENCES mrfweb.release_billing_codes (catalog_id, billing_code_type, billing_code)
        ON DELETE CASCADE,
    CONSTRAINT release_output_code_networks_network_name_check
        CHECK (network_name <> '' AND network_name !~ E'[\r\n]'),
    CONSTRAINT release_output_code_networks_observation_count_check
        CHECK (observation_count > 0)
);

CREATE INDEX release_output_code_networks_discovery_idx
    ON mrfweb.release_output_code_networks
        (catalog_id, billing_code_type, billing_code, output_id, network_name);

CREATE TABLE mrfweb.release_provider_filter_values (
    catalog_id bigint NOT NULL,
    filter_kind text NOT NULL,
    parent_value text NOT NULL DEFAULT '',
    filter_value text NOT NULL,
    provider_count bigint NOT NULL,
    CONSTRAINT release_provider_filter_values_pkey
        PRIMARY KEY (catalog_id, filter_kind, parent_value, filter_value),
    CONSTRAINT release_provider_filter_values_catalog_fkey
        FOREIGN KEY (catalog_id)
        REFERENCES mrfweb.release_catalogs (id)
        ON DELETE CASCADE,
    CONSTRAINT release_provider_filter_values_kind_check
        CHECK (filter_kind IN ('taxonomy', 'state', 'city')),
    CONSTRAINT release_provider_filter_values_text_check
        CHECK (filter_value <> '' AND filter_value !~ E'[\r\n]' AND parent_value !~ E'[\r\n]'),
    CONSTRAINT release_provider_filter_values_parent_check
        CHECK (
            (filter_kind IN ('taxonomy', 'state') AND parent_value = '')
            OR (filter_kind = 'city' AND parent_value <> '')
        ),
    CONSTRAINT release_provider_filter_values_count_check
        CHECK (provider_count > 0)
);

CREATE VIEW mrfweb.active_release_catalogs AS
WITH active_releases AS (
    SELECT payer_id, collection_month, publication_generation
    FROM mrfpipeline.monthly_releases
    WHERE status = 'active'
),
candidate_catalogs AS (
    SELECT c.*
    FROM mrfweb.release_catalogs c
    JOIN active_releases r
      ON r.payer_id = c.payer_id
     AND r.collection_month = c.collection_month
     AND r.publication_generation = c.publication_generation
    WHERE c.status = 'published'
),
valid_catalogs AS (
    SELECT c.*
    FROM candidate_catalogs c
    WHERE NOT EXISTS (
        SELECT 1
        FROM mrfpipeline.monthly_release_outputs o
        JOIN mrfpipeline.mrf_snapshots s
          ON s.payer_id = o.payer_id
         AND s.collection_month = o.collection_month
         AND s.id = o.mrf_snapshot_id
        WHERE o.payer_id = c.payer_id
          AND o.collection_month = c.collection_month
          AND NOT EXISTS (
              SELECT 1
              FROM mrfweb.release_outputs co
              WHERE co.catalog_id = c.id
                AND co.mrf_snapshot_id = o.mrf_snapshot_id
                AND co.output_id = 'mrf-' || s.id
          )
    )
    AND EXISTS (
        SELECT 1
        FROM mrfpipeline.monthly_release_outputs o
        WHERE o.payer_id = c.payer_id
          AND o.collection_month = c.collection_month
    )
    AND NOT EXISTS (
        SELECT 1
        FROM mrfweb.release_outputs co
        WHERE co.catalog_id = c.id
          AND NOT EXISTS (
              SELECT 1
              FROM mrfpipeline.monthly_release_outputs o
              JOIN mrfpipeline.mrf_snapshots s
                ON s.payer_id = o.payer_id
               AND s.collection_month = o.collection_month
               AND s.id = o.mrf_snapshot_id
              WHERE o.payer_id = c.payer_id
                AND o.collection_month = c.collection_month
                AND o.mrf_snapshot_id = co.mrf_snapshot_id
                AND co.output_id = 'mrf-' || s.id
          )
    )
),
invalid_active_payers AS (
    SELECT 1
    FROM active_releases r
    WHERE NOT EXISTS (
        SELECT 1
        FROM valid_catalogs c
        WHERE c.payer_id = r.payer_id
          AND c.collection_month = r.collection_month
          AND c.publication_generation = r.publication_generation
    )
)
SELECT c.id AS catalog_id,
       c.payer_id,
       c.collection_month,
       c.publication_generation,
       c.output_fingerprint,
       c.output_count,
       c.standard_fact_count,
       c.provider_catalog_schema_version,
       c.provider_catalog_release_month,
       c.published_at
FROM valid_catalogs c
WHERE NOT EXISTS (SELECT 1 FROM invalid_active_payers);

CREATE VIEW mrfweb.active_release_outputs AS
SELECT c.catalog_id,
       c.payer_id,
       c.collection_month,
       c.publication_generation,
       'mrf-' || s.id AS output_id
FROM mrfweb.active_release_catalogs c
JOIN mrfpipeline.monthly_release_outputs o
  ON o.payer_id = c.payer_id
 AND o.collection_month = c.collection_month
JOIN mrfpipeline.mrf_snapshots s
  ON s.payer_id = o.payer_id
 AND s.collection_month = o.collection_month
 AND s.id = o.mrf_snapshot_id;
