package database

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func seedCatalogRelease(t *testing.T, pool *pgxpool.Pool, payer string) int64 {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases
    (payer_id, collection_month, status, sealed_at, last_activated_at, publication_generation)
VALUES ($1, DATE '2026-08-01', 'active', transaction_timestamp(), transaction_timestamp(), 1)`, payer); err != nil {
		t.Fatal("seed release:", err)
	}
	var sourceID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month)
VALUES ($1, DATE '2026-08-01')
RETURNING id`, "https://example.invalid/"+payer+"/mrf.json").Scan(&sourceID); err != nil {
		t.Fatal("seed source:", err)
	}
	var snapshotID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month)
VALUES ($1, $2, DATE '2026-08-01')
RETURNING id`, sourceID, payer).Scan(&snapshotID); err != nil {
		t.Fatal("seed snapshot:", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_release_outputs
    (payer_id, collection_month, mrf_snapshot_id, published_generation)
VALUES ($1, DATE '2026-08-01', $2, 1)`, payer, snapshotID); err != nil {
		t.Fatal("seed release output:", err)
	}
	return snapshotID
}

func insertCatalog(t *testing.T, pool *pgxpool.Pool, payer string, generation int64, status string) int64 {
	return insertCatalogWithCount(t, pool, payer, generation, status, 1)
}

func insertCatalogWithCount(t *testing.T, pool *pgxpool.Pool, payer string, generation int64, status string, outputCount int64) int64 {
	t.Helper()
	var id int64
	err := pool.QueryRow(context.Background(), `
INSERT INTO mrfweb.release_catalogs (
    payer_id, collection_month, publication_generation, status,
    output_fingerprint, output_count, standard_fact_count,
    provider_catalog_schema_version, provider_catalog_release_month,
    billing_code_count, code_filter_value_count, plan_count,
    plan_output_count, output_code_network_count, provider_filter_value_count,
    failure_code, completed_at, published_at
) VALUES (
    $1, DATE '2026-08-01', $2, $3,
    repeat('a', 64), $4, 1, 1, DATE '2026-08-01',
    0, 0, 0, 0, 0, 0,
    CASE WHEN $3 = 'failed' THEN 'test_failure' END,
    CASE WHEN $3 IN ('ready', 'failed', 'published') THEN transaction_timestamp() END,
    CASE WHEN $3 = 'published' THEN transaction_timestamp() END
)
RETURNING id`, payer, generation, status, outputCount).Scan(&id)
	if err != nil {
		t.Fatal("insert catalog:", err)
	}
	return id
}

func insertCatalogOutput(t *testing.T, pool *pgxpool.Pool, catalogID, snapshotID int64, outputID string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
INSERT INTO mrfweb.release_outputs (catalog_id, mrf_snapshot_id, output_id)
VALUES ($1, $2, $3)`, catalogID, snapshotID, outputID); err != nil {
		t.Fatal("insert catalog output:", err)
	}
}

func TestIntegrationCatalogMigrationIsAdditive(t *testing.T) {
	url, pool := withTestDB(t)
	files, err := loadEmbeddedMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyMigrations(context.Background(), url, files[:8]); err != nil {
		t.Fatal("migrate through 0008:", err)
	}
	snapshotID := seedCatalogRelease(t, pool, "uhc")
	var beforeStatus string
	var beforeGeneration int64
	if err := pool.QueryRow(context.Background(), `
SELECT status, publication_generation
FROM mrfpipeline.monthly_releases
WHERE payer_id = 'uhc' AND collection_month = DATE '2026-08-01'`).Scan(&beforeStatus, &beforeGeneration); err != nil {
		t.Fatal(err)
	}
	if _, err := applyMigrations(context.Background(), url, files); err != nil {
		t.Fatal("migrate through 0009:", err)
	}
	var afterStatus string
	var afterGeneration int64
	if err := pool.QueryRow(context.Background(), `
SELECT status, publication_generation
FROM mrfpipeline.monthly_releases
WHERE payer_id = 'uhc' AND collection_month = DATE '2026-08-01'`).Scan(&afterStatus, &afterGeneration); err != nil {
		t.Fatal(err)
	}
	if beforeStatus != afterStatus || beforeGeneration != afterGeneration {
		t.Fatalf("release changed from %s/%d to %s/%d", beforeStatus, beforeGeneration, afterStatus, afterGeneration)
	}
	var outputCount int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM mrfpipeline.monthly_release_outputs
WHERE payer_id = 'uhc' AND collection_month = DATE '2026-08-01' AND mrf_snapshot_id = $1`, snapshotID).Scan(&outputCount); err != nil {
		t.Fatal(err)
	}
	if outputCount != 1 {
		t.Fatalf("release output count %d", outputCount)
	}
}

func TestIntegrationCatalogSchemaAndViews(t *testing.T) {
	url, pool := withTestDB(t)
	mustMigrate(t, url)
	ctx := context.Background()

	var schemaExists bool
	if err := pool.QueryRow(ctx, `
SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'mrfweb')`).Scan(&schemaExists); err != nil {
		t.Fatal(err)
	}
	if !schemaExists {
		t.Fatal("mrfweb schema missing")
	}
	var tableCount, viewCount int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM information_schema.tables
WHERE table_schema = 'mrfweb' AND table_type = 'BASE TABLE'`).Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 8 {
		t.Fatalf("mrfweb table count %d", tableCount)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM information_schema.views WHERE table_schema = 'mrfweb'`).Scan(&viewCount); err != nil {
		t.Fatal(err)
	}
	if viewCount != 2 {
		t.Fatalf("mrfweb view count %d", viewCount)
	}
	views := map[string]string{
		"active_release_catalogs": "catalog_id,payer_id,collection_month,publication_generation,output_fingerprint,output_count,standard_fact_count,provider_catalog_schema_version,provider_catalog_release_month,published_at",
		"active_release_outputs":  "catalog_id,payer_id,collection_month,publication_generation,output_id",
	}
	for view, want := range views {
		var got string
		if err := pool.QueryRow(ctx, `
SELECT string_agg(column_name, ',' ORDER BY ordinal_position)
FROM information_schema.columns
WHERE table_schema = 'mrfweb' AND table_name = $1`, view).Scan(&got); err != nil {
			t.Fatalf("columns %s: %v", view, err)
		}
		if got != want {
			t.Fatalf("columns %s: got %q want %q", view, got, want)
		}
	}

	columns := map[string]string{
		"release_catalogs":               "id,payer_id,collection_month,publication_generation,status,output_fingerprint,output_count,standard_fact_count,provider_catalog_schema_version,provider_catalog_release_month,billing_code_count,code_filter_value_count,plan_count,plan_output_count,output_code_network_count,provider_filter_value_count,failure_code,created_at,completed_at,published_at",
		"release_outputs":                "catalog_id,mrf_snapshot_id,output_id",
		"release_billing_codes":          "catalog_id,billing_code_type,billing_code,billing_code_type_version,warehouse_service_name,warehouse_service_description,observation_count,unmodified_observation_count",
		"release_code_filter_values":     "catalog_id,billing_code_type,billing_code,filter_kind,filter_value,display_label,observation_count",
		"release_plans":                  "id,catalog_id,plan_name,issuer_name,plan_id_type,plan_id,plan_market_type,search_text",
		"release_plan_outputs":           "catalog_id,plan_id,output_id",
		"release_output_code_networks":   "catalog_id,output_id,billing_code_type,billing_code,network_name,observation_count",
		"release_provider_filter_values": "catalog_id,filter_kind,parent_value,filter_value,provider_count",
	}
	for table, want := range columns {
		var got string
		if err := pool.QueryRow(ctx, `
SELECT string_agg(column_name, ',' ORDER BY ordinal_position)
FROM information_schema.columns
WHERE table_schema = 'mrfweb' AND table_name = $1`, table).Scan(&got); err != nil {
			t.Fatalf("columns %s: %v", table, err)
		}
		if got != want {
			t.Fatalf("columns %s: got %q want %q", table, got, want)
		}
	}
	constraintKinds := map[string]map[string]string{
		"release_catalogs": {
			"release_catalogs_pkey":                          "p",
			"release_catalogs_payer_id_format_check":         "c",
			"release_catalogs_collection_month_check":        "c",
			"release_catalogs_publication_generation_check":  "c",
			"release_catalogs_status_check":                  "c",
			"release_catalogs_output_fingerprint_check":      "c",
			"release_catalogs_output_count_check":            "c",
			"release_catalogs_standard_fact_count_check":     "c",
			"release_catalogs_provider_schema_version_check": "c",
			"release_catalogs_provider_release_month_check":  "c",
			"release_catalogs_optional_count_check":          "c",
			"release_catalogs_building_state_check":          "c",
			"release_catalogs_ready_state_check":             "c",
			"release_catalogs_failed_state_check":            "c",
			"release_catalogs_published_state_check":         "c",
			"release_catalogs_failure_code_check":            "c",
			"release_catalogs_completed_at_check":            "c",
			"release_catalogs_published_at_check":            "c",
			"release_catalogs_release_generation_key":        "u",
			"release_catalogs_release_fkey":                  "f",
		},
		"release_outputs": {
			"release_outputs_pkey": "p", "release_outputs_catalog_snapshot_key": "u",
			"release_outputs_catalog_fkey": "f", "release_outputs_snapshot_fkey": "f",
			"release_outputs_output_id_check": "c",
		},
		"release_billing_codes": {
			"release_billing_codes_pkey": "p", "release_billing_codes_catalog_fkey": "f",
			"release_billing_codes_identity_check": "c", "release_billing_codes_optional_text_check": "c",
			"release_billing_codes_observation_count_check": "c", "release_billing_codes_unmodified_count_check": "c",
		},
		"release_code_filter_values": {
			"release_code_filter_values_pkey": "p", "release_code_filter_values_code_fkey": "f",
			"release_code_filter_values_kind_check": "c", "release_code_filter_values_text_check": "c",
			"release_code_filter_values_observation_count_check": "c",
		},
		"release_plans": {
			"release_plans_pkey": "p", "release_plans_catalog_fkey": "f",
			"release_plans_canonical_key": "u", "release_plans_text_check": "c",
			"release_plans_id_type_check": "c", "release_plans_market_type_check": "c",
		},
		"release_plan_outputs": {
			"release_plan_outputs_pkey": "p", "release_plan_outputs_plan_fkey": "f",
			"release_plan_outputs_output_fkey": "f",
		},
		"release_output_code_networks": {
			"release_output_code_networks_pkey": "p", "release_output_code_networks_output_fkey": "f",
			"release_output_code_networks_code_fkey": "f", "release_output_code_networks_network_name_check": "c",
			"release_output_code_networks_observation_count_check": "c",
		},
		"release_provider_filter_values": {
			"release_provider_filter_values_pkey": "p", "release_provider_filter_values_catalog_fkey": "f",
			"release_provider_filter_values_kind_check": "c", "release_provider_filter_values_text_check": "c",
			"release_provider_filter_values_parent_check": "c", "release_provider_filter_values_count_check": "c",
		},
	}
	for table, want := range constraintKinds {
		rows, err := pool.Query(ctx, `
SELECT c.conname, c.contype::text
FROM pg_constraint c
JOIN pg_class r ON r.oid = c.conrelid
JOIN pg_namespace n ON n.oid = r.relnamespace
WHERE n.nspname = 'mrfweb' AND r.relname = $1
ORDER BY c.conname`, table)
		if err != nil {
			t.Fatalf("constraints %s: %v", table, err)
		}
		got := map[string]string{}
		for rows.Next() {
			var name, kind string
			if err := rows.Scan(&name, &kind); err != nil {
				rows.Close()
				t.Fatalf("constraints %s: %v", table, err)
			}
			got[name] = kind
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatalf("constraints %s: %v", table, err)
		}
		rows.Close()
		if len(got) != len(want) {
			t.Fatalf("constraints %s: got %v want %v", table, got, want)
		}
		for name, kind := range want {
			if got[name] != kind {
				t.Fatalf("constraint %s.%s: got %q want %q", table, name, got[name], kind)
			}
		}
	}

	foreignKeys := map[string]string{
		"release_catalogs.release_catalogs_release_fkey":                             "mrfpipeline.monthly_releases:RESTRICT",
		"release_outputs.release_outputs_catalog_fkey":                               "mrfweb.release_catalogs:CASCADE",
		"release_outputs.release_outputs_snapshot_fkey":                              "mrfpipeline.mrf_snapshots:RESTRICT",
		"release_billing_codes.release_billing_codes_catalog_fkey":                   "mrfweb.release_catalogs:CASCADE",
		"release_code_filter_values.release_code_filter_values_code_fkey":            "mrfweb.release_billing_codes:CASCADE",
		"release_plans.release_plans_catalog_fkey":                                   "mrfweb.release_catalogs:CASCADE",
		"release_plan_outputs.release_plan_outputs_plan_fkey":                        "mrfweb.release_plans:CASCADE",
		"release_plan_outputs.release_plan_outputs_output_fkey":                      "mrfweb.release_outputs:CASCADE",
		"release_output_code_networks.release_output_code_networks_output_fkey":      "mrfweb.release_outputs:CASCADE",
		"release_output_code_networks.release_output_code_networks_code_fkey":        "mrfweb.release_billing_codes:CASCADE",
		"release_provider_filter_values.release_provider_filter_values_catalog_fkey": "mrfweb.release_catalogs:CASCADE",
	}
	rows, err := pool.Query(ctx, `
SELECT n.nspname, r.relname, c.conname,
       tn.nspname, tr.relname,
       CASE c.confdeltype WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE' ELSE c.confdeltype::text END
FROM pg_constraint c
JOIN pg_class r ON r.oid = c.conrelid
JOIN pg_namespace n ON n.oid = r.relnamespace
JOIN pg_class tr ON tr.oid = c.confrelid
JOIN pg_namespace tn ON tn.oid = tr.relnamespace
WHERE n.nspname = 'mrfweb' AND c.contype = 'f'`)
	if err != nil {
		t.Fatal("foreign keys:", err)
	}
	gotForeignKeys := map[string]string{}
	for rows.Next() {
		var sourceSchema, sourceTable, name, targetSchema, targetTable, action string
		if err := rows.Scan(&sourceSchema, &sourceTable, &name, &targetSchema, &targetTable, &action); err != nil {
			rows.Close()
			t.Fatal("foreign keys:", err)
		}
		if sourceSchema != "mrfweb" {
			rows.Close()
			t.Fatalf("foreign key %s.%s has source schema %q", sourceTable, name, sourceSchema)
		}
		gotForeignKeys[sourceTable+"."+name] = targetSchema + "." + targetTable + ":" + action
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal("foreign keys:", err)
	}
	rows.Close()
	if len(gotForeignKeys) != len(foreignKeys) {
		t.Fatalf("foreign keys got %v want %v", gotForeignKeys, foreignKeys)
	}
	for name, want := range foreignKeys {
		if gotForeignKeys[name] != want {
			t.Fatalf("foreign key %s: got %q want %q", name, gotForeignKeys[name], want)
		}
	}

	optionalColumns := map[string]map[string]bool{
		"release_catalogs": {
			"standard_fact_count": true, "provider_catalog_schema_version": true,
			"provider_catalog_release_month": true, "billing_code_count": true,
			"code_filter_value_count": true, "plan_count": true, "plan_output_count": true,
			"output_code_network_count": true, "provider_filter_value_count": true,
			"failure_code": true, "completed_at": true, "published_at": true,
		},
		"release_billing_codes": {
			"billing_code_type_version": true, "warehouse_service_name": true,
			"warehouse_service_description": true,
		},
	}
	identityColumns := map[string]string{
		"release_catalogs.id": "ALWAYS",
		"release_plans.id":    "ALWAYS",
	}
	defaultColumns := map[string]string{
		"release_catalogs.status":                     "building",
		"release_catalogs.created_at":                 "transaction_timestamp()",
		"release_provider_filter_values.parent_value": "''",
	}
	for table := range columns {
		rows, err := pool.Query(ctx, `
SELECT column_name, is_nullable, is_identity, identity_generation, column_default
FROM information_schema.columns
WHERE table_schema = 'mrfweb' AND table_name = $1`, table)
		if err != nil {
			t.Fatalf("column metadata %s: %v", table, err)
		}
		for rows.Next() {
			var name, nullable, identity string
			var generation, defaultSQL *string
			if err := rows.Scan(&name, &nullable, &identity, &generation, &defaultSQL); err != nil {
				rows.Close()
				t.Fatalf("column metadata %s: %v", table, err)
			}
			key := table + "." + name
			wantNullable := optionalColumns[table][name]
			if (nullable == "YES") != wantNullable {
				t.Fatalf("column %s nullable=%s want=%v", key, nullable, wantNullable)
			}
			wantGeneration, isIdentity := identityColumns[key]
			if isIdentity {
				if identity != "YES" || generation == nil || *generation != wantGeneration {
					t.Fatalf("column %s identity=%s/%v", key, identity, generation)
				}
			} else if identity != "NO" || generation != nil {
				t.Fatalf("column %s unexpectedly identity=%s/%v", key, identity, generation)
			}
			if wantDefault, ok := defaultColumns[key]; ok {
				if defaultSQL == nil || !strings.Contains(*defaultSQL, wantDefault) {
					t.Fatalf("column %s default=%v want %q", key, defaultSQL, wantDefault)
				}
			} else if defaultSQL != nil && !isIdentity {
				t.Fatalf("column %s unexpected default %q", key, *defaultSQL)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatalf("column metadata %s: %v", table, err)
		}
		rows.Close()
	}

	indexColumns := map[string][]string{
		"release_plans_catalog_identity_idx": {
			"catalog_id", `plan_name COLLATE "C"`, `issuer_name COLLATE "C"`,
			`plan_id_type COLLATE "C"`, `plan_id COLLATE "C"`, `plan_market_type COLLATE "C"`,
		},
		"release_plans_search_idx": {
			"catalog_id", `search_text COLLATE "C"`, "text_pattern_ops", "id",
		},
		"release_output_code_networks_discovery_idx": {
			"catalog_id", "billing_code_type", "billing_code", "output_id", "network_name",
		},
	}
	indexAttributeCounts := map[string]int{
		"release_plans_catalog_identity_idx":         6,
		"release_plans_search_idx":                   3,
		"release_output_code_networks_discovery_idx": 5,
	}
	for name, want := range indexColumns {
		var definition string
		var attributeCount int
		if err := pool.QueryRow(ctx, `
SELECT i.indnatts
FROM pg_index i
JOIN pg_class index_rel ON index_rel.oid = i.indexrelid
JOIN pg_namespace index_ns ON index_ns.oid = index_rel.relnamespace
WHERE index_ns.nspname = 'mrfweb' AND index_rel.relname = $1`, name).Scan(&attributeCount); err != nil {
			t.Fatalf("index %s metadata: %v", name, err)
		}
		if attributeCount != indexAttributeCounts[name] {
			t.Fatalf("index %s attribute count %d want %d", name, attributeCount, indexAttributeCounts[name])
		}
		if err := pool.QueryRow(ctx, `
SELECT indexdef FROM pg_indexes
WHERE schemaname = 'mrfweb' AND indexname = $1`, name).Scan(&definition); err != nil {
			t.Fatalf("index %s: %v", name, err)
		}
		if !strings.Contains(definition, "USING btree") {
			t.Fatalf("index %s definition %q", name, definition)
		}
		if name == "release_plans_catalog_identity_idx" && strings.Count(definition, `COLLATE "C"`) != 5 {
			t.Fatalf("index %s collations %q", name, definition)
		}
		if name == "release_plans_search_idx" && strings.Count(definition, `COLLATE "C"`) != 1 {
			t.Fatalf("index %s collations %q", name, definition)
		}
		if name == "release_output_code_networks_discovery_idx" && strings.Contains(definition, "COLLATE") {
			t.Fatalf("index %s has unexpected collation %q", name, definition)
		}
		position := -1
		for _, column := range want {
			next := strings.Index(definition[position+1:], column)
			if next < 0 {
				t.Fatalf("index %s missing ordered column %q in %q", name, column, definition)
			}
			position += next + 1
		}
	}

	uhcSnapshot := seedCatalogRelease(t, pool, "uhc")
	uhcSecondSnapshot := seedCatalogSnapshot(t, pool, "uhc", "https://example.invalid/uhc/second.json")
	aetnaSnapshot := seedCatalogRelease(t, pool, "aetna")
	if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline.monthly_release_outputs
SET published_generation = 2
WHERE mrf_snapshot_id = $1`, uhcSecondSnapshot); err != nil {
		t.Fatal("seed second generation output:", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline.monthly_releases
SET publication_generation = 2
WHERE payer_id = 'uhc' AND collection_month = DATE '2026-08-01'`); err != nil {
		t.Fatal("seed current generation:", err)
	}
	var emptyCatalogs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_catalogs`).Scan(&emptyCatalogs); err != nil {
		t.Fatal(err)
	}
	if emptyCatalogs != 0 {
		t.Fatalf("active catalogs without catalog rows %d", emptyCatalogs)
	}
	var emptyOutputs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_outputs`).Scan(&emptyOutputs); err != nil {
		t.Fatal(err)
	}
	if emptyOutputs != 0 {
		t.Fatalf("active outputs without catalog rows %d", emptyOutputs)
	}
	uhcCatalog := insertCatalogWithCount(t, pool, "uhc", 2, "published", 2)
	aetnaCatalog := insertCatalog(t, pool, "aetna", 1, "published")
	insertCatalogOutput(t, pool, uhcCatalog, uhcSnapshot, "mrf-"+formatTestID(uhcSnapshot))
	insertCatalogOutput(t, pool, uhcCatalog, uhcSecondSnapshot, "mrf-"+formatTestID(uhcSecondSnapshot))
	insertCatalogOutput(t, pool, aetnaCatalog, aetnaSnapshot, "mrf-"+formatTestID(aetnaSnapshot))

	var activeCatalogs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_catalogs`).Scan(&activeCatalogs); err != nil {
		t.Fatal(err)
	}
	if activeCatalogs != 2 {
		t.Fatalf("active catalogs %d", activeCatalogs)
	}
	var activeOutputs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_outputs`).Scan(&activeOutputs); err != nil {
		t.Fatal(err)
	}
	if activeOutputs != 3 {
		t.Fatalf("active outputs %d", activeOutputs)
	}
	var exactOutputCount int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM mrfweb.active_release_outputs
WHERE payer_id = 'uhc' AND output_id = $1`, "mrf-"+formatTestID(uhcSnapshot)).Scan(&exactOutputCount); err != nil {
		t.Fatal(err)
	}
	if exactOutputCount != 1 {
		t.Fatalf("active output identity count %d", exactOutputCount)
	}

	readyCandidate := insertCatalogWithCount(t, pool, "uhc", 3, "ready", 2)
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfweb.release_outputs (catalog_id, mrf_snapshot_id, output_id)
VALUES ($1, $2, $3), ($1, $4, $5)`, readyCandidate, uhcSnapshot, "mrf-"+formatTestID(uhcSnapshot), uhcSecondSnapshot, "mrf-"+formatTestID(uhcSecondSnapshot)); err != nil {
		t.Fatal(err)
	}
	var currentGeneration int64
	if err := pool.QueryRow(ctx, `
SELECT publication_generation FROM mrfweb.active_release_catalogs WHERE payer_id = 'uhc'`).Scan(&currentGeneration); err != nil {
		t.Fatal(err)
	}
	if currentGeneration != 2 {
		t.Fatalf("ready candidate replaced generation %d", currentGeneration)
	}
	if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline.monthly_releases
SET publication_generation = 3
WHERE payer_id = 'uhc' AND collection_month = DATE '2026-08-01'`); err != nil {
		t.Fatal("set current generation:", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_catalogs`).Scan(&activeCatalogs); err != nil {
		t.Fatal(err)
	}
	if activeCatalogs != 0 {
		t.Fatalf("generation-mismatched catalog returned %d rows", activeCatalogs)
	}
	if _, err := pool.Exec(ctx, `
UPDATE mrfpipeline.monthly_releases
SET publication_generation = 2
WHERE payer_id = 'uhc' AND collection_month = DATE '2026-08-01'`); err != nil {
		t.Fatal("restore current generation:", err)
	}

	if _, err := pool.Exec(ctx, `DELETE FROM mrfweb.release_outputs WHERE catalog_id = $1`, aetnaCatalog); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_catalogs`).Scan(&activeCatalogs); err != nil {
		t.Fatal(err)
	}
	if activeCatalogs != 0 {
		t.Fatalf("missing catalog output returned %d rows", activeCatalogs)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_outputs`).Scan(&activeOutputs); err != nil {
		t.Fatal(err)
	}
	if activeOutputs != 0 {
		t.Fatalf("missing catalog output returned %d active outputs", activeOutputs)
	}
	insertCatalogOutput(t, pool, aetnaCatalog, aetnaSnapshot, "mrf-"+formatTestID(aetnaSnapshot))
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_catalogs`).Scan(&activeCatalogs); err != nil {
		t.Fatal(err)
	}
	if activeCatalogs != 2 {
		t.Fatalf("restored catalog output returned %d rows", activeCatalogs)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM mrfweb.release_outputs WHERE catalog_id = $1`, aetnaCatalog); err != nil {
		t.Fatal(err)
	}
	insertCatalogOutput(t, pool, aetnaCatalog, aetnaSnapshot, "mrf-wrong")
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_catalogs`).Scan(&activeCatalogs); err != nil {
		t.Fatal(err)
	}
	if activeCatalogs != 0 {
		t.Fatalf("extra catalog output returned %d rows", activeCatalogs)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_outputs`).Scan(&activeOutputs); err != nil {
		t.Fatal(err)
	}
	if activeOutputs != 0 {
		t.Fatalf("extra catalog output returned %d active outputs", activeOutputs)
	}
}

func formatTestID(id int64) string {
	return strconv.FormatInt(id, 10)
}

func TestIntegrationCatalogConstraintsAndRelationships(t *testing.T) {
	url, pool := withTestDB(t)
	mustMigrate(t, url)
	ctx := context.Background()
	fixtureSnapshot := seedCatalogRelease(t, pool, "uhc")
	catalogID := insertCatalog(t, pool, "uhc", 1, "building")
	otherCatalogID := insertCatalog(t, pool, "uhc", 2, "building")
	otherSnapshot := seedCatalogSnapshot(t, pool, "uhc", "https://example.invalid/uhc/other.json")

	reject := func(name, sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err == nil {
			t.Fatalf("accepted invalid %s", name)
		}
	}
	headerRejects := []struct {
		name string
		sql  string
	}{
		{"payer", `INSERT INTO mrfweb.release_catalogs (payer_id, collection_month, publication_generation, output_fingerprint, output_count) VALUES ('UHC', DATE '2026-08-01', 3, repeat('a', 64), 1)`},
		{"month", `INSERT INTO mrfweb.release_catalogs (payer_id, collection_month, publication_generation, output_fingerprint, output_count) VALUES ('uhc', DATE '2026-08-15', 3, repeat('a', 64), 1)`},
		{"generation", `INSERT INTO mrfweb.release_catalogs (payer_id, collection_month, publication_generation, output_fingerprint, output_count) VALUES ('uhc', DATE '2026-08-01', 0, repeat('a', 64), 1)`},
		{"status", `INSERT INTO mrfweb.release_catalogs (payer_id, collection_month, publication_generation, status, output_fingerprint, output_count) VALUES ('uhc', DATE '2026-08-01', 3, 'unknown', repeat('a', 64), 1)`},
		{"fingerprint", `INSERT INTO mrfweb.release_catalogs (payer_id, collection_month, publication_generation, output_fingerprint, output_count) VALUES ('uhc', DATE '2026-08-01', 3, repeat('A', 64), 1)`},
		{"output count", `INSERT INTO mrfweb.release_catalogs (payer_id, collection_month, publication_generation, output_fingerprint, output_count) VALUES ('uhc', DATE '2026-08-01', 3, repeat('a', 64), 0)`},
		{"duplicate generation", `INSERT INTO mrfweb.release_catalogs (payer_id, collection_month, publication_generation, output_fingerprint, output_count) VALUES ('uhc', DATE '2026-08-01', 1, repeat('a', 64), 1)`},
		{"monthly release foreign key", `INSERT INTO mrfweb.release_catalogs (payer_id, collection_month, publication_generation, output_fingerprint, output_count) VALUES ('aetna', DATE '2026-09-01', 1, repeat('a', 64), 1)`},
	}
	for _, tc := range headerRejects {
		reject(tc.name, tc.sql)
	}
	readyCatalog := insertCatalog(t, pool, "uhc", 3, "ready")
	failedCatalog := insertCatalog(t, pool, "uhc", 4, "failed")
	publishedCatalog := insertCatalog(t, pool, "uhc", 5, "published")
	headerUpdates := []struct {
		name string
		sql  string
		id   int64
	}{
		{"standard count", `UPDATE mrfweb.release_catalogs SET standard_fact_count = 0 WHERE id = $1`, readyCatalog},
		{"provider schema version", `UPDATE mrfweb.release_catalogs SET provider_catalog_schema_version = 0 WHERE id = $1`, readyCatalog},
		{"provider release month", `UPDATE mrfweb.release_catalogs SET provider_catalog_release_month = DATE '2026-08-15' WHERE id = $1`, readyCatalog},
		{"billing code count", `UPDATE mrfweb.release_catalogs SET billing_code_count = -1 WHERE id = $1`, readyCatalog},
		{"code filter count", `UPDATE mrfweb.release_catalogs SET code_filter_value_count = -1 WHERE id = $1`, readyCatalog},
		{"plan count", `UPDATE mrfweb.release_catalogs SET plan_count = -1 WHERE id = $1`, readyCatalog},
		{"plan output count", `UPDATE mrfweb.release_catalogs SET plan_output_count = -1 WHERE id = $1`, readyCatalog},
		{"network count", `UPDATE mrfweb.release_catalogs SET output_code_network_count = -1 WHERE id = $1`, readyCatalog},
		{"provider filter count", `UPDATE mrfweb.release_catalogs SET provider_filter_value_count = -1 WHERE id = $1`, readyCatalog},
		{"building failure", `UPDATE mrfweb.release_catalogs SET failure_code = 'x' WHERE id = $1`, catalogID},
		{"building completion", `UPDATE mrfweb.release_catalogs SET completed_at = transaction_timestamp() WHERE id = $1`, catalogID},
		{"building publication", `UPDATE mrfweb.release_catalogs SET published_at = transaction_timestamp() WHERE id = $1`, catalogID},
		{"ready failure", `UPDATE mrfweb.release_catalogs SET failure_code = 'x' WHERE id = $1`, readyCatalog},
		{"ready completion", `UPDATE mrfweb.release_catalogs SET completed_at = NULL WHERE id = $1`, readyCatalog},
		{"ready publication", `UPDATE mrfweb.release_catalogs SET published_at = transaction_timestamp() WHERE id = $1`, readyCatalog},
		{"ready provider identity", `UPDATE mrfweb.release_catalogs SET provider_catalog_schema_version = NULL WHERE id = $1`, readyCatalog},
		{"ready counts", `UPDATE mrfweb.release_catalogs SET plan_count = NULL WHERE id = $1`, readyCatalog},
		{"failed code", `UPDATE mrfweb.release_catalogs SET failure_code = NULL WHERE id = $1`, failedCatalog},
		{"empty failed code", `UPDATE mrfweb.release_catalogs SET failure_code = '' WHERE id = $1`, failedCatalog},
		{"failed completion", `UPDATE mrfweb.release_catalogs SET completed_at = NULL WHERE id = $1`, failedCatalog},
		{"failed publication", `UPDATE mrfweb.release_catalogs SET published_at = transaction_timestamp() WHERE id = $1`, failedCatalog},
		{"published no published_at", `UPDATE mrfweb.release_catalogs SET published_at = NULL WHERE id = $1`, publishedCatalog},
		{"published provider identity", `UPDATE mrfweb.release_catalogs SET provider_catalog_schema_version = NULL WHERE id = $1`, publishedCatalog},
		{"published counts", `UPDATE mrfweb.release_catalogs SET plan_count = NULL WHERE id = $1`, publishedCatalog},
		{"published publication order", `UPDATE mrfweb.release_catalogs SET published_at = completed_at - interval '1 second' WHERE id = $1`, publishedCatalog},
		{"published completion order", `UPDATE mrfweb.release_catalogs SET completed_at = created_at - interval '1 second' WHERE id = $1`, publishedCatalog},
	}
	for _, tc := range headerUpdates {
		reject(tc.name, tc.sql, tc.id)
	}

	insertCatalogOutput(t, pool, catalogID, fixtureSnapshot, "mrf-"+formatTestID(fixtureSnapshot))
	outputRejects := []struct {
		name string
		sql  string
		args []any
	}{
		{"output empty", `INSERT INTO mrfweb.release_outputs (catalog_id, mrf_snapshot_id, output_id) VALUES ($1, $2, '')`, []any{catalogID, otherSnapshot}},
		{"output CRLF", `INSERT INTO mrfweb.release_outputs (catalog_id, mrf_snapshot_id, output_id) VALUES ($1, $2, E'mrf-\n')`, []any{catalogID, otherSnapshot}},
		{"output missing snapshot", `INSERT INTO mrfweb.release_outputs (catalog_id, mrf_snapshot_id, output_id) VALUES ($1, 999999, 'mrf-99')`, []any{catalogID}},
		{"output catalog foreign key", `INSERT INTO mrfweb.release_outputs (catalog_id, mrf_snapshot_id, output_id) VALUES (999999, $1, 'mrf-99')`, []any{fixtureSnapshot}},
		{"output snapshot duplicate", `INSERT INTO mrfweb.release_outputs (catalog_id, mrf_snapshot_id, output_id) VALUES ($1, $2, 'mrf-other')`, []any{catalogID, fixtureSnapshot}},
		{"output id duplicate", `INSERT INTO mrfweb.release_outputs (catalog_id, mrf_snapshot_id, output_id) VALUES ($1, $2, $3)`, []any{catalogID, otherSnapshot, "mrf-" + formatTestID(fixtureSnapshot)}},
	}
	for _, tc := range outputRejects {
		reject(tc.name, tc.sql, tc.args...)
	}

	if _, err := pool.Exec(ctx, `
INSERT INTO mrfweb.release_billing_codes
    (catalog_id, billing_code_type, billing_code, warehouse_service_name, observation_count, unmodified_observation_count)
VALUES ($1, 'CPT', '99214', 'Office visit', 3, 1)`, catalogID); err != nil {
		t.Fatal("valid billing code:", err)
	}
	billingRejects := []struct {
		name string
		sql  string
	}{
		{"billing empty type", `INSERT INTO mrfweb.release_billing_codes (catalog_id, billing_code_type, billing_code, observation_count, unmodified_observation_count) VALUES ($1, '', 'x', 1, 0)`},
		{"billing type CRLF", `INSERT INTO mrfweb.release_billing_codes (catalog_id, billing_code_type, billing_code, observation_count, unmodified_observation_count) VALUES ($1, E'CPT\n', 'x', 1, 0)`},
		{"billing empty code", `INSERT INTO mrfweb.release_billing_codes (catalog_id, billing_code_type, billing_code, observation_count, unmodified_observation_count) VALUES ($1, 'CPT', '', 1, 0)`},
		{"billing code CRLF", `INSERT INTO mrfweb.release_billing_codes (catalog_id, billing_code_type, billing_code, observation_count, unmodified_observation_count) VALUES ($1, 'CPT', E'x\n', 1, 0)`},
		{"billing version empty", `INSERT INTO mrfweb.release_billing_codes (catalog_id, billing_code_type, billing_code, billing_code_type_version, observation_count, unmodified_observation_count) VALUES ($1, 'CPT', 'x', '', 1, 0)`},
		{"billing version CRLF", `INSERT INTO mrfweb.release_billing_codes (catalog_id, billing_code_type, billing_code, billing_code_type_version, observation_count, unmodified_observation_count) VALUES ($1, 'CPT', 'x', E'v1\n', 1, 0)`},
		{"billing name empty", `INSERT INTO mrfweb.release_billing_codes (catalog_id, billing_code_type, billing_code, warehouse_service_name, observation_count, unmodified_observation_count) VALUES ($1, 'CPT', 'x', '', 1, 0)`},
		{"billing name CRLF", `INSERT INTO mrfweb.release_billing_codes (catalog_id, billing_code_type, billing_code, warehouse_service_name, observation_count, unmodified_observation_count) VALUES ($1, 'CPT', 'x', E'name\n', 1, 0)`},
		{"billing description empty", `INSERT INTO mrfweb.release_billing_codes (catalog_id, billing_code_type, billing_code, warehouse_service_description, observation_count, unmodified_observation_count) VALUES ($1, 'CPT', 'x', '', 1, 0)`},
		{"billing description CRLF", `INSERT INTO mrfweb.release_billing_codes (catalog_id, billing_code_type, billing_code, warehouse_service_description, observation_count, unmodified_observation_count) VALUES ($1, 'CPT', 'x', E'description\n', 1, 0)`},
		{"billing count", `INSERT INTO mrfweb.release_billing_codes (catalog_id, billing_code_type, billing_code, observation_count, unmodified_observation_count) VALUES ($1, 'CPT', 'x', 0, 0)`},
		{"billing unmodified negative", `INSERT INTO mrfweb.release_billing_codes (catalog_id, billing_code_type, billing_code, observation_count, unmodified_observation_count) VALUES ($1, 'CPT', 'x', 1, -1)`},
		{"billing unmodified too large", `INSERT INTO mrfweb.release_billing_codes (catalog_id, billing_code_type, billing_code, observation_count, unmodified_observation_count) VALUES ($1, 'CPT', 'x', 1, 2)`},
		{"billing duplicate", `INSERT INTO mrfweb.release_billing_codes (catalog_id, billing_code_type, billing_code, observation_count, unmodified_observation_count) VALUES ($1, 'CPT', '99214', 1, 0)`},
	}
	for _, tc := range billingRejects {
		reject(tc.name, tc.sql, catalogID)
	}

	if _, err := pool.Exec(ctx, `
INSERT INTO mrfweb.release_code_filter_values
    (catalog_id, billing_code_type, billing_code, filter_kind, filter_value, display_label, observation_count)
VALUES ($1, 'CPT', '99214', 'place_of_service', 'CSTM-00', 'Broad or unspecified place of service', 1)`, catalogID); err != nil {
		t.Fatal("valid CSTM-00:", err)
	}
	var cstmValue, cstmLabel string
	if err := pool.QueryRow(ctx, `
SELECT filter_value, display_label
FROM mrfweb.release_code_filter_values
WHERE catalog_id = $1
  AND billing_code_type = 'CPT'
  AND billing_code = '99214'`, catalogID).Scan(&cstmValue, &cstmLabel); err != nil {
		t.Fatal("read CSTM-00:", err)
	}
	if cstmValue != "CSTM-00" || cstmLabel != "Broad or unspecified place of service" {
		t.Fatalf("CSTM-00 row %q/%q", cstmValue, cstmLabel)
	}
	codeFilterRejects := []struct {
		name string
		sql  string
	}{
		{"code filter kind", `INSERT INTO mrfweb.release_code_filter_values (catalog_id, billing_code_type, billing_code, filter_kind, filter_value, display_label, observation_count) VALUES ($1, 'CPT', '99214', 'other', 'x', 'x', 1)`},
		{"code filter foreign key", `INSERT INTO mrfweb.release_code_filter_values (catalog_id, billing_code_type, billing_code, filter_kind, filter_value, display_label, observation_count) VALUES ($1, 'HCPCS', 'x', 'modifier', 'x', 'x', 1)`},
		{"code filter empty value", `INSERT INTO mrfweb.release_code_filter_values (catalog_id, billing_code_type, billing_code, filter_kind, filter_value, display_label, observation_count) VALUES ($1, 'CPT', '99214', 'modifier', '', 'x', 1)`},
		{"code filter value CRLF", `INSERT INTO mrfweb.release_code_filter_values (catalog_id, billing_code_type, billing_code, filter_kind, filter_value, display_label, observation_count) VALUES ($1, 'CPT', '99214', 'modifier', E'x\n', 'x', 1)`},
		{"code filter empty label", `INSERT INTO mrfweb.release_code_filter_values (catalog_id, billing_code_type, billing_code, filter_kind, filter_value, display_label, observation_count) VALUES ($1, 'CPT', '99214', 'modifier', 'x', '', 1)`},
		{"code filter count", `INSERT INTO mrfweb.release_code_filter_values (catalog_id, billing_code_type, billing_code, filter_kind, filter_value, display_label, observation_count) VALUES ($1, 'CPT', '99214', 'modifier', 'x', 'x', 0)`},
		{"code filter duplicate", `INSERT INTO mrfweb.release_code_filter_values (catalog_id, billing_code_type, billing_code, filter_kind, filter_value, display_label, observation_count) VALUES ($1, 'CPT', '99214', 'place_of_service', 'CSTM-00', 'Broad or unspecified place of service', 1)`},
	}
	for _, tc := range codeFilterRejects {
		reject(tc.name, tc.sql, catalogID)
	}

	var planID, secondPlanID, otherPlanID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfweb.release_plans
    (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text)
VALUES ($1, 'Gold', 'Issuer', 'ein', '12-3456789', 'group', 'gold issuer ein 12-3456789 group')
RETURNING id`, catalogID).Scan(&planID); err != nil {
		t.Fatal("valid plan:", err)
	}
	reject("canonical duplicate", `INSERT INTO mrfweb.release_plans (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text) VALUES ($1, 'Gold', 'Issuer', 'ein', '12-3456789', 'group', 'duplicate')`, catalogID)
	planRejects := []struct {
		name string
		sql  string
	}{
		{"plan name empty", `INSERT INTO mrfweb.release_plans (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text) VALUES ($1, '', 'Issuer', 'ein', 'x', 'group', 'x')`},
		{"plan name CRLF", `INSERT INTO mrfweb.release_plans (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text) VALUES ($1, E'Name\n', 'Issuer', 'ein', 'x', 'group', 'x')`},
		{"plan issuer empty", `INSERT INTO mrfweb.release_plans (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text) VALUES ($1, 'Name', '', 'ein', 'x', 'group', 'x')`},
		{"plan issuer CRLF", `INSERT INTO mrfweb.release_plans (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text) VALUES ($1, 'Name', E'Issuer\n', 'ein', 'x', 'group', 'x')`},
		{"plan id empty", `INSERT INTO mrfweb.release_plans (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text) VALUES ($1, 'Name', 'Issuer', 'ein', '', 'group', 'x')`},
		{"plan id CRLF", `INSERT INTO mrfweb.release_plans (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text) VALUES ($1, 'Name', 'Issuer', 'ein', E'x\n', 'group', 'x')`},
		{"plan search empty", `INSERT INTO mrfweb.release_plans (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text) VALUES ($1, 'Name', 'Issuer', 'ein', 'x', 'group', '')`},
		{"plan search CRLF", `INSERT INTO mrfweb.release_plans (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text) VALUES ($1, 'Name', 'Issuer', 'ein', 'x', 'group', E'x\n')`},
		{"plan id type", `INSERT INTO mrfweb.release_plans (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text) VALUES ($1, 'Name', 'Issuer', 'other', 'x', 'group', 'x')`},
		{"plan market type", `INSERT INTO mrfweb.release_plans (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text) VALUES ($1, 'Name', 'Issuer', 'ein', 'x', 'other', 'x')`},
	}
	for _, tc := range planRejects {
		reject(tc.name, tc.sql, catalogID)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfweb.release_plans
    (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text)
VALUES ($1, 'Silver', 'Issuer', 'hios', 'H1', 'individual', 'silver issuer hios h1 individual')
RETURNING id`, catalogID).Scan(&secondPlanID); err != nil {
		t.Fatal("second plan:", err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfweb.release_plans
    (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text)
VALUES ($1, 'Gold', 'Issuer', 'ein', '12-3456789', 'group', 'gold issuer ein 12-3456789 group')
RETURNING id`, otherCatalogID).Scan(&otherPlanID); err != nil {
		t.Fatal("same plan in separate catalog:", err)
	}

	insertCatalogOutput(t, pool, catalogID, otherSnapshot, "mrf-"+formatTestID(otherSnapshot))
	insertCatalogOutput(t, pool, otherCatalogID, otherSnapshot, "mrf-"+formatTestID(otherSnapshot))
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfweb.release_billing_codes
    (catalog_id, billing_code_type, billing_code, observation_count, unmodified_observation_count)
VALUES ($1, 'CPT', '99214', 1, 0), ($1, 'HCPCS', 'X', 1, 0)`, otherCatalogID); err != nil {
		t.Fatal("other catalog billing codes:", err)
	}
	reject("code filter cross catalog", `INSERT INTO mrfweb.release_code_filter_values (catalog_id, billing_code_type, billing_code, filter_kind, filter_value, display_label, observation_count) VALUES ($1, 'HCPCS', 'X', 'modifier', 'x', 'x', 1)`, catalogID)
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfweb.release_plan_outputs (catalog_id, plan_id, output_id)
VALUES ($1, $2, $3), ($1, $2, $4), ($1, $5, $3)`, catalogID, planID, "mrf-"+formatTestID(fixtureSnapshot), "mrf-"+formatTestID(otherSnapshot), secondPlanID); err != nil {
		t.Fatal("plan output relationships:", err)
	}
	reject("cross catalog plan output", `INSERT INTO mrfweb.release_plan_outputs (catalog_id, plan_id, output_id) VALUES ($1, $2, $3)`, catalogID, otherPlanID, "mrf-"+formatTestID(fixtureSnapshot))
	reject("duplicate plan output", `INSERT INTO mrfweb.release_plan_outputs (catalog_id, plan_id, output_id) VALUES ($1, $2, $3)`, catalogID, planID, "mrf-"+formatTestID(fixtureSnapshot))
	reject("empty plan output", `INSERT INTO mrfweb.release_plan_outputs (catalog_id, plan_id, output_id) VALUES ($1, $2, '')`, catalogID, planID)
	reject("CRLF plan output", `INSERT INTO mrfweb.release_plan_outputs (catalog_id, plan_id, output_id) VALUES ($1, $2, E'mrf-\n')`, catalogID, planID)

	if _, err := pool.Exec(ctx, `
INSERT INTO mrfweb.release_output_code_networks
    (catalog_id, output_id, billing_code_type, billing_code, network_name, observation_count)
VALUES ($1, $2, 'CPT', '99214', 'Network A', 1),
       ($1, $2, 'CPT', '99214', 'Network B', 2)`, catalogID, "mrf-"+formatTestID(fixtureSnapshot)); err != nil {
		t.Fatal("network relationships:", err)
	}
	networkRejects := []struct {
		name string
		sql  string
		args []any
	}{
		{"network output foreign key", `INSERT INTO mrfweb.release_output_code_networks (catalog_id, output_id, billing_code_type, billing_code, network_name, observation_count) VALUES ($1, 'mrf-missing', 'CPT', '99214', 'Network', 1)`, []any{catalogID}},
		{"network code foreign key", `INSERT INTO mrfweb.release_output_code_networks (catalog_id, output_id, billing_code_type, billing_code, network_name, observation_count) VALUES ($1, $2, 'HCPCS', 'x', 'Network', 1)`, []any{catalogID, "mrf-" + formatTestID(fixtureSnapshot)}},
		{"network cross catalog output", `INSERT INTO mrfweb.release_output_code_networks (catalog_id, output_id, billing_code_type, billing_code, network_name, observation_count) VALUES ($1, $2, 'CPT', '99214', 'Network', 1)`, []any{otherCatalogID, "mrf-" + formatTestID(fixtureSnapshot)}},
		{"network cross catalog code", `INSERT INTO mrfweb.release_output_code_networks (catalog_id, output_id, billing_code_type, billing_code, network_name, observation_count) VALUES ($1, $2, 'HCPCS', 'X', 'Network', 1)`, []any{catalogID, "mrf-" + formatTestID(fixtureSnapshot)}},
		{"network empty name", `INSERT INTO mrfweb.release_output_code_networks (catalog_id, output_id, billing_code_type, billing_code, network_name, observation_count) VALUES ($1, $2, 'CPT', '99214', '', 1)`, []any{catalogID, "mrf-" + formatTestID(fixtureSnapshot)}},
		{"network name CRLF", `INSERT INTO mrfweb.release_output_code_networks (catalog_id, output_id, billing_code_type, billing_code, network_name, observation_count) VALUES ($1, $2, 'CPT', '99214', E'Network\n', 1)`, []any{catalogID, "mrf-" + formatTestID(fixtureSnapshot)}},
		{"network count", `INSERT INTO mrfweb.release_output_code_networks (catalog_id, output_id, billing_code_type, billing_code, network_name, observation_count) VALUES ($1, $2, 'CPT', '99214', 'Network C', 0)`, []any{catalogID, "mrf-" + formatTestID(fixtureSnapshot)}},
		{"network duplicate", `INSERT INTO mrfweb.release_output_code_networks (catalog_id, output_id, billing_code_type, billing_code, network_name, observation_count) VALUES ($1, $2, 'CPT', '99214', 'Network A', 1)`, []any{catalogID, "mrf-" + formatTestID(fixtureSnapshot)}},
	}
	for _, tc := range networkRejects {
		reject(tc.name, tc.sql, tc.args...)
	}

	if _, err := pool.Exec(ctx, `
INSERT INTO mrfweb.release_provider_filter_values
    (catalog_id, filter_kind, filter_value, provider_count)
VALUES ($1, 'taxonomy', '207Q00000X', 1), ($1, 'state', 'CA', 2)`, catalogID); err != nil {
		t.Fatal("taxonomy/state filters:", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfweb.release_provider_filter_values
    (catalog_id, filter_kind, parent_value, filter_value, provider_count)
VALUES ($1, 'city', 'CA', 'los angeles', 1)`, catalogID); err != nil {
		t.Fatal("city filter:", err)
	}
	providerRejects := []struct {
		name string
		sql  string
		args []any
	}{
		{"provider kind", `INSERT INTO mrfweb.release_provider_filter_values (catalog_id, filter_kind, filter_value, provider_count) VALUES ($1, 'other', 'x', 1)`, []any{catalogID}},
		{"city parent", `INSERT INTO mrfweb.release_provider_filter_values (catalog_id, filter_kind, filter_value, provider_count) VALUES ($1, 'city', 'los angeles', 1)`, []any{catalogID}},
		{"state parent", `INSERT INTO mrfweb.release_provider_filter_values (catalog_id, filter_kind, parent_value, filter_value, provider_count) VALUES ($1, 'state', 'CA', 'CA', 1)`, []any{catalogID}},
		{"taxonomy parent", `INSERT INTO mrfweb.release_provider_filter_values (catalog_id, filter_kind, parent_value, filter_value, provider_count) VALUES ($1, 'taxonomy', 'x', '207Q00000X', 1)`, []any{catalogID}},
		{"provider empty value", `INSERT INTO mrfweb.release_provider_filter_values (catalog_id, filter_kind, filter_value, provider_count) VALUES ($1, 'taxonomy', '', 1)`, []any{catalogID}},
		{"provider value CRLF", `INSERT INTO mrfweb.release_provider_filter_values (catalog_id, filter_kind, filter_value, provider_count) VALUES ($1, 'taxonomy', E'x\n', 1)`, []any{catalogID}},
		{"provider parent CRLF", `INSERT INTO mrfweb.release_provider_filter_values (catalog_id, filter_kind, parent_value, filter_value, provider_count) VALUES ($1, 'city', E'CA\n', 'x', 1)`, []any{catalogID}},
		{"provider count", `INSERT INTO mrfweb.release_provider_filter_values (catalog_id, filter_kind, filter_value, provider_count) VALUES ($1, 'taxonomy', 'x', 0)`, []any{catalogID}},
		{"provider catalog foreign key", `INSERT INTO mrfweb.release_provider_filter_values (catalog_id, filter_kind, filter_value, provider_count) VALUES (999999, 'taxonomy', 'x', 1)`, nil},
		{"provider duplicate", `INSERT INTO mrfweb.release_provider_filter_values (catalog_id, filter_kind, filter_value, provider_count) VALUES ($1, 'taxonomy', '207Q00000X', 1)`, []any{catalogID}},
	}
	for _, tc := range providerRejects {
		reject(tc.name, tc.sql, tc.args...)
	}
}

func seedCatalogSnapshot(t *testing.T, pool *pgxpool.Pool, payer, sourceURL string) int64 {
	t.Helper()
	ctx := context.Background()
	var sourceID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources (source_url, collection_month)
VALUES ($1, DATE '2026-08-01')
RETURNING id`, sourceURL).Scan(&sourceID); err != nil {
		t.Fatal("seed extra source:", err)
	}
	var snapshotID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots (mrf_source_id, payer_id, collection_month)
VALUES ($1, $2, DATE '2026-08-01')
RETURNING id`, sourceID, payer).Scan(&snapshotID); err != nil {
		t.Fatal("seed extra snapshot:", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_release_outputs
    (payer_id, collection_month, mrf_snapshot_id, published_generation)
VALUES ($1, DATE '2026-08-01', $2, 1)`, payer, snapshotID); err != nil {
		t.Fatal("seed extra release output:", err)
	}
	return snapshotID
}
