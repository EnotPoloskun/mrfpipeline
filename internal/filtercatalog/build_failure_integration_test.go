package filtercatalog

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5/pgxpool"
)

func buildFailureWarehouseDigest(t *testing.T, root string) string {
	t.Helper()
	hash := sha256.New()
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(hash, "%s\x00%s\x00%d\x00%d\x00", relative, info.Mode().String(), info.Size(), info.ModTime().UnixNano()); err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(hash, "%s\x00", link)
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		_, err = hash.Write(data)
		return err
	})
	if err != nil {
		t.Fatalf("digest test warehouse: %v", err)
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func writeCandidateWarehouse(t *testing.T, warehouse string, candidate release.CatalogCandidate) string {
	t.Helper()
	writeExtractionWarehouse(t, warehouse)
	arrangement, codeType, version, code, description, class, setting, negotiated := "ffs", "CPT", "2025", "99214", "Alpha description", "professional", "outpatient", "negotiated"
	alpha := "Alpha"
	oldDate := int32(1)
	for _, target := range candidate.Targets {
		fact := testRateFact{
			OutputID: target.OutputID, PayerID: target.PayerID, CollectionMonth: target.CollectionMonth,
			ServiceID: 1, RateGroupID: 1, PriceID: 1,
			NegotiationArrangement: &arrangement, ServiceName: &alpha, BillingCodeType: &codeType,
			BillingCodeTypeVersion: &version, BillingCode: &code, ServiceDescription: &description,
			ServiceCodes: []string{"11"}, BillingClass: &class, Setting: &setting, NegotiatedType: &negotiated,
			NetworkNames: []string{"Network A"}, NegotiatedRate: testDecimal(20), ExpirationDate: &oldDate,
		}
		writeExtractionSnapshot(t, warehouse, target.PayerID, target.CollectionMonth, target.OutputID,
			[]testRateFact{fact},
			[]testRateProviderGroup{{OutputID: target.OutputID, RateGroupID: 1, ProviderGroupID: "g1"}},
			[]testMembership{{OutputID: target.OutputID, ProviderGroupID: "g1", NPI: "222"}})
	}
	return warehouse
}

func TestIntegrationBuildCancellationAfterHeaderCleansUp(t *testing.T) {
	pool := filterCatalogTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, candidate, _ := seedFilterCatalogBuild(t, pool)
	warehouse := writeCandidateWarehouse(t, filepath.Join(t.TempDir(), "warehouse"), candidate)
	warehouseBefore := buildFailureWarehouseDigest(t, warehouse)

	fakeDir := t.TempDir()
	fakeDuckDB := filepath.Join(fakeDir, "duckdb")
	started := filepath.Join(fakeDir, "started")
	parentPath := filepath.Join(fakeDir, "parent")
	childPath := filepath.Join(fakeDir, "child")
	const fake = `#!/bin/sh
if [ "$1" = "--version" ]; then
  printf '%s\n' 'v1.5.5 12345678'
  exit 0
fi
printf '%s' started > "$MRFPIPELINE_TEST_STARTED"
printf '%s' "$$" > "$MRFPIPELINE_TEST_PARENT"
sleep 30 &
child="$!"
printf '%s' "$child" > "$MRFPIPELINE_TEST_CHILD"
cat >/dev/null
wait "$child"
`
	if err := os.WriteFile(fakeDuckDB, []byte(fake), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", fakeDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("MRFPIPELINE_TEST_STARTED", started)
	t.Setenv("MRFPIPELINE_TEST_PARENT", parentPath)
	t.Setenv("MRFPIPELINE_TEST_CHILD", childPath)

	result := make(chan error, 1)
	go func() {
		_, err := Build(ctx, BuildParams{
			Pool: pool, PayerID: candidate.PayerID,
			CollectionMonth: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			WarehousePath:   warehouse, ProviderCatalogPath: filepath.Join(warehouse, "provider_catalog"),
		})
		result <- err
	}()
	waitForBuildFailureFile(t, started)
	parentBytes := waitForBuildFailureFile(t, parentPath)
	childBytes := waitForBuildFailureFile(t, childPath)
	parentPID, err := strconv.Atoi(string(parentBytes))
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(string(childBytes))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-result:
		if !jobs.IsFailure(err, jobs.FailureFilterCatalogCancelled) || errors.Is(err, context.Canceled) {
			t.Fatalf("Build cancellation error=%v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("Build did not stop after cancellation")
	}
	waitForBuildFailureProcessExit(t, parentPID)
	waitForBuildFailureProcessExit(t, childPID)
	if got := buildFailureWarehouseDigest(t, warehouse); got != warehouseBefore {
		t.Fatalf("cancellation mutated warehouse: before=%s after=%s", warehouseBefore, got)
	}

	var status, failureCode string
	var catalogID int64
	var completedAt *time.Time
	if err := pool.QueryRow(context.Background(), `
SELECT id, status, failure_code, completed_at
FROM mrfweb.release_catalogs
WHERE payer_id = $1 AND collection_month = DATE '2026-08-01'
  AND publication_generation = 1`, candidate.PayerID).Scan(&catalogID, &status, &failureCode, &completedAt); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || failureCode != jobs.FailureFilterCatalogCancelled || completedAt == nil {
		t.Fatalf("catalog=%d status=%s failure=%s completed=%v", catalogID, status, failureCode, completedAt)
	}
	for _, table := range []string{
		"release_outputs", "release_billing_codes", "release_code_filter_values",
		"release_plans", "release_plan_outputs", "release_output_code_networks",
		"release_provider_filter_values",
	} {
		var count int64
		if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM mrfweb."+table+" WHERE catalog_id = $1", catalogID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("failed catalog %s rows=%d", table, count)
		}
	}
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	key := release.FilterCatalogLockKey(candidate.PayerID, candidate.CollectionMonth)
	locked, err := tryCatalogLock(context.Background(), conn, key)
	if err != nil || !locked {
		t.Fatalf("advisory lock not released: locked=%v err=%v", locked, err)
	}
	if _, err := conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1::bigint)", key); err != nil {
		t.Fatal(err)
	}
}

func waitForBuildFailureFile(t *testing.T, path string) []byte {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		data, err := os.ReadFile(path)
		if err == nil && len(data) > 0 {
			return data
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForBuildFailureProcessExit(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("process %d remains after cancellation", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestIntegrationBuildAdvisorySerializationAcrossKeys(t *testing.T) {
	pool := filterCatalogTestDB(t)
	_, candidate, _ := seedFilterCatalogBuild(t, pool)
	warehouse := writeCandidateWarehouse(t, filepath.Join(t.TempDir(), "warehouse"), candidate)
	fakeDir := t.TempDir()
	fakeDuckDB := filepath.Join(fakeDir, "duckdb")
	started := filepath.Join(fakeDir, "started")
	parentPath := filepath.Join(fakeDir, "parent")
	childPath := filepath.Join(fakeDir, "child")
	const fake = `#!/bin/sh
if [ "$1" = "--version" ]; then
  printf '%s\n' 'v1.5.5 12345678'
  exit 0
fi
printf '%s' started > "$MRFPIPELINE_TEST_STARTED"
printf '%s' "$$" > "$MRFPIPELINE_TEST_PARENT"
sleep 30 &
child="$!"
printf '%s' "$child" > "$MRFPIPELINE_TEST_CHILD"
cat >/dev/null
wait "$child"
`
	if err := os.WriteFile(fakeDuckDB, []byte(fake), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", fakeDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("MRFPIPELINE_TEST_STARTED", started)
	t.Setenv("MRFPIPELINE_TEST_PARENT", parentPath)
	t.Setenv("MRFPIPELINE_TEST_CHILD", childPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstResult := make(chan error, 1)
	go func() {
		_, err := Build(ctx, BuildParams{
			Pool: pool, PayerID: candidate.PayerID,
			CollectionMonth: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			WarehousePath:   warehouse, ProviderCatalogPath: filepath.Join(warehouse, "provider_catalog"),
		})
		firstResult <- err
	}()
	waitForBuildFailureFile(t, started)
	parentPIDBytes := waitForBuildFailureFile(t, parentPath)
	childPIDBytes := waitForBuildFailureFile(t, childPath)
	parentPID, err := strconv.Atoi(string(parentPIDBytes))
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(string(childPIDBytes))
	if err != nil {
		t.Fatal(err)
	}
	before := buildFailureCatalogState(t, pool)
	warehouseBefore := buildFailureWarehouseDigest(t, warehouse)
	start := time.Now()
	_, err = Build(context.Background(), BuildParams{
		Pool: pool, PayerID: candidate.PayerID,
		CollectionMonth: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		WarehousePath:   warehouse, ProviderCatalogPath: filepath.Join(warehouse, "provider_catalog"),
	})
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogBusy) {
		t.Fatalf("same-key build error=%v, want filter_catalog_busy", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("same-key build did not return busy promptly")
	}
	if got := buildFailureCatalogState(t, pool); got != before {
		t.Fatal("same-key busy build mutated catalog state")
	}
	if got := buildFailureWarehouseDigest(t, warehouse); got != warehouseBefore {
		t.Fatal("same-key busy build mutated warehouse")
	}
	otherConn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	otherKey := release.FilterCatalogLockKey("other-payer", "2026-09")
	otherLocked, err := tryCatalogLock(context.Background(), otherConn, otherKey)
	if err != nil || !otherLocked {
		otherConn.Release()
		t.Fatalf("different-key lock=%v err=%v", otherLocked, err)
	}
	if _, err := otherConn.Exec(context.Background(), "SELECT pg_advisory_unlock($1::bigint)", otherKey); err != nil {
		otherConn.Release()
		t.Fatal(err)
	}
	otherConn.Release()
	cancel()
	select {
	case err := <-firstResult:
		if !jobs.IsFailure(err, jobs.FailureFilterCatalogCancelled) {
			t.Fatalf("first build cancellation error=%v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("first build did not stop after cancellation")
	}
	waitForBuildFailureProcessExit(t, parentPID)
	waitForBuildFailureProcessExit(t, childPID)
	retryConn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer retryConn.Release()
	retryKey := release.FilterCatalogLockKey(candidate.PayerID, candidate.CollectionMonth)
	retryLocked, err := tryCatalogLock(context.Background(), retryConn, retryKey)
	if err != nil || !retryLocked {
		t.Fatalf("retry lock=%v err=%v", retryLocked, err)
	}
	if _, err := retryConn.Exec(context.Background(), "SELECT pg_advisory_unlock($1::bigint)", retryKey); err != nil {
		t.Fatal(err)
	}
}

func buildFailureCatalogState(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var state string
	if err := pool.QueryRow(context.Background(), `
SELECT COALESCE(string_agg(format('%s:%s:%s:%s', id, status, output_count, COALESCE(failure_code, '')), ',' ORDER BY id), '')
FROM mrfweb.release_catalogs`).Scan(&state); err != nil {
		t.Fatal(err)
	}

	return state
}
func TestIntegrationAbandonedBuildingRecoveryAfterBackendTermination(t *testing.T) {
	pool := filterCatalogTestDB(t)
	ctx := context.Background()
	oldID, candidate, _ := seedFilterCatalogBuild(t, pool)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := release.FilterCatalogLockKey(candidate.PayerID, candidate.CollectionMonth)
	locked, err := tryCatalogLock(ctx, conn, key)
	if err != nil || !locked {
		conn.Release()
		t.Fatalf("initial lock=%v err=%v", locked, err)
	}
	buildingID, err := createBuildingCatalog(ctx, conn, candidate, true, oldID)
	if err != nil {
		conn.Release()
		t.Fatalf("create abandoned building catalog: %v", err)
	}
	var backendPID int
	if err := conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&backendPID); err != nil {
		conn.Release()
		t.Fatal(err)
	}
	terminator, err := pool.Acquire(ctx)
	if err != nil {
		conn.Release()
		t.Fatal(err)
	}
	var terminated bool
	if err := terminator.QueryRow(ctx, "SELECT pg_terminate_backend($1)", backendPID).Scan(&terminated); err != nil {
		terminator.Release()
		conn.Release()
		t.Fatal(err)
	}
	terminator.Release()
	if !terminated {
		conn.Release()
		t.Fatal("backend termination was refused")
	}
	conn.Release()

	fresh, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Release()
	released, err := tryCatalogLock(ctx, fresh, key)
	if err != nil || !released {
		t.Fatalf("lock after backend death=%v err=%v", released, err)
	}
	if _, err := fresh.Exec(ctx, "SELECT pg_advisory_unlock($1::bigint)", key); err != nil {
		t.Fatal(err)
	}
	var status string
	var failureCode *string
	if err := pool.QueryRow(ctx, `SELECT status, failure_code FROM mrfweb.release_catalogs WHERE id = $1`, buildingID).Scan(&status, &failureCode); err != nil {
		t.Fatal(err)
	}
	if status != "building" || failureCode != nil {
		t.Fatalf("abandoned catalog status=%s failure=%v", status, failureCode)
	}
	var outputs int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.release_outputs WHERE catalog_id = $1`, buildingID).Scan(&outputs); err != nil {
		t.Fatal(err)
	}
	if outputs != 1 {
		t.Fatalf("abandoned catalog outputs=%d", outputs)
	}
	replacementID, err := createBuildingCatalog(ctx, fresh, candidate, true, buildingID)
	if err != nil {
		t.Fatalf("replace abandoned building catalog: %v", err)
	}
	if replacementID == buildingID {
		t.Fatal("abandoned building catalog was not replaced")
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.release_catalogs WHERE id = $1`, buildingID).Scan(&outputs); err != nil {
		t.Fatal(err)
	}
	if outputs != 0 {
		t.Fatalf("abandoned catalog retained after replacement: %d", outputs)
	}

	failedID, err := createBuildingCatalog(ctx, fresh, candidate, true, replacementID)
	if err != nil {
		t.Fatalf("create failed-header candidate: %v", err)
	}
	if err := markCatalogFailed(ctx, fresh, failedID, jobs.FailureFilterCatalogQueryFailed); err != nil {
		t.Fatalf("mark failed header: %v", err)
	}
	finalID, err := createBuildingCatalog(ctx, fresh, candidate, true, failedID)
	if err != nil {
		t.Fatalf("replace failed header: %v", err)
	}
	if finalID == failedID {
		t.Fatal("failed header was not replaced")
	}
}

func TestIntegrationBuildPostgresFailureAfterHeader(t *testing.T) {
	pool := filterCatalogTestDB(t)
	_, candidate, _ := seedFilterCatalogBuild(t, pool)
	if _, err := pool.Exec(context.Background(), `DELETE FROM mrfweb.release_catalogs`); err != nil {
		t.Fatal(err)
	}
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	catalogID, err := createBuildingCatalog(context.Background(), conn, candidate, false, 0)
	if err != nil {
		t.Fatalf("create building header: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `ALTER TABLE mrfpipeline.mrf_plans DROP COLUMN plan_name`); err != nil {
		t.Fatal(err)
	}
	_, err = rereadPlanSnapshot(context.Background(), conn, candidate.Targets)
	if err == nil {
		t.Fatal("plan reread after header unexpectedly succeeded")
	}
	var status string
	var failure *string
	if err := pool.QueryRow(context.Background(), `SELECT status, failure_code FROM mrfweb.release_catalogs WHERE id=$1`, catalogID).Scan(&status, &failure); err != nil {
		t.Fatal(err)
	}
	if status != "building" || failure != nil {
		t.Fatalf("after-header catalog status=%s failure=%v", status, failure)
	}
}

func installRowTrigger(t *testing.T, pool *pgxpool.Pool, name, table, timing, whenExpr, body string) {
	t.Helper()
	fnSQL := "CREATE FUNCTION mrfweb." + name + `() RETURNS trigger AS $$
BEGIN
` + body + `
END;
$$ LANGUAGE plpgsql;`
	if _, err := pool.Exec(context.Background(), fnSQL); err != nil {
		t.Fatalf("install function %s: %v", name, err)
	}
	triggerSQL := "CREATE TRIGGER " + name + " " + timing + " ON " + table + " FOR EACH ROW"
	if whenExpr != "" {
		triggerSQL += " WHEN (" + whenExpr + ")"
	}
	triggerSQL += " EXECUTE FUNCTION mrfweb." + name + "();"
	if _, err := pool.Exec(context.Background(), triggerSQL); err != nil {
		t.Fatalf("install trigger %s: %v", name, err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP TRIGGER IF EXISTS "+name+" ON "+table)
		_, _ = pool.Exec(context.Background(), "DROP FUNCTION IF EXISTS mrfweb."+name+"()")
	})
}

func writeFakeDuckDBVersion(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "duckdb"), []byte(`#!/bin/sh
if [ "$1" = "--version" ]; then
  printf '%s\n' 'v1.5.5 12345678'
  exit 0
fi
exit 1
`), 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestIntegrationBuildPostgresFailureBeforeHeader(t *testing.T) {
	pool := filterCatalogTestDB(t)
	_, candidate, _ := seedFilterCatalogBuild(t, pool)
	if _, err := pool.Exec(context.Background(), `DELETE FROM mrfweb.release_catalogs`); err != nil {
		t.Fatal(err)
	}
	installRowTrigger(t, pool, "story32_fail_header", "mrfweb.release_catalogs", "BEFORE INSERT", "", "RAISE EXCEPTION 'header insert failed';")
	t.Setenv("PATH", writeFakeDuckDBVersion(t)+string(os.PathListSeparator)+os.Getenv("PATH"))
	warehouse := writeCandidateWarehouse(t, filepath.Join(t.TempDir(), "warehouse"), candidate)
	_, err := Build(context.Background(), BuildParams{
		Pool: pool, PayerID: candidate.PayerID,
		CollectionMonth: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		WarehousePath:   warehouse, ProviderCatalogPath: filepath.Join(warehouse, "provider_catalog"),
	})
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogDatabaseFailed) {
		t.Fatalf("before-header error=%v", err)
	}
	var count int64
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfweb.release_catalogs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("catalog rows after before-header failure=%d", count)
	}
}

func TestIntegrationPopulateCatalogCopyFailureRollsBack(t *testing.T) {
	pool := filterCatalogTestDB(t)
	cat, candidate, identity := seedFilterCatalogBuild(t, pool)
	installRowTrigger(t, pool, "story32_fail_copy", "mrfweb.release_billing_codes", "BEFORE INSERT", "", "RAISE EXCEPTION 'copy failed';")
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	plan := planIdentity{PlanName: "Plan", IssuerName: "Issuer", PlanIDType: "hios", PlanID: "id", PlanMarketType: "group"}
	projection := planProjection{Plans: []canonicalPlan{{planIdentity: plan, Outputs: []string{candidate.Targets[0].OutputID}}}, PlanOutputs: []planOutput{{Plan: plan, Output: candidate.Targets[0].OutputID}}}
	extracted := Result{WarehouseSchemaVersion: "2.0.0", ProviderCatalogSchemaVersion: 1, ProviderCatalogReleaseMonth: identity.ReleaseMonth, OutputFingerprint: candidate.OutputFingerprint, StandardFactCount: 1, BillingCodes: []BillingCode{{BillingCodeType: "CPT", BillingCode: "1", ObservationCount: 1, UnmodifiedObservationCount: 1}}}
	if _, err := populateCatalog(context.Background(), conn, cat, candidate, identity, extracted, projection, nil); err == nil {
		t.Fatal("copy failure unexpectedly succeeded")
	}
	var status string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM mrfweb.release_catalogs WHERE id=$1`, cat).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "building" {
		t.Fatalf("status after copy failure=%s", status)
	}
	for _, table := range []string{"release_billing_codes", "release_code_filter_values", "release_plans", "release_plan_outputs", "release_output_code_networks", "release_provider_filter_values"} {
		var count int64
		if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM mrfweb."+table+" WHERE catalog_id=$1", cat).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s rows=%d", table, count)
		}
	}
}

func TestIntegrationPopulateCatalogReadyUpdateFailureRollsBack(t *testing.T) {
	pool := filterCatalogTestDB(t)
	cat, candidate, identity := seedFilterCatalogBuild(t, pool)
	installRowTrigger(t, pool, "story32_fail_ready", "mrfweb.release_catalogs", "BEFORE UPDATE", "NEW.status = 'ready'", "RAISE EXCEPTION 'ready update failed';")
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	plan := planIdentity{PlanName: "Plan", IssuerName: "Issuer", PlanIDType: "hios", PlanID: "id", PlanMarketType: "group"}
	projection := planProjection{Plans: []canonicalPlan{{planIdentity: plan, Outputs: []string{candidate.Targets[0].OutputID}}}, PlanOutputs: []planOutput{{Plan: plan, Output: candidate.Targets[0].OutputID}}}
	extracted := Result{WarehouseSchemaVersion: "2.0.0", ProviderCatalogSchemaVersion: 1, ProviderCatalogReleaseMonth: identity.ReleaseMonth, OutputFingerprint: candidate.OutputFingerprint, StandardFactCount: 1, BillingCodes: []BillingCode{{BillingCodeType: "CPT", BillingCode: "1", ObservationCount: 1, UnmodifiedObservationCount: 1}}}
	if _, err := populateCatalog(context.Background(), conn, cat, candidate, identity, extracted, projection, nil); err == nil {
		t.Fatal("ready-update failure unexpectedly succeeded")
	}
	var status string
	var failure *string
	if err := pool.QueryRow(context.Background(), `SELECT status, failure_code FROM mrfweb.release_catalogs WHERE id=$1`, cat).Scan(&status, &failure); err != nil {
		t.Fatal(err)
	}
	if status != "building" || failure != nil {
		t.Fatalf("status=%s failure=%v", status, failure)
	}
	var billing int64
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfweb.release_billing_codes WHERE catalog_id=$1`, cat).Scan(&billing); err != nil {
		t.Fatal(err)
	}
	if billing != 0 {
		t.Fatalf("billing rows after ready failure=%d", billing)
	}
}

func TestIntegrationCancellationCleanupDatabaseFailureLeavesBuilding(t *testing.T) {
	pool := filterCatalogTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, candidate, _ := seedFilterCatalogBuild(t, pool)
	if _, err := pool.Exec(context.Background(), `DELETE FROM mrfweb.release_catalogs`); err != nil {
		t.Fatal(err)
	}
	installRowTrigger(t, pool, "story32_fail_cleanup", "mrfweb.release_catalogs", "BEFORE UPDATE", "NEW.status = 'failed'", "RAISE EXCEPTION 'cleanup failed';")
	warehouse := writeCandidateWarehouse(t, filepath.Join(t.TempDir(), "warehouse"), candidate)
	fakeDir := t.TempDir()
	fakeDuckDB := filepath.Join(fakeDir, "duckdb")
	started := filepath.Join(fakeDir, "started")
	parentPath := filepath.Join(fakeDir, "parent")
	childPath := filepath.Join(fakeDir, "child")
	const fake = `#!/bin/sh
if [ "$1" = "--version" ]; then
  printf '%s\n' 'v1.5.5 12345678'
  exit 0
fi
printf '%s' started > "$MRFPIPELINE_TEST_STARTED"
printf '%s' "$$" > "$MRFPIPELINE_TEST_PARENT"
sleep 30 &
child="$!"
printf '%s' "$child" > "$MRFPIPELINE_TEST_CHILD"
cat >/dev/null
wait "$child"
`
	if err := os.WriteFile(fakeDuckDB, []byte(fake), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", fakeDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("MRFPIPELINE_TEST_STARTED", started)
	t.Setenv("MRFPIPELINE_TEST_PARENT", parentPath)
	t.Setenv("MRFPIPELINE_TEST_CHILD", childPath)
	result := make(chan error, 1)
	go func() {
		_, err := Build(ctx, BuildParams{
			Pool: pool, PayerID: candidate.PayerID,
			CollectionMonth: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			WarehousePath:   warehouse, ProviderCatalogPath: filepath.Join(warehouse, "provider_catalog"),
		})
		result <- err
	}()
	waitForBuildFailureFile(t, started)
	parentPID, err := strconv.Atoi(string(waitForBuildFailureFile(t, parentPath)))
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(string(waitForBuildFailureFile(t, childPath)))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-result:
		if !jobs.IsFailure(err, jobs.FailureFilterCatalogCancelled) {
			t.Fatalf("Build cancellation error=%v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("Build did not stop after cancellation")
	}
	waitForBuildFailureProcessExit(t, parentPID)
	waitForBuildFailureProcessExit(t, childPID)
	var catalogID int64
	var status string
	var failureCode *string
	if err := pool.QueryRow(context.Background(), `
SELECT id, status, failure_code
FROM mrfweb.release_catalogs
WHERE payer_id = $1 AND collection_month = DATE '2026-08-01'
  AND publication_generation = 1`, candidate.PayerID).Scan(&catalogID, &status, &failureCode); err != nil {
		t.Fatal(err)
	}
	if status != "building" || failureCode != nil {
		t.Fatalf("cleanup-failure catalog status=%s failure=%v", status, failureCode)
	}
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	replacementID, err := createBuildingCatalog(context.Background(), conn, candidate, true, catalogID)
	if err != nil {
		t.Fatalf("replace leftover building catalog: %v", err)
	}
	if replacementID == catalogID {
		t.Fatal("leftover building catalog was not replaced")
	}
}
