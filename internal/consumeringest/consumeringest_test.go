package consumeringest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfconsumer"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

func TestConfigOnlyPublicFields(t *testing.T) {
	t.Parallel()
	typ := reflect.TypeOf(mrfconsumer.Config{})
	allowed := map[string]bool{
		"InputPath": true, "ProviderCatalogPath": true, "OutputPath": true,
		"PayerID": true, "FeedID": true, "CollectionMonth": true, "OutputID": true, "OnProgress": true,
	}
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if !allowed[name] {
			t.Fatalf("unexpected field %s", name)
		}
		if name == "Plans" || name == "BatchID" || name == "PlanAttachmentBatchID" {
			t.Fatal(name)
		}
	}
}

func TestFormatMonthAndOutputID(t *testing.T) {
	t.Parallel()
	if formatMonth(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)) != "2026-08" {
		t.Fatal(formatMonth(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)))
	}
	if formatSnapshotOutputID(211) != "mrf-211" || formatSnapshotOutputID(1) != "mrf-1" {
		t.Fatal(formatSnapshotOutputID(211), formatSnapshotOutputID(1))
	}
	if strings.Contains(formatSnapshotOutputID(12), "012") {
		t.Fatal("padded")
	}
}

func TestClassifyClaim(t *testing.T) {
	t.Parallel()
	job := int64(211)
	same := job
	other := int64(9)
	cases := []struct {
		name    string
		consume string
		parse   string
		stored  *int64
		want    string
		err     string
	}{
		{"succeeded", jobs.StatusSucceeded, jobs.StatusSucceeded, &same, jobs.ClaimNoop, ""},
		{"failed", jobs.StatusFailed, jobs.StatusSucceeded, &same, jobs.ClaimNoop, ""},
		{"stale pending", jobs.StatusPending, jobs.StatusSucceeded, &other, jobs.ClaimNoop, ""},
		{"pending nil", jobs.StatusPending, jobs.StatusSucceeded, nil, "", jobs.FailureDomainInvariant},
		{"pending ready", jobs.StatusPending, jobs.StatusSucceeded, &same, jobs.ClaimWork, ""},
		{"running ready", jobs.StatusRunning, jobs.StatusSucceeded, &same, jobs.ClaimWork, ""},
		{"running mismatch", jobs.StatusRunning, jobs.StatusSucceeded, &other, jobs.ClaimNoop, ""},
		{"pending parse not succeeded", jobs.StatusPending, jobs.StatusPending, &same, "", jobs.FailureDomainInvariant},
		{"blocked", jobs.StatusBlocked, jobs.StatusSucceeded, &same, "", jobs.FailureDomainInvariant},
		{"blocked null", jobs.StatusBlocked, jobs.StatusSucceeded, nil, "", jobs.FailureDomainInvariant},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := classifyClaim(tc.consume, tc.parse, tc.stored, job)
			if tc.err != "" {
				if !jobs.IsFailure(err, tc.err) {
					t.Fatalf("got %v", err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %s %v want %s", got, err, tc.want)
			}
		})
	}
}

func TestMapWorkErrorTypedOnly(t *testing.T) {
	t.Parallel()
	if err := mapWorkError(context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !jobs.IsFailure(mapWorkError(mrfconsumer.ErrInvalidConfig), jobs.FailureConsumerIngestConfigInvalid) {
		t.Fatal("config")
	}
	if !jobs.IsFailure(mapWorkError(mrfconsumer.ErrInvalidInput), jobs.FailureConsumerIngestInputInvalid) {
		t.Fatal("input")
	}
	secret := errors.Join(mrfconsumer.ErrOutput, errors.New("catalog identity https://secret.example/npi"))
	err := mapWorkError(secret)
	if !jobs.IsFailure(err, jobs.FailureConsumerIngestOutputFailed) {
		t.Fatalf("output %v", err)
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "npi") {
		t.Fatalf("exposed %v", err)
	}
}

func TestStartupWarehouseAndOwnedCatalog(t *testing.T) {
	t.Parallel()
	svc := mustServices(t)
	ws := mustWorkspace(t)
	cat := mustCatalog(t)
	absent := filepath.Join(t.TempDir(), "missing-wh")
	st, err := InspectWarehouse(absent)
	if err != nil || st.Kind != warehouseAbsent {
		t.Fatalf("%+v %v", st, err)
	}
	if err := CheckWarehouseCatalog(st, cat, ws.Root, svc); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(t.TempDir(), "empty-wh")
	if err := os.Mkdir(empty, 0700); err != nil {
		t.Fatal(err)
	}
	st, err = InspectWarehouse(empty)
	if err != nil || st.Kind != warehouseEmpty {
		t.Fatalf("%+v %v", st, err)
	}
	owned := filepath.Join(empty, "provider_catalog")
	if err := os.Mkdir(owned, 0700); err != nil {
		t.Fatal(err)
	}
	writeCatalogFixture(t, owned)
	ownedID, err := InspectCatalog(owned)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckWarehouseCatalog(st, ownedID, ws.Root, svc); err == nil {
		t.Fatal("owned catalog on empty warehouse")
	}
	recognized := filepath.Join(t.TempDir(), "ready-wh")
	if err := os.Mkdir(recognized, 0700); err != nil {
		t.Fatal(err)
	}
	writeWarehouseJSON(t, recognized, catalogIdentity{SchemaVersion: 1, ReleaseMonth: testCatalogMonth})
	st, err = InspectWarehouse(recognized)
	if err != nil || st.Kind != warehouseRecognized {
		t.Fatalf("%+v %v", st, err)
	}
	ownedReady := filepath.Join(recognized, "provider_catalog")
	if err := os.Mkdir(ownedReady, 0700); err != nil {
		t.Fatal(err)
	}
	writeCatalogFixture(t, ownedReady)
	readyID, err := InspectCatalog(ownedReady)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckWarehouseCatalog(st, readyID, ws.Root, svc); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(recognized, "other")
	if err := os.Mkdir(nested, 0700); err != nil {
		t.Fatal(err)
	}
	writeCatalogFixture(t, nested)
	badID, err := InspectCatalog(nested)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckWarehouseCatalog(st, badID, ws.Root, svc); err == nil {
		t.Fatal("nested catalog")
	}
	inside := filepath.Join(recognized, "services.csv")
	if err := os.WriteFile(inside, []byte("a"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := CheckWarehouseCatalog(st, readyID, ws.Root, inside); err == nil {
		t.Fatal("services inside warehouse")
	}
	dirty := filepath.Join(t.TempDir(), "dirty-wh")
	if err := os.Mkdir(dirty, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dirty, "noise"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := InspectWarehouse(dirty); err == nil {
		t.Fatal("nonempty without warehouse.json")
	}
}

func TestCatalogMetadataGuard(t *testing.T) {
	t.Parallel()
	id := mustCatalog(t)
	again, err := InspectCatalog(id.Path)
	if err != nil || !id.same(again) {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(id)
	if strings.Contains(string(raw), "sha") || strings.Contains(string(raw), "digest") {
		t.Fatal(string(raw))
	}
	if err := os.WriteFile(filepath.Join(id.Path, fileManifest), append([]byte(`{"schema_version":1}`), '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	changed, err := InspectCatalog(id.Path)
	if err != nil || id.same(changed) {
		t.Fatal("manifest metadata should change")
	}
}

func TestRecognizerPartsAndRejects(t *testing.T) {
	t.Parallel()
	if fmt.Sprintf("%s-part-%05d.parquet", "mrf-9", 100000) != "mrf-9-part-100000.parquet" {
		t.Fatal("wider ordinal")
	}
	warehouse := filepath.Join(t.TempDir(), "wh")
	writePublishedSnapshot(t, warehouse, "uhc", "mrf-source-1", "2026-08", "mrf-9")
	ident := catalogIdentity{SchemaVersion: 1, ReleaseMonth: testCatalogMonth}
	if err := inspectCompletedSnapshot(warehouse, "uhc", "mrf-source-1", "2026-08", "mrf-9", ident); err != nil {
		t.Fatal(err)
	}
	final, _ := expectedFinalPath(warehouse, "uhc", "2026-08", "mrf-9")
	if err := os.WriteFile(filepath.Join(final, "rate_facts", "mrf-9-part-000000.parquet"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := inspectCompletedSnapshot(warehouse, "uhc", "mrf-source-1", "2026-08", "mrf-9", ident); err == nil {
		t.Fatal("extra padding")
	}
	if err := os.Remove(filepath.Join(final, "rate_facts", "mrf-9-part-000000.parquet")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(final, ".DS_Store"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := inspectCompletedSnapshot(warehouse, "uhc", "mrf-source-1", "2026-08", "mrf-9", ident); err == nil {
		t.Fatal("extra root")
	}
	if err := os.Remove(filepath.Join(final, ".DS_Store")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(final, "plans"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := inspectCompletedSnapshot(warehouse, "uhc", "mrf-source-1", "2026-08", "mrf-9", ident); err == nil {
		t.Fatal("plans")
	}
	if err := os.Remove(filepath.Join(final, "plans")); err != nil {
		t.Fatal(err)
	}
	if err := inspectCompletedSnapshot(warehouse, "uhc", "other-feed", "2026-08", "mrf-9", ident); err == nil {
		t.Fatal("feed mismatch")
	}
}

func TestReuseSkipsIngestAndProgress(t *testing.T) {
	ws := mustWorkspace(t)
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	id := int64(4)
	writeValidParsed(t, ws, id, svc)
	writePublishedSnapshot(t, warehouse, "uhc", "mrf-source-4", "2026-08", "mrf-4")
	var buf bytes.Buffer
	called := false
	w := &Worker{
		Workspace: ws, WarehousePath: warehouse, ServicesPath: svc, Catalog: cat,
		Progress: jobs.NewProgress(jobs.NewLogger(&buf)),
		Ingest: func(context.Context, mrfconsumer.Config) (mrfconsumer.Report, error) {
			called = true
			return mrfconsumer.Report{}, errors.New("should not ingest")
		},
	}
	err := w.ingest(context.Background(), ingestJob(8, 4), claimIdentity{
		SnapshotID: 4, SourceID: 4, FeedRowID: 1, PayerID: "uhc", FeedID: "mrf-source-4",
		Month: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), MonthText: "2026-08", ConsumeJobID: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("ingested reuse")
	}
	if strings.Contains(buf.String(), "validating_input") || strings.Contains(buf.String(), "publishing") {
		t.Fatalf("progress %s", buf.String())
	}
}

func TestProgressWiredAndRedacted(t *testing.T) {
	ws := mustWorkspace(t)
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	if err := os.Mkdir(warehouse, 0700); err != nil {
		t.Fatal(err)
	}
	id := int64(6)
	writeValidParsed(t, ws, id, svc)
	var buf bytes.Buffer
	w := &Worker{
		Workspace: ws, WarehousePath: warehouse, ServicesPath: svc, Catalog: cat,
		Progress: jobs.NewProgress(jobs.NewLogger(&buf)),
		Ingest: func(ctx context.Context, cfg mrfconsumer.Config) (mrfconsumer.Report, error) {
			if cfg.OnProgress == nil || cfg.PayerID != "uhc" || cfg.OutputID != "mrf-6" {
				t.Fatalf("cfg %+v", cfg)
			}
			if _, ok := reflect.TypeOf(cfg).FieldByName("Plans"); ok {
				t.Fatal("plans")
			}
			cfg.OnProgress(mrfconsumer.IngestProgress{Phase: "validating_input", Percent: 0})
			cfg.OnProgress(mrfconsumer.IngestProgress{Phase: "publishing", Percent: 40})
			writePublishedSnapshot(t, warehouse, cfg.PayerID, cfg.FeedID, cfg.CollectionMonth, cfg.OutputID)
			final, _ := expectedFinalPath(warehouse, cfg.PayerID, cfg.CollectionMonth, cfg.OutputID)
			return mrfconsumer.Report{OutputID: cfg.OutputID, FinalPath: final}, nil
		},
	}
	err := w.ingest(context.Background(), ingestJob(3, 6), claimIdentity{
		SnapshotID: 6, SourceID: 6, FeedRowID: 1, PayerID: "uhc", FeedID: "mrf-source-6",
		Month: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), MonthText: "2026-08", ConsumeJobID: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	text := buf.String()
	if !strings.Contains(text, `"phase":"validating_input"`) || !strings.Contains(text, `"percent":0`) {
		t.Fatalf("progress %s", text)
	}
	if strings.Contains(text, "copied_bytes") || strings.Contains(text, warehouse) || strings.Contains(text, "mrf-6") {
		t.Fatalf("leaked %s", text)
	}
}

func TestProviderChangedOnlyFromMetadata(t *testing.T) {
	ws := mustWorkspace(t)
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	if err := os.Mkdir(warehouse, 0700); err != nil {
		t.Fatal(err)
	}
	writeValidParsed(t, ws, 2, svc)
	if err := os.WriteFile(filepath.Join(cat.Path, fileManifest), []byte("changed\n"), 0600); err != nil {
		t.Fatal(err)
	}
	called := false
	w := &Worker{
		Workspace: ws, WarehousePath: warehouse, ServicesPath: svc, Catalog: cat,
		Ingest: func(context.Context, mrfconsumer.Config) (mrfconsumer.Report, error) {
			called = true
			return mrfconsumer.Report{}, nil
		},
	}
	err := w.ingest(context.Background(), ingestJob(1, 2), claimIdentity{
		SnapshotID: 2, SourceID: 2, FeedRowID: 1, PayerID: "uhc", FeedID: "mrf-source-2",
		Month: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), MonthText: "2026-08", ConsumeJobID: 1,
	})
	if !jobs.IsFailure(err, jobs.FailureConsumerIngestProviderChanged) {
		t.Fatalf("got %v", err)
	}
	if called {
		t.Fatal("ingested after change")
	}
}

func TestInvalidTargetPreserved(t *testing.T) {
	ws := mustWorkspace(t)
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	writePublishedSnapshot(t, warehouse, "uhc", "mrf-source-3", "2026-08", "mrf-3")
	final, _ := expectedFinalPath(warehouse, "uhc", "2026-08", "mrf-3")
	if err := os.WriteFile(filepath.Join(final, ".DS_Store"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	writeValidParsed(t, ws, 3, svc)
	called := false
	w := &Worker{Workspace: ws, WarehousePath: warehouse, ServicesPath: svc, Catalog: cat,
		Ingest: func(context.Context, mrfconsumer.Config) (mrfconsumer.Report, error) {
			called = true
			return mrfconsumer.Report{}, nil
		}}
	err := w.ingest(context.Background(), ingestJob(1, 3), claimIdentity{
		SnapshotID: 3, SourceID: 3, FeedRowID: 1, PayerID: "uhc", FeedID: "mrf-source-3",
		Month: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), MonthText: "2026-08", ConsumeJobID: 1,
	})
	if !jobs.IsFailure(err, jobs.FailureConsumerIngestOutputInvalid) {
		t.Fatalf("got %v", err)
	}
	if called {
		t.Fatal("re-ingested corrupt")
	}
	if _, err := os.Lstat(filepath.Join(final, fileManifest)); err != nil {
		t.Fatal("deleted target")
	}
}

func TestStrictJSONRejectsNestedDuplicates(t *testing.T) {
	t.Parallel()
	warehouse := []byte(`{"warehouse_schema_version":"1.5.0","provider_catalog":{"schema_version":1,"schema_version":1,"release_month":"2026-08"}}`)
	if _, err := decodeWarehouseJSON(warehouse); err == nil {
		t.Fatal("duplicate nested schema_version")
	}
	if _, err := decodeSnapshotManifest(snapshotJSONWithDataset(`{"row_count":0,"row_count":0,"part_count":1}`)); err == nil {
		t.Fatal("duplicate nested row_count")
	}
	for _, raw := range []string{"-0", "+1", "01", "1e2"} {
		body := []byte(`{"warehouse_schema_version":"1.5.0","provider_catalog":{"schema_version":` + raw + `,"release_month":"2026-08"}}`)
		if _, err := decodeWarehouseJSON(body); err == nil {
			t.Fatalf("accepted non-canonical %s", raw)
		}
	}
}

func TestExistingTargetRejectedWithoutIngest(t *testing.T) {
	cases := []struct {
		name   string
		id     int64
		mutate func(t *testing.T, warehouse, final string)
	}{
		{"wrong warehouse version", 31, func(t *testing.T, warehouse, _ string) {
			if err := os.WriteFile(filepath.Join(warehouse, fileWarehouse), []byte(`{"warehouse_schema_version":"1.4.0","provider_catalog":{"schema_version":1,"release_month":"2026-08"}}`), 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"wrong snapshot version", 32, func(t *testing.T, _, final string) {
			raw := bytes.ReplaceAll(mustRead(t, filepath.Join(final, fileManifest)), []byte(`"1.5.0"`), []byte(`"1.1.0"`))
			if err := os.WriteFile(filepath.Join(final, fileManifest), raw, 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"feed identity mismatch", 33, func(t *testing.T, _, final string) {
			rewriteSnapshotFeed(t, final, "mrf-source-999")
		}},
		{"month identity mismatch", 34, func(t *testing.T, _, final string) {
			raw := bytes.ReplaceAll(mustRead(t, filepath.Join(final, fileManifest)), []byte(`"collection_month":"2026-08"`), []byte(`"collection_month":"2026-09"`))
			if err := os.WriteFile(filepath.Join(final, fileManifest), raw, 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"symlink snapshot target", 35, func(t *testing.T, _, final string) {
			dest := filepath.Join(t.TempDir(), "moved-target")
			if err := os.Rename(final, dest); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(dest, final); err != nil {
				t.Fatal(err)
			}
		}},
		{"symlink manifest", 36, func(t *testing.T, _, final string) {
			man := filepath.Join(final, fileManifest)
			alt := filepath.Join(t.TempDir(), fileManifest)
			if err := os.Rename(man, alt); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(alt, man); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ws := mustWorkspace(t)
			svc := mustServices(t)
			cat := mustCatalog(t)
			warehouse := filepath.Join(t.TempDir(), "wh")
			writeValidParsed(t, ws, tc.id, svc)
			feedID := fmt.Sprintf("mrf-source-%d", tc.id)
			outputID := formatSnapshotOutputID(tc.id)
			writePublishedSnapshot(t, warehouse, "uhc", feedID, "2026-08", outputID)
			final, err := expectedFinalPath(warehouse, "uhc", "2026-08", outputID)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, warehouse, final)
			called := false
			w := &Worker{Workspace: ws, WarehousePath: warehouse, ServicesPath: svc, Catalog: cat,
				Ingest: func(context.Context, mrfconsumer.Config) (mrfconsumer.Report, error) {
					called = true
					return mrfconsumer.Report{}, errors.New("should not ingest")
				}}
			err = w.ingest(context.Background(), ingestJob(1, tc.id), claimIdentity{
				SnapshotID: tc.id, SourceID: tc.id, FeedRowID: 1, PayerID: "uhc", FeedID: feedID,
				Month: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), MonthText: "2026-08", ConsumeJobID: 1,
			})
			if !jobs.IsFailure(err, jobs.FailureConsumerIngestOutputInvalid) {
				t.Fatalf("got %v", err)
			}
			if called {
				t.Fatal("ingested invalid existing target")
			}
			if _, err := os.Lstat(final); err != nil {
				t.Fatal("target not preserved")
			}
		})
	}
}

func TestIngestRepairsMissingCatalogCopy(t *testing.T) {
	ws := mustWorkspace(t)
	t.Setenv("TMPDIR", ws.StagingDir())
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	if err := os.Mkdir(warehouse, 0700); err != nil {
		t.Fatal(err)
	}
	writeWarehouseJSON(t, warehouse, catalogIdentity{SchemaVersion: 1, ReleaseMonth: testCatalogMonth})
	if _, err := os.Lstat(filepath.Join(warehouse, "provider_catalog")); !os.IsNotExist(err) {
		t.Fatal("pre-created catalog")
	}
	const id int64 = 41
	writeRealParsed(t, ws, id, svc)
	final, err := expectedFinalPath(warehouse, "uhc", "2026-08", "mrf-41")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(final); !os.IsNotExist(err) {
		t.Fatal("pre-created target")
	}
	w := &Worker{Workspace: ws, WarehousePath: warehouse, ServicesPath: svc, Catalog: cat}
	err = w.ingest(context.Background(), ingestJob(1, id), claimIdentity{
		SnapshotID: id, SourceID: id, FeedRowID: 1, PayerID: "uhc", FeedID: "mrf-source-41",
		Month: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), MonthText: "2026-08", ConsumeJobID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(warehouse, "provider_catalog", fileManifest)); err != nil {
		t.Fatal("catalog copy not restored")
	}
	if _, err := os.Stat(filepath.Join(warehouse, "plan_associations", "_schema", "part-00000.parquet")); err != nil {
		t.Fatal("seed not restored")
	}
	if err := inspectCompletedSnapshot(warehouse, "uhc", "mrf-source-41", "2026-08", "mrf-41", catalogIdentity{SchemaVersion: 1, ReleaseMonth: testCatalogMonth}); err != nil {
		t.Fatal(err)
	}
}

func TestIngestRepairsMissingSeed(t *testing.T) {
	ws := mustWorkspace(t)
	t.Setenv("TMPDIR", ws.StagingDir())
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	if err := os.Mkdir(warehouse, 0700); err != nil {
		t.Fatal(err)
	}
	writeWarehouseJSON(t, warehouse, catalogIdentity{SchemaVersion: 1, ReleaseMonth: testCatalogMonth})
	owned := filepath.Join(warehouse, "provider_catalog")
	if err := os.Mkdir(owned, 0700); err != nil {
		t.Fatal(err)
	}
	writeCatalogFixture(t, owned)
	if _, err := os.Lstat(filepath.Join(warehouse, "plan_associations", "_schema", "part-00000.parquet")); !os.IsNotExist(err) {
		t.Fatal("pre-created seed")
	}
	const id int64 = 42
	writeRealParsed(t, ws, id, svc)
	final, err := expectedFinalPath(warehouse, "uhc", "2026-08", "mrf-42")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(final); !os.IsNotExist(err) {
		t.Fatal("pre-created target")
	}
	w := &Worker{Workspace: ws, WarehousePath: warehouse, ServicesPath: svc, Catalog: cat}
	err = w.ingest(context.Background(), ingestJob(1, id), claimIdentity{
		SnapshotID: id, SourceID: id, FeedRowID: 1, PayerID: "uhc", FeedID: "mrf-source-42",
		Month: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), MonthText: "2026-08", ConsumeJobID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(warehouse, "plan_associations", "_schema", "part-00000.parquet")); err != nil {
		t.Fatal("seed not restored")
	}
	if err := inspectCompletedSnapshot(warehouse, "uhc", "mrf-source-42", "2026-08", "mrf-42", catalogIdentity{SchemaVersion: 1, ReleaseMonth: testCatalogMonth}); err != nil {
		t.Fatal(err)
	}
}

func TestCancelThenRetrySucceeds(t *testing.T) {
	ws := mustWorkspace(t)
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	if err := os.Mkdir(warehouse, 0700); err != nil {
		t.Fatal(err)
	}
	const id int64 = 43
	writeValidParsed(t, ws, id, svc)
	calls := 0
	w := &Worker{Workspace: ws, WarehousePath: warehouse, ServicesPath: svc, Catalog: cat,
		Ingest: func(ctx context.Context, cfg mrfconsumer.Config) (mrfconsumer.Report, error) {
			calls++
			if calls == 1 {
				return mrfconsumer.Report{}, context.Canceled
			}
			writePublishedSnapshot(t, warehouse, cfg.PayerID, cfg.FeedID, cfg.CollectionMonth, cfg.OutputID)
			final, _ := expectedFinalPath(warehouse, cfg.PayerID, cfg.CollectionMonth, cfg.OutputID)
			return mrfconsumer.Report{OutputID: cfg.OutputID, FinalPath: final}, nil
		}}
	ident := claimIdentity{
		SnapshotID: id, SourceID: id, FeedRowID: 1, PayerID: "uhc", FeedID: "mrf-source-43",
		Month: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), MonthText: "2026-08", ConsumeJobID: 1,
	}
	err := w.ingest(context.Background(), ingestJob(1, id), ident)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	final, _ := expectedFinalPath(warehouse, "uhc", "2026-08", "mrf-43")
	if _, err := os.Lstat(final); !os.IsNotExist(err) {
		t.Fatal("target created on cancel")
	}
	if err := w.ingest(context.Background(), ingestJob(1, id), ident); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls %d", calls)
	}
	if _, err := os.Lstat(filepath.Join(final, fileManifest)); err != nil {
		t.Fatal("retry did not publish")
	}
}

func TestWorkerRegistration(t *testing.T) {
	t.Parallel()
	if (jobs.ConsumerIngestArgs{}).Kind() != jobs.KindConsumerIngest {
		t.Fatal("kind")
	}
	if (jobs.ConsumerIngestArgs{}).InsertOpts().Queue != jobs.QueueConsumer {
		t.Fatal("queue")
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{})
}

func snapshotJSONWithDataset(rateFacts string) []byte {
	return []byte(`{"manifest_schema_version":"1.5.0","output_schema_version":"1.5.0","output_id":"mrf-1","payer_id":"uhc","feed_id":"mrf-source-1","collection_month":"2026-08","provider_catalog":{"schema_version":1,"release_month":"2026-08"},"datasets":{"rate_facts":` + rateFacts + `,"rate_provider_groups":{"row_count":0,"part_count":1},"provider_groups":{"row_count":0,"part_count":1},"provider_group_memberships":{"row_count":0,"part_count":1},"ingestions":{"row_count":1,"part_count":1},"network_names":{"row_count":0,"part_count":1}}}`)
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func rewriteSnapshotFeed(t *testing.T, final, feedID string) {
	t.Helper()
	raw := bytes.ReplaceAll(mustRead(t, filepath.Join(final, fileManifest)), []byte(`"feed_id":"mrf-source-33"`), []byte(`"feed_id":"`+feedID+`"`))
	if err := os.WriteFile(filepath.Join(final, fileManifest), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func ingestJob(jobID, snapshotID int64) *river.Job[jobs.ConsumerIngestArgs] {
	return &river.Job[jobs.ConsumerIngestArgs]{
		JobRow: &rivertype.JobRow{ID: jobID},
		Args:   jobs.ConsumerIngestArgs{MRFSnapshotID: snapshotID},
	}
}
