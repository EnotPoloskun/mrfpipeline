package planattach

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/enotpoloskun/mrfconsumer"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/planbatch"
	"github.com/riverqueue/river"
)

func TestAttachConfigOnlyPublicFields(t *testing.T) {
	t.Parallel()
	typ := reflect.TypeOf(mrfconsumer.AttachPlansConfig{})
	allowed := map[string]bool{"PlansPath": true, "OutputPath": true, "OutputID": true, "PlanBatchID": true}
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if !allowed[name] {
			t.Fatalf("unexpected field %s", name)
		}
		if name == "OnProgress" {
			t.Fatal("OnProgress")
		}
	}
}

func TestFormatIDs(t *testing.T) {
	t.Parallel()
	if formatBatchID(230) != "plan-batch-230" {
		t.Fatal(formatBatchID(230))
	}
	if planbatch.FormatPlanBatchID(7) != "plan-batch-7" {
		t.Fatal(planbatch.FormatPlanBatchID(7))
	}
}

func TestEncodePlansExactJSON(t *testing.T) {
	t.Parallel()
	sponsor := "Acme & Co <x>"
	got, err := encodePlans([]planRow{
		{PlanName: "Alpha", IssuerName: "Iss", PlanSponsorName: &sponsor, PlanIDType: "ein", PlanID: "12-345", PlanMarketType: "group"},
		{PlanName: "Beta", IssuerName: "Iss", PlanIDType: "hios", PlanID: "H1", PlanMarketType: "individual"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(got, []byte("\n")) || bytes.HasSuffix(got, []byte("\n\n")) {
		t.Fatalf("newline %q", got)
	}
	if bytes.Contains(got, []byte("\\u003c")) || bytes.Contains(got, []byte("\\u0026")) {
		t.Fatalf("html escaped %s", got)
	}
	if bytes.Contains(got, []byte(`"plan_sponsor_name":""`)) {
		t.Fatal("empty sponsor")
	}
	var decoded []map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(got), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 2 {
		t.Fatal(len(decoded))
	}
	keys := make([]string, 0, 6)
	dec := json.NewDecoder(bytes.NewReader(got))
	tok, _ := dec.Token()
	if tok != json.Delim('[') {
		t.Fatal(tok)
	}
	tok, _ = dec.Token()
	if tok != json.Delim('{') {
		t.Fatal(tok)
	}
	for dec.More() {
		k, _ := dec.Token()
		keys = append(keys, k.(string))
		_, _ = dec.Token()
	}
	want := []string{"plan_name", "issuer_name", "plan_sponsor_name", "plan_id_type", "plan_id", "plan_market_type"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("order %v", keys)
	}
	if !strings.Contains(string(got), `"plan_sponsor_name":null`) {
		t.Fatal(string(got))
	}
	if !strings.Contains(string(got), `"plan_sponsor_name":"Acme & Co <x>"`) {
		t.Fatal(string(got))
	}
	empty := ""
	if _, err := encodePlans([]planRow{{PlanName: "X", IssuerName: "I", PlanSponsorName: &empty, PlanIDType: "hios", PlanID: "1", PlanMarketType: "group"}}); err == nil {
		t.Fatal("empty hios sponsor")
	}
}

func TestEncodePlansNullableEINSponsor(t *testing.T) {
	t.Parallel()
	got, err := encodePlans([]planRow{{
		PlanName: "A", IssuerName: "I", PlanIDType: "ein", PlanID: "1", PlanMarketType: "group",
	}})
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"plan_name":"A","issuer_name":"I","plan_sponsor_name":null,"plan_id_type":"ein","plan_id":"1","plan_market_type":"group"}]` + "\n"
	if string(got) != want {
		t.Fatalf("got %q want %q", got, want)
	}
	empty := ""
	if _, err := encodePlans([]planRow{{
		PlanName: "A", IssuerName: "I", PlanSponsorName: &empty, PlanIDType: "ein", PlanID: "1", PlanMarketType: "group",
	}}); err == nil {
		t.Fatal("empty ein sponsor")
	}
}

func TestPublishReuseAndPreserve(t *testing.T) {
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := encodePlans([]planRow{{PlanName: "A", IssuerName: "I", PlanIDType: "hios", PlanID: "1", PlanMarketType: "group"}})
	if err != nil {
		t.Fatal(err)
	}
	path, err := publishPlansJSON(ws, 9, canonical)
	if err != nil {
		t.Fatal(err)
	}
	again, err := publishPlansJSON(ws, 9, canonical)
	if err != nil || again != path {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("[]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := publishPlansJSON(ws, 9, canonical); !jobs.IsFailure(err, jobs.FailurePlanAttachInputInvalid) {
		t.Fatalf("mismatch %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, []byte("[]\n")) {
		t.Fatal("rewrote invalid")
	}
	_ = info
	dir, _ := plansDir(ws, 10)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path, err = publishPlansJSON(ws, 10, canonical)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatal(err)
	}
	dir, _ = plansDir(ws, 11)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, filePlans), canonical, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "extra"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := publishPlansJSON(ws, 11, canonical); !jobs.IsFailure(err, jobs.FailurePlanAttachInputInvalid) {
		t.Fatalf("extra %v", err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "extra")); err != nil {
		t.Fatal("deleted extra")
	}
}

func TestMapWorkError(t *testing.T) {
	t.Parallel()
	if err := mapWorkError(context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !jobs.IsFailure(mapWorkError(mrfconsumer.ErrInvalidConfig), jobs.FailurePlanAttachConfigInvalid) {
		t.Fatal("config")
	}
	if !jobs.IsFailure(mapWorkError(mrfconsumer.ErrInvalidInput), jobs.FailurePlanAttachInputInvalid) {
		t.Fatal("input")
	}
	secret := errors.Join(mrfconsumer.ErrOutput, errors.New("plan Acme https://secret"))
	err := mapWorkError(secret)
	if !jobs.IsFailure(err, jobs.FailurePlanAttachOutputFailed) {
		t.Fatal(err)
	}
	if strings.Contains(err.Error(), "Acme") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("leaked %v", err)
	}
	if !jobs.IsFailure(jobs.Failure(jobs.FailurePlanAttachOutputInvalid), jobs.FailurePlanAttachOutputInvalid) {
		t.Fatal("output invalid")
	}
}

func TestAttachPlansEmptyDirAndStrayPart(t *testing.T) {
	ws := mustWorkspace(t)
	t.Setenv("TMPDIR", ws.StagingDir())
	svc := mustServices(t)
	cat := mustCatalog(t)
	warehouse := filepath.Join(t.TempDir(), "wh")
	writeRealParsed(t, ws, 1, svc)
	parsed, err := ws.ParsedDir(artifact.KindMRF, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mrfconsumer.Ingest(context.Background(), mrfconsumer.Config{
		InputPath: parsed, ProviderCatalogPath: cat, OutputPath: warehouse,
		PayerID: "uhc", CollectionMonth: "2026-08", OutputID: "mrf-1",
	}); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(warehouse, "plan_associations", "output_id=other")
	if err := os.MkdirAll(empty, 0700); err != nil {
		t.Fatal(err)
	}
	canonical, err := encodePlans([]planRow{{PlanName: "A", IssuerName: "issuer", PlanIDType: "hios", PlanID: "1", PlanMarketType: "group"}})
	if err != nil {
		t.Fatal(err)
	}
	path, err := publishPlansJSON(ws, 5, canonical)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mrfconsumer.AttachPlans(context.Background(), mrfconsumer.AttachPlansConfig{
		PlansPath: path, OutputPath: warehouse, OutputID: "mrf-1", PlanBatchID: "plan-batch-5",
	}); err != nil {
		t.Fatal(err)
	}
	stray := filepath.Join(warehouse, "plan_associations", "stray.parquet")
	if err := os.WriteFile(stray, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err = mrfconsumer.AttachPlans(context.Background(), mrfconsumer.AttachPlansConfig{
		PlansPath: path, OutputPath: warehouse, OutputID: "mrf-1", PlanBatchID: "plan-batch-6",
	})
	if !errors.Is(err, mrfconsumer.ErrOutput) {
		t.Fatalf("got %v", err)
	}
	if _, err := os.Lstat(stray); err != nil {
		t.Fatal("stray removed")
	}
}

func TestLockSnapshotForSucceedErrors(t *testing.T) {
	t.Parallel()
	if !jobs.IsFailure(lockSnapshotForSucceed(context.Background(), nil, 1), jobs.FailurePlanAttachDatabaseFailed) {
		t.Fatal("nil tx")
	}
	if !jobs.IsFailure(lockSnapshotForSucceed(context.Background(), nil, 0), jobs.FailurePlanAttachDatabaseFailed) {
		t.Fatal("zero id")
	}
}

func TestWorkerRegistration(t *testing.T) {
	t.Parallel()
	if (jobs.ConsumerAttachPlansArgs{}).Kind() != jobs.KindConsumerAttachPlans {
		t.Fatal("kind")
	}
	if (jobs.ConsumerAttachPlansArgs{}).InsertOpts().Queue != jobs.QueueConsumer {
		t.Fatal("queue")
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{})
}
