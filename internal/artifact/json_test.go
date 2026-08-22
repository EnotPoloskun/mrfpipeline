package artifact

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestWorkspaceMarkerJSON(t *testing.T) {
	t.Parallel()
	raw := markerJSON()
	if string(raw) != "{\"application\":\"mrfpipeline\",\"schema_version\":\"1.0.0\"}\n" {
		t.Fatalf("got %q", raw)
	}
	if err := parseWorkspaceMarker(raw); err != nil {
		t.Fatal(err)
	}
	if err := parseWorkspaceMarker([]byte(`{ "application" : "mrfpipeline" , "schema_version" : "1.0.0" }`)); err != nil {
		t.Fatal(err)
	}
	rejects := []string{
		`{}`,
		`{"application":"mrfpipeline"}`,
		`{"application":"other","schema_version":"1.0.0"}`,
		`{"application":"mrfpipeline","schema_version":"2.0.0"}`,
		`{"application":"mrfpipeline","schema_version":"1.0.0","extra":"x"}`,
		`{"application":"mrfpipeline","schema_version":"1.0.0","application":"mrfpipeline"}`,
		`[]`,
	}
	for _, raw := range rejects {
		if err := parseWorkspaceMarker([]byte(raw)); !errors.Is(err, ErrArtifact) {
			t.Fatalf("%s: %v", raw, err)
		}
	}
}

func TestDownloadManifestJSON(t *testing.T) {
	t.Parallel()
	raw, err := downloadManifestJSON(123)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "{\"schema_version\":\"1.0.0\",\"byte_count\":123}\n" {
		t.Fatalf("got %q", raw)
	}
	n, err := parseDownloadManifest(raw)
	if err != nil || n != 123 {
		t.Fatalf("%d %v", n, err)
	}
	n, err = parseDownloadManifest([]byte(`{ "schema_version": "1.0.0", "byte_count": 0 }`))
	if err != nil || n != 0 {
		t.Fatalf("zero %d %v", n, err)
	}
	rejects := []string{
		`{}`,
		`{"schema_version":"1.0.0"}`,
		`{"schema_version":"1.0.0","byte_count":-1}`,
		`{"schema_version":"1.0.0","byte_count":1.5}`,
		`{"schema_version":"1.0.0","byte_count":"1"}`,
		`{"schema_version":"1.0.0","byte_count":null}`,
		`{"schema_version":"2.0.0","byte_count":1}`,
		`{"schema_version":"1.0.0","byte_count":1,"extra":2}`,
		`{"schema_version":"1.0.0","byte_count":1,"byte_count":2}`,
	}
	for _, raw := range rejects {
		if _, err := parseDownloadManifest([]byte(raw)); !errors.Is(err, ErrArtifact) {
			t.Fatalf("%s: %v", raw, err)
		}
	}
	var obj map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(raw), &obj); err != nil {
		t.Fatal(err)
	}
}

func TestErrorsRedactFixtures(t *testing.T) {
	t.Parallel()
	err := artErr("stat")
	hostile := []string{"https://payer.example/toc.json", "/tmp/secret", "10.0.0.1", "postgres://", "gzip"}
	msg := err.Error()
	for _, h := range hostile {
		if strings.Contains(msg, h) {
			t.Fatalf("leaked %s: %s", h, msg)
		}
	}
	if !errors.Is(err, ErrArtifact) {
		t.Fatal(err)
	}
}
