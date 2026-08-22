package artifact

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRecordDirName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		kind string
		id   int64
		want string
	}{
		{KindTOC, 1, "toc-1"},
		{KindTOC, 41, "toc-41"},
		{KindMRF, 81, "mrf-source-81"},
		{KindPlanBatch, 230, "plan-batch-230"},
	}
	for _, tc := range cases {
		got, err := RecordDirName(tc.kind, tc.id)
		if err != nil || got != tc.want {
			t.Fatalf("%s %d: %q %v", tc.kind, tc.id, got, err)
		}
		if strings.Contains(got, "http") || strings.Contains(got, "uhc") || strings.Contains(got, ".") {
			t.Fatalf("derived identity: %s", got)
		}
	}
	for _, id := range []int64{0, -1} {
		if _, err := RecordDirName(KindTOC, id); err == nil {
			t.Fatal("invalid id")
		}
	}
	if _, err := RecordDirName("other", 1); err == nil {
		t.Fatal("invalid kind")
	}
}

func TestStagingPrefixParser(t *testing.T) {
	t.Parallel()
	p, err := stagingPrefix(KindTOC, 1)
	if err != nil || p != "toc-download-1-" {
		t.Fatalf("%q %v", p, err)
	}
	p, err = stagingPrefix(KindMRF, 12)
	if err != nil || p != "mrf-download-12-" {
		t.Fatalf("%q %v", p, err)
	}
	kind, id, ok := parseStagingName("toc-download-1-xyz")
	if !ok || kind != KindTOC || id != 1 {
		t.Fatal("parse 1")
	}
	kind, id, ok = parseStagingName("toc-download-12-xyz")
	if !ok || kind != KindTOC || id != 12 {
		t.Fatal("parse 12")
	}
	if _, _, ok := parseStagingName("toc-download-1"); ok {
		t.Fatal("missing hyphen after id")
	}
	if _, _, ok := parseStagingName("other-1-x"); ok {
		t.Fatal("unknown prefix")
	}
}

func TestStagingDir(t *testing.T) {
	t.Parallel()
	got := StagingDir(filepath.Join(string(filepath.Separator), "artifacts"))
	if !strings.HasSuffix(got, filepath.Join("artifacts", ".staging")) {
		t.Fatalf("got %q", got)
	}
}
