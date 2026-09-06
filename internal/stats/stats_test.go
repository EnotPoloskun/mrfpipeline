package stats

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestFormatTableHeaderAlignmentAndNewline(t *testing.T) {
	t.Parallel()
	rows := []Row{
		{Stage: "toc.download", PayerID: "uhc", CollectionMonth: "2026-08", Blocked: 0, Pending: 3, Running: 2, Succeeded: 110, Failed: 5, Total: 120},
		{Stage: "consumer.attach_plans", PayerID: "aetna", CollectionMonth: "2026-09", Blocked: 0, Pending: 4, Running: 1, Succeeded: 460, Failed: 5, Total: 470},
	}
	out := FormatTable(rows)
	if !strings.HasSuffix(out, "\n") {
		t.Fatal("missing trailing newline")
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines=%d", len(lines))
	}
	if !strings.HasPrefix(lines[0], "stage") || !strings.Contains(lines[0], "total") {
		t.Fatalf("header %q", lines[0])
	}
	for _, col := range []string{"blocked", "pending", "running", "succeeded", "failed", "total"} {
		if !strings.Contains(lines[0], col) {
			t.Fatalf("header missing %s: %q", col, lines[0])
		}
	}
	if !strings.HasPrefix(lines[1], "toc.download") || !strings.Contains(lines[2], "consumer.attach_plans") {
		t.Fatalf("rows %q", out)
	}
}

func TestFormatTableEmptyHeaderOnly(t *testing.T) {
	t.Parallel()
	out := FormatTable(nil)
	if out != "stage payer month blocked pending running succeeded failed total\n" {
		t.Fatalf("got %q", out)
	}
}

func TestSortRowsOrdersByPayerMonthStage(t *testing.T) {
	t.Parallel()
	rows := []Row{
		{Stage: "mrf.download", PayerID: "uhc", CollectionMonth: "2026-09"},
		{Stage: "toc.download", PayerID: "aetna", CollectionMonth: "2026-08"},
		{Stage: "toc.parse", PayerID: "aetna", CollectionMonth: "2026-08"},
		{Stage: "toc.download", PayerID: "uhc", CollectionMonth: "2026-08"},
	}
	sortRows(rows)
	want := []string{
		"aetna/2026-08/toc.download",
		"aetna/2026-08/toc.parse",
		"uhc/2026-08/toc.download",
		"uhc/2026-09/mrf.download",
	}
	for i, key := range want {
		got := rows[i].PayerID + "/" + rows[i].CollectionMonth + "/" + rows[i].Stage
		if got != key {
			t.Fatalf("%d: got %s want %s", i, got, key)
		}
	}
}

func TestFormatJSONCompactAndOrder(t *testing.T) {
	t.Parallel()
	rows := []Row{
		{Stage: "toc.download", PayerID: "uhc", CollectionMonth: "2026-08", Total: 1},
		{Stage: "toc.parse", PayerID: "uhc", CollectionMonth: "2026-08", Total: 1},
	}
	out, err := FormatJSON(rows)
	if err != nil {
		t.Fatal(err)
	}
	if out != `[{"stage":"toc.download","payer_id":"uhc","collection_month":"2026-08","blocked":0,"pending":0,"running":0,"succeeded":0,"failed":0,"total":1},{"stage":"toc.parse","payer_id":"uhc","collection_month":"2026-08","blocked":0,"pending":0,"running":0,"succeeded":0,"failed":0,"total":1}]`+"\n" {
		t.Fatalf("got %q", out)
	}
	if strings.Contains(out, "\n\n") || strings.Contains(out, "  ") {
		t.Fatalf("pretty printed: %q", out)
	}
}

func TestFormatJSONEmptyArray(t *testing.T) {
	t.Parallel()
	out, err := FormatJSON(nil)
	if err != nil || out != "[]\n" {
		t.Fatalf("got %q err=%v", out, err)
	}
}

func TestFormatJSONMatchesTableOrder(t *testing.T) {
	t.Parallel()
	rows := []Row{
		{Stage: "mrf.download", PayerID: "uhc", CollectionMonth: "2026-09", Total: 2},
		{Stage: "toc.download", PayerID: "aetna", CollectionMonth: "2026-08", Total: 1},
	}
	sortRows(rows)
	table := FormatTable(rows)
	jsonOut, err := FormatJSON(rows)
	if err != nil {
		t.Fatal(err)
	}
	tableLines := strings.Split(strings.TrimSuffix(table, "\n"), "\n")[1:]
	var decoded []Row
	if err := json.Unmarshal([]byte(strings.TrimSuffix(jsonOut, "\n")), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(tableLines) != len(decoded) {
		t.Fatalf("count table=%d json=%d", len(tableLines), len(decoded))
	}
	for i := range decoded {
		if !strings.Contains(tableLines[i], decoded[i].Stage) || !strings.Contains(tableLines[i], decoded[i].PayerID) {
			t.Fatalf("row %d table=%q json=%+v", i, tableLines[i], decoded[i])
		}
	}
}

func TestQueryTextsContainNoSourceURL(t *testing.T) {
	t.Parallel()
	for _, sql := range QueryTexts() {
		if strings.Contains(sql, "source_url") {
			t.Fatalf("stats SQL contains source_url: %s", sql)
		}
	}
}
