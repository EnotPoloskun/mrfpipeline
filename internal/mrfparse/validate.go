package mrfparse

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	manifestSchemaVersion = "1.1.0"
	outputSchemaVersion   = "1.1.0"
	statusComplete        = "complete"
	sourceKindLocal       = "local"
	selectionServiceCSV   = "service_csv"
	maxManifestBytes      = 4 << 20
	maxStatsBytes         = 64 << 10
	fileManifest          = "manifest.json"
	fileStats             = "processing_stats.json"
)

var (
	errOutputInvalid = errors.New("mrf parse output invalid")
	datasetNames     = []string{
		"mrf_files", "services", "service_relations", "rate_groups",
		"negotiated_prices", "rate_provider_groups", "provider_groups", "providers",
	}
	warningCategories = []string{
		"unknown_field", "missing_required", "invalid_value", "source_schema",
		"unresolved_provider_reference", "duplicate_provider_definition",
		"unused_provider_definition", "duplicate_selector_entry",
	}
)

type completedOutput struct {
	SourceURI    string
	SelectorURI  string
	Counts       datasetCounts
	SourceCounts sourceCounts
}

type datasetCounts struct {
	MRFFiles           int64
	Services           int64
	ServiceRelations   int64
	RateGroups         int64
	NegotiatedPrices   int64
	RateProviderGroups int64
	ProviderGroups     int64
	Providers          int64
}

type sourceCounts struct {
	Source   int64
	Retained int64
	Filtered int64
}

func validateCompletedOutput(dir, sourceURI, servicesPath string) error {
	if err := validateRootLayout(dir); err != nil {
		return err
	}
	raw, err := readLimited(filepath.Join(dir, fileManifest), maxManifestBytes)
	if err != nil {
		return errOutputInvalid
	}
	m, err := decodeManifest(raw)
	if err != nil {
		return errOutputInvalid
	}
	if m.SourceURI != sourceURI {
		return errOutputInvalid
	}
	wantSel, err := normalizePath(servicesPath)
	if err != nil {
		return errOutputInvalid
	}
	if resolved, rerr := filepath.EvalSymlinks(wantSel); rerr == nil {
		wantSel = resolved
	}
	if m.SelectorURI != wantSel {
		return errOutputInvalid
	}
	statsRaw, err := readLimited(filepath.Join(dir, fileStats), maxStatsBytes)
	if err != nil {
		return errOutputInvalid
	}
	if err := decodeProcessingStats(statsRaw); err != nil {
		return errOutputInvalid
	}
	return nil
}

func validateRootLayout(dir string) error {
	info, err := os.Lstat(dir)
	if err != nil || isSymlink(info) || !info.IsDir() {
		return errOutputInvalid
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != len(datasetNames)+2 {
		return errOutputInvalid
	}
	seen := map[string]bool{}
	for _, e := range entries {
		seen[e.Name()] = true
		p := filepath.Join(dir, e.Name())
		fi, err := os.Lstat(p)
		if err != nil || isSymlink(fi) {
			return errOutputInvalid
		}
		switch e.Name() {
		case fileManifest, fileStats:
			if !fi.Mode().IsRegular() {
				return errOutputInvalid
			}
		default:
			if !fi.IsDir() {
				return errOutputInvalid
			}
			if err := validateParts(p); err != nil {
				return err
			}
		}
	}
	if !seen[fileManifest] || !seen[fileStats] {
		return errOutputInvalid
	}
	for _, name := range datasetNames {
		if !seen[name] {
			return errOutputInvalid
		}
	}
	return nil
}

func validateParts(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) == 0 {
		return errOutputInvalid
	}
	named := map[int]bool{}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "part-") || !strings.HasSuffix(name, ".parquet") {
			return errOutputInvalid
		}
		num, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "part-"), ".parquet"))
		if err != nil || num < 0 || fmt.Sprintf("part-%05d.parquet", num) != name {
			return errOutputInvalid
		}
		fi, err := os.Lstat(filepath.Join(dir, name))
		if err != nil || isSymlink(fi) || !fi.Mode().IsRegular() {
			return errOutputInvalid
		}
		if named[num] {
			return errOutputInvalid
		}
		named[num] = true
	}
	for i := 0; i < len(named); i++ {
		if !named[i] {
			return errOutputInvalid
		}
	}
	return nil
}

func decodeManifest(data []byte) (completedOutput, error) {
	var zero completedOutput
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return zero, err
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return zero, errOutputInvalid
	}
	seen := map[string]bool{}
	var m completedOutput
	var manifestVer, outputVer, status string
	var haveSource, haveSel, haveCounts, haveWarn bool
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return zero, err
		}
		key, ok := keyTok.(string)
		if !ok || seen[key] {
			return zero, errOutputInvalid
		}
		seen[key] = true
		switch key {
		case "manifest_schema_version":
			manifestVer, err = decodeString(dec)
		case "output_schema_version":
			outputVer, err = decodeString(dec)
		case "status":
			status, err = decodeString(dec)
		case "source":
			haveSource = true
			m.SourceURI, err = decodeSource(dec)
		case "selection":
			haveSel = true
			m.SelectorURI, err = decodeSelection(dec)
		case "counts":
			haveCounts = true
			m.Counts, m.SourceCounts, err = decodeCounts(dec)
		case "warnings":
			haveWarn = true
			err = decodeWarnings(dec)
		default:
			return zero, errOutputInvalid
		}
		if err != nil {
			return zero, err
		}
	}
	end, err := dec.Token()
	if err != nil {
		return zero, err
	}
	if delim, ok := end.(json.Delim); !ok || delim != '}' {
		return zero, errOutputInvalid
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return zero, errOutputInvalid
	}
	if off := dec.InputOffset(); int(off) < len(data) && len(bytes.TrimSpace(data[off:])) > 0 {
		return zero, errOutputInvalid
	}
	if len(seen) != 7 || !haveSource || !haveSel || !haveCounts || !haveWarn {
		return zero, errOutputInvalid
	}
	if manifestVer != manifestSchemaVersion || outputVer != outputSchemaVersion || status != statusComplete {
		return zero, errOutputInvalid
	}
	if m.SourceURI == "" || !filepath.IsAbs(m.SourceURI) || filepath.Clean(m.SourceURI) != m.SourceURI {
		return zero, errOutputInvalid
	}
	if m.SelectorURI == "" || !filepath.IsAbs(m.SelectorURI) {
		return zero, errOutputInvalid
	}
	if m.Counts.MRFFiles != 1 || m.SourceCounts.Source != m.SourceCounts.Retained+m.SourceCounts.Filtered {
		return zero, errOutputInvalid
	}
	if m.Counts.Services != m.SourceCounts.Retained {
		return zero, errOutputInvalid
	}
	return m, nil
}

func decodeSource(dec *json.Decoder) (string, error) {
	obj, err := decodeExactObject(dec, []string{"uri", "kind"})
	if err != nil {
		return "", err
	}
	uri, ok := obj["uri"].(string)
	if !ok || uri == "" || !utf8.ValidString(uri) {
		return "", errOutputInvalid
	}
	kind, ok := obj["kind"].(string)
	if !ok || kind != sourceKindLocal {
		return "", errOutputInvalid
	}
	return uri, nil
}

func decodeSelection(dec *json.Decoder) (string, error) {
	obj, err := decodeExactObject(dec, []string{
		"mode", "selector_uri", "requested_unique_pair_count",
		"matched_unique_pair_count", "unmatched_unique_pair_count",
	})
	if err != nil {
		return "", err
	}
	mode, ok := obj["mode"].(string)
	if !ok || mode != selectionServiceCSV {
		return "", errOutputInvalid
	}
	uri, ok := obj["selector_uri"].(string)
	if !ok || uri == "" || !utf8.ValidString(uri) {
		return "", errOutputInvalid
	}
	req, err := decodeInt64(obj["requested_unique_pair_count"])
	if err != nil {
		return "", err
	}
	matched, err := decodeInt64(obj["matched_unique_pair_count"])
	if err != nil {
		return "", err
	}
	unmatched, err := decodeInt64(obj["unmatched_unique_pair_count"])
	if err != nil {
		return "", err
	}
	if req != matched+unmatched {
		return "", errOutputInvalid
	}
	return uri, nil
}

func decodeCounts(dec *json.Decoder) (datasetCounts, sourceCounts, error) {
	var zeroD datasetCounts
	var zeroS sourceCounts
	obj, err := decodeExactObject(dec, []string{
		"source_service_count", "retained_service_count", "filtered_service_count",
		"mrf_files", "services", "service_relations", "rate_groups",
		"negotiated_prices", "rate_provider_groups", "provider_groups", "providers",
	})
	if err != nil {
		return zeroD, zeroS, err
	}
	n := func(key string) (int64, error) { return decodeInt64(obj[key]) }
	var d datasetCounts
	var s sourceCounts
	if s.Source, err = n("source_service_count"); err != nil {
		return zeroD, zeroS, err
	}
	if s.Retained, err = n("retained_service_count"); err != nil {
		return zeroD, zeroS, err
	}
	if s.Filtered, err = n("filtered_service_count"); err != nil {
		return zeroD, zeroS, err
	}
	if d.MRFFiles, err = n("mrf_files"); err != nil {
		return zeroD, zeroS, err
	}
	if d.Services, err = n("services"); err != nil {
		return zeroD, zeroS, err
	}
	if d.ServiceRelations, err = n("service_relations"); err != nil {
		return zeroD, zeroS, err
	}
	if d.RateGroups, err = n("rate_groups"); err != nil {
		return zeroD, zeroS, err
	}
	if d.NegotiatedPrices, err = n("negotiated_prices"); err != nil {
		return zeroD, zeroS, err
	}
	if d.RateProviderGroups, err = n("rate_provider_groups"); err != nil {
		return zeroD, zeroS, err
	}
	if d.ProviderGroups, err = n("provider_groups"); err != nil {
		return zeroD, zeroS, err
	}
	if d.Providers, err = n("providers"); err != nil {
		return zeroD, zeroS, err
	}
	return d, s, nil
}

func decodeWarnings(dec *json.Decoder) error {
	obj, err := decodeExactObject(dec, []string{"totals", "examples", "examples_truncated", "examples_omitted_count"})
	if err != nil {
		return err
	}
	totalsRaw, err := json.Marshal(obj["totals"])
	if err != nil {
		return err
	}
	totalsDec := json.NewDecoder(bytes.NewReader(totalsRaw))
	totalsDec.UseNumber()
	totalsObj, err := decodeExactObject(totalsDec, warningCategories)
	if err != nil {
		return err
	}
	totals := map[string]int64{}
	var eligible int64
	for _, cat := range warningCategories {
		n, err := decodeInt64(totalsObj[cat])
		if err != nil {
			return err
		}
		totals[cat] = n
		if cat != "duplicate_selector_entry" {
			eligible += n
		}
	}
	examples, ok := obj["examples"].([]any)
	if !ok {
		return errOutputInvalid
	}
	truncated, ok := obj["examples_truncated"].(bool)
	if !ok {
		return errOutputInvalid
	}
	omitted, err := decodeInt64(obj["examples_omitted_count"])
	if err != nil || omitted < 0 || len(examples) > 100 {
		return errOutputInvalid
	}
	wantExamples := eligible
	if wantExamples > 100 {
		wantExamples = 100
	}
	if int64(len(examples)) != wantExamples || omitted != eligible-wantExamples || truncated != (omitted > 0) {
		return errOutputInvalid
	}
	byCat := map[string]int64{}
	for _, ex := range examples {
		m, ok := ex.(map[string]any)
		if !ok || len(m) != 2 {
			return errOutputInvalid
		}
		cat, ok := m["category"].(string)
		path, pok := m["path"].(string)
		if !ok || !pok || totals[cat] == 0 && cat == "" || !validWarningCategory(cat) || !validPointer(path) {
			return errOutputInvalid
		}
		byCat[cat]++
		if byCat[cat] > totals[cat] {
			return errOutputInvalid
		}
	}
	return nil
}

func decodeProcessingStats(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	obj, err := decodeExactObject(dec, []string{
		"stored_bytes", "decoded_bytes", "started_at", "finished_at",
		"wall_seconds", "decoded_mib_per_second",
	})
	if err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errOutputInvalid
	}
	stored, err := decodeInt64(obj["stored_bytes"])
	if err != nil {
		return err
	}
	decoded, err := decodeInt64(obj["decoded_bytes"])
	if err != nil {
		return err
	}
	_ = stored
	_ = decoded
	started, ok := obj["started_at"].(string)
	finished, fok := obj["finished_at"].(string)
	if !ok || !fok {
		return errOutputInvalid
	}
	st, err := time.Parse(time.RFC3339Nano, started)
	if err != nil {
		return errOutputInvalid
	}
	fn, err := time.Parse(time.RFC3339Nano, finished)
	if err != nil || fn.Before(st) {
		return errOutputInvalid
	}
	wall, err := decodeFloat64(obj["wall_seconds"])
	if err != nil || wall < 0 || math.IsNaN(wall) || math.IsInf(wall, 0) {
		return errOutputInvalid
	}
	rate, err := decodeFloat64(obj["decoded_mib_per_second"])
	if err != nil || rate < 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return errOutputInvalid
	}
	return nil
}

func decodeString(dec *json.Decoder) (string, error) {
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok || !utf8.ValidString(s) {
		return "", errOutputInvalid
	}
	return s, nil
}

func decodeExactObject(dec *json.Decoder, keys []string) (map[string]any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return nil, errOutputInvalid
	}
	allowed := map[string]bool{}
	for _, k := range keys {
		allowed[k] = true
	}
	out := map[string]any{}
	seen := map[string]bool{}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyTok.(string)
		if !ok || !allowed[key] || seen[key] {
			return nil, errOutputInvalid
		}
		seen[key] = true
		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		out[key] = v
	}
	end, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := end.(json.Delim); !ok || delim != '}' || len(out) != len(keys) {
		return nil, errOutputInvalid
	}
	return out, nil
}

func decodeInt64(v any) (int64, error) {
	num, ok := v.(json.Number)
	if !ok {
		return 0, errOutputInvalid
	}
	raw := string(num)
	if raw == "" || strings.ContainsAny(raw, ".eE+") || (len(raw) > 1 && raw[0] == '0') {
		return 0, errOutputInvalid
	}
	n, err := num.Int64()
	if err != nil || n < 0 {
		return 0, errOutputInvalid
	}
	return n, nil
}

func decodeFloat64(v any) (float64, error) {
	num, ok := v.(json.Number)
	if !ok {
		return 0, errOutputInvalid
	}
	n, err := num.Float64()
	if err != nil {
		return 0, errOutputInvalid
	}
	return n, nil
}

func validWarningCategory(c string) bool {
	for _, cat := range warningCategories {
		if cat == c {
			return true
		}
	}
	return false
}

func validPointer(p string) bool {
	if !utf8.ValidString(p) || p == "" || !strings.HasPrefix(p, "/") {
		return false
	}
	for i := 0; i < len(p); i++ {
		if p[i] == '~' {
			if i+1 >= len(p) || (p[i+1] != '0' && p[i+1] != '1') {
				return false
			}
			i++
		}
	}
	return true
}

func isSymlink(info os.FileInfo) bool {
	return info.Mode()&os.ModeSymlink != 0
}

func readLimited(path string, max int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > int64(max) {
		return nil, errOutputInvalid
	}
	data, err := io.ReadAll(io.LimitReader(f, int64(max)+1))
	if err != nil || len(data) > max {
		return nil, errOutputInvalid
	}
	return data, nil
}
