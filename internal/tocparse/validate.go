package tocparse

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/EnotPoloskun/mrftocparser"
)

const (
	manifestSchemaVersion = "1.0.0"
	outputSchemaVersion   = "1.0.0"
	maxManifestBytes      = 1 << 20
	datasetTOCFiles       = "toc_files"
	datasetAssociations   = "mrf_plan_associations"
	fileManifest          = "manifest.json"
)

var errOutputInvalid = errors.New("toc parse output invalid")

type parsedManifest struct {
	TOCOutputID     string
	PayerID         string
	CollectionMonth string
	SourceURI       string
	SourceEncoding  string
	Counts          mrftocparser.Counts
	WarningTotals   mrftocparser.WarningTotals
}

func validateCompletedOutput(dir, tocOutputID, payerID, collectionMonth, sourceURI string) (parsedManifest, error) {
	var zero parsedManifest
	if err := validateRootLayout(dir); err != nil {
		return zero, err
	}
	raw, err := readLimited(filepath.Join(dir, fileManifest), maxManifestBytes)
	if err != nil {
		return zero, errOutputInvalid
	}
	m, err := decodeManifest(raw)
	if err != nil {
		return zero, errOutputInvalid
	}
	if m.TOCOutputID != tocOutputID || m.PayerID != payerID || m.CollectionMonth != collectionMonth || m.SourceURI != sourceURI {
		return zero, errOutputInvalid
	}
	return m, nil
}

func validateRootLayout(dir string) error {
	info, err := os.Lstat(dir)
	if err != nil || isSymlink(info) || !info.IsDir() {
		return errOutputInvalid
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 3 {
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
		case fileManifest:
			if !fi.Mode().IsRegular() {
				return errOutputInvalid
			}
		case datasetTOCFiles, datasetAssociations:
			if !fi.IsDir() {
				return errOutputInvalid
			}
			if err := validateParts(p, e.Name() == datasetTOCFiles); err != nil {
				return err
			}
		default:
			return errOutputInvalid
		}
	}
	if !seen[fileManifest] || !seen[datasetTOCFiles] || !seen[datasetAssociations] {
		return errOutputInvalid
	}
	return nil
}

func validateParts(dir string, tocOnly bool) error {
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
	if tocOnly && len(named) != 1 {
		return errOutputInvalid
	}
	for i := 0; i < len(named); i++ {
		if !named[i] {
			return errOutputInvalid
		}
	}
	return nil
}

func reportMatches(report mrftocparser.Report, m parsedManifest, tocOutputID, finalPath string) error {
	if report.TOCOutputID != tocOutputID || report.FinalPath != finalPath {
		return errOutputInvalid
	}
	if report.Counts != m.Counts || report.WarningTotals != m.WarningTotals {
		return errOutputInvalid
	}
	if report.Counts.TOCFiles < 0 || report.Counts.MRFPlanAssociations < 0 ||
		report.WarningTotals.UnknownField < 0 || report.WarningTotals.MissingRequired < 0 ||
		report.WarningTotals.InvalidValue < 0 || report.WarningTotals.SourceSchema < 0 ||
		report.WarningTotals.FilenameUnavailable < 0 {
		return errOutputInvalid
	}
	return nil
}

func decodeManifest(data []byte) (parsedManifest, error) {
	var zero parsedManifest
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
	var m parsedManifest
	var haveSource, haveTOC, haveCounts, haveWarnings bool
	var manifestVer, outputVer string
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
		case "toc_output_id":
			m.TOCOutputID, err = decodeString(dec)
		case "payer_id":
			m.PayerID, err = decodeString(dec)
		case "collection_month":
			m.CollectionMonth, err = decodeString(dec)
		case "source":
			haveSource = true
			m.SourceURI, m.SourceEncoding, err = decodeSource(dec)
		case "toc":
			haveTOC = true
			err = decodeTOC(dec)
		case "counts":
			haveCounts = true
			m.Counts, err = decodeCounts(dec)
		case "warnings":
			haveWarnings = true
			m.WarningTotals, err = decodeWarnings(dec)
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
	endDelim, ok := end.(json.Delim)
	if !ok || endDelim != '}' {
		return zero, errOutputInvalid
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return zero, errOutputInvalid
	}
	if off := dec.InputOffset(); int(off) < len(data) && len(bytes.TrimSpace(data[off:])) > 0 {
		return zero, errOutputInvalid
	}
	if len(seen) != 9 || !haveSource || !haveTOC || !haveCounts || !haveWarnings {
		return zero, errOutputInvalid
	}
	if manifestVer != manifestSchemaVersion || outputVer != outputSchemaVersion {
		return zero, errOutputInvalid
	}
	if !validIdentifier(m.TOCOutputID) || !validIdentifier(m.PayerID) || !validMonth(m.CollectionMonth) {
		return zero, errOutputInvalid
	}
	if m.SourceURI == "" || !filepath.IsAbs(m.SourceURI) || filepath.Clean(m.SourceURI) != m.SourceURI {
		return zero, errOutputInvalid
	}
	if m.SourceEncoding != "json" && m.SourceEncoding != "gzip" {
		return zero, errOutputInvalid
	}
	if m.Counts.TOCFiles != 1 || m.Counts.MRFPlanAssociations < 0 {
		return zero, errOutputInvalid
	}
	return m, nil
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

func decodeSource(dec *json.Decoder) (uri, encoding string, err error) {
	obj, err := decodeExactObject(dec, []string{"uri", "encoding"})
	if err != nil {
		return "", "", err
	}
	uri, ok := obj["uri"].(string)
	if !ok || uri == "" || !utf8.ValidString(uri) {
		return "", "", errOutputInvalid
	}
	encoding, ok = obj["encoding"].(string)
	if !ok {
		return "", "", errOutputInvalid
	}
	return uri, encoding, nil
}

func decodeTOC(dec *json.Decoder) error {
	obj, err := decodeExactObject(dec, []string{
		"reporting_entity_name", "reporting_entity_type",
		"last_updated_on", "last_updated_on_raw", "source_schema_version",
	})
	if err != nil {
		return err
	}
	name, ok := obj["reporting_entity_name"].(string)
	if !ok || name == "" || !utf8.ValidString(name) {
		return errOutputInvalid
	}
	typ, ok := obj["reporting_entity_type"].(string)
	if !ok || typ == "" || !utf8.ValidString(typ) {
		return errOutputInvalid
	}
	last, lastOK := optionalString(obj["last_updated_on"])
	raw, rawOK := optionalString(obj["last_updated_on_raw"])
	schema, schemaOK := optionalString(obj["source_schema_version"])
	if !lastOK || !rawOK || !schemaOK {
		return errOutputInvalid
	}
	if schema != nil && *schema == "" {
		return errOutputInvalid
	}
	if last != nil {
		if raw == nil || *raw != *last {
			return errOutputInvalid
		}
		if _, err := time.Parse("2006-01-02", *last); err != nil {
			return errOutputInvalid
		}
		t, err := time.Parse("2006-01-02", *last)
		if err != nil || t.Format("2006-01-02") != *last {
			return errOutputInvalid
		}
	}
	return nil
}

func decodeCounts(dec *json.Decoder) (mrftocparser.Counts, error) {
	var zero mrftocparser.Counts
	obj, err := decodeExactObject(dec, []string{
		"source_reporting_structure_count", "source_reporting_plan_count",
		"source_in_network_file_count", "skipped_invalid_plan_count",
		"skipped_invalid_in_network_file_count", "ignored_allowed_amount_file_count",
		"candidate_association_count", "duplicate_association_count",
		"toc_files", "mrf_plan_associations",
	})
	if err != nil {
		return zero, err
	}
	n := func(key string) (int64, error) { return decodeInt64(obj[key]) }
	c := mrftocparser.Counts{}
	var errn error
	if c.SourceReportingStructureCount, errn = n("source_reporting_structure_count"); errn != nil {
		return zero, errn
	}
	if c.SourceReportingPlanCount, errn = n("source_reporting_plan_count"); errn != nil {
		return zero, errn
	}
	if c.SourceInNetworkFileCount, errn = n("source_in_network_file_count"); errn != nil {
		return zero, errn
	}
	if c.SkippedInvalidPlanCount, errn = n("skipped_invalid_plan_count"); errn != nil {
		return zero, errn
	}
	if c.SkippedInvalidInNetworkFileCount, errn = n("skipped_invalid_in_network_file_count"); errn != nil {
		return zero, errn
	}
	if c.IgnoredAllowedAmountFileCount, errn = n("ignored_allowed_amount_file_count"); errn != nil {
		return zero, errn
	}
	if c.CandidateAssociationCount, errn = n("candidate_association_count"); errn != nil {
		return zero, errn
	}
	if c.DuplicateAssociationCount, errn = n("duplicate_association_count"); errn != nil {
		return zero, errn
	}
	if c.TOCFiles, errn = n("toc_files"); errn != nil {
		return zero, errn
	}
	if c.MRFPlanAssociations, errn = n("mrf_plan_associations"); errn != nil {
		return zero, errn
	}
	if err := validateCountEquations(c); err != nil {
		return zero, err
	}
	return c, nil
}

func validateCountEquations(c mrftocparser.Counts) error {
	vals := []int64{
		c.SourceReportingStructureCount, c.SourceReportingPlanCount, c.SourceInNetworkFileCount,
		c.SkippedInvalidPlanCount, c.SkippedInvalidInNetworkFileCount, c.IgnoredAllowedAmountFileCount,
		c.CandidateAssociationCount, c.DuplicateAssociationCount, c.TOCFiles, c.MRFPlanAssociations,
	}
	for _, v := range vals {
		if v < 0 {
			return errOutputInvalid
		}
	}
	if c.TOCFiles != 1 || c.SkippedInvalidPlanCount > c.SourceReportingPlanCount ||
		c.SkippedInvalidInNetworkFileCount > c.SourceInNetworkFileCount {
		return errOutputInvalid
	}
	if c.DuplicateAssociationCount > c.CandidateAssociationCount || c.MRFPlanAssociations > c.CandidateAssociationCount {
		return errOutputInvalid
	}
	if c.MRFPlanAssociations > int64(^uint64(0)>>1)-c.DuplicateAssociationCount ||
		c.MRFPlanAssociations+c.DuplicateAssociationCount != c.CandidateAssociationCount {
		return errOutputInvalid
	}
	return nil
}

func decodeWarnings(dec *json.Decoder) (mrftocparser.WarningTotals, error) {
	var zero mrftocparser.WarningTotals
	obj, err := decodeExactObject(dec, []string{"totals", "examples", "examples_truncated", "examples_omitted_count"})
	if err != nil {
		return zero, err
	}
	totalsRaw, err := json.Marshal(obj["totals"])
	if err != nil {
		return zero, err
	}
	totalsDec := json.NewDecoder(bytes.NewReader(totalsRaw))
	totalsDec.UseNumber()
	totalsObj, err := decodeExactObject(totalsDec, []string{
		"unknown_field", "missing_required", "invalid_value", "source_schema", "filename_unavailable",
	})
	if err != nil {
		return zero, err
	}
	n := func(key string) (int64, error) { return decodeInt64(totalsObj[key]) }
	var t mrftocparser.WarningTotals
	if t.UnknownField, err = n("unknown_field"); err != nil {
		return zero, err
	}
	if t.MissingRequired, err = n("missing_required"); err != nil {
		return zero, err
	}
	if t.InvalidValue, err = n("invalid_value"); err != nil {
		return zero, err
	}
	if t.SourceSchema, err = n("source_schema"); err != nil {
		return zero, err
	}
	if t.FilenameUnavailable, err = n("filename_unavailable"); err != nil {
		return zero, err
	}
	examples, ok := obj["examples"].([]any)
	if !ok {
		return zero, errOutputInvalid
	}
	truncated, ok := obj["examples_truncated"].(bool)
	if !ok {
		return zero, errOutputInvalid
	}
	omitted, err := decodeInt64(obj["examples_omitted_count"])
	if err != nil || omitted < 0 || len(examples) > 100 {
		return zero, errOutputInvalid
	}
	total := t.UnknownField + t.MissingRequired + t.InvalidValue + t.SourceSchema + t.FilenameUnavailable
	if omitted > total || omitted != total-int64(len(examples)) || truncated != (omitted > 0) {
		return zero, errOutputInvalid
	}
	counts := map[string]int64{}
	for _, ex := range examples {
		m, ok := ex.(map[string]any)
		if !ok {
			return zero, errOutputInvalid
		}
		if len(m) != 2 {
			return zero, errOutputInvalid
		}
		cat, ok := m["category"].(string)
		path, pok := m["path"].(string)
		if !ok || !pok || !validWarningCategory(cat) || !validPointer(path) {
			return zero, errOutputInvalid
		}
		counts[cat]++
	}
	totalsByCat := map[string]int64{
		"unknown_field": t.UnknownField, "missing_required": t.MissingRequired,
		"invalid_value": t.InvalidValue, "source_schema": t.SourceSchema,
		"filename_unavailable": t.FilenameUnavailable,
	}
	for cat, n := range counts {
		if n > totalsByCat[cat] {
			return zero, errOutputInvalid
		}
	}
	return t, nil
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

func optionalString(v any) (*string, bool) {
	if v == nil {
		return nil, true
	}
	s, ok := v.(string)
	if !ok || !utf8.ValidString(s) {
		return nil, false
	}
	return &s, true
}

func validIdentifier(v string) bool {
	if len(v) < 1 || len(v) > 128 || !utf8.ValidString(v) {
		return false
	}
	c := v[0]
	if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
		return false
	}
	for i := 1; i < len(v); i++ {
		c := v[i]
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func validMonth(v string) bool {
	if len(v) != 7 || v[4] != '-' || v[:4] == "0000" {
		return false
	}
	for _, i := range []int{0, 1, 2, 3, 5, 6} {
		if v[i] < '0' || v[i] > '9' {
			return false
		}
	}
	m := int(v[5]-'0')*10 + int(v[6]-'0')
	return m >= 1 && m <= 12
}

func validWarningCategory(c string) bool {
	switch c {
	case "unknown_field", "missing_required", "invalid_value", "source_schema", "filename_unavailable":
		return true
	}
	return false
}

func validPointer(p string) bool {
	if !utf8.ValidString(p) || p != "" && !strings.HasPrefix(p, "/") {
		return false
	}
	if strings.HasPrefix(p, "#") {
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
