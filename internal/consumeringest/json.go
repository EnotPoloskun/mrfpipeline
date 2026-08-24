package consumeringest

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"
)

var (
	errOutputInvalid    = errors.New("consumer ingest output invalid")
	errOutputUnreadable = errors.New("consumer ingest output unreadable")
)

type jsonField func(*json.Decoder) error

func decodeWarehouseJSON(data []byte) (catalogIdentity, error) {
	var zero catalogIdentity
	if !utf8.Valid(data) || rejectUnpairedSurrogates(data) != nil {
		return zero, errOutputInvalid
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var ver string
	var ident catalogIdentity
	if err := readStrictObject(dec, map[string]jsonField{
		"warehouse_schema_version": func(d *json.Decoder) error {
			s, err := readStrictString(d)
			ver = s
			return err
		},
		"provider_catalog": func(d *json.Decoder) error {
			got, err := decodeCatalogIdentity(d)
			ident = got
			return err
		},
	}); err != nil {
		return zero, err
	}
	if err := requireEOF(dec, data); err != nil {
		return zero, err
	}
	if ver != warehouseVersion {
		return zero, errOutputInvalid
	}
	return ident, nil
}

func decodeCatalogIdentity(dec *json.Decoder) (catalogIdentity, error) {
	var zero catalogIdentity
	var ident catalogIdentity
	if err := readStrictObject(dec, map[string]jsonField{
		"schema_version": func(d *json.Decoder) error {
			n, err := readCanonicalInt64(d)
			ident.SchemaVersion = n
			return err
		},
		"release_month": func(d *json.Decoder) error {
			s, err := readStrictString(d)
			ident.ReleaseMonth = s
			return err
		},
	}); err != nil {
		return zero, err
	}
	if ident.SchemaVersion != catalogSchema || !validCatalogMonth(ident.ReleaseMonth) {
		return zero, errOutputInvalid
	}
	return ident, nil
}

func decodeSnapshotManifest(data []byte) (snapshotManifest, error) {
	var zero snapshotManifest
	if !utf8.Valid(data) || rejectUnpairedSurrogates(data) != nil {
		return zero, errOutputInvalid
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var m snapshotManifest
	var manVer, outVer string
	if err := readSnapshotObject(dec, map[string]jsonField{
		"manifest_schema_version": func(d *json.Decoder) error {
			s, err := readStrictString(d)
			manVer = s
			return err
		},
		"output_schema_version": func(d *json.Decoder) error {
			s, err := readStrictString(d)
			outVer = s
			return err
		},
		"output_id": func(d *json.Decoder) error {
			s, err := readStrictString(d)
			m.OutputID = s
			return err
		},
		"payer_id": func(d *json.Decoder) error {
			s, err := readStrictString(d)
			m.PayerID = s
			return err
		},
		"collection_month": func(d *json.Decoder) error {
			s, err := readStrictString(d)
			m.CollectionMonth = s
			return err
		},
		"provider_catalog": func(d *json.Decoder) error {
			got, err := decodeCatalogIdentity(d)
			m.Catalog = got
			return err
		},
		"datasets": func(d *json.Decoder) error {
			got, err := decodeDatasetCounts(d)
			m.Datasets = got
			return err
		},
	}); err != nil {
		return zero, err
	}
	if err := requireEOF(dec, data); err != nil {
		return zero, err
	}
	if manVer != warehouseVersion || outVer != warehouseVersion {
		return zero, errOutputInvalid
	}
	if m.OutputID == "" || m.PayerID == "" || !validCatalogMonth(m.CollectionMonth) {
		return zero, errOutputInvalid
	}
	return m, nil
}

func readSnapshotObject(dec *json.Decoder, fields map[string]jsonField) error {
	tok, err := dec.Token()
	if err != nil {
		return errOutputInvalid
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return errOutputInvalid
	}
	seen := map[string]bool{}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return errOutputInvalid
		}
		key, ok := keyTok.(string)
		if !ok || seen[key] {
			return errOutputInvalid
		}
		seen[key] = true
		if key == "feed_id" {
			return errOutputInvalid
		}
		field, ok := fields[key]
		if ok {
			if err := field(dec); err != nil {
				return err
			}
			continue
		}
		var ignored json.RawMessage
		if err := dec.Decode(&ignored); err != nil {
			return errOutputInvalid
		}
	}
	end, err := dec.Token()
	if err != nil {
		return errOutputInvalid
	}
	if delim, ok := end.(json.Delim); !ok || delim != '}' || len(seen) < len(fields) {
		return errOutputInvalid
	}
	for name := range fields {
		if !seen[name] {
			return errOutputInvalid
		}
	}
	return nil
}

type snapshotManifest struct {
	OutputID        string
	PayerID         string
	CollectionMonth string
	Catalog         catalogIdentity
	Datasets        map[string]datasetCount
}

type datasetCount struct {
	RowCount  int64
	PartCount int64
}

var snapshotDatasets = []string{
	"rate_facts", "rate_provider_groups", "provider_groups",
	"provider_group_memberships", "ingestions", "network_names",
}

func decodeDatasetCounts(dec *json.Decoder) (map[string]datasetCount, error) {
	out := map[string]datasetCount{}
	fields := map[string]jsonField{}
	for _, name := range snapshotDatasets {
		name := name
		fields[name] = func(d *json.Decoder) error {
			c, err := decodeDatasetCount(d)
			out[name] = c
			return err
		}
	}
	if err := readStrictObject(dec, fields); err != nil {
		return nil, err
	}
	if out["ingestions"].RowCount != 1 {
		return nil, errOutputInvalid
	}
	return out, nil
}

func decodeDatasetCount(dec *json.Decoder) (datasetCount, error) {
	var c datasetCount
	if err := readStrictObject(dec, map[string]jsonField{
		"row_count": func(d *json.Decoder) error {
			n, err := readCanonicalInt64(d)
			c.RowCount = n
			return err
		},
		"part_count": func(d *json.Decoder) error {
			n, err := readCanonicalInt64(d)
			c.PartCount = n
			return err
		},
	}); err != nil {
		return datasetCount{}, err
	}
	if c.PartCount < 1 {
		return datasetCount{}, errOutputInvalid
	}
	return c, nil
}

func readStrictObject(dec *json.Decoder, fields map[string]jsonField) error {
	tok, err := dec.Token()
	if err != nil {
		return errOutputInvalid
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return errOutputInvalid
	}
	seen := map[string]bool{}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return errOutputInvalid
		}
		key, ok := keyTok.(string)
		if !ok || seen[key] {
			return errOutputInvalid
		}
		field, ok := fields[key]
		if !ok {
			return errOutputInvalid
		}
		seen[key] = true
		if err := field(dec); err != nil {
			return err
		}
	}
	end, err := dec.Token()
	if err != nil {
		return errOutputInvalid
	}
	if delim, ok := end.(json.Delim); !ok || delim != '}' || len(seen) != len(fields) {
		return errOutputInvalid
	}
	return nil
}

func readStrictString(dec *json.Decoder) (string, error) {
	tok, err := dec.Token()
	if err != nil {
		return "", errOutputInvalid
	}
	s, ok := tok.(string)
	if !ok || !utf8.ValidString(s) {
		return "", errOutputInvalid
	}
	return s, nil
}

func readCanonicalInt64(dec *json.Decoder) (int64, error) {
	tok, err := dec.Token()
	if err != nil {
		return 0, errOutputInvalid
	}
	num, ok := tok.(json.Number)
	if !ok || !isCanonicalInt64Token(string(num)) {
		return 0, errOutputInvalid
	}
	n, err := strconv.ParseInt(string(num), 10, 64)
	if err != nil || n < 0 {
		return 0, errOutputInvalid
	}
	return n, nil
}

func isCanonicalInt64Token(value string) bool {
	if value == "0" {
		return true
	}
	if value == "" || value[0] < '1' || value[0] > '9' {
		return false
	}
	for i := 1; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	_, err := strconv.ParseInt(value, 10, 64)
	return err == nil
}

func requireEOF(dec *json.Decoder, data []byte) error {
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errOutputInvalid
	}
	if off := dec.InputOffset(); int(off) < len(data) && len(bytes.TrimSpace(data[off:])) > 0 {
		return errOutputInvalid
	}
	return nil
}

func validCatalogMonth(value string) bool {
	if len(value) != 7 || value[4] != '-' {
		return false
	}
	for i := 0; i < len(value); i++ {
		if i == 4 {
			continue
		}
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	month := int(value[5]-'0')*10 + int(value[6]-'0')
	return value[:4] != "0000" && month >= 1 && month <= 12
}

func rejectUnpairedSurrogates(data []byte) error {
	for i := 0; i < len(data); i++ {
		if data[i] != '"' {
			continue
		}
		i++
		for i < len(data) {
			switch data[i] {
			case '"':
				goto nextString
			case '\\':
				if i+1 >= len(data) {
					return errOutputInvalid
				}
				if data[i+1] != 'u' {
					i += 2
					continue
				}
				if i+5 >= len(data) {
					return errOutputInvalid
				}
				value, ok := unicodeEscapeValue(data[i+2 : i+6])
				if !ok {
					return errOutputInvalid
				}
				if value >= 0xDC00 && value <= 0xDFFF {
					return errOutputInvalid
				}
				if value >= 0xD800 && value <= 0xDBFF {
					if i+11 >= len(data) || data[i+6] != '\\' || data[i+7] != 'u' {
						return errOutputInvalid
					}
					low, ok := unicodeEscapeValue(data[i+8 : i+12])
					if !ok || low < 0xDC00 || low > 0xDFFF {
						return errOutputInvalid
					}
					i += 11
					continue
				}
				i += 5
			default:
				i++
			}
		}
		return errOutputInvalid
	nextString:
	}
	return nil
}

func unicodeEscapeValue(value []byte) (int, bool) {
	if len(value) != 4 {
		return 0, false
	}
	result := 0
	for _, digit := range value {
		result <<= 4
		switch {
		case digit >= '0' && digit <= '9':
			result += int(digit - '0')
		case digit >= 'a' && digit <= 'f':
			result += int(digit-'a') + 10
		case digit >= 'A' && digit <= 'F':
			result += int(digit-'A') + 10
		default:
			return 0, false
		}
	}
	return result, true
}
