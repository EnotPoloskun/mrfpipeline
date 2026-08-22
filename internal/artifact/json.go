package artifact

import (
	"bytes"
	"encoding/json"
	"strings"
)

func parseExactStrings(data []byte, want map[string]string) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil {
		return artErr("manifest")
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return artErr("manifest")
	}
	seen := make(map[string]bool, len(want))
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return artErr("manifest")
		}
		key, ok := keyTok.(string)
		if !ok {
			return artErr("manifest")
		}
		if _, allowed := want[key]; !allowed || seen[key] {
			return artErr("manifest")
		}
		seen[key] = true
		var v any
		if err := dec.Decode(&v); err != nil {
			return artErr("manifest")
		}
		s, ok := v.(string)
		if !ok || s != want[key] {
			return artErr("manifest")
		}
	}
	end, err := dec.Token()
	if err != nil {
		return artErr("manifest")
	}
	endDelim, ok := end.(json.Delim)
	if !ok || endDelim != '}' || len(seen) != len(want) {
		return artErr("manifest")
	}
	if dec.More() {
		return artErr("manifest")
	}
	return nil
}

func parseWorkspaceMarker(data []byte) error {
	return parseExactStrings(data, map[string]string{
		"application":    Application,
		"schema_version": SchemaVersion,
	})
}

func parseDownloadManifest(data []byte) (int64, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return 0, artErr("manifest")
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return 0, artErr("manifest")
	}
	var version string
	var count int64
	var haveVersion, haveCount bool
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return 0, artErr("manifest")
		}
		key, ok := keyTok.(string)
		if !ok {
			return 0, artErr("manifest")
		}
		switch key {
		case "schema_version":
			if haveVersion {
				return 0, artErr("manifest")
			}
			haveVersion = true
			var v any
			if err := dec.Decode(&v); err != nil {
				return 0, artErr("manifest")
			}
			s, ok := v.(string)
			if !ok {
				return 0, artErr("manifest")
			}
			version = s
		case "byte_count":
			if haveCount {
				return 0, artErr("manifest")
			}
			haveCount = true
			var v any
			if err := dec.Decode(&v); err != nil {
				return 0, artErr("manifest")
			}
			num, ok := v.(json.Number)
			if !ok {
				return 0, artErr("manifest")
			}
			raw := string(num)
			if raw == "" || strings.ContainsAny(raw, ".eE") {
				return 0, artErr("manifest")
			}
			n, err := num.Int64()
			if err != nil || n < 0 {
				return 0, artErr("manifest")
			}
			count = n
		default:
			return 0, artErr("manifest")
		}
	}
	end, err := dec.Token()
	if err != nil {
		return 0, artErr("manifest")
	}
	endDelim, ok := end.(json.Delim)
	if !ok || endDelim != '}' || !haveVersion || !haveCount || version != SchemaVersion {
		return 0, artErr("manifest")
	}
	if dec.More() {
		return 0, artErr("manifest")
	}
	return count, nil
}

func markerJSON() []byte {
	return []byte(`{"application":"mrfpipeline","schema_version":"1.0.0"}` + "\n")
}

func downloadManifestJSON(n int64) ([]byte, error) {
	if n < 0 {
		return nil, artErr("manifest")
	}
	raw, err := json.Marshal(struct {
		SchemaVersion string `json:"schema_version"`
		ByteCount     int64  `json:"byte_count"`
	}{SchemaVersion: SchemaVersion, ByteCount: n})
	if err != nil {
		return nil, artErr("manifest")
	}
	return append(raw, '\n'), nil
}
