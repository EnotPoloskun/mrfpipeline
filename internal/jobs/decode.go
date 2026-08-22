package jobs

import (
	"bytes"
	"encoding/json"
	"strings"
)

func unmarshalOnePositiveInt64(data []byte, field string, dest *int64) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return jobErr(FailureInvalidArguments)
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return jobErr(FailureInvalidArguments)
	}
	seen := false
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return jobErr(FailureInvalidArguments)
		}
		key, ok := keyTok.(string)
		if !ok {
			return jobErr(FailureInvalidArguments)
		}
		if key != field {
			return jobErr(FailureInvalidArguments)
		}
		if seen {
			return jobErr(FailureInvalidArguments)
		}
		seen = true
		var v any
		if err := dec.Decode(&v); err != nil {
			return jobErr(FailureInvalidArguments)
		}
		num, ok := v.(json.Number)
		if !ok {
			return jobErr(FailureInvalidArguments)
		}
		raw := string(num)
		if raw == "" || strings.ContainsAny(raw, ".eE") {
			return jobErr(FailureInvalidArguments)
		}
		n, err := num.Int64()
		if err != nil || n <= 0 {
			return jobErr(FailureInvalidArguments)
		}
		*dest = n
	}
	end, err := dec.Token()
	if err != nil {
		return jobErr(FailureInvalidArguments)
	}
	endDelim, ok := end.(json.Delim)
	if !ok || endDelim != '}' || !seen {
		return jobErr(FailureInvalidArguments)
	}
	if dec.More() {
		return jobErr(FailureInvalidArguments)
	}
	return nil
}

func decodePositiveID(data []byte, field string) (int64, error) {
	var id int64
	if err := unmarshalOnePositiveInt64(data, field, &id); err != nil {
		return 0, err
	}
	return id, nil
}
