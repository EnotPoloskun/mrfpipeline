package tocimport

import (
	"encoding/json"
	"io"
	"strings"
	"unicode/utf8"
)

func validateAdditionalJSON(value string) error {
	if !utf8.ValidString(value) || hasUnpairedSurrogateEscape(value) {
		return errRow
	}
	d := json.NewDecoder(strings.NewReader(value))
	d.UseNumber()
	if err := validateJSONValue(d, true); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errRow
	}
	return nil
}

func validateJSONValue(d *json.Decoder, root bool) error {
	t, err := d.Token()
	if err != nil {
		return errRow
	}
	switch x := t.(type) {
	case json.Delim:
		if x == '{' {
			count := 0
			names := map[string]bool{}
			for d.More() {
				keyToken, keyErr := d.Token()
				key, ok := keyToken.(string)
				if keyErr != nil || !ok || names[key] {
					return errRow
				}
				names[key] = true
				count++
				if err := validateJSONValue(d, false); err != nil {
					return err
				}
			}
			end, err := d.Token()
			if err != nil || end != json.Delim('}') || root && count == 0 {
				return errRow
			}
			return nil
		}
		if x == '[' {
			if root {
				return errRow
			}
			for d.More() {
				if err := validateJSONValue(d, false); err != nil {
					return err
				}
			}
			end, err := d.Token()
			if err != nil || end != json.Delim(']') {
				return errRow
			}
			return nil
		}
		return errRow
	default:
		if root {
			return errRow
		}
		return nil
	}
}

func hasUnpairedSurrogateEscape(value string) bool {
	for i := 0; i+5 < len(value); i++ {
		if value[i] != '\\' || value[i+1] != 'u' || (i > 0 && precedingBackslashes(value, i)%2 == 1) {
			continue
		}
		code, ok := hex4(value[i+2 : i+6])
		if !ok {
			continue
		}
		if code >= 0xD800 && code <= 0xDBFF {
			if i+11 >= len(value) || value[i+6] != '\\' || value[i+7] != 'u' || precedingBackslashes(value, i+6)%2 == 1 {
				return true
			}
			low, ok := hex4(value[i+8 : i+12])
			if !ok || low < 0xDC00 || low > 0xDFFF {
				return true
			}
			i += 11
		} else if code >= 0xDC00 && code <= 0xDFFF {
			return true
		}
	}
	return false
}

func precedingBackslashes(value string, i int) int {
	count := 0
	for i--; i >= 0 && value[i] == '\\'; i-- {
		count++
	}
	return count
}

func hex4(value string) (int, bool) {
	if len(value) != 4 {
		return 0, false
	}
	n := 0
	for i := range value {
		n <<= 4
		switch c := value[i]; {
		case c >= '0' && c <= '9':
			n += int(c - '0')
		case c >= 'a' && c <= 'f':
			n += int(c-'a') + 10
		case c >= 'A' && c <= 'F':
			n += int(c-'A') + 10
		default:
			return 0, false
		}
	}
	return n, true
}
