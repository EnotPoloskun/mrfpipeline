package config

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"
)

// ErrInvalidConfig is the sentinel wrapped by every semantic configuration error.
var ErrInvalidConfig = errors.New("invalid configuration")

const (
	EnvDatabaseURL         = "MRFPIPELINE_DATABASE_URL"
	EnvArtifactRoot        = "MRFPIPELINE_ARTIFACT_ROOT"
	EnvWarehousePath       = "MRFPIPELINE_WAREHOUSE_PATH"
	EnvProviderCatalogPath = "MRFPIPELINE_PROVIDER_CATALOG_PATH"
	EnvServicesPath        = "MRFPIPELINE_SERVICES_PATH"

	FieldPayer           = "payer"
	FieldCollectionMonth = "collection_month"
	FieldLimit           = "limit"
)

// ValidateDatabaseURL accepts a nonempty opaque secret. It does not parse,
// normalize, or echo the value.
func ValidateDatabaseURL(raw string) error {
	return checkOpaqueText(EnvDatabaseURL, raw)
}

// NormalizeLocalPath lexically validates and absolutizes a required local path.
// It does not stat, create, or follow the path.
func NormalizeLocalPath(name, raw string) (string, error) {
	if err := checkOpaqueText(name, raw); err != nil {
		return "", err
	}
	if hasURISchemePrefix(raw) {
		return "", wrap(name, "must be a local filesystem path")
	}
	return normalizeLocalPath(name, raw, filepath.Abs)
}

func normalizeLocalPath(name, raw string, abs func(string) (string, error)) (string, error) {
	got, err := abs(raw)
	if err != nil {
		return "", wrap(name, "could not normalize path")
	}
	return filepath.Clean(got), nil
}

// ValidatePayer accepts only the exact adapter identifier uhc.
func ValidatePayer(raw string) error {
	if raw != "uhc" {
		return wrap(FieldPayer, "must be the exact adapter identifier uhc")
	}
	return nil
}

// ValidateCollectionMonth accepts exactly YYYY-MM with year 0001-9999.
func ValidateCollectionMonth(raw string) error {
	if len(raw) != 7 || raw[4] != '-' || !digits(raw[:4]) || !digits(raw[5:]) {
		return wrap(FieldCollectionMonth, "must be YYYY-MM")
	}
	year, _ := strconv.Atoi(raw[:4])
	month, _ := strconv.Atoi(raw[5:])
	if year < 1 || year > 9999 || month < 1 || month > 12 {
		return wrap(FieldCollectionMonth, "must be YYYY-MM")
	}
	return nil
}

// ValidateLimit parses a required positive base-10 int64. Leading zeroes have
// ordinary decimal meaning. A leading plus sign is invalid.
func ValidateLimit(raw string) (int64, error) {
	if raw == "" {
		return 0, wrap(FieldLimit, "must be a positive integer")
	}
	if !utf8.ValidString(raw) {
		return 0, wrap(FieldLimit, "invalid UTF-8")
	}
	if raw[0] == '+' || raw[0] == '-' {
		return 0, wrap(FieldLimit, "must be a positive integer")
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return 0, wrap(FieldLimit, "must be a positive integer")
		}
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, wrap(FieldLimit, "must fit in a signed 64-bit integer")
	}
	if n <= 0 {
		return 0, wrap(FieldLimit, "must be a positive integer")
	}
	return n, nil
}

func checkOpaqueText(name, raw string) error {
	if raw == "" {
		return wrap(name, "missing")
	}
	if !utf8.ValidString(raw) {
		return wrap(name, "invalid UTF-8")
	}
	if strings.IndexByte(raw, 0) >= 0 || strings.IndexByte(raw, '\r') >= 0 || strings.IndexByte(raw, '\n') >= 0 {
		return wrap(name, "contains a forbidden byte")
	}
	return nil
}

func hasURISchemePrefix(s string) bool {
	if s == "" || !isASCIILetter(s[0]) {
		return false
	}
	i := 1
	for i < len(s) && isSchemeByte(s[i]) {
		i++
	}
	return strings.HasPrefix(s[i:], "://")
}

func isASCIILetter(b byte) bool {
	return b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z'
}

func isSchemeByte(b byte) bool {
	return isASCIILetter(b) || b >= '0' && b <= '9' || b == '+' || b == '-' || b == '.'
}

func digits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func wrap(name, msg string) error {
	return fmt.Errorf("%w: %s: %s", ErrInvalidConfig, name, msg)
}
