package config

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestValidateDatabaseURL(t *testing.T) {
	t.Parallel()
	secret := "postgres://user:supersecret@example.invalid/db"
	if err := ValidateDatabaseURL(secret); err != nil {
		t.Fatalf("valid url: %v", err)
	}

	cases := []struct {
		name string
		raw  string
	}{
		{"empty", ""},
		{"invalid UTF-8", string([]byte{0xff, 0xfe})},
		{"NUL", "postgres://x\x00y"},
		{"carriage return", "postgres://x\ry"},
		{"newline", "postgres://x\ny"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateDatabaseURL(tc.raw)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("got %v, want ErrInvalidConfig", err)
			}
			msg := err.Error()
			if !strings.Contains(msg, EnvDatabaseURL) {
				t.Fatalf("error %q missing field name", msg)
			}
			if tc.raw != "" && utf8.ValidString(tc.raw) && strings.Contains(msg, tc.raw) {
				t.Fatalf("error %q exposed configured value", msg)
			}
			if strings.Contains(msg, "supersecret") {
				t.Fatalf("error %q exposed a secret substring", msg)
			}
		})
	}
}

func TestNormalizeLocalPath(t *testing.T) {
	t.Parallel()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	rel, err := NormalizeLocalPath(EnvArtifactRoot, "rel/nested")
	if err != nil {
		t.Fatal(err)
	}
	wantRel := filepath.Clean(filepath.Join(wd, "rel/nested"))
	if rel != wantRel {
		t.Fatalf("relative: got %q want %q", rel, wantRel)
	}

	absIn := filepath.Join(string(filepath.Separator), "tmp", "foo", "..", "bar")
	gotAbs, err := NormalizeLocalPath(EnvWarehousePath, absIn)
	if err != nil {
		t.Fatal(err)
	}
	wantAbs, err := filepath.Abs(absIn)
	if err != nil {
		t.Fatal(err)
	}
	wantAbs = filepath.Clean(wantAbs)
	if gotAbs != wantAbs {
		t.Fatalf("absolute clean: got %q want %q", gotAbs, wantAbs)
	}

	if _, err := NormalizeLocalPath(EnvArtifactRoot, "disk:artifacts"); err != nil {
		t.Fatalf("colon without // should be a path: %v", err)
	}

	secretPath := "/secret/path/do-not-echo"
	invalids := []struct {
		name string
		raw  string
	}{
		{"empty", ""},
		{"s3", "s3://bucket/key"},
		{"http", "http://example.invalid/x"},
		{"https", "https://example.invalid/x"},
		{"file", "file:///tmp/x"},
		{"HTTP", "HTTP://example.invalid/x"},
		{"NUL", "/tmp/\x00x"},
		{"CR", "/tmp/\rx"},
		{"LF", "/tmp/\nx"},
		{"invalid UTF-8", string([]byte{'/', 0xff})},
	}
	for _, tc := range invalids {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NormalizeLocalPath(EnvArtifactRoot, tc.raw)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("got %v, want ErrInvalidConfig", err)
			}
			msg := err.Error()
			if !strings.Contains(msg, EnvArtifactRoot) {
				t.Fatalf("error %q missing field name", msg)
			}
			if strings.Contains(msg, secretPath) || (tc.raw != "" && utf8.ValidString(tc.raw) && !strings.Contains(tc.raw, "\x00") && strings.Contains(msg, tc.raw)) {
				t.Fatalf("error %q exposed path value", msg)
			}
		})
	}
}

func TestNormalizeLocalPathAbsFailure(t *testing.T) {
	t.Parallel()
	_, err := normalizeLocalPath(EnvArtifactRoot, "relative", func(string) (string, error) {
		return "", errors.New("abs failed")
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("got %v, want ErrInvalidConfig", err)
	}
	if strings.Contains(err.Error(), "relative") || strings.Contains(err.Error(), "abs failed") {
		t.Fatalf("error exposed path value: %v", err)
	}
}

func TestValidatePayer(t *testing.T) {
	t.Parallel()
	if err := ValidatePayer("uhc"); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"UHC", "united-healthcare", "uhc ", " uhc", ""} {
		err := ValidatePayer(raw)
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%q: got %v", raw, err)
		}
		if !strings.Contains(err.Error(), FieldPayer) {
			t.Fatalf("%q: missing field name in %v", raw, err)
		}
	}
}

func TestValidatePayerIdentifier(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"uhc", "aetna", "payer_2", "a.b-c"} {
		if err := ValidatePayerIdentifier(raw); err != nil {
			t.Fatalf("%q: %v", raw, err)
		}
	}
	for _, raw := range []string{"", "UHC", "-payer", "payer ", "payer/2"} {
		if err := ValidatePayerIdentifier(raw); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%q: got %v", raw, err)
		}
	}
}

func TestValidateCollectionMonth(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"0001-01", "9999-12", "2026-08", "2026-01", "2026-12"} {
		if err := ValidateCollectionMonth(raw); err != nil {
			t.Fatalf("%q: %v", raw, err)
		}
	}
	for _, raw := range []string{
		"0000-01",
		"2026-13",
		"2026-00",
		"2026-8",
		"2026-08-01",
		"202608",
		"2026/08",
		" 2026-08",
		"2026-08 ",
		"August",
	} {
		err := ValidateCollectionMonth(raw)
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%q: got %v", raw, err)
		}
		if !strings.Contains(err.Error(), FieldCollectionMonth) {
			t.Fatalf("%q: missing field name in %v", raw, err)
		}
		if raw != "" && strings.Contains(err.Error(), raw) {
			t.Fatalf("%q: error exposed value: %v", raw, err)
		}
	}
}

func TestValidateLimit(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"1", "2", "5", "10", "01", "05", "10", strconv.FormatInt(1<<63-1, 10)} {
		n, err := ValidateLimit(raw)
		if err != nil {
			t.Fatalf("%q: %v", raw, err)
		}
		want, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if n != want {
			t.Fatalf("%q: got %d want %d", raw, n, want)
		}
	}

	invalids := []string{
		"",
		"0",
		"00",
		"-1",
		"+1",
		"+5",
		"1.5",
		"1e2",
		"0x10",
		"1 ",
		" 1",
		"9223372036854775808",
	}
	for _, raw := range invalids {
		_, err := ValidateLimit(raw)
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%q: got %v", raw, err)
		}
		if !strings.Contains(err.Error(), FieldLimit) {
			t.Fatalf("%q: missing field name in %v", raw, err)
		}
	}
}
