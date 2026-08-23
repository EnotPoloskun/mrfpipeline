package reconcile

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/enotpoloskun/mrfpipeline/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	EnvRealAcceptance = "MRFPIPELINE_REAL_ACCEPTANCE"
	EnvTestDatabase   = "MRFPIPELINE_TEST_DATABASE_URL"
	EnvRealTOCLimit   = "MRFPIPELINE_REAL_TOC_LIMIT"
)

// AcceptanceGuardError is a safe refusal before network or deletion.
type AcceptanceGuardError struct {
	Reason string
}

func (e AcceptanceGuardError) Error() string { return e.Reason }

// CheckAcceptanceGuards refuses unsafe live-run configuration. It does not
// connect to UHC or delete anything.
func CheckAcceptanceGuards(getenv func(string) string) (int64, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	if getenv(EnvRealAcceptance) != "1" {
		return 0, AcceptanceGuardError{Reason: "opt-in missing"}
	}
	dbURL := getenv(EnvTestDatabase)
	if dbURL == "" {
		return 0, AcceptanceGuardError{Reason: "missing disposable database"}
	}
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil || cfg.ConnConfig.Database == "" {
		return 0, AcceptanceGuardError{Reason: "nondisposable database"}
	}
	if !strings.HasPrefix(cfg.ConnConfig.Database, "mrfpipeline_test_") {
		return 0, AcceptanceGuardError{Reason: "nondisposable database"}
	}
	art := getenv(config.EnvArtifactRoot)
	wh := getenv(config.EnvWarehousePath)
	cat := getenv(config.EnvProviderCatalogPath)
	svc := getenv(config.EnvServicesPath)
	for _, p := range []string{art, wh, cat, svc} {
		if p == "" {
			return 0, AcceptanceGuardError{Reason: "missing dedicated root"}
		}
	}
	abs := make([]string, 0, 2)
	markers := []string{"workspace.json", "warehouse.json"}
	for i, p := range []string{art, wh} {
		info, err := os.Lstat(p)
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			return 0, AcceptanceGuardError{Reason: "symlink root"}
		}
		if err := rejectNonemptyUnmarked(p, markers[i]); err != nil {
			return 0, err
		}
		cleaned, err := filepath.Abs(p)
		if err != nil {
			return 0, AcceptanceGuardError{Reason: "invalid root"}
		}
		abs = append(abs, filepath.Clean(cleaned))
	}
	if abs[0] == abs[1] || strings.HasPrefix(abs[0]+string(os.PathSeparator), abs[1]+string(os.PathSeparator)) ||
		strings.HasPrefix(abs[1]+string(os.PathSeparator), abs[0]+string(os.PathSeparator)) {
		return 0, AcceptanceGuardError{Reason: "overlapping roots"}
	}
	limitRaw := getenv(EnvRealTOCLimit)
	limit := int64(1)
	if limitRaw != "" {
		n, err := config.ValidateLimit(limitRaw)
		if err != nil || n > 10 {
			return 0, AcceptanceGuardError{Reason: "limit above ten"}
		}
		limit = n
	}
	return limit, nil
}

func rejectNonemptyUnmarked(path, marker string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return AcceptanceGuardError{Reason: "invalid root"}
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return AcceptanceGuardError{Reason: "symlink root"}
	}
	if !info.IsDir() {
		return AcceptanceGuardError{Reason: "invalid root"}
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return AcceptanceGuardError{Reason: "invalid root"}
	}
	if len(entries) == 0 {
		return nil
	}
	mi, err := os.Lstat(filepath.Join(path, marker))
	if err != nil || mi.Mode()&os.ModeSymlink != 0 || !mi.Mode().IsRegular() {
		return AcceptanceGuardError{Reason: "nonempty unmarked root"}
	}
	return nil
}
