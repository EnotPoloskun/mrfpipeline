package artifact

import (
	"os"
	"path/filepath"
	"strings"
)

// CheckOverlap rejects equality or containment among the artifact root,
// warehouse, provider catalog, and services paths. Warehouse vs catalog is
// omitted here because a recognized warehouse may own `<warehouse>/provider_catalog`.
// Comparisons use Story 01 cleaned paths, then physical/anticipated physical paths.
func CheckOverlap(artifactRoot, warehouse, catalog, services string) error {
	if artifactRoot == "" || warehouse == "" || catalog == "" || services == "" {
		return artErr("overlap")
	}
	pairs := [][2]string{
		{artifactRoot, warehouse},
		{artifactRoot, catalog},
		{artifactRoot, services},
		{warehouse, services},
		{catalog, services},
	}
	for _, p := range pairs {
		if err := CheckPairOverlap(p[0], p[1]); err != nil {
			return err
		}
	}
	return nil
}

// CheckPairOverlap rejects equality or containment between two local paths.
func CheckPairOverlap(a, b string) error {
	if a == "" || b == "" {
		return artErr("overlap")
	}
	a = filepath.Clean(a)
	b = filepath.Clean(b)
	if overlaps(a, b) {
		return artErr("overlap")
	}
	aPhys, err := anticipatedPhysical(a)
	if err != nil {
		return err
	}
	bPhys, err := anticipatedPhysical(b)
	if err != nil {
		return err
	}
	if overlaps(aPhys, bPhys) {
		return artErr("overlap")
	}
	return nil
}

func overlaps(a, b string) bool {
	if a == b {
		return true
	}
	return hasPrefixPath(a, b) || hasPrefixPath(b, a)
}

func hasPrefixPath(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil || rel == "." {
		return false
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false
	}
	return true
}

func anticipatedPhysical(path string) (string, error) {
	path = filepath.Clean(path)
	if _, err := os.Lstat(path); err == nil {
		phys, err := filepath.EvalSymlinks(path)
		if err != nil {
			return "", artErr("overlap")
		}
		return phys, nil
	} else if !os.IsNotExist(err) {
		return "", artErr("overlap")
	}
	var missing []string
	cur := path
	for {
		info, err := os.Lstat(cur)
		if err == nil {
			_ = info
			base, err := filepath.EvalSymlinks(cur)
			if err != nil {
				return "", artErr("overlap")
			}
			return filepath.Join(append([]string{base}, missing...)...), nil
		}
		if !os.IsNotExist(err) {
			return "", artErr("overlap")
		}
		missing = append([]string{filepath.Base(cur)}, missing...)
		next := filepath.Dir(cur)
		if next == cur {
			return "", artErr("overlap")
		}
		cur = next
	}
}
