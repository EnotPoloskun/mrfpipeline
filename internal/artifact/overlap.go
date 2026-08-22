package artifact

import (
	"os"
	"path/filepath"
	"strings"
)

// CheckOverlap rejects equality or containment between the artifact root and
// warehouse, provider catalog, or services paths. It compares Story 01 cleaned
// paths first, then physical/anticipated physical paths. Sharing an ancestor
// is not overlap.
func CheckOverlap(artifactRoot, warehouse, catalog, services string) error {
	if artifactRoot == "" || warehouse == "" || catalog == "" || services == "" {
		return artErr("overlap")
	}
	art := filepath.Clean(artifactRoot)
	for _, other := range []string{warehouse, catalog, services} {
		other = filepath.Clean(other)
		if overlaps(art, other) {
			return artErr("overlap")
		}
		artPhys, err := anticipatedPhysical(art)
		if err != nil {
			return err
		}
		otherPhys, err := anticipatedPhysical(other)
		if err != nil {
			return err
		}
		if overlaps(artPhys, otherPhys) {
			return artErr("overlap")
		}
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
