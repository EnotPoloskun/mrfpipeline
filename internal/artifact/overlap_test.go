package artifact

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCheckOverlapLexical(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	art := filepath.Join(base, "artifacts")
	wh := filepath.Join(base, "warehouse")
	cat := filepath.Join(base, "catalog")
	svc := filepath.Join(base, "services.csv")
	if err := CheckOverlap(art, wh, cat, svc); err != nil {
		t.Fatal(err)
	}
	if err := CheckOverlap(art, filepath.Join(art, "wh"), cat, svc); !errors.Is(err, ErrArtifact) {
		t.Fatalf("contained warehouse: %v", err)
	}
	if err := CheckOverlap(filepath.Join(wh, "nested"), wh, cat, svc); !errors.Is(err, ErrArtifact) {
		t.Fatalf("artifact inside warehouse: %v", err)
	}
	if err := CheckOverlap(art, art, cat, svc); !errors.Is(err, ErrArtifact) {
		t.Fatalf("equal: %v", err)
	}
	if err := CheckOverlap(art, wh, cat, filepath.Join(wh, "services.csv")); !errors.Is(err, ErrArtifact) {
		t.Fatalf("services inside warehouse: %v", err)
	}
	if err := CheckOverlap(art, wh, cat, filepath.Join(cat, "services.csv")); !errors.Is(err, ErrArtifact) {
		t.Fatalf("services inside catalog: %v", err)
	}
	owned := filepath.Join(wh, "provider_catalog")
	if err := CheckOverlap(art, wh, owned, svc); err != nil {
		t.Fatalf("owned warehouse catalog path: %v", err)
	}
}

func TestCheckOverlapPhysical(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	realArt := filepath.Join(base, "real-art")
	if err := os.Mkdir(realArt, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link-art")
	if err := os.Symlink(realArt, link); err != nil {
		t.Fatal(err)
	}
	wh := filepath.Join(link, "warehouse")
	cat := filepath.Join(base, "catalog")
	svc := filepath.Join(base, "svc")
	if err := CheckOverlap(realArt, wh, cat, svc); !errors.Is(err, ErrArtifact) {
		t.Fatalf("physical containment: %v", err)
	}
}
