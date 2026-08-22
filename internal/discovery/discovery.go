package discovery

import (
	"context"

	"github.com/EnotPoloskun/mrfdiscoverer"
)

// TOCFile is one listing entry. URL is the exact payer listing string.
type TOCFile struct {
	URL string
}

// DiscoverFunc matches mrfdiscoverer.Discover's input/output shape.
type DiscoverFunc func(ctx context.Context, payer string) ([]TOCFile, error)

// DefaultDiscover calls mrfdiscoverer for the stored payer identifier.
func DefaultDiscover(ctx context.Context, payer string) ([]TOCFile, error) {
	files, err := mrfdiscoverer.Discover(ctx, mrfdiscoverer.Config{Payer: payer})
	if err != nil {
		return nil, err
	}
	out := make([]TOCFile, len(files))
	for i, f := range files {
		out[i] = TOCFile{URL: f.URL}
	}
	return out, nil
}

func resolveDiscover(fn DiscoverFunc) DiscoverFunc {
	if fn != nil {
		return fn
	}
	return DefaultDiscover
}
