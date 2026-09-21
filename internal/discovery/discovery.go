package discovery

import (
	"context"
	"fmt"
	"time"

	"github.com/EnotPoloskun/mrfdiscoverer"
)

// TOCFile is one listing entry. URL is the exact payer listing string.
type TOCFile struct {
	URL string
}

// DiscoverFunc lists TOC URLs for one payer and pipeline collection month
// (YYYY-MM). DefaultDiscover converts that month to mrfdiscoverer's mm-yyyy.
type DiscoverFunc func(ctx context.Context, payer, collectionMonth string) ([]TOCFile, error)

// DefaultDiscover calls mrfdiscoverer for the stored payer identifier and
// collection month.
func DefaultDiscover(ctx context.Context, payer, collectionMonth string) ([]TOCFile, error) {
	month, err := toDiscovererMonth(collectionMonth)
	if err != nil {
		return nil, err
	}
	files, err := mrfdiscoverer.Discover(ctx, mrfdiscoverer.Config{Payer: payer, Month: month})
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

func formatCollectionMonth(month time.Time) string {
	y, m, _ := month.Date()
	return fmt.Sprintf("%04d-%02d", y, int(m))
}

func toDiscovererMonth(collectionMonth string) (string, error) {
	if len(collectionMonth) != 7 || collectionMonth[4] != '-' {
		return "", mrfdiscoverer.ErrInvalidConfig
	}
	return collectionMonth[5:] + "-" + collectionMonth[:4], nil
}
