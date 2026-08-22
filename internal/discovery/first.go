package discovery

import (
	"strings"
	"unicode/utf8"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

type firstHit struct {
	URL     string
	Ordinal int64
}

func firstOccurrences(files []TOCFile) ([]firstHit, error) {
	seen := make(map[string]struct{}, len(files))
	out := make([]firstHit, 0, len(files))
	for i, f := range files {
		if err := validateSourceURL(f.URL); err != nil {
			return nil, err
		}
		if _, ok := seen[f.URL]; ok {
			continue
		}
		seen[f.URL] = struct{}{}
		out = append(out, firstHit{URL: f.URL, Ordinal: int64(i)})
	}
	return out, nil
}

func validateSourceURL(raw string) error {
	if raw == "" || !utf8.ValidString(raw) {
		return jobs.Failure(jobs.FailureDiscoveryResultInvalid)
	}
	if strings.IndexByte(raw, 0) >= 0 || strings.IndexByte(raw, '\r') >= 0 || strings.IndexByte(raw, '\n') >= 0 {
		return jobs.Failure(jobs.FailureDiscoveryResultInvalid)
	}
	return nil
}
