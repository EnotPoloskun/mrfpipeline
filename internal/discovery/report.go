package discovery

import (
	"encoding/json"
	"fmt"
)

// Report is the compact discover enqueue result.
type Report struct {
	DiscoveryRunID  int64  `json:"discovery_run_id"`
	RiverJobID      int64  `json:"river_job_id"`
	PayerID         string `json:"payer_id"`
	CollectionMonth string `json:"collection_month"`
	TOCLimit        int64  `json:"toc_limit"`
}

// FormatReport returns compact JSON plus a trailing newline.
func FormatReport(r Report) (string, error) {
	if r.DiscoveryRunID <= 0 || r.RiverJobID <= 0 || r.TOCLimit <= 0 || r.PayerID == "" || r.CollectionMonth == "" {
		return "", fmt.Errorf("encode result")
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("encode result")
	}
	return string(raw) + "\n", nil
}
