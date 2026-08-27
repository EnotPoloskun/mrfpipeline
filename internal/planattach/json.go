package planattach

import (
	"bytes"
	"encoding/json"
)

type planRow struct {
	PlanName        string
	IssuerName      string
	PlanSponsorName *string
	PlanIDType      string
	PlanID          string
	PlanMarketType  string
}

type planJSON struct {
	PlanName        string  `json:"plan_name"`
	IssuerName      string  `json:"issuer_name"`
	PlanSponsorName *string `json:"plan_sponsor_name"`
	PlanIDType      string  `json:"plan_id_type"`
	PlanID          string  `json:"plan_id"`
	PlanMarketType  string  `json:"plan_market_type"`
}

func encodePlans(rows []planRow) ([]byte, error) {
	out := make([]planJSON, 0, len(rows))
	for _, row := range rows {
		sponsor := row.PlanSponsorName
		if row.PlanIDType == "hios" {
			if sponsor != nil && *sponsor == "" {
				return nil, errInvariant
			}
			sponsor = nil
		}
		if row.PlanIDType == "ein" && sponsor != nil && *sponsor == "" {
			return nil, errInvariant
		}
		out = append(out, planJSON{
			PlanName:        row.PlanName,
			IssuerName:      row.IssuerName,
			PlanSponsorName: sponsor,
			PlanIDType:      row.PlanIDType,
			PlanID:          row.PlanID,
			PlanMarketType:  row.PlanMarketType,
		})
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(out); err != nil {
		return nil, errInvariant
	}
	return buf.Bytes(), nil
}
