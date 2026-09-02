package planattach

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/EnotPoloskun/mrfparser"
	"github.com/enotpoloskun/mrfconsumer"
	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/consumeringest"
	"github.com/enotpoloskun/mrfpipeline/internal/filtercatalog"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/mrfparse"
	"github.com/enotpoloskun/mrfpipeline/internal/planbatch"
	"github.com/enotpoloskun/mrfpipeline/internal/reconcile"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

type story32ProviderRow struct {
	NPI        string  `parquet:"npi"`
	State      *string `parquet:"state,optional"`
	City       *string `parquet:"city,optional"`
	PostalCode *string `parquet:"postal_code,optional"`
}

type story32ProviderTaxonomyRow struct {
	NPI          string `parquet:"npi"`
	TaxonomyCode string `parquet:"taxonomy_code"`
}

func writeStory32ProviderCatalog(t testing.TB, root string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "providers"), 0700); err != nil {
		t.Fatalf("create provider catalog directory: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "provider_taxonomies"), 0700); err != nil {
		t.Fatalf("create provider taxonomy directory: %v", err)
	}
	fl, miami, flZip := "fl", "miami", "33101"
	flZip2, otherState, otherCity, otherZip := "33102", "ny", "albany", "12207"
	reachable := []story32ProviderRow{
		{NPI: "1111111111", State: &fl, City: &miami, PostalCode: &flZip},
		{NPI: "2222222222", State: &fl, City: &miami, PostalCode: &flZip2},
		{NPI: "3333333333", State: &otherState, City: &otherCity, PostalCode: &otherZip},
		{NPI: "9999999999", State: &otherState, City: &otherCity, PostalCode: &otherZip},
	}
	taxonomies := []story32ProviderTaxonomyRow{
		{NPI: "1111111111", TaxonomyCode: "2084P0800X"},
		{NPI: "2222222222", TaxonomyCode: "208D00000X"},
		{NPI: "3333333333", TaxonomyCode: "2084P0800X"},
		{NPI: "9999999999", TaxonomyCode: "111N00000X"},
	}
	writeParquet(t, filepath.Join(root, "providers", "part-00000.parquet"), reachable)
	writeParquet(t, filepath.Join(root, "provider_taxonomies", "part-00000.parquet"), taxonomies)
	manifest, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"release_month":  "2026-08",
		"providers": map[string]any{
			"path": "providers", "rows": len(reachable),
		},
		"provider_taxonomies": map[string]any{
			"path": "provider_taxonomies", "rows": len(taxonomies),
		},
	})
	if err != nil {
		t.Fatalf("encode provider catalog manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), append(manifest, '\n'), 0600); err != nil {
		t.Fatalf("write provider catalog manifest: %v", err)
	}
}

func mustStory32Services(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "services.csv")
	const data = "billing_code_type,billing_code\nCPT,99213\nHCPCS,G0463\nHCPCS,G0473\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatalf("write Story 32 services: %v", err)
	}
	return path
}

func story32MRFA() []byte {
	return []byte(`{
  "reporting_entity_name": "Story 32 Synthetic Issuer",
  "reporting_entity_type": "issuer",
  "last_updated_on": "2026-07-27",
  "version": "2.2.1",
  "provider_references": [
    {
      "provider_group_id": 1,
      "network_name": ["A Network", "Shared Network"],
      "provider_groups": [
        {"npi": [1111111111, 2222222222], "tin": {"type": "ein", "value": "11-1111111"}}
      ]
    },
    {
      "provider_group_id": 2,
      "network_name": ["Shared Network", "B Network"],
      "provider_groups": [
        {"npi": [1111111111, 3333333333], "tin": {"type": "ein", "value": "22-2222222"}}
      ]
    },
    {
      "provider_group_id": 3,
      "network_name": ["Unreachable Network"],
      "provider_groups": [
        {"npi": [9999999999], "tin": {"type": "ein", "value": "99-9999999"}}
      ]
    }
  ],
  "in_network": [
    {
      "negotiation_arrangement": "ffs",
      "name": "Standard service",
      "billing_code_type": "CPT",
      "billing_code_type_version": "2026",
      "billing_code": "99213",
      "description": "Story 32 standard service",
      "negotiated_rates": [
        {
          "provider_references": [1, 2],
          "negotiated_prices": [
            {
              "service_code": ["11"],
              "billing_class": "professional",
              "setting": "outpatient",
              "negotiated_type": "negotiated",
              "negotiated_rate": 125,
              "expiration_date": "2027-01-01"
            }
          ]
        },
        {
          "provider_references": [1],
          "negotiated_prices": [
            {
              "service_code": ["CSTM-00"],
              "billing_class": "both",
              "setting": "both",
              "negotiated_type": "percentage",
              "negotiated_rate": 20.5,
              "expiration_date": "2027-01-01"
            }
          ]
        },
        {
          "provider_references": [2],
          "negotiated_prices": [
            {
              "service_code": ["22"],
              "billing_class": "institutional",
              "setting": "inpatient",
              "negotiated_type": "negotiated",
              "negotiated_rate": null,
              "expiration_date": "2027-01-01"
            }
          ]
        },
        {
          "provider_references": [1, 2],
          "negotiated_prices": [
            {
              "service_code": ["11", "11"],
              "billing_class": "professional",
              "setting": "outpatient",
              "negotiated_type": "negotiated",
              "negotiated_rate": 130,
              "expiration_date": "2027-01-01",
              "billing_code_modifier": ["25", "25"]
            }
          ]
        },
        {
          "provider_references": [1],
          "negotiated_prices": [
            {
              "service_code": ["23"],
              "billing_class": "professional",
              "setting": "outpatient",
              "negotiated_type": "negotiated",
              "negotiated_rate": 135,
              "expiration_date": "2027-01-01",
              "billing_code_modifier": ["25", "59"]
            }
          ]
          }
      ]
    },
    {
      "negotiation_arrangement": "bundle",
      "name": "Alternative service",
      "billing_code_type": "CPT",
      "billing_code_type_version": "2025",
      "billing_code": "99213",
      "description": "Alternative description",
      "negotiated_rates": [
        {
          "provider_references": [1],
          "negotiated_prices": [
            {
              "service_code": ["CSTM-00"],
              "billing_class": "professional",
              "setting": "outpatient",
              "negotiated_type": "negotiated",
              "negotiated_rate": 145,
              "expiration_date": "2027-01-01"
            }
          ]
        },
        {
          "provider_references": [2],
          "negotiated_prices": [
            {
              "service_code": ["??"],
              "billing_class": "professional",
              "setting": "outpatient",
              "negotiated_type": "negotiated",
              "negotiated_rate": 140,
              "expiration_date": "2027-01-01"
            }
          ]
        },
        {
          "provider_references": [1],
          "negotiated_prices": [
            {
              "service_code": ["24"],
              "billing_class": "professional",
              "setting": "outpatient",
              "negotiated_type": "negotiated",
              "negotiated_rate": 80,
              "expiration_date": "2020-01-01"
            }
          ]
        }
      ]
    }
  ]
}`)
}

func story32MRFB() []byte {
	return []byte(`{
  "reporting_entity_name": "Story 32 Synthetic Issuer B",
  "reporting_entity_type": "issuer",
  "last_updated_on": "2026-07-27",
  "version": "2.2.1",
  "provider_references": [
    {
      "provider_group_id": 1,
      "network_name": ["Shared Network"],
      "provider_groups": [
        {"npi": [1111111111, 2222222222], "tin": {"type": "ein", "value": "11-1111111"}}
      ]
    },
    {
      "provider_group_id": 2,
      "network_name": ["B Network"],
      "provider_groups": [
        {"npi": [1111111111, 3333333333], "tin": {"type": "ein", "value": "22-2222222"}}
      ]
    }
  ],
  "in_network": [
    {
      "negotiation_arrangement": "ffs",
      "name": "Shared CPT service",
      "billing_code_type": "CPT",
      "billing_code_type_version": "2026",
      "billing_code": "99213",
      "description": "Shared CPT description",
      "negotiated_rates": [
        {
          "provider_references": [1],
          "negotiated_prices": [
            {
              "service_code": ["11"],
              "billing_class": "professional",
              "setting": "outpatient",
              "negotiated_type": "negotiated",
              "negotiated_rate": 155,
              "expiration_date": "2027-01-01"
            }
          ]
        }
      ]
    },
    {
      "negotiation_arrangement": "ffs",
      "name": "B-only HCPCS service",
      "billing_code_type": "HCPCS",
      "billing_code_type_version": "2026",
      "billing_code": "G0463",
      "description": "B-only HCPCS description",
      "negotiated_rates": [
        {
          "provider_references": [2],
          "negotiated_prices": [
            {
              "service_code": ["22"],
              "billing_class": "professional",
              "setting": "outpatient",
              "negotiated_type": "negotiated",
              "negotiated_rate": 175,
              "expiration_date": "2027-01-01"
            }
          ]
        }
      ]
    }
  ]
}`)
}

func story32MRFD() []byte {
	return []byte(`{
  "reporting_entity_name": "Story 32 Synthetic Issuer D",
  "reporting_entity_type": "issuer",
  "last_updated_on": "2026-09-01",
  "version": "2.2.1",
  "provider_references": [
    {
      "provider_group_id": 1,
      "network_name": ["D Network"],
      "provider_groups": [
        {"npi": [2222222222], "tin": {"type": "ein", "value": "33-3333333"}}
      ]
    }
  ],
  "in_network": [
    {
      "negotiation_arrangement": "ffs",
      "name": "D-only HCPCS service",
      "billing_code_type": "HCPCS",
      "billing_code_type_version": "2026",
      "billing_code": "G0463",
      "description": "D-only HCPCS description",
      "negotiated_rates": [
        {
          "provider_references": [1],
          "negotiated_prices": [
            {
              "service_code": ["22"],
              "billing_class": "professional",
              "setting": "outpatient",
              "negotiated_type": "negotiated",
              "negotiated_rate": 210,
              "expiration_date": "2027-01-01"
            }
          ]
        }
      ]
    }
  ]
}`)
}

func story32MRFE() []byte {
	return []byte(`{
  "reporting_entity_name": "Story 32 Synthetic Issuer E",
  "reporting_entity_type": "issuer",
  "last_updated_on": "2026-07-01",
  "version": "2.2.1",
  "provider_references": [
    {
      "provider_group_id": 1,
      "network_name": ["E Network"],
      "provider_groups": [
        {"npi": [3333333333], "tin": {"type": "ein", "value": "44-4444444"}}
      ]
    }
  ],
  "in_network": [
    {
      "negotiation_arrangement": "bundle",
      "name": "E-only HCPCS service",
      "billing_code_type": "HCPCS",
      "billing_code_type_version": "2026",
      "billing_code": "G0473",
      "description": "E-only HCPCS description",
      "negotiated_rates": [
        {
          "provider_references": [1],
          "negotiated_prices": [
            {
              "service_code": ["23"],
              "billing_class": "professional",
              "setting": "outpatient",
              "negotiated_type": "negotiated",
              "negotiated_rate": 220,
              "expiration_date": "2027-01-01"
            }
          ]
        }
      ]
    }
  ]
}`)
}

func story32MRFC() []byte {
	return []byte(`{
  "reporting_entity_name": "Story 32 Excluded C",
  "reporting_entity_type": "issuer",
  "last_updated_on": "2026-08-01",
  "version": "2.2.1",
  "provider_references": [
    {
      "provider_group_id": 1,
      "network_name": ["Excluded C Network"],
      "provider_groups": [
        {"npi": [9999999999], "tin": {"type": "ein", "value": "55-5555555"}}
      ]
    }
  ],
  "in_network": [
    {
      "negotiation_arrangement": "ffs",
      "name": "Excluded C service",
      "billing_code_type": "CPT",
      "billing_code_type_version": "2026",
      "billing_code": "99213",
      "description": "Excluded C description",
      "negotiated_rates": [
        {
          "provider_references": [1],
          "negotiated_prices": [
            {
              "service_code": ["24"],
              "billing_class": "professional",
              "setting": "outpatient",
              "negotiated_type": "negotiated",
              "negotiated_rate": 230,
              "expiration_date": "2027-01-01"
            }
          ]
        }
      ]
    }
  ]
}`)
}

type story32ExpectedBillingCode struct {
	Type, Code, Version, Name, Description string
	Observations, Unmodified               int64
}

type story32ExpectedPlan struct {
	Name, Issuer, IDType, ID, Market, Search string
}

type story32ExpectedCodeFilter struct {
	Kind, Value, Label string
	Observations       int64
}

type story32ExpectedNetwork struct {
	Name         string
	Observations int64
}

type story32ExpectedProviderFilter struct {
	Kind, Parent, Value string
	Providers           int64
}

type story32ExpectedCatalog struct {
	StandardFacts        int64
	BillingCode          story32ExpectedBillingCode
	Plans                []story32ExpectedPlan
	CodeFilterValues     []story32ExpectedCodeFilter
	CodeNetworks         []story32ExpectedNetwork
	ProviderFilterValues []story32ExpectedProviderFilter
}

// story32ExpectedA is deliberately literal: acceptance assertions must not
// derive expected values by querying the catalog being tested.
var story32ExpectedA = story32ExpectedCatalog{
	StandardFacts: 6,
	BillingCode: story32ExpectedBillingCode{
		Type: "CPT", Code: "99213", Version: "2025",
		Name: "Alternative service", Description: "Alternative description",
		Observations: 6, Unmodified: 4,
	},
	Plans: []story32ExpectedPlan{
		{Name: "A-only Plan", Issuer: "Story32 Issuer", IDType: "hios", ID: "A-ONLY-1", Market: "group", Search: "a-only plan story32 issuer hios a-only-1 group"},
		{Name: "Alt Shared Plan", Issuer: "Other Issuer", IDType: "hios", ID: "SHARED-1", Market: "group", Search: "alt shared plan other issuer hios shared-1 group"},
		{Name: "Shared Plan", Issuer: "Story32 Issuer", IDType: "hios", ID: "SHARED-1", Market: "group", Search: "shared plan story32 issuer hios shared-1 group"},
	},
	CodeFilterValues: []story32ExpectedCodeFilter{
		{Kind: "billing_class", Value: "professional", Label: "professional", Observations: 6},
		{Kind: "modifier", Value: "25", Label: "25", Observations: 2},
		{Kind: "modifier", Value: "59", Label: "59", Observations: 1},
		{Kind: "negotiation_arrangement", Value: "bundle", Label: "bundle", Observations: 3},
		{Kind: "negotiation_arrangement", Value: "ffs", Label: "ffs", Observations: 3},
		{Kind: "place_of_service", Value: "11", Label: "11", Observations: 2},
		{Kind: "place_of_service", Value: "23", Label: "23", Observations: 1},
		{Kind: "place_of_service", Value: "24", Label: "24", Observations: 1},
		{Kind: "place_of_service", Value: "??", Label: "??", Observations: 1},
		{Kind: "place_of_service", Value: "CSTM-00", Label: "Broad or unspecified place of service", Observations: 1},
		{Kind: "setting", Value: "outpatient", Label: "outpatient", Observations: 6},
	},
	CodeNetworks: []story32ExpectedNetwork{
		{Name: "A Network", Observations: 5},
		{Name: "B Network", Observations: 3},
		{Name: "Shared Network", Observations: 6},
	},
	ProviderFilterValues: []story32ExpectedProviderFilter{
		{Kind: "city", Parent: "fl", Value: "miami", Providers: 2},
		{Kind: "city", Parent: "ny", Value: "albany", Providers: 1},
		{Kind: "state", Value: "fl", Providers: 2},
		{Kind: "state", Value: "ny", Providers: 1},
		{Kind: "taxonomy", Value: "2084P0800X", Providers: 2},
		{Kind: "taxonomy", Value: "208D00000X", Providers: 1},
	},
}

type story32ExpectedOutputNetwork struct {
	Output, Type, Code, Name string
	Observations             int64
}

type story32ExpectedCodeFilterRow struct {
	Type, Code string
	story32ExpectedCodeFilter
}

type story32ExpectedCombined struct {
	StandardFacts        int64
	BillingCodes         []story32ExpectedBillingCode
	Plans                []story32ExpectedPlan
	CodeFilterValues     []story32ExpectedCodeFilterRow
	CodeNetworks         []story32ExpectedOutputNetwork
	ProviderFilterValues []story32ExpectedProviderFilter
}

var story32ExpectedAB = story32ExpectedCombined{
	StandardFacts: 8,
	BillingCodes: []story32ExpectedBillingCode{
		{Type: "CPT", Code: "99213", Version: "2026", Name: "Alternative service", Description: "Alternative description", Observations: 7, Unmodified: 5},
		{Type: "HCPCS", Code: "G0463", Version: "2026", Name: "B-only HCPCS service", Description: "B-only HCPCS description", Observations: 1, Unmodified: 1},
	},
	Plans: []story32ExpectedPlan{
		{Name: "A-only Plan", Issuer: "Story32 Issuer", IDType: "hios", ID: "A-ONLY-1", Market: "group", Search: "a-only plan story32 issuer hios a-only-1 group"},
		{Name: "Alt Shared Plan", Issuer: "Other Issuer", IDType: "hios", ID: "SHARED-1", Market: "group", Search: "alt shared plan other issuer hios shared-1 group"},
		{Name: "B-only Plan", Issuer: "Story32 Issuer", IDType: "hios", ID: "B-ONLY-1", Market: "group", Search: "b-only plan story32 issuer hios b-only-1 group"},
		{Name: "Shared Plan", Issuer: "Story32 Issuer", IDType: "hios", ID: "SHARED-1", Market: "group", Search: "shared plan story32 issuer hios shared-1 group"},
	},
	CodeFilterValues: []story32ExpectedCodeFilterRow{
		{Type: "CPT", Code: "99213", story32ExpectedCodeFilter: story32ExpectedCodeFilter{Kind: "billing_class", Value: "professional", Label: "professional", Observations: 7}},
		{Type: "CPT", Code: "99213", story32ExpectedCodeFilter: story32ExpectedCodeFilter{Kind: "modifier", Value: "25", Label: "25", Observations: 2}},
		{Type: "CPT", Code: "99213", story32ExpectedCodeFilter: story32ExpectedCodeFilter{Kind: "modifier", Value: "59", Label: "59", Observations: 1}},
		{Type: "CPT", Code: "99213", story32ExpectedCodeFilter: story32ExpectedCodeFilter{Kind: "negotiation_arrangement", Value: "bundle", Label: "bundle", Observations: 3}},
		{Type: "CPT", Code: "99213", story32ExpectedCodeFilter: story32ExpectedCodeFilter{Kind: "negotiation_arrangement", Value: "ffs", Label: "ffs", Observations: 4}},
		{Type: "CPT", Code: "99213", story32ExpectedCodeFilter: story32ExpectedCodeFilter{Kind: "place_of_service", Value: "11", Label: "11", Observations: 3}},
		{Type: "CPT", Code: "99213", story32ExpectedCodeFilter: story32ExpectedCodeFilter{Kind: "place_of_service", Value: "23", Label: "23", Observations: 1}},
		{Type: "CPT", Code: "99213", story32ExpectedCodeFilter: story32ExpectedCodeFilter{Kind: "place_of_service", Value: "24", Label: "24", Observations: 1}},
		{Type: "CPT", Code: "99213", story32ExpectedCodeFilter: story32ExpectedCodeFilter{Kind: "place_of_service", Value: "??", Label: "??", Observations: 1}},
		{Type: "CPT", Code: "99213", story32ExpectedCodeFilter: story32ExpectedCodeFilter{Kind: "place_of_service", Value: "CSTM-00", Label: "Broad or unspecified place of service", Observations: 1}},
		{Type: "CPT", Code: "99213", story32ExpectedCodeFilter: story32ExpectedCodeFilter{Kind: "setting", Value: "outpatient", Label: "outpatient", Observations: 7}},
		{Type: "HCPCS", Code: "G0463", story32ExpectedCodeFilter: story32ExpectedCodeFilter{Kind: "billing_class", Value: "professional", Label: "professional", Observations: 1}},
		{Type: "HCPCS", Code: "G0463", story32ExpectedCodeFilter: story32ExpectedCodeFilter{Kind: "negotiation_arrangement", Value: "ffs", Label: "ffs", Observations: 1}},
		{Type: "HCPCS", Code: "G0463", story32ExpectedCodeFilter: story32ExpectedCodeFilter{Kind: "place_of_service", Value: "22", Label: "22", Observations: 1}},
		{Type: "HCPCS", Code: "G0463", story32ExpectedCodeFilter: story32ExpectedCodeFilter{Kind: "setting", Value: "outpatient", Label: "outpatient", Observations: 1}},
	},
	CodeNetworks: []story32ExpectedOutputNetwork{
		{Output: "A", Type: "CPT", Code: "99213", Name: "A Network", Observations: 5},
		{Output: "A", Type: "CPT", Code: "99213", Name: "B Network", Observations: 3},
		{Output: "A", Type: "CPT", Code: "99213", Name: "Shared Network", Observations: 6},
		{Output: "B", Type: "CPT", Code: "99213", Name: "Shared Network", Observations: 1},
		{Output: "B", Type: "HCPCS", Code: "G0463", Name: "B Network", Observations: 1},
	},
	ProviderFilterValues: []story32ExpectedProviderFilter{
		{Kind: "city", Parent: "fl", Value: "miami", Providers: 2},
		{Kind: "city", Parent: "ny", Value: "albany", Providers: 1},
		{Kind: "state", Value: "fl", Providers: 2},
		{Kind: "state", Value: "ny", Providers: 1},
		{Kind: "taxonomy", Value: "2084P0800X", Providers: 2},
		{Kind: "taxonomy", Value: "208D00000X", Providers: 1},
	},
}

var story32ExpectedD = story32ExpectedCatalog{
	StandardFacts: 1,
	BillingCode: story32ExpectedBillingCode{
		Type: "HCPCS", Code: "G0463", Version: "2026",
		Name: "D-only HCPCS service", Description: "D-only HCPCS description",
		Observations: 1, Unmodified: 1,
	},
	Plans: []story32ExpectedPlan{
		{Name: "D-only Plan", Issuer: "Story32 Issuer", IDType: "hios", ID: "D-ONLY-1", Market: "group", Search: "d-only plan story32 issuer hios d-only-1 group"},
	},
	CodeFilterValues: []story32ExpectedCodeFilter{
		{Kind: "billing_class", Value: "professional", Label: "professional", Observations: 1},
		{Kind: "negotiation_arrangement", Value: "ffs", Label: "ffs", Observations: 1},
		{Kind: "place_of_service", Value: "22", Label: "22", Observations: 1},
		{Kind: "setting", Value: "outpatient", Label: "outpatient", Observations: 1},
	},
	CodeNetworks: []story32ExpectedNetwork{
		{Name: "D Network", Observations: 1},
	},
	ProviderFilterValues: []story32ExpectedProviderFilter{
		{Kind: "city", Parent: "fl", Value: "miami", Providers: 1},
		{Kind: "state", Value: "fl", Providers: 1},
		{Kind: "taxonomy", Value: "208D00000X", Providers: 1},
	},
}

var story32ExpectedE = story32ExpectedCatalog{
	StandardFacts: 1,
	BillingCode: story32ExpectedBillingCode{
		Type: "HCPCS", Code: "G0473", Version: "2026",
		Name: "E-only HCPCS service", Description: "E-only HCPCS description",
		Observations: 1, Unmodified: 1,
	},
	Plans: []story32ExpectedPlan{
		{Name: "E-only Plan", Issuer: "Story32 Issuer", IDType: "hios", ID: "E-ONLY-1", Market: "group", Search: "e-only plan story32 issuer hios e-only-1 group"},
	},
	CodeFilterValues: []story32ExpectedCodeFilter{
		{Kind: "billing_class", Value: "professional", Label: "professional", Observations: 1},
		{Kind: "negotiation_arrangement", Value: "bundle", Label: "bundle", Observations: 1},
		{Kind: "place_of_service", Value: "23", Label: "23", Observations: 1},
		{Kind: "setting", Value: "outpatient", Label: "outpatient", Observations: 1},
	},
	CodeNetworks: []story32ExpectedNetwork{
		{Name: "E Network", Observations: 1},
	},
	ProviderFilterValues: []story32ExpectedProviderFilter{
		{Kind: "city", Parent: "ny", Value: "albany", Providers: 1},
		{Kind: "state", Value: "ny", Providers: 1},
		{Kind: "taxonomy", Value: "2084P0800X", Providers: 1},
	},
}

type story32Plan struct {
	PlanName        string  `json:"plan_name"`
	IssuerName      string  `json:"issuer_name"`
	PlanSponsorName *string `json:"plan_sponsor_name"`
	PlanIDType      string  `json:"plan_id_type"`
	PlanID          string  `json:"plan_id"`
	PlanMarketType  string  `json:"plan_market_type"`
}

func story32Sponsor(name string) *string {
	return &name
}

func story32PlansA() []story32Plan {
	return []story32Plan{
		{PlanName: "Shared Plan", IssuerName: "Story32 Issuer", PlanSponsorName: story32Sponsor("Sponsor Alpha"), PlanIDType: "hios", PlanID: "SHARED-1", PlanMarketType: "group"},
		{PlanName: "Shared Plan", IssuerName: "Story32 Issuer", PlanSponsorName: story32Sponsor("Sponsor Beta"), PlanIDType: "hios", PlanID: "SHARED-1", PlanMarketType: "group"},
		{PlanName: "A-only Plan", IssuerName: "Story32 Issuer", PlanIDType: "hios", PlanID: "A-ONLY-1", PlanMarketType: "group"},
		{PlanName: "Alt Shared Plan", IssuerName: "Other Issuer", PlanIDType: "hios", PlanID: "SHARED-1", PlanMarketType: "group"},
	}
}

func story32PlansB() []story32Plan {
	return []story32Plan{
		{PlanName: "Shared Plan", IssuerName: "Story32 Issuer", PlanSponsorName: story32Sponsor("Sponsor Gamma"), PlanIDType: "hios", PlanID: "SHARED-1", PlanMarketType: "group"},
		{PlanName: "B-only Plan", IssuerName: "Story32 Issuer", PlanIDType: "hios", PlanID: "B-ONLY-1", PlanMarketType: "group"},
	}
}

// seedStory32Snapshot creates the durable, successful pipeline prerequisites
// consumed by the Story 32 parser/consumer acceptance flow. The caller owns
// the workspace and must publish the returned snapshot's warehouse artifacts
// through the public consumer APIs.
func seedStory32Snapshot(t *testing.T, pool *pgxpool.Pool, payer string, month time.Time, suffix string) (sourceID, snapshotID int64) {
	t.Helper()
	ctx := context.Background()
	month = month.UTC()
	month = time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
	url := "https://files.test/story32/" + suffix

	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases
    (payer_id, collection_month, mrf_source_target_kind)
VALUES ($1, $2::date, 'all')
ON CONFLICT (payer_id, collection_month) DO NOTHING`, payer, month); err != nil {
		t.Fatalf("insert monthly release: %v", err)
	}
	var releaseStatus, targetKind string
	if err := pool.QueryRow(ctx, `
SELECT status, mrf_source_target_kind
FROM mrfpipeline.monthly_releases
WHERE payer_id = $1 AND collection_month = $2::date`, payer, month).Scan(&releaseStatus, &targetKind); err != nil {
		t.Fatalf("read monthly release: %v", err)
	}
	if releaseStatus != "building" && releaseStatus != "active" {
		t.Fatalf("monthly release is %s/%s, want building/all or active/all", releaseStatus, targetKind)
	}

	var discoveryID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.discovery_runs
    (payer_id, collection_month, status, discovered_count, existing_count,
     admitted_count, overflow_count, completed_at)
VALUES ($1, $2::date, 'succeeded', 1, 0, 1, 0, transaction_timestamp())
RETURNING id`, payer, month).Scan(&discoveryID); err != nil {
		t.Fatalf("insert discovery run: %v", err)
	}

	var tocID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.toc_files
    (payer_id, collection_month, source_url, first_discovery_run_id,
     download_status, parse_status, import_status)
VALUES ($1, $2::date, $3, $4, 'succeeded', 'succeeded', 'succeeded')
RETURNING id`, payer, month, url, discoveryID).Scan(&tocID); err != nil {
		t.Fatalf("insert TOC inventory: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.discovery_run_toc_files
    (discovery_run_id, toc_file_id, listing_ordinal, was_new)
VALUES ($1, $2, 0, true)`, discoveryID, tocID); err != nil {
		t.Fatalf("insert discovery TOC membership: %v", err)
	}

	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_sources
    (source_url, collection_month, download_status, parse_status)
VALUES ($1, $2::date, 'succeeded', 'succeeded')
RETURNING id`, url, month).Scan(&sourceID); err != nil {
		t.Fatalf("insert succeeded source: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_release_mrf_sources
    (payer_id, collection_month, mrf_source_id)
VALUES ($1, $2::date, $3)`, payer, month, sourceID); err != nil {
		t.Fatalf("insert selected source: %v", err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_snapshots
    (mrf_source_id, payer_id, collection_month, consume_status, consume_river_job_id)
VALUES ($1, $2, $3::date, 'succeeded', 1)
RETURNING id`, sourceID, payer, month).Scan(&snapshotID); err != nil {
		t.Fatalf("insert consumed snapshot: %v", err)
	}
	return sourceID, snapshotID
}

func publishStory32Output(
	t *testing.T,
	pool *pgxpool.Pool,
	ws *artifact.Workspace,
	providerCatalog, warehouse, services string,
	sourceID, snapshotID int64,
	payer string,
	month time.Time,
	rawMRF []byte,
	plans []story32Plan,
) (release.Target, int64) {
	t.Helper()
	ctx := context.Background()
	month = month.UTC()
	month = time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
	monthText := month.Format("2006-01")
	outputID := consumeringest.FormatSnapshotOutputID(snapshotID)

	dataPath, err := ws.DownloadDataPath(artifact.KindMRF, sourceID)
	if err != nil {
		t.Fatalf("resolve MRF download path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(dataPath), 0700); err != nil {
		t.Fatalf("create MRF download directory: %v", err)
	}
	if err := os.WriteFile(dataPath, rawMRF, 0600); err != nil {
		t.Fatalf("write raw MRF input: %v", err)
	}
	parsedPath, err := ws.ParsedDir(artifact.KindMRF, sourceID)
	if err != nil {
		t.Fatalf("resolve parsed MRF directory: %v", err)
	}
	if err := os.MkdirAll(ws.StagingDir(), 0700); err != nil {
		t.Fatalf("create parser staging directory: %v", err)
	}
	t.Setenv("TMPDIR", ws.StagingDir())

	parseConfig := mrfparser.DefaultConfig()
	parseConfig.Input = dataPath
	parseConfig.Output = parsedPath
	parseConfig.Services = services
	parseConfig.TempDir = ws.StagingDir()
	if err := mrfparser.Parse(ctx, parseConfig); err != nil {
		t.Fatalf("parse MRF input: %v", err)
	}
	sourcePath, err := filepath.EvalSymlinks(dataPath)
	if err != nil {
		t.Fatalf("resolve MRF input identity: %v", err)
	}
	if err := mrfparse.ValidateCompletedOutput(parsedPath, mrfparse.ExpectedSourceURI(sourcePath), services); err != nil {
		t.Fatalf("validate parsed MRF output: %v", err)
	}
	if _, err := mrfconsumer.Ingest(ctx, mrfconsumer.Config{
		InputPath: parsedPath, ProviderCatalogPath: providerCatalog, OutputPath: warehouse,
		PayerID: payer, CollectionMonth: monthText, OutputID: outputID,
	}); err != nil {
		t.Fatalf("ingest MRF output: %v", err)
	}

	planIDs := make([]int64, 0, len(plans))
	for _, plan := range plans {
		var planID int64
		if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_plans
    (mrf_snapshot_id, plan_name, issuer_name, plan_sponsor_name,
     plan_id_type, plan_id, plan_market_type)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id`,
			snapshotID, plan.PlanName, plan.IssuerName, plan.PlanSponsorName,
			plan.PlanIDType, plan.PlanID, plan.PlanMarketType,
		).Scan(&planID); err != nil {
			t.Fatalf("insert authoritative plan %q: %v", plan.PlanName, err)
		}
		planIDs = append(planIDs, planID)
	}

	var batchID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batches
    (mrf_snapshot_id, status, river_job_id, requested_plan_count,
     added_plan_count, started_at, completed_at)
VALUES ($1, 'succeeded', 1, $2, $2,
        transaction_timestamp(), transaction_timestamp())
RETURNING id`, snapshotID, len(planIDs)).Scan(&batchID); err != nil {
		t.Fatalf("insert succeeded plan attachment batch: %v", err)
	}
	for _, planID := range planIDs {
		if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batch_items
    (plan_attachment_batch_id, mrf_plan_id)
VALUES ($1, $2)`, batchID, planID); err != nil {
			t.Fatalf("insert plan attachment item %d: %v", planID, err)
		}
	}

	rows := make([]planRow, 0, len(plans))
	for _, plan := range plans {
		rows = append(rows, planRow{
			PlanName: plan.PlanName, IssuerName: plan.IssuerName,
			PlanSponsorName: plan.PlanSponsorName, PlanIDType: plan.PlanIDType,
			PlanID: plan.PlanID, PlanMarketType: plan.PlanMarketType,
		})
	}
	canonical, err := encodePlans(rows)
	if err != nil {
		t.Fatalf("encode canonical plans: %v", err)
	}
	plansPath, err := ws.PublishPlanBatch(batchID, canonical)
	if err != nil {
		t.Fatalf("publish canonical plans: %v", err)
	}
	if _, err := mrfconsumer.AttachPlans(ctx, mrfconsumer.AttachPlansConfig{
		PlansPath: plansPath, OutputPath: warehouse,
		OutputID: outputID, PlanBatchID: planbatch.FormatPlanBatchID(batchID),
	}); err != nil {
		t.Fatalf("attach plans: %v", err)
	}
	return release.Target{
		PayerID: payer, CollectionMonth: monthText,
		OutputID: outputID, SnapshotID: snapshotID,
	}, batchID
}

func publishStory32ExcludedC(t *testing.T, ws *artifact.Workspace, providerCatalog, warehouse, services string, rawMRF []byte) string {
	t.Helper()
	ctx := context.Background()
	const sourceID int64 = 3200000001
	const batchID int64 = 3200000002
	const outputID = "mrf-story32-excluded-c"
	dataPath, err := ws.DownloadDataPath(artifact.KindMRF, sourceID)
	if err != nil {
		t.Fatalf("resolve excluded C input path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(dataPath), 0700); err != nil {
		t.Fatalf("create excluded C input directory: %v", err)
	}
	if err := os.WriteFile(dataPath, rawMRF, 0600); err != nil {
		t.Fatalf("write excluded C input: %v", err)
	}
	parsedPath, err := ws.ParsedDir(artifact.KindMRF, sourceID)
	if err != nil {
		t.Fatalf("resolve excluded C parsed path: %v", err)
	}
	t.Setenv("TMPDIR", ws.StagingDir())
	parseConfig := mrfparser.DefaultConfig()
	parseConfig.Input = dataPath
	parseConfig.Output = parsedPath
	parseConfig.Services = services
	parseConfig.TempDir = ws.StagingDir()
	if err := mrfparser.Parse(ctx, parseConfig); err != nil {
		t.Fatalf("parse excluded C input: %v", err)
	}
	sourcePath, err := filepath.EvalSymlinks(dataPath)
	if err != nil {
		t.Fatalf("resolve excluded C input identity: %v", err)
	}
	if err := mrfparse.ValidateCompletedOutput(parsedPath, mrfparse.ExpectedSourceURI(sourcePath), services); err != nil {
		t.Fatalf("validate excluded C parsed output: %v", err)
	}
	if _, err := mrfconsumer.Ingest(ctx, mrfconsumer.Config{
		InputPath: parsedPath, ProviderCatalogPath: providerCatalog, OutputPath: warehouse,
		PayerID: "payer-a", CollectionMonth: "2026-08", OutputID: outputID,
	}); err != nil {
		t.Fatalf("ingest excluded C output: %v", err)
	}
	canonical, err := encodePlans([]planRow{{
		PlanName: "C-only Plan", IssuerName: "Story32 Issuer",
		PlanIDType: "hios", PlanID: "C-ONLY-1", PlanMarketType: "group",
	}})
	if err != nil {
		t.Fatalf("encode excluded C plans: %v", err)
	}
	plansPath, err := ws.PublishPlanBatch(batchID, canonical)
	if err != nil {
		t.Fatalf("publish excluded C plans: %v", err)
	}
	if _, err := mrfconsumer.AttachPlans(ctx, mrfconsumer.AttachPlansConfig{
		PlansPath: plansPath, OutputPath: warehouse,
		OutputID: outputID, PlanBatchID: planbatch.FormatPlanBatchID(batchID),
	}); err != nil {
		t.Fatalf("attach excluded C plans: %v", err)
	}
	return outputID
}

func publishStory32PlanBatch(t *testing.T, ws *artifact.Workspace, warehouse, outputID string, batchID int64, plans []story32Plan) {
	t.Helper()
	canonicalRows := make([]planRow, 0, len(plans))
	for _, plan := range plans {
		canonicalRows = append(canonicalRows, planRow{
			PlanName: plan.PlanName, IssuerName: plan.IssuerName,
			PlanSponsorName: plan.PlanSponsorName, PlanIDType: plan.PlanIDType,
			PlanID: plan.PlanID, PlanMarketType: plan.PlanMarketType,
		})
	}
	canonical, err := encodePlans(canonicalRows)
	if err != nil {
		t.Fatalf("encode plan batch: %v", err)
	}
	plansPath, err := ws.PublishPlanBatch(batchID, canonical)
	if err != nil {
		t.Fatalf("publish plan batch: %v", err)
	}
	if _, err := mrfconsumer.AttachPlans(context.Background(), mrfconsumer.AttachPlansConfig{
		PlansPath: plansPath, OutputPath: warehouse,
		OutputID: outputID, PlanBatchID: planbatch.FormatPlanBatchID(batchID),
	}); err != nil {
		t.Fatalf("attach plan batch: %v", err)
	}
}

func story32DatabaseState(t *testing.T, pool *pgxpool.Pool, payer string, month time.Time, catalogID int64) string {
	t.Helper()
	ctx := context.Background()
	var releaseState, outputState string
	if err := pool.QueryRow(ctx, `
SELECT COALESCE((SELECT row_to_json(r)::text FROM (
    SELECT payer_id, collection_month, status, publication_generation,
           mrf_source_target_kind, mrf_source_target_count, sealed_at,
           last_activated_at, created_at, updated_at
    FROM mrfpipeline.monthly_releases
    WHERE payer_id = $1 AND collection_month = $2
) r), '')`, payer, month).Scan(&releaseState); err != nil {
		t.Fatalf("read release state: %v", err)
	}
	if err := pool.QueryRow(ctx, `
SELECT COALESCE(json_agg(r ORDER BY r.mrf_snapshot_id)::text, '[]')
FROM (
    SELECT payer_id, collection_month, mrf_snapshot_id, published_generation, published_at
    FROM mrfpipeline.monthly_release_outputs
    WHERE payer_id = $1 AND collection_month = $2
) r`, payer, month).Scan(&outputState); err != nil {
		t.Fatalf("read release output state: %v", err)
	}
	state := "release=" + releaseState + "\noutputs=" + outputState
	for _, table := range []string{
		"release_catalogs", "release_outputs", "release_billing_codes",
		"release_code_filter_values", "release_plans", "release_plan_outputs",
		"release_output_code_networks", "release_provider_filter_values",
	} {
		var tableState string
		keyColumn := "catalog_id"
		if table == "release_catalogs" {
			keyColumn = "id"
		}
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
SELECT COALESCE(json_agg(r ORDER BY row_to_json(r)::text)::text, '[]')
FROM (SELECT * FROM mrfweb.%s WHERE %s = $1) r`, table, keyColumn), catalogID).Scan(&tableState); err != nil {
			t.Fatalf("read %s state: %v", table, err)
		}
		state += "\n" + table + "=" + tableState
	}
	return state
}

func story32WarehouseDigest(t *testing.T, root string) string {
	t.Helper()
	hash := sha256.New()
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(hash, "%s\x00%s\x00%d\x00%d\x00", relative, info.Mode().String(), info.Size(), info.ModTime().UnixNano()); err != nil {
			return err
		}
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(hash, "%s\x00", target)
			return err
		case info.Mode().IsRegular():
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			_, err = hash.Write(data)
			return err
		default:
			return nil
		}
	})
	if err != nil {
		t.Fatalf("digest warehouse: %v", err)
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func TestIntegrationStory32FilterCatalogAcceptance(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	if _, err := filtercatalog.ResolveDuckDB(ctx); err != nil {
		if os.Getenv("MRFPIPELINE_REQUIRE_DUCKDB_ACCEPTANCE") == "1" {
			t.Fatalf("exact DuckDB v1.5.5 is required: %v", err)
		}
		t.Skipf("exact DuckDB v1.5.5 unavailable: %v", err)
	}

	payer := "payer-a"
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	providerCatalogMonth := month
	ws := mustWorkspace(t)
	providerCatalog := filepath.Join(t.TempDir(), "provider-catalog")
	writeStory32ProviderCatalog(t, providerCatalog)
	warehouse := filepath.Join(t.TempDir(), "warehouse")
	services := mustStory32Services(t)
	sourceID, snapshotID := seedStory32Snapshot(t, pool, payer, month, "a")
	target, batchID := publishStory32Output(t, pool, ws, providerCatalog, warehouse, services, sourceID, snapshotID, payer, month, story32MRFA(), story32PlansA())
	if target.OutputID != "mrf-"+fmt.Sprint(snapshotID) || target.SnapshotID != snapshotID || batchID <= 0 {
		t.Fatalf("target=%+v batch=%d", target, batchID)
	}
	before := story32WarehouseDigest(t, warehouse)

	var activeCatalogs, activeOutputs int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_catalogs`).Scan(&activeCatalogs); err != nil {
		t.Fatalf("count active catalogs before activation: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_outputs`).Scan(&activeOutputs); err != nil {
		t.Fatalf("count active outputs before activation: %v", err)
	}
	if activeCatalogs != 0 || activeOutputs != 0 {
		t.Fatalf("active views before activation = %d/%d", activeCatalogs, activeOutputs)
	}

	build, err := filtercatalog.Build(ctx, filtercatalog.BuildParams{
		Pool: pool, PayerID: payer, CollectionMonth: month,
		WarehousePath: warehouse, ProviderCatalogPath: providerCatalog,
		Preflight: func(targets []release.Target) error {
			return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
		},
	})
	if err != nil {
		t.Fatalf("build filter catalog: %v", err)
	}
	if build.CatalogStatus != "ready" || build.PublicationGeneration != 1 || build.OutputCount != 1 ||
		build.StandardFactCount != story32ExpectedA.StandardFacts || build.BillingCodeCount != 1 ||
		build.CodeFilterValueCount != int64(len(story32ExpectedA.CodeFilterValues)) ||
		build.PlanCount != int64(len(story32ExpectedA.Plans)) || build.PlanOutputCount != 3 ||
		build.OutputCodeNetworkCount != int64(len(story32ExpectedA.CodeNetworks)) ||
		build.ProviderFilterValueCount != int64(len(story32ExpectedA.ProviderFilterValues)) {
		t.Fatalf("unexpected build result: %+v", build)
	}
	afterBuild := story32WarehouseDigest(t, warehouse)
	if before != afterBuild {
		t.Fatalf("catalog build mutated warehouse: before=%s after=%s", before, afterBuild)
	}

	var status string
	var generation, outputCount, standardFacts, schema, billingCodes, codeFilters, planCount, planOutputs, networks, providers int64
	var providerMonth time.Time
	if err := pool.QueryRow(ctx, `
SELECT status, publication_generation, output_count, standard_fact_count,
       provider_catalog_schema_version, provider_catalog_release_month,
       billing_code_count, code_filter_value_count, plan_count,
       plan_output_count, output_code_network_count, provider_filter_value_count
FROM mrfweb.release_catalogs WHERE id = $1`, build.CatalogID).Scan(
		&status, &generation, &outputCount, &standardFacts, &schema, &providerMonth,
		&billingCodes, &codeFilters, &planCount, &planOutputs, &networks, &providers,
	); err != nil {
		t.Fatalf("read ready catalog header: %v", err)
	}
	if status != "ready" || generation != 1 || outputCount != 1 || standardFacts != story32ExpectedA.StandardFacts ||
		schema != 1 || !providerMonth.Equal(month) || billingCodes != 1 ||
		codeFilters != int64(len(story32ExpectedA.CodeFilterValues)) || planCount != 3 ||
		planOutputs != 3 || networks != int64(len(story32ExpectedA.CodeNetworks)) ||
		providers != int64(len(story32ExpectedA.ProviderFilterValues)) {
		t.Fatalf("unexpected ready header: %s/%d/%d/%d/%d/%s/%d/%d/%d/%d/%d/%d", status, generation, outputCount, standardFacts, schema, providerMonth, billingCodes, codeFilters, planCount, planOutputs, networks, providers)
	}

	var outputSnapshot int64
	var outputID string
	if err := pool.QueryRow(ctx, `
SELECT mrf_snapshot_id, output_id
FROM mrfweb.release_outputs WHERE catalog_id = $1`, build.CatalogID).Scan(
		&outputSnapshot, &outputID,
	); err != nil {
		t.Fatalf("read ready output: %v", err)
	}
	if outputSnapshot != snapshotID || outputID != target.OutputID {
		t.Fatalf("unexpected ready output: %d/%s", outputSnapshot, outputID)
	}

	var billing story32ExpectedBillingCode
	if err := pool.QueryRow(ctx, `
SELECT billing_code_type, billing_code, billing_code_type_version,
       warehouse_service_name, warehouse_service_description,
       observation_count, unmodified_observation_count
FROM mrfweb.release_billing_codes
WHERE catalog_id = $1`, build.CatalogID).Scan(
		&billing.Type, &billing.Code, &billing.Version, &billing.Name, &billing.Description,
		&billing.Observations, &billing.Unmodified,
	); err != nil {
		t.Fatalf("read billing-code row: %v", err)
	}
	if !reflect.DeepEqual(billing, story32ExpectedA.BillingCode) {
		t.Fatalf("billing-code row=%+v want=%+v", billing, story32ExpectedA.BillingCode)
	}

	codeRows, err := pool.Query(ctx, `
SELECT filter_kind, filter_value, display_label, observation_count
FROM mrfweb.release_code_filter_values
WHERE catalog_id = $1
ORDER BY filter_kind COLLATE "C", filter_value COLLATE "C"`, build.CatalogID)
	if err != nil {
		t.Fatalf("read code-filter rows: %v", err)
	}
	var gotCodeRows []story32ExpectedCodeFilter
	for codeRows.Next() {
		var row story32ExpectedCodeFilter
		if err := codeRows.Scan(&row.Kind, &row.Value, &row.Label, &row.Observations); err != nil {
			codeRows.Close()
			t.Fatalf("scan code-filter row: %v", err)
		}
		gotCodeRows = append(gotCodeRows, row)
	}
	if err := codeRows.Err(); err != nil {
		codeRows.Close()
		t.Fatalf("read code-filter rows: %v", err)
	}
	codeRows.Close()
	if !reflect.DeepEqual(gotCodeRows, story32ExpectedA.CodeFilterValues) {
		t.Fatalf("code-filter rows=%+v want=%+v", gotCodeRows, story32ExpectedA.CodeFilterValues)
	}

	planRows, err := pool.Query(ctx, `
SELECT plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text
FROM mrfweb.release_plans WHERE catalog_id = $1
ORDER BY plan_name COLLATE "C", issuer_name COLLATE "C", plan_id COLLATE "C"`, build.CatalogID)
	if err != nil {
		t.Fatalf("read catalog plan rows: %v", err)
	}
	var gotPlanRows []story32ExpectedPlan
	planIDs := make(map[string]int64)
	for planRows.Next() {
		var row story32ExpectedPlan
		var id int64
		if err := planRows.Scan(&row.Name, &row.Issuer, &row.IDType, &row.ID, &row.Market, &row.Search); err != nil {
			planRows.Close()
			t.Fatalf("scan catalog plan row: %v", err)
		}
		gotPlanRows = append(gotPlanRows, row)
		if err := pool.QueryRow(ctx, `
SELECT id FROM mrfweb.release_plans
WHERE catalog_id = $1 AND plan_name = $2 AND issuer_name = $3
  AND plan_id_type = $4 AND plan_id = $5 AND plan_market_type = $6`,
			build.CatalogID, row.Name, row.Issuer, row.IDType, row.ID, row.Market).Scan(&id); err != nil {
			planRows.Close()
			t.Fatalf("resolve catalog plan id: %v", err)
		}
		planIDs[row.ID] = id
	}
	if err := planRows.Err(); err != nil {
		planRows.Close()
		t.Fatalf("read catalog plan rows: %v", err)
	}
	planRows.Close()
	if !reflect.DeepEqual(gotPlanRows, story32ExpectedA.Plans) {
		t.Fatalf("catalog plan rows=%+v want=%+v", gotPlanRows, story32ExpectedA.Plans)
	}

	type planOutputRow struct {
		PlanID int64
		Output string
	}
	outputRows, err := pool.Query(ctx, `
SELECT plan_id, output_id FROM mrfweb.release_plan_outputs
WHERE catalog_id = $1 ORDER BY plan_id, output_id COLLATE "C"`, build.CatalogID)
	if err != nil {
		t.Fatalf("read plan-output rows: %v", err)
	}
	var gotOutputRows []planOutputRow
	for outputRows.Next() {
		var row planOutputRow
		if err := outputRows.Scan(&row.PlanID, &row.Output); err != nil {
			outputRows.Close()
			t.Fatalf("scan plan-output row: %v", err)
		}
		gotOutputRows = append(gotOutputRows, row)
	}
	if err := outputRows.Err(); err != nil {
		outputRows.Close()
		t.Fatalf("read plan-output rows: %v", err)
	}
	outputRows.Close()
	if len(gotOutputRows) != 3 || gotOutputRows[0].Output != target.OutputID || gotOutputRows[1].Output != target.OutputID || gotOutputRows[2].Output != target.OutputID {
		t.Fatalf("plan-output rows=%+v", gotOutputRows)
	}
	for _, plan := range story32ExpectedA.Plans {
		if planIDs[plan.ID] == 0 {
			t.Fatalf("missing catalog plan %s", plan.ID)
		}
	}

	networkRows, err := pool.Query(ctx, `
SELECT network_name, observation_count
FROM mrfweb.release_output_code_networks WHERE catalog_id = $1
ORDER BY network_name COLLATE "C"`, build.CatalogID)
	if err != nil {
		t.Fatalf("read network rows: %v", err)
	}
	var gotNetworks []story32ExpectedNetwork
	for networkRows.Next() {
		var row story32ExpectedNetwork
		if err := networkRows.Scan(&row.Name, &row.Observations); err != nil {
			networkRows.Close()
			t.Fatalf("scan network row: %v", err)
		}
		gotNetworks = append(gotNetworks, row)
	}
	if err := networkRows.Err(); err != nil {
		networkRows.Close()
		t.Fatalf("read network rows: %v", err)
	}
	networkRows.Close()
	if !reflect.DeepEqual(gotNetworks, story32ExpectedA.CodeNetworks) {
		t.Fatalf("network rows=%+v want=%+v", gotNetworks, story32ExpectedA.CodeNetworks)
	}

	providerRows, err := pool.Query(ctx, `
SELECT filter_kind, parent_value, filter_value, provider_count
FROM mrfweb.release_provider_filter_values WHERE catalog_id = $1
ORDER BY filter_kind COLLATE "C", parent_value COLLATE "C", filter_value COLLATE "C"`, build.CatalogID)
	if err != nil {
		t.Fatalf("read provider-filter rows: %v", err)
	}
	var gotProviders []story32ExpectedProviderFilter
	for providerRows.Next() {
		var row story32ExpectedProviderFilter
		if err := providerRows.Scan(&row.Kind, &row.Parent, &row.Value, &row.Providers); err != nil {
			providerRows.Close()
			t.Fatalf("scan provider-filter row: %v", err)
		}
		gotProviders = append(gotProviders, row)
	}
	if err := providerRows.Err(); err != nil {
		providerRows.Close()
		t.Fatalf("read provider-filter rows: %v", err)
	}
	providerRows.Close()
	if !reflect.DeepEqual(gotProviders, story32ExpectedA.ProviderFilterValues) {
		t.Fatalf("provider-filter rows=%+v want=%+v", gotProviders, story32ExpectedA.ProviderFilterValues)
	}
	if afterBuild != story32WarehouseDigest(t, warehouse) {
		t.Fatal("warehouse changed while reading catalog")
	}
	activation, err := release.Activate(ctx, pool, payer, month, func(targets []release.Target, schema int64, providerReleaseMonth time.Time) error {
		if schema != 1 || !providerReleaseMonth.Equal(providerCatalogMonth) {
			return fmt.Errorf("provider catalog identity mismatch")
		}
		return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
	})
	if err != nil {
		t.Fatalf("activate release: %v", err)
	}
	if activation.PayerID != payer || activation.CollectionMonth != "2026-08" || activation.FilterCatalogID != build.CatalogID {
		t.Fatalf("activation=%+v", activation)
	}
	if final := story32WarehouseDigest(t, warehouse); final != afterBuild {
		t.Fatalf("activation mutated warehouse: before=%s after=%s", afterBuild, final)
	}

	if err := pool.QueryRow(ctx, `
SELECT status FROM mrfpipeline.monthly_releases
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&status); err != nil {
		t.Fatalf("read activated release: %v", err)
	}
	if status != "active" {
		t.Fatalf("release status=%s", status)
	}
	if err := pool.QueryRow(ctx, `
SELECT status FROM mrfweb.release_catalogs WHERE id = $1`, build.CatalogID).Scan(&status); err != nil {
		t.Fatalf("read published catalog: %v", err)
	}
	if status != "published" {
		t.Fatalf("catalog status=%s", status)
	}
	if err := pool.QueryRow(ctx, `
SELECT published_generation FROM mrfpipeline.monthly_release_outputs
WHERE payer_id = $1 AND collection_month = $2 AND mrf_snapshot_id = $3`,
		payer, month, snapshotID).Scan(&generation); err != nil {
		t.Fatalf("read published release output: %v", err)
	}
	if generation != 1 {
		t.Fatalf("published output generation=%d", generation)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_catalogs`).Scan(&activeCatalogs); err != nil {
		t.Fatalf("count active catalogs after activation: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_outputs`).Scan(&activeOutputs); err != nil {
		t.Fatalf("count active outputs after activation: %v", err)
	}
	if activeCatalogs != 1 || activeOutputs != 1 {
		t.Fatalf("active views after activation = %d/%d", activeCatalogs, activeOutputs)
	}
	var activePayer, activeMonth, activeOutput string
	if err := pool.QueryRow(ctx, `
SELECT payer_id, collection_month::text, output_id
FROM mrfweb.active_release_outputs`).Scan(&activePayer, &activeMonth, &activeOutput); err != nil {
		t.Fatalf("read active output: %v", err)
	}
	if activePayer != payer || activeMonth != "2026-08-01" || activeOutput != target.OutputID {
		t.Fatalf("unexpected active output=%s/%s/%s", activePayer, activeMonth, activeOutput)
	}
	var activeCatalogID int64
	var activeCatalogPayer, activeCatalogMonth string
	var activeCatalogGeneration int64
	if err := pool.QueryRow(ctx, `
SELECT catalog_id, payer_id, collection_month::text, publication_generation
FROM mrfweb.active_release_catalogs`).Scan(
		&activeCatalogID, &activeCatalogPayer, &activeCatalogMonth, &activeCatalogGeneration,
	); err != nil {
		t.Fatalf("read active catalog: %v", err)
	}
	if activeCatalogID != build.CatalogID || activeCatalogPayer != payer ||
		activeCatalogMonth != "2026-08-01" || activeCatalogGeneration != 1 {
		t.Fatalf("unexpected active catalog=%d/%s/%s/%d", activeCatalogID, activeCatalogPayer, activeCatalogMonth, activeCatalogGeneration)
	}
	catalog1State := story32DatabaseState(t, pool, payer, month, build.CatalogID)
	catalog1OnlyBefore := story32CatalogOnlyState(t, pool, build.CatalogID)
	sourceB, snapshotB := seedStory32Snapshot(t, pool, payer, month, "b")
	targetB, batchB := publishStory32Output(t, pool, ws, providerCatalog, warehouse, services, sourceB, snapshotB, payer, month, story32MRFB(), story32PlansB())
	if targetB.SnapshotID != snapshotB || batchB <= 0 {
		t.Fatalf("target B=%+v batch=%d", targetB, batchB)
	}
	beforeMissing := story32DatabaseState(t, pool, payer, month, build.CatalogID)
	_, err = release.Activate(ctx, pool, payer, month, func(targets []release.Target, schema int64, providerReleaseMonth time.Time) error {
		if schema != 1 || !providerReleaseMonth.Equal(providerCatalogMonth) {
			return fmt.Errorf("provider catalog identity mismatch")
		}
		return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
	})
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogMissing) {
		t.Fatalf("pre-build B activation error=%v, want filter_catalog_missing", err)
	}
	if afterMissing := story32DatabaseState(t, pool, payer, month, build.CatalogID); afterMissing != beforeMissing {
		t.Fatalf("missing-catalog activation mutated state")
	}

	build2, err := filtercatalog.Build(ctx, filtercatalog.BuildParams{
		Pool: pool, PayerID: payer, CollectionMonth: month,
		WarehousePath: warehouse, ProviderCatalogPath: providerCatalog,
		Preflight: func(targets []release.Target) error {
			return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
		},
	})
	if err != nil {
		t.Fatalf("build combined filter catalog: %v", err)
	}
	if build2.CatalogStatus != "ready" || build2.PublicationGeneration != 2 ||
		build2.OutputCount != 2 || build2.StandardFactCount != story32ExpectedAB.StandardFacts ||
		build2.BillingCodeCount != int64(len(story32ExpectedAB.BillingCodes)) ||
		build2.CodeFilterValueCount != int64(len(story32ExpectedAB.CodeFilterValues)) ||
		build2.PlanCount != int64(len(story32ExpectedAB.Plans)) || build2.PlanOutputCount != 5 ||
		build2.OutputCodeNetworkCount != int64(len(story32ExpectedAB.CodeNetworks)) ||
		build2.ProviderFilterValueCount != int64(len(story32ExpectedAB.ProviderFilterValues)) {
		t.Fatalf("unexpected combined build result: %+v", build2)
	}
	if catalog1State != story32DatabaseState(t, pool, payer, month, build.CatalogID) {
		t.Fatalf("generation 2 build changed catalog 1 or release state")
	}
	story32AssertCombinedCatalog(t, pool, build2.CatalogID, target, targetB, story32ExpectedAB)

	oldPath := os.Getenv("PATH")
	t.Setenv("PATH", t.TempDir())
	repeated, err := filtercatalog.Build(ctx, filtercatalog.BuildParams{
		Pool: pool, PayerID: payer, CollectionMonth: month,
		WarehousePath: warehouse, ProviderCatalogPath: providerCatalog,
	})
	t.Setenv("PATH", oldPath)
	if err != nil {
		t.Fatalf("repeat combined build without DuckDB: %v", err)
	}
	if !repeated.Unchanged || repeated.CatalogID != build2.CatalogID ||
		repeated.OutputCount != build2.OutputCount || repeated.StandardFactCount != build2.StandardFactCount ||
		repeated.CodeFilterValueCount != build2.CodeFilterValueCount || repeated.PlanCount != build2.PlanCount ||
		repeated.PlanOutputCount != build2.PlanOutputCount || repeated.OutputCodeNetworkCount != build2.OutputCodeNetworkCount ||
		repeated.ProviderFilterValueCount != build2.ProviderFilterValueCount {
		t.Fatalf("unexpected repeated build result: %+v", repeated)
	}

	activation2, err := release.Activate(ctx, pool, payer, month, func(targets []release.Target, schema int64, providerReleaseMonth time.Time) error {
		if schema != 1 || !providerReleaseMonth.Equal(providerCatalogMonth) {
			return fmt.Errorf("provider catalog identity mismatch")
		}
		return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
	})
	if err != nil {
		t.Fatalf("activate combined release: %v", err)
	}
	if activation2.FilterCatalogID != build2.CatalogID || activation2.PublicationGeneration != 2 || activation2.AddedOutputCount != 1 {
		t.Fatalf("combined activation=%+v", activation2)
	}
	if catalog1StateAfter := story32CatalogOnlyState(t, pool, build.CatalogID); catalog1StateAfter != catalog1OnlyBefore {
		t.Fatal("catalog 1 changed after publication")
	}
	var activeCount int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_catalogs`).Scan(&activeCount); err != nil {
		t.Fatalf("count combined active catalogs: %v", err)
	}
	if activeCount != 1 {
		t.Fatalf("combined active catalogs=%d", activeCount)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_outputs`).Scan(&activeCount); err != nil {
		t.Fatalf("count combined active outputs: %v", err)
	}
	if activeCount != 2 {
		t.Fatalf("combined active outputs=%d", activeCount)
	}
	beforeRepeatActivate := story32DatabaseState(t, pool, payer, month, build2.CatalogID)
	repeatedActivation, err := release.Activate(ctx, pool, payer, month, func(targets []release.Target, schema int64, providerReleaseMonth time.Time) error {
		if schema != 1 || !providerReleaseMonth.Equal(providerCatalogMonth) {
			return fmt.Errorf("provider catalog identity mismatch")
		}
		return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
	})
	if err != nil {
		t.Fatalf("repeat activation: %v", err)
	}
	if repeatedActivation.FilterCatalogID != build2.CatalogID || repeatedActivation.PublicationGeneration != 2 ||
		repeatedActivation.AddedOutputCount != 0 || repeatedActivation.OutputCount != 2 {
		t.Fatalf("repeat activation=%+v", repeatedActivation)
	}
	if afterRepeatActivate := story32DatabaseState(t, pool, payer, month, build2.CatalogID); afterRepeatActivate != beforeRepeatActivate {
		t.Fatal("repeat activation changed release/catalog state")
	}
	excludedOutput := publishStory32ExcludedC(t, ws, providerCatalog, warehouse, services, story32MRFC())
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.release_outputs WHERE output_id = $1`, excludedOutput).Scan(&activeCount); err != nil {
		t.Fatalf("check excluded output catalog rows: %v", err)
	}
	if activeCount != 0 {
		t.Fatalf("excluded output entered catalog: %d", activeCount)
	}
	candidateA, err := release.SelectCatalogCandidate(ctx, pool, payer, month)
	if err != nil {
		t.Fatalf("select August candidate: %v", err)
	}
	for _, candidateTarget := range candidateA.Targets {
		if candidateTarget.OutputID == excludedOutput {
			t.Fatalf("excluded output entered candidate")
		}
	}

	monthD := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	sourceD, snapshotD := seedStory32Snapshot(t, pool, payer, monthD, "d")
	targetD, batchD := publishStory32Output(t, pool, ws, providerCatalog, warehouse, services, sourceD, snapshotD, payer, monthD, story32MRFD(), []story32Plan{
		{PlanName: "D-only Plan", IssuerName: "Story32 Issuer", PlanIDType: "hios", PlanID: "D-ONLY-1", PlanMarketType: "group"},
	})
	if targetD.SnapshotID != snapshotD || batchD <= 0 {
		t.Fatalf("target D=%+v batch=%d", targetD, batchD)
	}
	buildD, err := filtercatalog.Build(ctx, filtercatalog.BuildParams{
		Pool: pool, PayerID: payer, CollectionMonth: monthD,
		WarehousePath: warehouse, ProviderCatalogPath: providerCatalog,
		Preflight: func(targets []release.Target) error {
			return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, monthD, targets)
		},
	})
	if err != nil {
		t.Fatalf("build D catalog: %v", err)
	}
	if buildD.CatalogStatus != "ready" || buildD.PublicationGeneration != 1 || buildD.OutputCount != 1 ||
		buildD.StandardFactCount != story32ExpectedD.StandardFacts || buildD.BillingCodeCount != 1 ||
		buildD.CodeFilterValueCount != int64(len(story32ExpectedD.CodeFilterValues)) ||
		buildD.PlanCount != 1 || buildD.PlanOutputCount != 1 ||
		buildD.OutputCodeNetworkCount != 1 || buildD.ProviderFilterValueCount != int64(len(story32ExpectedD.ProviderFilterValues)) {
		t.Fatalf("unexpected D build result: %+v", buildD)
	}
	story32AssertSingleCatalog(t, pool, buildD.CatalogID, targetD, story32ExpectedD)
	catalog2OnlyBeforeDActivation := story32CatalogOnlyState(t, pool, build2.CatalogID)
	activationD, err := release.Activate(ctx, pool, payer, monthD, func(targets []release.Target, schema int64, providerReleaseMonth time.Time) error {
		if schema != 1 || !providerReleaseMonth.Equal(providerCatalogMonth) {
			return fmt.Errorf("provider catalog identity mismatch")
		}
		return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, monthD, targets)
	})
	if err != nil {
		t.Fatalf("activate D release: %v", err)
	}
	if activationD.FilterCatalogID != buildD.CatalogID || activationD.PublicationGeneration != 1 || activationD.AddedOutputCount != 1 {
		t.Fatalf("D activation=%+v", activationD)
	}
	if status, err := release.Readiness(ctx, pool, payer, monthD); err != nil || status.Status != release.Active {
		t.Fatalf("D release status=%+v err=%v", status, err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM mrfweb.active_release_outputs
WHERE payer_id = $1 AND collection_month = $2 AND output_id = $3`, payer, monthD, targetD.OutputID).Scan(&activeCount); err != nil {
		t.Fatalf("check active D output: %v", err)
	}
	if activeCount != 1 {
		t.Fatalf("D active output count=%d", activeCount)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM mrfweb.active_release_outputs
WHERE NOT (payer_id = $1 AND collection_month = $2 AND output_id = $3)`, payer, monthD, targetD.OutputID).Scan(&activeCount); err != nil {
		t.Fatalf("check active views only D: %v", err)
	}
	if activeCount != 0 {
		t.Fatalf("other active outputs while D active: %d", activeCount)
	}
	if catalog2OnlyBeforeDActivation != story32CatalogOnlyState(t, pool, build2.CatalogID) {
		t.Fatal("catalog 2 changed while publishing D")
	}

	activationA2, err := release.Activate(ctx, pool, payer, month, func(targets []release.Target, schema int64, providerReleaseMonth time.Time) error {
		if schema != 1 || !providerReleaseMonth.Equal(providerCatalogMonth) {
			return fmt.Errorf("provider catalog identity mismatch")
		}
		return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
	})
	if err != nil {
		t.Fatalf("reactivate August release: %v", err)
	}
	if activationA2.FilterCatalogID != build2.CatalogID || activationA2.PublicationGeneration != 2 || activationA2.AddedOutputCount != 0 {
		t.Fatalf("August reactivation=%+v", activationA2)
	}
	if status, err := release.Readiness(ctx, pool, payer, monthD); err != nil || status.Status != release.Inactive {
		t.Fatalf("September release after August reactivation=%+v err=%v", status, err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM mrfweb.active_release_outputs
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&activeCount); err != nil {
		t.Fatalf("check active August outputs: %v", err)
	}
	if activeCount != 2 {
		t.Fatalf("active August outputs=%d", activeCount)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM mrfweb.active_release_outputs
WHERE payer_id = $1 AND collection_month = $2`, payer, monthD).Scan(&activeCount); err != nil {
		t.Fatalf("check inactive September outputs: %v", err)
	}
	if activeCount != 0 {
		t.Fatalf("inactive September outputs=%d", activeCount)
	}
	if catalog2OnlyBeforeDActivation != story32CatalogOnlyState(t, pool, build2.CatalogID) {
		t.Fatal("catalog 2 changed while reactivating August")
	}

	payerB := "payer-b"
	monthE := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	sourceE, snapshotE := seedStory32Snapshot(t, pool, payerB, monthE, "e")
	targetE, batchE := publishStory32Output(t, pool, ws, providerCatalog, warehouse, services, sourceE, snapshotE, payerB, monthE, story32MRFE(), []story32Plan{
		{PlanName: "E-only Plan", IssuerName: "Story32 Issuer", PlanIDType: "hios", PlanID: "E-ONLY-1", PlanMarketType: "group"},
	})
	if targetE.SnapshotID != snapshotE || batchE <= 0 {
		t.Fatalf("target E=%+v batch=%d", targetE, batchE)
	}
	buildE, err := filtercatalog.Build(ctx, filtercatalog.BuildParams{
		Pool: pool, PayerID: payerB, CollectionMonth: monthE,
		WarehousePath: warehouse, ProviderCatalogPath: providerCatalog,
		Preflight: func(targets []release.Target) error {
			return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payerB, monthE, targets)
		},
	})
	if err != nil {
		t.Fatalf("build E catalog: %v", err)
	}
	if buildE.CatalogStatus != "ready" || buildE.PublicationGeneration != 1 || buildE.OutputCount != 1 ||
		buildE.StandardFactCount != story32ExpectedE.StandardFacts || buildE.BillingCodeCount != 1 ||
		buildE.CodeFilterValueCount != int64(len(story32ExpectedE.CodeFilterValues)) ||
		buildE.PlanCount != 1 || buildE.PlanOutputCount != 1 ||
		buildE.OutputCodeNetworkCount != 1 || buildE.ProviderFilterValueCount != int64(len(story32ExpectedE.ProviderFilterValues)) {
		t.Fatalf("unexpected E build result: %+v", buildE)
	}
	story32AssertSingleCatalog(t, pool, buildE.CatalogID, targetE, story32ExpectedE)
	activationE, err := release.Activate(ctx, pool, payerB, monthE, func(targets []release.Target, schema int64, providerReleaseMonth time.Time) error {
		if schema != 1 || !providerReleaseMonth.Equal(providerCatalogMonth) {
			return fmt.Errorf("provider catalog identity mismatch")
		}
		return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payerB, monthE, targets)
	})
	if err != nil {
		t.Fatalf("activate E release: %v", err)
	}
	if activationE.FilterCatalogID != buildE.CatalogID || activationE.PublicationGeneration != 1 || activationE.AddedOutputCount != 1 {
		t.Fatalf("E activation=%+v", activationE)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_catalogs`).Scan(&activeCount); err != nil {
		t.Fatalf("count union active catalogs: %v", err)
	}
	if activeCount != 2 {
		t.Fatalf("union active catalogs=%d", activeCount)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfweb.active_release_outputs`).Scan(&activeCount); err != nil {
		t.Fatalf("count union active outputs: %v", err)
	}
	if activeCount != 3 {
		t.Fatalf("union active outputs=%d", activeCount)
	}
	for _, want := range []release.Target{target, targetB, targetE} {
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM mrfweb.active_release_outputs
WHERE payer_id = $1 AND collection_month = $2::date AND output_id = $3`,
			want.PayerID, want.CollectionMonth+"-01", want.OutputID).Scan(&activeCount); err != nil {
			t.Fatalf("check active target %s: %v", want.OutputID, err)
		}
		if activeCount != 1 {
			t.Fatalf("active target %s count=%d", want.OutputID, activeCount)
		}
	}
	if catalog1OnlyBefore != story32CatalogOnlyState(t, pool, build.CatalogID) ||
		catalog2OnlyBeforeDActivation != story32CatalogOnlyState(t, pool, build2.CatalogID) {
		t.Fatal("older catalog changed after later publications")
	}
	for _, query := range []struct {
		name string
		sql  string
		args []any
	}{
		{"excluded output", `SELECT count(*) FROM mrfweb.release_outputs WHERE output_id = $1`, []any{excludedOutput}},
		{"excluded plan", `SELECT count(*) FROM mrfweb.release_plans WHERE plan_id = $1`, []any{"C-ONLY-1"}},
		{"excluded network", `SELECT count(*) FROM mrfweb.release_output_code_networks WHERE network_name = $1`, []any{"Excluded C Network"}},
	} {
		if err := pool.QueryRow(ctx, query.sql, query.args...).Scan(&activeCount); err != nil {
			t.Fatalf("check excluded %s: %v", query.name, err)
		}
		if activeCount != 0 {
			t.Fatalf("excluded %s entered catalog: %d", query.name, activeCount)
		}
	}
	story32AssertReadOnlyServing(t, pool, payer, month, target, targetB, build2.CatalogID, payerB, monthE, targetE, buildE.CatalogID)
}

func story32CatalogOnlyState(t *testing.T, pool *pgxpool.Pool, catalogID int64) string {
	t.Helper()
	ctx := context.Background()
	state := ""
	for _, table := range []string{
		"release_catalogs", "release_outputs", "release_billing_codes",
		"release_code_filter_values", "release_plans", "release_plan_outputs",
		"release_output_code_networks", "release_provider_filter_values",
	} {
		var tableState string
		keyColumn := "catalog_id"
		if table == "release_catalogs" {
			keyColumn = "id"
		}
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
SELECT COALESCE(json_agg(r ORDER BY row_to_json(r)::text)::text, '[]')
FROM (SELECT * FROM mrfweb.%s WHERE %s = $1) r`, table, keyColumn), catalogID).Scan(&tableState); err != nil {
			t.Fatalf("read %s state: %v", table, err)
		}
		state += table + "=" + tableState + "\n"
	}
	return state
}

func story32AssertCombinedCatalog(t *testing.T, pool *pgxpool.Pool, catalogID int64, targetA, targetB release.Target, want story32ExpectedCombined) {
	t.Helper()
	ctx := context.Background()
	var status string
	var generation, outputCount, standardFacts, schema, billingCodes, codeFilters, planCount, planOutputs, networks, providers int64
	var providerMonth time.Time
	if err := pool.QueryRow(ctx, `
SELECT status, publication_generation, output_count, standard_fact_count,
       provider_catalog_schema_version, provider_catalog_release_month,
       billing_code_count, code_filter_value_count, plan_count,
       plan_output_count, output_code_network_count, provider_filter_value_count
FROM mrfweb.release_catalogs WHERE id = $1`, catalogID).Scan(
		&status, &generation, &outputCount, &standardFacts, &schema, &providerMonth,
		&billingCodes, &codeFilters, &planCount, &planOutputs, &networks, &providers,
	); err != nil {
		t.Fatalf("read combined catalog header: %v", err)
	}
	if status != "ready" || generation != 2 || outputCount != 2 || standardFacts != want.StandardFacts ||
		schema != 1 || !providerMonth.Equal(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)) ||
		billingCodes != int64(len(want.BillingCodes)) || codeFilters != int64(len(want.CodeFilterValues)) ||
		planCount != int64(len(want.Plans)) || planOutputs != 5 ||
		networks != int64(len(want.CodeNetworks)) || providers != int64(len(want.ProviderFilterValues)) {
		t.Fatalf("unexpected combined header: %s/%d/%d/%d/%d/%s/%d/%d/%d/%d/%d/%d", status, generation, outputCount, standardFacts, schema, providerMonth, billingCodes, codeFilters, planCount, planOutputs, networks, providers)
	}

	outputRows, err := pool.Query(ctx, `
SELECT output_id FROM mrfweb.release_outputs
WHERE catalog_id = $1 ORDER BY output_id COLLATE "C"`, catalogID)
	if err != nil {
		t.Fatalf("read combined outputs: %v", err)
	}
	var outputs []string
	for outputRows.Next() {
		var output string
		if err := outputRows.Scan(&output); err != nil {
			outputRows.Close()
			t.Fatalf("scan combined output: %v", err)
		}
		outputs = append(outputs, output)
	}
	if err := outputRows.Err(); err != nil {
		outputRows.Close()
		t.Fatalf("read combined outputs: %v", err)
	}
	outputRows.Close()
	if !reflect.DeepEqual(outputs, []string{targetA.OutputID, targetB.OutputID}) {
		t.Fatalf("combined outputs=%v", outputs)
	}

	billingRows, err := pool.Query(ctx, `
SELECT billing_code_type, billing_code, billing_code_type_version,
       warehouse_service_name, warehouse_service_description,
       observation_count, unmodified_observation_count
FROM mrfweb.release_billing_codes WHERE catalog_id = $1
ORDER BY billing_code_type COLLATE "C", billing_code COLLATE "C"`, catalogID)
	if err != nil {
		t.Fatalf("read combined billing codes: %v", err)
	}
	var gotBilling []story32ExpectedBillingCode
	for billingRows.Next() {
		var row story32ExpectedBillingCode
		if err := billingRows.Scan(&row.Type, &row.Code, &row.Version, &row.Name, &row.Description, &row.Observations, &row.Unmodified); err != nil {
			billingRows.Close()
			t.Fatalf("scan combined billing code: %v", err)
		}
		gotBilling = append(gotBilling, row)
	}
	if err := billingRows.Err(); err != nil {
		billingRows.Close()
		t.Fatalf("read combined billing codes: %v", err)
	}
	billingRows.Close()
	if !reflect.DeepEqual(gotBilling, want.BillingCodes) {
		t.Fatalf("combined billing codes=%+v want=%+v", gotBilling, want.BillingCodes)
	}

	codeRows, err := pool.Query(ctx, `
SELECT billing_code_type, billing_code, filter_kind, filter_value, display_label, observation_count
FROM mrfweb.release_code_filter_values WHERE catalog_id = $1
ORDER BY billing_code_type COLLATE "C", billing_code COLLATE "C",
         filter_kind COLLATE "C", filter_value COLLATE "C"`, catalogID)
	if err != nil {
		t.Fatalf("read combined code filters: %v", err)
	}
	var gotCode []story32ExpectedCodeFilterRow
	for codeRows.Next() {
		var row story32ExpectedCodeFilterRow
		if err := codeRows.Scan(&row.Type, &row.Code, &row.Kind, &row.Value, &row.Label, &row.Observations); err != nil {
			codeRows.Close()
			t.Fatalf("scan combined code filter: %v", err)
		}
		gotCode = append(gotCode, row)
	}
	if err := codeRows.Err(); err != nil {
		codeRows.Close()
		t.Fatalf("read combined code filters: %v", err)
	}
	codeRows.Close()
	if !reflect.DeepEqual(gotCode, want.CodeFilterValues) {
		t.Fatalf("combined code filters=%+v want=%+v", gotCode, want.CodeFilterValues)
	}

	planRows, err := pool.Query(ctx, `
SELECT plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text
FROM mrfweb.release_plans WHERE catalog_id = $1
ORDER BY plan_name COLLATE "C", issuer_name COLLATE "C", plan_id COLLATE "C"`, catalogID)
	if err != nil {
		t.Fatalf("read combined plans: %v", err)
	}
	var gotPlans []story32ExpectedPlan
	for planRows.Next() {
		var row story32ExpectedPlan
		if err := planRows.Scan(&row.Name, &row.Issuer, &row.IDType, &row.ID, &row.Market, &row.Search); err != nil {
			planRows.Close()
			t.Fatalf("scan combined plan: %v", err)
		}
		gotPlans = append(gotPlans, row)
	}
	if err := planRows.Err(); err != nil {
		planRows.Close()
		t.Fatalf("read combined plans: %v", err)
	}
	planRows.Close()
	if !reflect.DeepEqual(gotPlans, want.Plans) {
		t.Fatalf("combined plans=%+v want=%+v", gotPlans, want.Plans)
	}

	planOutputRows, err := pool.Query(ctx, `
SELECT p.plan_name, p.plan_id, o.output_id
FROM mrfweb.release_plan_outputs o
JOIN mrfweb.release_plans p ON p.catalog_id = o.catalog_id AND p.id = o.plan_id
WHERE o.catalog_id = $1
ORDER BY p.plan_name COLLATE "C", o.output_id COLLATE "C"`, catalogID)
	if err != nil {
		t.Fatalf("read combined plan outputs: %v", err)
	}
	type expectedPlanOutput struct {
		Name, ID, Output string
	}
	var gotPlanOutputs []expectedPlanOutput
	for planOutputRows.Next() {
		var row expectedPlanOutput
		if err := planOutputRows.Scan(&row.Name, &row.ID, &row.Output); err != nil {
			planOutputRows.Close()
			t.Fatalf("scan combined plan output: %v", err)
		}
		gotPlanOutputs = append(gotPlanOutputs, row)
	}
	if err := planOutputRows.Err(); err != nil {
		planOutputRows.Close()
		t.Fatalf("read combined plan outputs: %v", err)
	}
	planOutputRows.Close()
	wantPlanOutputs := []expectedPlanOutput{
		{Name: "A-only Plan", ID: "A-ONLY-1", Output: targetA.OutputID},
		{Name: "Alt Shared Plan", ID: "SHARED-1", Output: targetA.OutputID},
		{Name: "B-only Plan", ID: "B-ONLY-1", Output: targetB.OutputID},
		{Name: "Shared Plan", ID: "SHARED-1", Output: targetA.OutputID},
		{Name: "Shared Plan", ID: "SHARED-1", Output: targetB.OutputID},
	}
	if !reflect.DeepEqual(gotPlanOutputs, wantPlanOutputs) {
		t.Fatalf("combined plan outputs=%+v want=%+v", gotPlanOutputs, wantPlanOutputs)
	}

	networkRows, err := pool.Query(ctx, `
SELECT output_id, billing_code_type, billing_code, network_name, observation_count
FROM mrfweb.release_output_code_networks WHERE catalog_id = $1
ORDER BY output_id COLLATE "C", billing_code_type COLLATE "C",
         billing_code COLLATE "C", network_name COLLATE "C"`, catalogID)
	if err != nil {
		t.Fatalf("read combined networks: %v", err)
	}
	var gotNetworks []story32ExpectedOutputNetwork
	for networkRows.Next() {
		var row story32ExpectedOutputNetwork
		if err := networkRows.Scan(&row.Output, &row.Type, &row.Code, &row.Name, &row.Observations); err != nil {
			networkRows.Close()
			t.Fatalf("scan combined network: %v", err)
		}
		gotNetworks = append(gotNetworks, row)
	}
	if err := networkRows.Err(); err != nil {
		networkRows.Close()
		t.Fatalf("read combined networks: %v", err)
	}
	networkRows.Close()
	wantNetworks := append([]story32ExpectedOutputNetwork(nil), want.CodeNetworks...)
	for i := range wantNetworks {
		switch wantNetworks[i].Output {
		case "A":
			wantNetworks[i].Output = targetA.OutputID
		case "B":
			wantNetworks[i].Output = targetB.OutputID
		}
	}
	if !reflect.DeepEqual(gotNetworks, wantNetworks) {
		t.Fatalf("combined networks=%+v want=%+v", gotNetworks, wantNetworks)
	}

	providerRows, err := pool.Query(ctx, `
SELECT filter_kind, parent_value, filter_value, provider_count
FROM mrfweb.release_provider_filter_values WHERE catalog_id = $1
ORDER BY filter_kind COLLATE "C", parent_value COLLATE "C", filter_value COLLATE "C"`, catalogID)
	if err != nil {
		t.Fatalf("read combined provider filters: %v", err)
	}
	var gotProviders []story32ExpectedProviderFilter
	for providerRows.Next() {
		var row story32ExpectedProviderFilter
		if err := providerRows.Scan(&row.Kind, &row.Parent, &row.Value, &row.Providers); err != nil {
			providerRows.Close()
			t.Fatalf("scan combined provider filter: %v", err)
		}
		gotProviders = append(gotProviders, row)
	}
	if err := providerRows.Err(); err != nil {
		providerRows.Close()
		t.Fatalf("read combined provider filters: %v", err)
	}
	providerRows.Close()
	if !reflect.DeepEqual(gotProviders, want.ProviderFilterValues) {
		t.Fatalf("combined provider filters=%+v want=%+v", gotProviders, want.ProviderFilterValues)
	}
}

func story32AssertSingleCatalog(t *testing.T, pool *pgxpool.Pool, catalogID int64, target release.Target, want story32ExpectedCatalog) {
	t.Helper()
	ctx := context.Background()
	var status string
	var generation, outputCount, standardFacts, schema, billingCodes, codeFilters, planCount, planOutputs, networks, providers int64
	var providerMonth time.Time
	if err := pool.QueryRow(ctx, `
SELECT status, publication_generation, output_count, standard_fact_count,
       provider_catalog_schema_version, provider_catalog_release_month,
       billing_code_count, code_filter_value_count, plan_count,
       plan_output_count, output_code_network_count, provider_filter_value_count
FROM mrfweb.release_catalogs WHERE id = $1`, catalogID).Scan(
		&status, &generation, &outputCount, &standardFacts, &schema, &providerMonth,
		&billingCodes, &codeFilters, &planCount, &planOutputs, &networks, &providers,
	); err != nil {
		t.Fatalf("read single catalog header: %v", err)
	}
	if status != "ready" || generation != 1 || outputCount != 1 || standardFacts != want.StandardFacts ||
		schema != 1 || !providerMonth.Equal(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)) ||
		billingCodes != 1 || codeFilters != int64(len(want.CodeFilterValues)) ||
		planCount != int64(len(want.Plans)) || planOutputs != int64(len(want.Plans)) ||
		networks != int64(len(want.CodeNetworks)) || providers != int64(len(want.ProviderFilterValues)) {
		t.Fatalf("unexpected single catalog header: %s/%d/%d/%d/%d/%s/%d/%d/%d/%d/%d/%d", status, generation, outputCount, standardFacts, schema, providerMonth, billingCodes, codeFilters, planCount, planOutputs, networks, providers)
	}
	var outputSnapshot int64
	var outputID string
	if err := pool.QueryRow(ctx, `
SELECT mrf_snapshot_id, output_id
FROM mrfweb.release_outputs WHERE catalog_id = $1`, catalogID).Scan(
		&outputSnapshot, &outputID,
	); err != nil {
		t.Fatalf("read single catalog output: %v", err)
	}
	if outputSnapshot != target.SnapshotID || outputID != target.OutputID {
		t.Fatalf("unexpected single catalog output: %d/%s", outputSnapshot, outputID)
	}
	var billing story32ExpectedBillingCode
	if err := pool.QueryRow(ctx, `
SELECT billing_code_type, billing_code, billing_code_type_version,
       warehouse_service_name, warehouse_service_description,
       observation_count, unmodified_observation_count
FROM mrfweb.release_billing_codes WHERE catalog_id = $1`, catalogID).Scan(
		&billing.Type, &billing.Code, &billing.Version, &billing.Name, &billing.Description,
		&billing.Observations, &billing.Unmodified,
	); err != nil {
		t.Fatalf("read single billing code: %v", err)
	}
	if !reflect.DeepEqual(billing, want.BillingCode) {
		t.Fatalf("single billing code=%+v want=%+v", billing, want.BillingCode)
	}
	codeRows, err := pool.Query(ctx, `
SELECT filter_kind, filter_value, display_label, observation_count
FROM mrfweb.release_code_filter_values WHERE catalog_id = $1
ORDER BY filter_kind COLLATE "C", filter_value COLLATE "C"`, catalogID)
	if err != nil {
		t.Fatalf("read single code filters: %v", err)
	}
	var gotCode []story32ExpectedCodeFilter
	for codeRows.Next() {
		var row story32ExpectedCodeFilter
		if err := codeRows.Scan(&row.Kind, &row.Value, &row.Label, &row.Observations); err != nil {
			codeRows.Close()
			t.Fatalf("scan single code filter: %v", err)
		}
		gotCode = append(gotCode, row)
	}
	if err := codeRows.Err(); err != nil {
		codeRows.Close()
		t.Fatalf("read single code filters: %v", err)
	}
	codeRows.Close()
	if !reflect.DeepEqual(gotCode, want.CodeFilterValues) {
		t.Fatalf("single code filters=%+v want=%+v", gotCode, want.CodeFilterValues)
	}
	planRows, err := pool.Query(ctx, `
SELECT plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text
FROM mrfweb.release_plans WHERE catalog_id = $1
ORDER BY plan_name COLLATE "C", issuer_name COLLATE "C", plan_id COLLATE "C"`, catalogID)
	if err != nil {
		t.Fatalf("read single plans: %v", err)
	}
	var gotPlans []story32ExpectedPlan
	for planRows.Next() {
		var row story32ExpectedPlan
		if err := planRows.Scan(&row.Name, &row.Issuer, &row.IDType, &row.ID, &row.Market, &row.Search); err != nil {
			planRows.Close()
			t.Fatalf("scan single plan: %v", err)
		}
		gotPlans = append(gotPlans, row)
	}
	if err := planRows.Err(); err != nil {
		planRows.Close()
		t.Fatalf("read single plans: %v", err)
	}
	planRows.Close()
	if !reflect.DeepEqual(gotPlans, want.Plans) {
		t.Fatalf("single plans=%+v want=%+v", gotPlans, want.Plans)
	}
	planOutputsRows, err := pool.Query(ctx, `
SELECT p.plan_name, o.output_id
FROM mrfweb.release_plan_outputs o
JOIN mrfweb.release_plans p ON p.catalog_id = o.catalog_id AND p.id = o.plan_id
WHERE o.catalog_id = $1 ORDER BY p.plan_name COLLATE "C"`, catalogID)
	if err != nil {
		t.Fatalf("read single plan outputs: %v", err)
	}
	var gotPlanOutputs []string
	for planOutputsRows.Next() {
		var name, output string
		if err := planOutputsRows.Scan(&name, &output); err != nil {
			planOutputsRows.Close()
			t.Fatalf("scan single plan output: %v", err)
		}
		gotPlanOutputs = append(gotPlanOutputs, name+"="+output)
	}
	if err := planOutputsRows.Err(); err != nil {
		planOutputsRows.Close()
		t.Fatalf("read single plan outputs: %v", err)
	}
	planOutputsRows.Close()
	if len(gotPlanOutputs) != len(want.Plans) {
		t.Fatalf("single plan outputs=%v", gotPlanOutputs)
	}
	for _, plan := range want.Plans {
		found := false
		for _, row := range gotPlanOutputs {
			if row == plan.Name+"="+target.OutputID {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing single plan output for %s", plan.Name)
		}
	}
	networkRows, err := pool.Query(ctx, `
SELECT network_name, observation_count
FROM mrfweb.release_output_code_networks WHERE catalog_id = $1
ORDER BY network_name COLLATE "C"`, catalogID)
	if err != nil {
		t.Fatalf("read single networks: %v", err)
	}
	var gotNetworks []story32ExpectedNetwork
	for networkRows.Next() {
		var row story32ExpectedNetwork
		if err := networkRows.Scan(&row.Name, &row.Observations); err != nil {
			networkRows.Close()
			t.Fatalf("scan single network: %v", err)
		}
		gotNetworks = append(gotNetworks, row)
	}
	if err := networkRows.Err(); err != nil {
		networkRows.Close()
		t.Fatalf("read single networks: %v", err)
	}
	networkRows.Close()
	if !reflect.DeepEqual(gotNetworks, want.CodeNetworks) {
		t.Fatalf("single networks=%+v want=%+v", gotNetworks, want.CodeNetworks)
	}
	providerRows, err := pool.Query(ctx, `
SELECT filter_kind, parent_value, filter_value, provider_count
FROM mrfweb.release_provider_filter_values WHERE catalog_id = $1
ORDER BY filter_kind COLLATE "C", parent_value COLLATE "C", filter_value COLLATE "C"`, catalogID)
	if err != nil {
		t.Fatalf("read single provider filters: %v", err)
	}
	var gotProviders []story32ExpectedProviderFilter
	for providerRows.Next() {
		var row story32ExpectedProviderFilter
		if err := providerRows.Scan(&row.Kind, &row.Parent, &row.Value, &row.Providers); err != nil {
			providerRows.Close()
			t.Fatalf("scan single provider filter: %v", err)
		}
		gotProviders = append(gotProviders, row)
	}
	if err := providerRows.Err(); err != nil {
		providerRows.Close()
		t.Fatalf("read single provider filters: %v", err)
	}
	providerRows.Close()

	if !reflect.DeepEqual(gotProviders, want.ProviderFilterValues) {
		t.Fatalf("single provider filters=%+v want=%+v", gotProviders, want.ProviderFilterValues)
	}
}
func story32QuoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func story32QuoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func story32EscapeLikePrefix(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `%`, `\%`)
	return strings.ReplaceAll(value, `_`, `\_`)
}

func story32ReadOnlyPool(t *testing.T, owner *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	config := owner.Config()
	if config == nil || config.ConnConfig == nil || config.ConnConfig.Database == "" {
		t.Fatal("owner pool missing database config")
	}
	role := fmt.Sprintf("story32_reader_%d", time.Now().UnixNano())
	password := fmt.Sprintf("story32_password_%d", time.Now().UnixNano())
	roleSQL := story32QuoteIdentifier(role)
	if _, err := owner.Exec(context.Background(), "CREATE ROLE "+roleSQL+" LOGIN PASSWORD "+story32QuoteLiteral(password)+" NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT"); err != nil {
		t.Fatalf("create reader role: %v", err)
	}
	databaseSQL := story32QuoteIdentifier(config.ConnConfig.Database)
	if _, err := owner.Exec(context.Background(), "GRANT CONNECT ON DATABASE "+databaseSQL+" TO "+roleSQL); err != nil {
		t.Fatalf("grant reader connect: %v", err)
	}
	if _, err := owner.Exec(context.Background(), "GRANT USAGE ON SCHEMA mrfweb TO "+roleSQL); err != nil {
		t.Fatalf("grant reader schema usage: %v", err)
	}
	for _, object := range []string{
		"active_release_catalogs", "active_release_outputs",
		"release_billing_codes", "release_code_filter_values",
		"release_plans", "release_plan_outputs",
		"release_output_code_networks", "release_provider_filter_values",
	} {
		if _, err := owner.Exec(context.Background(), "GRANT SELECT ON mrfweb."+story32QuoteIdentifier(object)+" TO "+roleSQL); err != nil {
			t.Fatalf("grant reader select on %s: %v", object, err)
		}
	}
	config.ConnConfig.User = role
	config.ConnConfig.Password = password
	config.MaxConns = 4
	reader, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("connect reader role: %v", err)
	}
	t.Cleanup(func() {
		reader.Close()
		if _, err := owner.Exec(context.Background(), "DROP OWNED BY "+roleSQL); err != nil {
			t.Errorf("drop reader role owned objects: %v", err)
		}
		if _, err := owner.Exec(context.Background(), "DROP ROLE IF EXISTS "+roleSQL); err != nil {
			t.Errorf("drop reader role: %v", err)
		}
	})
	return reader
}

func story32RequirePermissionDenied(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err == nil {
		t.Fatalf("statement unexpectedly succeeded: %s", sql)
	} else {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Fatalf("statement error=%v, want permission denied", err)
		}
	}
}

func story32AssertReadOnlyServing(t *testing.T, owner *pgxpool.Pool, payerA string, monthA time.Time, targetA, targetB release.Target, catalogA int64, payerB string, monthB time.Time, targetE release.Target, catalogE int64) {
	t.Helper()
	reader := story32ReadOnlyPool(t, owner)
	ctx := context.Background()
	tx, err := reader.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatalf("begin reader transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	type activeCatalog struct {
		ID            int64
		Payer         string
		Month         time.Time
		Generation    int64
		Schema        int64
		ProviderMonth time.Time
	}
	activeRows, err := tx.Query(ctx, `
SELECT catalog_id, payer_id, collection_month, publication_generation,
       provider_catalog_schema_version, provider_catalog_release_month
FROM mrfweb.active_release_catalogs
WHERE ($1::text IS NULL OR payer_id = $1)
ORDER BY payer_id COLLATE "C"`, nil)
	if err != nil {
		t.Fatalf("read active catalogs: %v", err)
	}
	var gotCatalogs []activeCatalog
	for activeRows.Next() {
		var row activeCatalog
		if err := activeRows.Scan(&row.ID, &row.Payer, &row.Month, &row.Generation, &row.Schema, &row.ProviderMonth); err != nil {
			activeRows.Close()
			t.Fatalf("scan active catalog: %v", err)
		}
		gotCatalogs = append(gotCatalogs, row)
	}
	if err := activeRows.Err(); err != nil {
		activeRows.Close()
		t.Fatalf("read active catalogs: %v", err)
	}
	activeRows.Close()
	if (catalogE == 0 && len(gotCatalogs) != 1) || (catalogE != 0 && len(gotCatalogs) != 2) {
		t.Fatalf("active catalogs=%+v", gotCatalogs)
	}
	if len(gotCatalogs) == 0 || gotCatalogs[0].ID != catalogA || gotCatalogs[0].Payer != payerA ||
		!gotCatalogs[0].Month.Equal(monthA) || gotCatalogs[0].Generation != 2 ||
		gotCatalogs[0].Schema != 1 || !gotCatalogs[0].ProviderMonth.Equal(monthA) {
		t.Fatalf("active primary catalog=%+v", gotCatalogs)
	}
	if catalogE != 0 && (gotCatalogs[1].ID != catalogE || gotCatalogs[1].Payer != payerB ||
		!gotCatalogs[1].Month.Equal(monthB) || gotCatalogs[1].Generation != 1 ||
		gotCatalogs[1].Schema != 1 || !gotCatalogs[1].ProviderMonth.Equal(monthA)) {
		t.Fatalf("active secondary catalog=%+v", gotCatalogs)
	}
	for _, catalog := range gotCatalogs {
		outputRows, err := tx.Query(ctx, `
SELECT output_id FROM mrfweb.active_release_outputs
WHERE catalog_id = $1
ORDER BY output_id COLLATE "C"`, catalog.ID)
		if err != nil {
			t.Fatalf("read active outputs: %v", err)
		}
		var outputs []string
		for outputRows.Next() {
			var output string
			if err := outputRows.Scan(&output); err != nil {
				outputRows.Close()
				t.Fatalf("scan active output: %v", err)
			}
			outputs = append(outputs, output)
		}
		if err := outputRows.Err(); err != nil {
			outputRows.Close()
			t.Fatalf("read active outputs: %v", err)
		}
		outputRows.Close()
		var want []string
		switch catalog.ID {
		case catalogA:
			want = []string{targetA.OutputID, targetB.OutputID}
		case catalogE:
			want = []string{targetE.OutputID}
		}
		if !reflect.DeepEqual(outputs, want) {
			t.Fatalf("active outputs for catalog %d=%v want=%v", catalog.ID, outputs, want)
		}
	}

	billingRows, err := tx.Query(ctx, `
SELECT billing_code_type, billing_code, billing_code_type_version,
       warehouse_service_name, warehouse_service_description,
       observation_count, unmodified_observation_count
FROM mrfweb.release_billing_codes
WHERE catalog_id = $1
  AND ($2::text IS NULL OR billing_code_type = $2)
  AND billing_code LIKE $3 || '%'
ORDER BY billing_code_type COLLATE "C", billing_code COLLATE "C"
LIMIT $4`, catalogA, nil, "9", 20)
	if err != nil {
		t.Fatalf("read serving billing codes: %v", err)
	}
	var billing []story32ExpectedBillingCode
	for billingRows.Next() {
		var row story32ExpectedBillingCode
		if err := billingRows.Scan(&row.Type, &row.Code, &row.Version, &row.Name, &row.Description, &row.Observations, &row.Unmodified); err != nil {
			billingRows.Close()
			t.Fatalf("scan serving billing code: %v", err)
		}
		billing = append(billing, row)
	}
	if err := billingRows.Err(); err != nil {
		billingRows.Close()
		t.Fatalf("read serving billing codes: %v", err)
	}
	billingRows.Close()
	if !reflect.DeepEqual(billing, []story32ExpectedBillingCode{story32ExpectedAB.BillingCodes[0]}) {
		t.Fatalf("serving billing=%+v", billing)
	}

	codeRows, err := tx.Query(ctx, `
SELECT filter_kind, filter_value, display_label, observation_count
FROM mrfweb.release_code_filter_values
WHERE catalog_id = $1
  AND billing_code_type = $2
  AND billing_code = $3
  AND filter_kind = $4
ORDER BY filter_value COLLATE "C"`, catalogA, "CPT", "99213", "place_of_service")
	if err != nil {
		t.Fatalf("read serving code options: %v", err)
	}
	var options []story32ExpectedCodeFilter
	for codeRows.Next() {
		var row story32ExpectedCodeFilter
		if err := codeRows.Scan(&row.Kind, &row.Value, &row.Label, &row.Observations); err != nil {
			codeRows.Close()
			t.Fatalf("scan serving code option: %v", err)
		}
		options = append(options, row)
	}
	if err := codeRows.Err(); err != nil {
		codeRows.Close()
		t.Fatalf("read serving code options: %v", err)
	}
	codeRows.Close()
	wantOptions := []story32ExpectedCodeFilter{
		{Kind: "place_of_service", Value: "11", Label: "11", Observations: 3},
		{Kind: "place_of_service", Value: "23", Label: "23", Observations: 1},
		{Kind: "place_of_service", Value: "24", Label: "24", Observations: 1},
		{Kind: "place_of_service", Value: "??", Label: "??", Observations: 1},
		{Kind: "place_of_service", Value: "CSTM-00", Label: "Broad or unspecified place of service", Observations: 1},
	}
	if !reflect.DeepEqual(options, wantOptions) {
		t.Fatalf("serving code options=%+v want=%+v", options, wantOptions)
	}

	planQuery := `
SELECT id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type,
       search_text
FROM mrfweb.release_plans
WHERE catalog_id = $1
  AND search_text COLLATE "C" LIKE ($2 || '%') ESCAPE E'\\'
  AND (search_text COLLATE "C", id) >
      ($3::text COLLATE "C", $4::bigint)
ORDER BY search_text COLLATE "C", id
LIMIT $5`
	planRows, err := tx.Query(ctx, planQuery, catalogA, story32EscapeLikePrefix(""), "", int64(0), 1)
	if err != nil {
		t.Fatalf("read serving first plan page: %v", err)
	}
	var firstID int64
	var first story32ExpectedPlan
	if !planRows.Next() || planRows.Scan(&firstID, &first.Name, &first.Issuer, &first.IDType, &first.ID, &first.Market, &first.Search) != nil {
		planRows.Close()
		t.Fatalf("read first plan page")
	}
	if err := planRows.Err(); err != nil {
		planRows.Close()
		t.Fatalf("read first plan page: %v", err)
	}
	planRows.Close()
	if first != story32ExpectedAB.Plans[0] {
		t.Fatalf("first plan=%+v want=%+v", first, story32ExpectedAB.Plans[0])
	}
	planRows, err = tx.Query(ctx, planQuery, catalogA, story32EscapeLikePrefix(""), first.Search, firstID, 1)
	if err != nil {
		t.Fatalf("read serving second plan page: %v", err)
	}
	var secondID int64
	var second story32ExpectedPlan
	if !planRows.Next() || planRows.Scan(&secondID, &second.Name, &second.Issuer, &second.IDType, &second.ID, &second.Market, &second.Search) != nil {
		planRows.Close()
		t.Fatalf("read second plan page")
	}
	planRows.Close()
	if second != story32ExpectedAB.Plans[1] {
		t.Fatalf("second plan=%+v want=%+v", second, story32ExpectedAB.Plans[1])
	}
	planRows, err = tx.Query(ctx, planQuery, catalogA, story32EscapeLikePrefix(`%_\`), "", int64(0), 10)
	if err != nil {
		t.Fatalf("read escaped serving plan prefix: %v", err)
	}
	if planRows.Next() {
		planRows.Close()
		t.Fatalf("escaped wildcard plan prefix matched rows")
	}
	planRows.Close()

	networkRows, err := tx.Query(ctx, `
SELECT network_name, SUM(observation_count)::bigint AS observation_count
FROM mrfweb.release_output_code_networks
WHERE catalog_id = $1
  AND billing_code_type = $2
  AND billing_code = $3
GROUP BY network_name
ORDER BY network_name COLLATE "C"`, catalogA, "CPT", "99213")
	if err != nil {
		t.Fatalf("read serving networks: %v", err)
	}
	var networks []story32ExpectedNetwork
	for networkRows.Next() {
		var row story32ExpectedNetwork
		if err := networkRows.Scan(&row.Name, &row.Observations); err != nil {
			networkRows.Close()
			t.Fatalf("scan serving network: %v", err)
		}
		networks = append(networks, row)
	}
	networkRows.Close()
	wantCombinedNetworks := []story32ExpectedNetwork{
		{Name: "A Network", Observations: 5},
		{Name: "B Network", Observations: 3},
		{Name: "Shared Network", Observations: 7},
	}
	if !reflect.DeepEqual(networks, wantCombinedNetworks) {
		t.Fatalf("serving networks=%+v want=%+v", networks, wantCombinedNetworks)
	}
	planIDs := map[string]int64{}
	idRows, err := tx.Query(ctx, `
SELECT id, plan_name, issuer_name, plan_id
FROM mrfweb.release_plans WHERE catalog_id = $1`, catalogA)
	if err != nil {
		t.Fatalf("read serving plan ids: %v", err)
	}
	for idRows.Next() {
		var id int64
		var name, issuer, planID string
		if err := idRows.Scan(&id, &name, &issuer, &planID); err != nil {
			idRows.Close()
			t.Fatalf("scan serving plan id: %v", err)
		}
		planIDs[name+"|"+issuer+"|"+planID] = id
	}
	if err := idRows.Err(); err != nil {
		idRows.Close()
		t.Fatalf("read serving plan ids: %v", err)
	}
	idRows.Close()
	aOnlyID := planIDs["A-only Plan|Story32 Issuer|A-ONLY-1"]
	altID := planIDs["Alt Shared Plan|Other Issuer|SHARED-1"]
	bOnlyID := planIDs["B-only Plan|Story32 Issuer|B-ONLY-1"]
	sharedID := planIDs["Shared Plan|Story32 Issuer|SHARED-1"]
	if aOnlyID == 0 || altID == 0 || bOnlyID == 0 || sharedID == 0 {
		t.Fatalf("serving plan ids=%v", planIDs)
	}

	selectedPlanNetworkSQL := `
WITH selected_outputs AS (
    SELECT DISTINCT output_id
    FROM mrfweb.release_plan_outputs
    WHERE catalog_id = $1
      AND plan_id = ANY($4::bigint[])
)
SELECT n.network_name,
       SUM(n.observation_count)::bigint AS observation_count
FROM selected_outputs AS selected
JOIN mrfweb.release_output_code_networks AS n
  ON n.catalog_id = $1 AND n.output_id = selected.output_id
WHERE n.billing_code_type = $2
  AND n.billing_code = $3
GROUP BY n.network_name
ORDER BY n.network_name COLLATE "C"`
	querySelectedNetworks := func(codeType, code string, ids []int64) []story32ExpectedNetwork {
		t.Helper()
		rows, err := tx.Query(ctx, selectedPlanNetworkSQL, catalogA, codeType, code, ids)
		if err != nil {
			t.Fatalf("read serving selected-plan networks: %v", err)
		}
		var got []story32ExpectedNetwork
		for rows.Next() {
			var row story32ExpectedNetwork
			if err := rows.Scan(&row.Name, &row.Observations); err != nil {
				rows.Close()
				t.Fatalf("scan serving selected-plan network: %v", err)
			}
			got = append(got, row)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatalf("read serving selected-plan networks: %v", err)
		}
		rows.Close()
		return got
	}
	wantANetworks := []story32ExpectedNetwork{
		{Name: "A Network", Observations: 5},
		{Name: "B Network", Observations: 3},
		{Name: "Shared Network", Observations: 6},
	}
	wantBOnlyCPT := []story32ExpectedNetwork{
		{Name: "Shared Network", Observations: 1},
	}
	if got := querySelectedNetworks("CPT", "99213", []int64{aOnlyID}); !reflect.DeepEqual(got, wantANetworks) {
		t.Fatalf("A-only networks=%+v want=%+v", got, wantANetworks)
	}
	if got := querySelectedNetworks("CPT", "99213", []int64{bOnlyID}); !reflect.DeepEqual(got, wantBOnlyCPT) {
		t.Fatalf("B-only CPT networks=%+v want=%+v", got, wantBOnlyCPT)
	}
	if got := querySelectedNetworks("CPT", "99213", []int64{aOnlyID, altID}); !reflect.DeepEqual(got, wantANetworks) {
		t.Fatalf("shared-output selected plans counted more than once: %+v", got)
	}
	if got := querySelectedNetworks("CPT", "99213", []int64{sharedID, bOnlyID}); !reflect.DeepEqual(got, wantCombinedNetworks) {
		t.Fatalf("union selected-plan networks=%+v want=%+v", got, wantCombinedNetworks)
	}
	if got := querySelectedNetworks("HCPCS", "G0463", []int64{bOnlyID}); !reflect.DeepEqual(got, []story32ExpectedNetwork{{Name: "B Network", Observations: 1}}) {
		t.Fatalf("B-only HCPCS networks=%+v", got)
	}
	if got := querySelectedNetworks("CPT", "99213", []int64{bOnlyID}); len(got) != 1 || got[0].Name == "B Network" {
		t.Fatalf("network present only for another code leaked into CPT lookup: %+v", got)
	}
	var crossProducts int64
	if err := tx.QueryRow(ctx, `
SELECT count(*)
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'mrfweb'
  AND c.relkind IN ('r', 'p', 'v', 'm')
  AND c.relname LIKE '%plan%network%'`).Scan(&crossProducts); err != nil {
		t.Fatalf("count plan/network cross-product relations: %v", err)
	}
	if crossProducts != 0 {
		t.Fatalf("materialized plan/network cross-product relations=%d", crossProducts)
	}

	providerCases := []struct {
		kind, parent string
		want         []story32ExpectedProviderFilter
	}{
		{"taxonomy", "", []story32ExpectedProviderFilter{{Kind: "taxonomy", Value: "2084P0800X", Providers: 2}, {Kind: "taxonomy", Value: "208D00000X", Providers: 1}}},
		{"state", "", []story32ExpectedProviderFilter{{Kind: "state", Value: "fl", Providers: 2}, {Kind: "state", Value: "ny", Providers: 1}}},
		{"city", "fl", []story32ExpectedProviderFilter{{Kind: "city", Parent: "fl", Value: "miami", Providers: 2}}},
	}
	for _, providerCase := range providerCases {
		providerRows, err := tx.Query(ctx, `
SELECT filter_value, provider_count
FROM mrfweb.release_provider_filter_values
WHERE catalog_id = $1
  AND filter_kind = $2
  AND parent_value = $3
ORDER BY filter_value COLLATE "C"`, catalogA, providerCase.kind, providerCase.parent)
		if err != nil {
			t.Fatalf("read serving %s providers: %v", providerCase.kind, err)
		}
		var got []story32ExpectedProviderFilter
		for providerRows.Next() {
			var row story32ExpectedProviderFilter
			row.Kind, row.Parent = providerCase.kind, providerCase.parent
			if err := providerRows.Scan(&row.Value, &row.Providers); err != nil {
				providerRows.Close()
				t.Fatalf("scan serving %s provider: %v", providerCase.kind, err)
			}
			got = append(got, row)
		}
		providerRows.Close()
		if !reflect.DeepEqual(got, providerCase.want) {
			t.Fatalf("serving %s providers=%+v want=%+v", providerCase.kind, got, providerCase.want)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit reader transaction: %v", err)
	}

	before := story32DatabaseState(t, owner, payerA, monthA, catalogA)
	for _, denied := range []string{
		"SELECT * FROM mrfweb.release_catalogs",
		"SELECT * FROM mrfweb.release_outputs",
		"SELECT * FROM mrfpipeline.monthly_releases",
		"SELECT * FROM mrfpipeline_river.river_job",
	} {
		story32RequirePermissionDenied(t, reader, denied)
	}
	for _, denied := range []string{
		"INSERT INTO mrfweb.release_plans (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text) VALUES (1, 'x', 'x', 'hios', 'x', 'group', 'x')",
		"UPDATE mrfweb.release_plans SET search_text = 'x'",
		"DELETE FROM mrfweb.release_plans",
		"CREATE TABLE mrfweb.story32_forbidden_ddl (id integer)",
	} {
		story32RequirePermissionDenied(t, reader, denied)
	}
	if after := story32DatabaseState(t, owner, payerA, monthA, catalogA); after != before {
		t.Fatal("permission-denied serving mutations changed state")
	}
}

func story32RequireDuckDB(t *testing.T) {
	t.Helper()
	if _, err := filtercatalog.ResolveDuckDB(context.Background()); err != nil {
		if os.Getenv("MRFPIPELINE_REQUIRE_DUCKDB_ACCEPTANCE") == "1" {
			t.Fatalf("exact DuckDB v1.5.5 is required: %v", err)
		}
		t.Skipf("exact DuckDB v1.5.5 unavailable: %v", err)
	}
}

func TestIntegrationStory32CatalogStaleOutputRace(t *testing.T) {
	pool := testDB(t)
	story32RequireDuckDB(t)
	ctx := context.Background()
	payer := "payer-a"
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	ws := mustWorkspace(t)
	providerCatalog := filepath.Join(t.TempDir(), "provider-catalog")
	writeStory32ProviderCatalog(t, providerCatalog)
	warehouse := filepath.Join(t.TempDir(), "warehouse")
	services := mustStory32Services(t)
	sourceA, snapshotA := seedStory32Snapshot(t, pool, payer, month, "stale-output-a")
	targetA, batchA := publishStory32Output(t, pool, ws, providerCatalog, warehouse, services, sourceA, snapshotA, payer, month, story32MRFA(), story32PlansA())
	if batchA <= 0 || targetA.SnapshotID != snapshotA {
		t.Fatalf("target A=%+v batch=%d", targetA, batchA)
	}
	ready, err := filtercatalog.Build(ctx, filtercatalog.BuildParams{
		Pool: pool, PayerID: payer, CollectionMonth: month,
		WarehousePath: warehouse, ProviderCatalogPath: providerCatalog,
		Preflight: func(targets []release.Target) error {
			return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
		},
	})
	if err != nil {
		t.Fatalf("build ready output-race catalog: %v", err)
	}
	sourceB, snapshotB := seedStory32Snapshot(t, pool, payer, month, "stale-output-b")
	targetB, batchB := publishStory32Output(t, pool, ws, providerCatalog, warehouse, services, sourceB, snapshotB, payer, month, story32MRFB(), story32PlansB())
	if batchB <= 0 || targetB.SnapshotID != snapshotB {
		t.Fatalf("target B=%+v batch=%d", targetB, batchB)
	}
	before := story32DatabaseState(t, pool, payer, month, ready.CatalogID)
	warehouseBefore := story32WarehouseDigest(t, warehouse)
	if _, err := release.Activate(ctx, pool, payer, month, nil); !jobs.IsFailure(err, jobs.FailureFilterCatalogStale) {
		t.Fatalf("output-race activation error=%v, want filter_catalog_stale", err)
	}
	if got := story32DatabaseState(t, pool, payer, month, ready.CatalogID); got != before {
		t.Fatal("stale output activation changed database state")
	}
	if got := story32WarehouseDigest(t, warehouse); got != warehouseBefore {
		t.Fatal("stale output activation changed warehouse")
	}
	rebuilt, err := filtercatalog.Build(ctx, filtercatalog.BuildParams{
		Pool: pool, PayerID: payer, CollectionMonth: month,
		WarehousePath: warehouse, ProviderCatalogPath: providerCatalog,
		Preflight: func(targets []release.Target) error {
			return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
		},
	})
	if err != nil || rebuilt.CatalogStatus != "ready" || rebuilt.OutputCount != 2 || rebuilt.CatalogID == ready.CatalogID {
		t.Fatalf("stale output rebuild=%+v err=%v", rebuilt, err)
	}
	if _, err := release.Activate(ctx, pool, payer, month, func(targets []release.Target, schema int64, providerMonth time.Time) error {
		if schema != 1 || !providerMonth.Equal(month) {
			return fmt.Errorf("provider catalog identity mismatch")
		}
		return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
	}); err != nil {
		t.Fatalf("activate rebuilt output-race catalog: %v", err)
	}
}

func TestIntegrationStory32CatalogStalePlanRace(t *testing.T) {
	pool := testDB(t)
	story32RequireDuckDB(t)
	ctx := context.Background()
	payer := "payer-a"
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	ws := mustWorkspace(t)
	providerCatalog := filepath.Join(t.TempDir(), "provider-catalog")
	writeStory32ProviderCatalog(t, providerCatalog)
	warehouse := filepath.Join(t.TempDir(), "warehouse")
	services := mustStory32Services(t)
	sourceA, snapshotA := seedStory32Snapshot(t, pool, payer, month, "stale-plan-a")
	targetA, batchA := publishStory32Output(t, pool, ws, providerCatalog, warehouse, services, sourceA, snapshotA, payer, month, story32MRFA(), story32PlansA())
	if batchA <= 0 {
		t.Fatalf("batch A=%d", batchA)
	}
	ready, err := filtercatalog.Build(ctx, filtercatalog.BuildParams{
		Pool: pool, PayerID: payer, CollectionMonth: month,
		WarehousePath: warehouse, ProviderCatalogPath: providerCatalog,
		Preflight: func(targets []release.Target) error {
			return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
		},
	})
	if err != nil {
		t.Fatalf("build ready plan-race catalog: %v", err)
	}
	var planID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.mrf_plans
    (mrf_snapshot_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type)
VALUES ($1, 'Race Plan', 'Story32 Issuer', 'hios', 'RACE-1', 'group')
RETURNING id`, snapshotA).Scan(&planID); err != nil {
		t.Fatalf("insert race plan: %v", err)
	}
	var raceBatch int64
	if err := pool.QueryRow(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batches
    (mrf_snapshot_id, status, river_job_id, requested_plan_count,
     added_plan_count, started_at, completed_at)
VALUES ($1, 'succeeded', 1, 1, 1, transaction_timestamp(), transaction_timestamp())
RETURNING id`, snapshotA).Scan(&raceBatch); err != nil {
		t.Fatalf("insert race attachment batch: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.plan_attachment_batch_items
    (plan_attachment_batch_id, mrf_plan_id)
VALUES ($1, $2)`, raceBatch, planID); err != nil {
		t.Fatalf("insert race attachment item: %v", err)
	}
	publishStory32PlanBatch(t, ws, warehouse, targetA.OutputID, raceBatch, []story32Plan{
		{PlanName: "Race Plan", IssuerName: "Story32 Issuer", PlanIDType: "hios", PlanID: "RACE-1", PlanMarketType: "group"},
	})
	before := story32DatabaseState(t, pool, payer, month, ready.CatalogID)
	warehouseBefore := story32WarehouseDigest(t, warehouse)
	if _, err := release.Activate(ctx, pool, payer, month, nil); !jobs.IsFailure(err, jobs.FailureFilterCatalogStale) {
		t.Fatalf("plan-race activation error=%v, want filter_catalog_stale", err)
	}
	if got := story32DatabaseState(t, pool, payer, month, ready.CatalogID); got != before {
		t.Fatal("stale plan activation changed database state")
	}
	if got := story32WarehouseDigest(t, warehouse); got != warehouseBefore {
		t.Fatal("stale plan activation changed warehouse")
	}
	rebuilt, err := filtercatalog.Build(ctx, filtercatalog.BuildParams{
		Pool: pool, PayerID: payer, CollectionMonth: month,
		WarehousePath: warehouse, ProviderCatalogPath: providerCatalog,
		Preflight: func(targets []release.Target) error {
			return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
		},
	})
	if err != nil || rebuilt.CatalogStatus != "ready" || rebuilt.PlanCount != 4 {
		t.Fatalf("stale plan rebuild=%+v err=%v", rebuilt, err)
	}
	if _, err := release.Activate(ctx, pool, payer, month, func(targets []release.Target, schema int64, providerMonth time.Time) error {
		if schema != 1 || !providerMonth.Equal(month) {
			return fmt.Errorf("provider catalog identity mismatch")
		}
		return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
	}); err != nil {
		t.Fatalf("activate rebuilt plan-race catalog: %v", err)
	}
}

func story32RequireBackupTools(t *testing.T) {
	t.Helper()
	for _, name := range []string{"pg_dump", "pg_restore"} {
		if _, err := exec.LookPath(name); err != nil {
			if os.Getenv("MRFPIPELINE_REQUIRE_BACKUP_ACCEPTANCE") == "1" {
				t.Fatalf("backup acceptance requires %s", name)
			}
			t.Skipf("backup acceptance tool unavailable")
		}
	}
}

func story32RunTool(ctx context.Context, name string, env []string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = env
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("backup operation failed")
	}
	return nil
}

func story32CopyTree(t *testing.T, source, destination string) {
	t.Helper()
	err := filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if info.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
				return err
			}
			return os.Symlink(link, target)
		}
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported warehouse file")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
			return err
		}
		if err := os.WriteFile(target, data, info.Mode().Perm()); err != nil {
			return err
		}
		return os.Chtimes(target, info.ModTime(), info.ModTime())
	})
	if err != nil {
		t.Fatalf("copy warehouse backup: %v", err)
	}
}

func story32PostgresToolEnv(config *pgxpool.Config) []string {
	env := os.Environ()
	env = append(env,
		"PGHOST="+config.ConnConfig.Host,
		"PGPORT="+strconv.Itoa(int(config.ConnConfig.Port)),
		"PGUSER="+config.ConnConfig.User,
		"PGDATABASE="+config.ConnConfig.Database,
	)
	if config.ConnConfig.Password != "" {
		env = append(env, "PGPASSWORD="+config.ConnConfig.Password)
	}
	return env
}

func TestIntegrationStory32BackupRestoreAcceptance(t *testing.T) {
	pool := testDB(t)
	story32RequireDuckDB(t)
	story32RequireBackupTools(t)
	ctx := context.Background()
	payer := "payer-a"
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	ws := mustWorkspace(t)
	providerCatalog := filepath.Join(t.TempDir(), "provider-catalog")
	writeStory32ProviderCatalog(t, providerCatalog)
	warehouse := filepath.Join(t.TempDir(), "warehouse")
	services := mustStory32Services(t)
	sourceA, snapshotA := seedStory32Snapshot(t, pool, payer, month, "backup-a")
	targetA, batchA := publishStory32Output(t, pool, ws, providerCatalog, warehouse, services, sourceA, snapshotA, payer, month, story32MRFA(), story32PlansA())
	if batchA <= 0 {
		t.Fatalf("batch A=%d", batchA)
	}
	buildA, err := filtercatalog.Build(ctx, filtercatalog.BuildParams{
		Pool: pool, PayerID: payer, CollectionMonth: month,
		WarehousePath: warehouse, ProviderCatalogPath: providerCatalog,
		Preflight: func(targets []release.Target) error {
			return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
		},
	})
	if err != nil {
		t.Fatalf("build backup generation 1: %v", err)
	}
	if buildA.CatalogStatus != "ready" || buildA.PublicationGeneration != 1 || buildA.OutputCount != 1 {
		t.Fatalf("backup generation 1=%+v", buildA)
	}
	if _, err := release.Activate(ctx, pool, payer, month, func(targets []release.Target, schema int64, providerMonth time.Time) error {
		if schema != 1 || !providerMonth.Equal(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)) {
			return fmt.Errorf("provider catalog identity mismatch")
		}
		return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
	}); err != nil {
		t.Fatalf("activate backup generation 1: %v", err)
	}
	sourceB, snapshotB := seedStory32Snapshot(t, pool, payer, month, "backup-b")
	targetB, batchB := publishStory32Output(t, pool, ws, providerCatalog, warehouse, services, sourceB, snapshotB, payer, month, story32MRFB(), story32PlansB())
	if batchB <= 0 {
		t.Fatalf("batch B=%d", batchB)
	}
	buildAB, err := filtercatalog.Build(ctx, filtercatalog.BuildParams{
		Pool: pool, PayerID: payer, CollectionMonth: month,
		WarehousePath: warehouse, ProviderCatalogPath: providerCatalog,
		Preflight: func(targets []release.Target) error {
			return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
		},
	})
	if err != nil {
		t.Fatalf("build backup generation 2: %v", err)
	}
	if buildAB.CatalogStatus != "ready" || buildAB.PublicationGeneration != 2 || buildAB.OutputCount != 2 {
		t.Fatalf("backup generation 2=%+v", buildAB)
	}
	if _, err := release.Activate(ctx, pool, payer, month, func(targets []release.Target, schema int64, providerMonth time.Time) error {
		if schema != 1 || !providerMonth.Equal(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)) {
			return fmt.Errorf("provider catalog identity mismatch")
		}
		return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
	}); err != nil {
		t.Fatalf("activate backup generation 2: %v", err)
	}
	sourceReport, err := reconcile.Run(ctx, reconcile.Params{
		Pool: pool, Workspace: ws, WarehousePath: warehouse,
		ProviderCatalogPath: providerCatalog, ServicesPath: services,
		Logger: jobs.NewLogger(io.Discard),
	})
	if err != nil || sourceReport.SealedReleaseInconsistencyCount != 0 || sourceReport.FilterCatalogBackfillRequiredCount != 0 {
		t.Fatalf("source reconcile=%+v err=%v", sourceReport, err)
	}
	sourceTargets, err := release.ListPublishedTargets(ctx, pool, payer, month)
	if err != nil {
		t.Fatalf("list source published targets: %v", err)
	}
	if err := reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, sourceTargets); err != nil {
		t.Fatalf("validate source published targets: %v", err)
	}
	sourceWarehouseDigest := story32WarehouseDigest(t, warehouse)
	backupWarehouse := filepath.Join(t.TempDir(), "warehouse")
	story32CopyTree(t, warehouse, backupWarehouse)
	if story32WarehouseDigest(t, backupWarehouse) != sourceWarehouseDigest {
		t.Fatal("warehouse backup differs from source")
	}
	backupArtifact := filepath.Join(t.TempDir(), "artifacts")
	story32CopyTree(t, ws.Root, backupArtifact)
	backupWorkspace, err := artifact.Open(ctx, backupArtifact)
	if err != nil {
		t.Fatalf("open restored artifact workspace: %v", err)
	}
	originalState := story32DatabaseState(t, pool, payer, month, buildAB.CatalogID)
	raw := os.Getenv("MRFPIPELINE_TEST_DATABASE_URL")
	dumpPath := filepath.Join(t.TempDir(), "pipeline.dump")
	if err := story32RunTool(ctx, "pg_dump", nil, "--format=custom", "--no-owner", "--no-acl", "--file", dumpPath, raw); err != nil {
		t.Fatalf("dump disposable database: %v", err)
	}
	restoreName := fmt.Sprintf("mrfpipeline_test_restore_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, "CREATE DATABASE "+story32QuoteIdentifier(restoreName)); err != nil {
		t.Fatalf("create restore database: %v", err)
	}
	restoreConfig, err := pgxpool.ParseConfig(raw)
	if err != nil {
		t.Fatalf("parse restore database config: %v", err)
	}
	restoreConfig.ConnConfig.Database = restoreName
	restoreConfig.MaxConns = 8
	restorePool, err := pgxpool.NewWithConfig(ctx, restoreConfig)
	if err != nil {
		t.Fatalf("connect restored database: %v", err)
	}
	t.Cleanup(func() {
		restorePool.Close()
		if _, err := pool.Exec(context.Background(), "DROP DATABASE IF EXISTS "+story32QuoteIdentifier(restoreName)); err != nil {
			t.Errorf("drop restore database: %v", err)
		}
	})
	restoreEnv := story32PostgresToolEnv(restoreConfig)
	if err := story32RunTool(ctx, "pg_restore", restoreEnv, "--no-owner", "--no-acl", "--exit-on-error", "--dbname", restoreName, dumpPath); err != nil {
		t.Fatalf("restore disposable database: %v", err)
	}
	restoredTargets, err := release.ListPublishedTargets(ctx, restorePool, payer, month)
	if err != nil {
		t.Fatalf("list restored published targets: %v", err)
	}
	if err := reconcile.ValidateActivationTargets(ctx, restorePool, backupWarehouse, payer, month, restoredTargets); err != nil {
		t.Fatalf("validate restored published targets: %v", err)
	}
	restoredReport, err := reconcile.Run(ctx, reconcile.Params{
		Pool: restorePool, Workspace: backupWorkspace, WarehousePath: backupWarehouse,
		ProviderCatalogPath: providerCatalog, ServicesPath: services,
		Logger: jobs.NewLogger(io.Discard),
	})
	if err != nil || restoredReport.SealedReleaseInconsistencyCount != 0 || restoredReport.FilterCatalogBackfillRequiredCount != 0 {
		t.Fatalf("restored reconcile=%+v err=%v", restoredReport, err)
	}
	restoredStatus, err := filtercatalog.Status(ctx, restorePool, payer, month)
	if err != nil || restoredStatus.ReleaseStatus != release.Active ||
		restoredStatus.CurrentPublicationGeneration != 2 || restoredStatus.CurrentCatalogID == nil ||
		*restoredStatus.CurrentCatalogID != buildAB.CatalogID {
		t.Fatalf("restored filter status=%+v err=%v", restoredStatus, err)
	}
	restoredRelease, err := release.Readiness(ctx, restorePool, payer, month)
	if err != nil || restoredRelease.Status != release.Active || restoredRelease.FilterCatalogGeneration == nil ||
		*restoredRelease.FilterCatalogGeneration != 2 {
		t.Fatalf("restored release status=%+v err=%v", restoredRelease, err)
	}
	story32AssertReadOnlyServing(t, restorePool, payer, month, targetA, targetB, buildAB.CatalogID, "", month, release.Target{}, 0)
	if got := story32DatabaseState(t, restorePool, payer, month, buildAB.CatalogID); got != originalState {
		t.Fatal("restored database differs from source state")
	}
	missingWarehouse := filepath.Join(t.TempDir(), "missing-warehouse")
	beforeWrong := story32DatabaseState(t, restorePool, payer, month, buildAB.CatalogID)
	wrongReport, err := reconcile.Run(ctx, reconcile.Params{
		Pool: restorePool, Workspace: backupWorkspace, WarehousePath: missingWarehouse,
		ProviderCatalogPath: providerCatalog, ServicesPath: services,
		Logger: jobs.NewLogger(io.Discard),
	})
	if err != nil || wrongReport.SealedReleaseInconsistencyCount == 0 {
		t.Fatalf("wrong-warehouse reconcile=%+v err=%v", wrongReport, err)
	}
	if _, err := filtercatalog.Status(ctx, restorePool, payer, month); err != nil {
		t.Fatalf("wrong-warehouse filter status: %v", err)
	}
	if _, err := release.Activate(ctx, restorePool, payer, month, func(targets []release.Target, schema int64, providerMonth time.Time) error {
		return reconcile.ValidateActivationTargets(ctx, restorePool, missingWarehouse, payer, month, targets)
	}); !jobs.IsFailure(err, jobs.FailureArtifactReconciliationFailed) {
		t.Fatalf("wrong-warehouse activation error=%v, want artifact_reconciliation_failed", err)
	}
	if afterWrong := story32DatabaseState(t, restorePool, payer, month, buildAB.CatalogID); afterWrong != beforeWrong {
		t.Fatal("wrong-warehouse activation changed restored database")
	}
}

func TestIntegrationStory32MeasureAcceptance(t *testing.T) {
	pool := testDB(t)
	story32RequireDuckDB(t)
	ctx := context.Background()
	payer := "payer-a"
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	ws := mustWorkspace(t)
	providerCatalog := filepath.Join(t.TempDir(), "provider-catalog")
	writeStory32ProviderCatalog(t, providerCatalog)
	warehouse := filepath.Join(t.TempDir(), "warehouse")
	services := mustStory32Services(t)
	sourceID, snapshotID := seedStory32Snapshot(t, pool, payer, month, "measure-a")
	if _, batchID := publishStory32Output(t, pool, ws, providerCatalog, warehouse, services, sourceID, snapshotID, payer, month, story32MRFA(), story32PlansA()); batchID <= 0 {
		t.Fatalf("batch=%d", batchID)
	}
	beforeWarehouse := story32WarehouseDigest(t, warehouse)
	params := filtercatalog.BuildParams{
		Pool: pool, PayerID: payer, CollectionMonth: month,
		WarehousePath: warehouse, ProviderCatalogPath: providerCatalog,
		Preflight: func(targets []release.Target) error {
			return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
		},
	}
	measurement, err := filtercatalog.Measure(ctx, params)
	if err != nil {
		t.Fatalf("measure: %v", err)
	}
	if measurement.WarehouseSchemaVersion != "2.0.0" || measurement.ProviderCatalogSchemaVersion != 1 ||
		measurement.ProviderCatalogReleaseMonth != "2026-08" || measurement.PublicationGeneration != 1 ||
		measurement.CandidateOutputCount != 1 || measurement.StandardFactCount != story32ExpectedA.StandardFacts ||
		measurement.BillingCodeCount != 1 || measurement.CodeFilterValueCount != int64(len(story32ExpectedA.CodeFilterValues)) ||
		measurement.PlanCount != int64(len(story32ExpectedA.Plans)) || measurement.PlanOutputCount != 3 ||
		measurement.OutputCodeNetworkCount != int64(len(story32ExpectedA.CodeNetworks)) ||
		measurement.ProviderFilterValueCount != int64(len(story32ExpectedA.ProviderFilterValues)) ||
		measurement.PeakRSSBytes != 0 || measurement.DuckDBWallTimeMS < 0 ||
		measurement.PostgresPopulationWallTimeMS < 0 || measurement.TotalWallTimeMS < 0 ||
		measurement.CatalogDatabaseBytes <= 0 {
		t.Fatalf("measurement=%+v", measurement)
	}
	if measurement.TotalWallTimeMS < measurement.DuckDBWallTimeMS || measurement.TotalWallTimeMS < measurement.PostgresPopulationWallTimeMS {
		t.Fatalf("total wall time shorter than a phase: %+v", measurement)
	}
	raw, err := json.Marshal(measurement)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 17 || got["peak_rss_bytes"] != float64(0) {
		t.Fatalf("measure json=%v", got)
	}
	if story32WarehouseDigest(t, warehouse) != beforeWarehouse {
		t.Fatal("measure mutated warehouse")
	}
	var catalogID int64
	var releaseStatus, catalogStatus string
	if err := pool.QueryRow(ctx, `
SELECT status FROM mrfpipeline.monthly_releases
WHERE payer_id=$1 AND collection_month=$2`, payer, month).Scan(&releaseStatus); err != nil {
		t.Fatal(err)
	}
	if releaseStatus != "building" {
		t.Fatalf("measure activated release status=%s", releaseStatus)
	}
	if err := pool.QueryRow(ctx, `
SELECT id, status FROM mrfweb.release_catalogs
WHERE payer_id=$1 AND collection_month=$2 AND publication_generation=1`, payer, month).Scan(&catalogID, &catalogStatus); err != nil {
		t.Fatal(err)
	}
	if catalogStatus != "ready" || catalogID <= 0 {
		t.Fatalf("measured catalog id=%d status=%s", catalogID, catalogStatus)
	}
	before := story32DatabaseState(t, pool, payer, month, catalogID)
	_, err = filtercatalog.Measure(ctx, params)
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogMeasurePrecondition) {
		t.Fatalf("repeat measure error=%v", err)
	}
	if got := story32DatabaseState(t, pool, payer, month, catalogID); got != before {
		t.Fatal("precondition measure mutated state")
	}
	if _, err := release.Activate(ctx, pool, payer, month, func(targets []release.Target, schema int64, providerMonth time.Time) error {
		if schema != 1 || !providerMonth.Equal(month) {
			return fmt.Errorf("provider catalog identity mismatch")
		}
		return reconcile.ValidateActivationTargets(ctx, pool, warehouse, payer, month, targets)
	}); err != nil {
		t.Fatalf("activate measured catalog: %v", err)
	}
	beforePublished := story32DatabaseState(t, pool, payer, month, catalogID)
	_, err = filtercatalog.Measure(ctx, params)
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogMeasurePrecondition) {
		t.Fatalf("published measure error=%v", err)
	}
	if got := story32DatabaseState(t, pool, payer, month, catalogID); got != beforePublished {
		t.Fatal("published measure mutated state")
	}
}
