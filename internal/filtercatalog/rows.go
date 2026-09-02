package filtercatalog

import "time"

// DuckDB is a verified DuckDB CLI selected by ResolveDuckDB.
type DuckDB struct {
	path string
}

// Output identifies one exact warehouse snapshot in a candidate release.
type Output struct {
	PayerID         string
	CollectionMonth string
	OutputID        string
	SnapshotID      int64
}

// Params describes one immutable extraction request.
type Params struct {
	WarehousePath     string
	OutputFingerprint string
	Outputs           []Output
	processObserver   func(BuildPhase)
}

// Result is the complete compact extraction result for one candidate release.
type Result struct {
	WarehouseSchemaVersion       string
	ProviderCatalogSchemaVersion int64
	ProviderCatalogReleaseMonth  time.Time
	OutputFingerprint            string
	StandardFactCount            int64
	BillingCodes                 []BillingCode
	CodeFilterValues             []CodeFilterValue
	OutputCodeNetworks           []OutputCodeNetwork
	ProviderFilterValues         []ProviderFilterValue
}

// BillingCode is one exact billing-code identity and its weighted observation counts.
type BillingCode struct {
	BillingCodeType             string
	BillingCode                 string
	BillingCodeTypeVersion      *string
	WarehouseServiceName        *string
	WarehouseServiceDescription *string
	ObservationCount            int64
	UnmodifiedObservationCount  int64
}

// CodeFilterValue is one code-scoped context or list filter value.
type CodeFilterValue struct {
	BillingCodeType  string
	BillingCode      string
	FilterKind       string
	FilterValue      string
	DisplayLabel     string
	ObservationCount int64
}

// OutputCodeNetwork is one output/code/network observation count.
type OutputCodeNetwork struct {
	OutputID         string
	BillingCodeType  string
	BillingCode      string
	NetworkName      string
	ObservationCount int64
}

// ProviderFilterValue is one release-scoped provider choice.
type ProviderFilterValue struct {
	FilterKind    string
	ParentValue   string
	FilterValue   string
	ProviderCount int64
}
