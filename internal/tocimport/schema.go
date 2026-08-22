package tocimport

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
)

const (
	datasetTOCFiles     = "toc_files"
	datasetAssociations = "mrf_plan_associations"
	maxBatchRows        = 1000
)

type parquetDate int32

type tocFileRow struct {
	TOCOutputID          string       `parquet:"toc_output_id"`
	PayerID              string       `parquet:"payer_id"`
	CollectionMonth      string       `parquet:"collection_month"`
	SourceURI            string       `parquet:"source_uri"`
	ReportingEntityName  string       `parquet:"reporting_entity_name"`
	ReportingEntityType  string       `parquet:"reporting_entity_type"`
	LastUpdatedOn        *parquetDate `parquet:"last_updated_on,date"`
	LastUpdatedOnRaw     *string      `parquet:"last_updated_on_raw,optional"`
	SourceSchemaVersion  *string      `parquet:"source_schema_version,optional"`
	AdditionalFieldsJSON *string      `parquet:"additional_fields_json,optional"`
}

type assocRow struct {
	TOCOutputID                            string  `parquet:"toc_output_id"`
	PayerID                                string  `parquet:"payer_id"`
	CollectionMonth                        string  `parquet:"collection_month"`
	MRFLocation                            string  `parquet:"mrf_location"`
	MRFFilename                            *string `parquet:"mrf_filename,optional"`
	PlanName                               string  `parquet:"plan_name"`
	IssuerName                             string  `parquet:"issuer_name"`
	PlanSponsorName                        *string `parquet:"plan_sponsor_name,optional"`
	PlanIDType                             string  `parquet:"plan_id_type"`
	PlanID                                 string  `parquet:"plan_id"`
	PlanMarketType                         string  `parquet:"plan_market_type"`
	ReportingStructureAdditionalFieldsJSON *string `parquet:"reporting_structure_additional_fields_json,optional"`
	PlanAdditionalFieldsJSON               *string `parquet:"plan_additional_fields_json,optional"`
	FileAdditionalFieldsJSON               *string `parquet:"file_additional_fields_json,optional"`
}

var (
	errManifest = errors.New("manifest")
	errSchema   = errors.New("schema")
	errRow      = errors.New("row")
	errOrder    = errors.New("order")
)

var (
	tocFileSchema = parquet.SchemaOf(tocFileRow{})
	assocSchema   = parquet.SchemaOf(assocRow{})
)

func listParts(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) == 0 {
		return nil, errManifest
	}
	named := map[int]bool{}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "part-") || !strings.HasSuffix(name, ".parquet") {
			return nil, errManifest
		}
		num, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "part-"), ".parquet"))
		if err != nil || num < 0 || fmt.Sprintf("part-%05d.parquet", num) != name {
			return nil, errManifest
		}
		if named[num] {
			return nil, errManifest
		}
		named[num] = true
	}
	out := make([]string, len(named))
	for i := 0; i < len(named); i++ {
		if !named[i] {
			return nil, errManifest
		}
		out[i] = filepath.Join(dir, fmt.Sprintf("part-%05d.parquet", i))
	}
	return out, nil
}

func validatePartSchema(path string, want *parquet.Schema) error {
	f, err := os.Open(path)
	if err != nil {
		return errSchema
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return errSchema
	}
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return errSchema
	}
	return schemasEqual(pf.Schema(), want)
}

func schemasEqual(got, want *parquet.Schema) error {
	if got == nil || want == nil {
		return errSchema
	}
	gf, wf := got.Fields(), want.Fields()
	if len(gf) != len(wf) {
		return errSchema
	}
	for i := range gf {
		if !fieldsEqual(gf[i], wf[i]) {
			return errSchema
		}
	}
	return nil
}

func fieldsEqual(got, want parquet.Field) bool {
	if got.Name() != want.Name() || got.ID() != want.ID() {
		return false
	}
	if got.Optional() != want.Optional() || got.Required() != want.Required() || got.Repeated() != want.Repeated() {
		return false
	}
	gt, wt := got.Type(), want.Type()
	if gt == nil || wt == nil {
		return false
	}
	if gt.PhysicalType() == nil || wt.PhysicalType() == nil || gt.PhysicalType().String() != wt.PhysicalType().String() {
		return false
	}
	gl, wl := "", ""
	if gt.LogicalType() != nil {
		gl = gt.LogicalType().String()
	}
	if wt.LogicalType() != nil {
		wl = wt.LogicalType().String()
	}
	if gl != wl {
		return false
	}
	if convertedTypeString(gt) != convertedTypeString(wt) {
		return false
	}
	return true
}

func convertedTypeString(t parquet.Type) string {
	if t == nil {
		return ""
	}
	c := t.ConvertedType()
	if c == nil {
		return ""
	}
	return fmt.Sprintf("%v", *c)
}

func openTyped[T any](path string) (*os.File, *parquet.File, *parquet.GenericReader[T], error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, errSchema
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, nil, errSchema
	}
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		_ = f.Close()
		return nil, nil, nil, errSchema
	}
	return f, pf, parquet.NewGenericReader[T](pf), nil
}

func readTyped[T any](r *parquet.GenericReader[T], buf []T) (int, error) {
	n, err := r.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, errRow
	}
	return n, err
}
