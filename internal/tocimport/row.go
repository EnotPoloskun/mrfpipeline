package tocimport

import (
	"strings"
	"time"
	"unicode/utf8"
)

func requiredString(v string) bool {
	return v != "" && utf8.ValidString(v)
}

func validateTOCFileRow(row tocFileRow, tocOutputID, payer, month, sourceURI string) error {
	if row.TOCOutputID != tocOutputID || row.PayerID != payer || row.CollectionMonth != month {
		return errRow
	}
	if row.SourceURI != sourceURI {
		return errRow
	}
	if !requiredString(row.ReportingEntityName) || !requiredString(row.ReportingEntityType) {
		return errRow
	}
	if row.LastUpdatedOn != nil && row.LastUpdatedOnRaw == nil {
		return errRow
	}
	if row.LastUpdatedOn != nil && row.LastUpdatedOnRaw != nil {
		d, err := parseDate(*row.LastUpdatedOnRaw)
		if err != nil || d != *row.LastUpdatedOn {
			return errRow
		}
	}
	if row.LastUpdatedOnRaw != nil && !utf8.ValidString(*row.LastUpdatedOnRaw) {
		return errRow
	}
	if row.SourceSchemaVersion != nil && (*row.SourceSchemaVersion == "" || !utf8.ValidString(*row.SourceSchemaVersion)) {
		return errRow
	}
	if row.AdditionalFieldsJSON != nil {
		if err := validateAdditionalJSON(*row.AdditionalFieldsJSON); err != nil {
			return err
		}
	}
	return nil
}

func validateAssocRow(row assocRow, tocOutputID, payer, month string) error {
	if row.TOCOutputID != tocOutputID || row.PayerID != payer || row.CollectionMonth != month {
		return errRow
	}
	if !validHTTPSLocation(row.MRFLocation) {
		return errRow
	}
	want, ok := deriveFilename(row.MRFLocation)
	if ok {
		if row.MRFFilename == nil || *row.MRFFilename != want {
			return errRow
		}
	} else if row.MRFFilename != nil {
		return errRow
	}
	if !requiredString(row.PlanName) || !requiredString(row.IssuerName) || !requiredString(row.PlanID) {
		return errRow
	}
	if row.PlanIDType != "ein" && row.PlanIDType != "hios" {
		return errRow
	}
	if row.PlanMarketType != "group" && row.PlanMarketType != "individual" {
		return errRow
	}
	if row.PlanSponsorName != nil && !requiredString(*row.PlanSponsorName) {
		return errRow
	}
	for _, extra := range []*string{
		row.ReportingStructureAdditionalFieldsJSON,
		row.PlanAdditionalFieldsJSON,
		row.FileAdditionalFieldsJSON,
	} {
		if extra != nil {
			if err := validateAdditionalJSON(*extra); err != nil {
				return err
			}
		}
	}
	return nil
}

func compareAssoc(a, b assocRow) int {
	if c := strings.Compare(a.MRFLocation, b.MRFLocation); c != 0 {
		return c
	}
	if c := strings.Compare(a.PlanName, b.PlanName); c != 0 {
		return c
	}
	if c := strings.Compare(a.IssuerName, b.IssuerName); c != 0 {
		return c
	}
	if a.PlanSponsorName == nil && b.PlanSponsorName != nil {
		return -1
	}
	if a.PlanSponsorName != nil && b.PlanSponsorName == nil {
		return 1
	}
	if a.PlanSponsorName != nil {
		if c := strings.Compare(*a.PlanSponsorName, *b.PlanSponsorName); c != 0 {
			return c
		}
	}
	if c := strings.Compare(a.PlanIDType, b.PlanIDType); c != 0 {
		return c
	}
	if c := strings.Compare(a.PlanID, b.PlanID); c != 0 {
		return c
	}
	return strings.Compare(a.PlanMarketType, b.PlanMarketType)
}

func parseDate(value string) (parquetDate, error) {
	if len(value) != 10 || value[4] != '-' || value[7] != '-' {
		return 0, errRow
	}
	t, err := time.Parse("2006-01-02", value)
	if err != nil || t.Format("2006-01-02") != value {
		return 0, errRow
	}
	days := t.Unix() / 86400
	if days < -2147483648 || days > 2147483647 {
		return 0, errRow
	}
	return parquetDate(days), nil
}
