package filtercatalog

import (
	"strings"

	"github.com/enotpoloskun/mrfpipeline/internal/release"
)

// candidateFingerprint uses the release authority's ASCII-output-sorted
// fingerprint. Keeping conversion here prevents the extractor's numeric target
// ordering from becoming the persistence ordering by accident.
func candidateFingerprint(targets []release.Target) string {
	return release.OutputFingerprint(targets)
}

func comparePlanIdentity(left, right planIdentity) int {
	for _, pair := range [][2]string{
		{left.PlanName, right.PlanName},
		{left.IssuerName, right.IssuerName},
		{left.PlanIDType, right.PlanIDType},
		{left.PlanID, right.PlanID},
		{left.PlanMarketType, right.PlanMarketType},
	} {
		if cmp := strings.Compare(pair[0], pair[1]); cmp != 0 {
			return cmp
		}
	}
	return 0
}
