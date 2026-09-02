package filtercatalog

import (
	"testing"
)

func TestProjectPlansDeduplicatesSponsorIndependentIdentity(t *testing.T) {
	identity := planIdentity{PlanName: "Plan", IssuerName: "Issuer", PlanIDType: "ein", PlanID: "1", PlanMarketType: "group"}
	snapshot := planSnapshot{Rows: []planSnapshotRow{
		{OutputID: "mrf-20", planIdentity: identity},
		{OutputID: "mrf-20", planIdentity: identity},
		{OutputID: "mrf-3", planIdentity: identity},
	}}
	projection, err := projectPlans(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(projection.Plans) != 1 || len(projection.PlanOutputs) != 2 {
		t.Fatalf("projection=%+v", projection)
	}
	if projection.Plans[0].Outputs[0] != "mrf-20" || projection.Plans[0].Outputs[1] != "mrf-3" {
		t.Fatalf("outputs=%v", projection.Plans[0].Outputs)
	}
}

func TestProjectPlansKeepsSamePlanIDWithDifferentIssuer(t *testing.T) {
	snapshot := planSnapshot{Rows: []planSnapshotRow{
		{OutputID: "mrf-1", planIdentity: planIdentity{PlanName: "Shared Plan", IssuerName: "Issuer", PlanIDType: "hios", PlanID: "SHARED-1", PlanMarketType: "group"}},
		{OutputID: "mrf-1", planIdentity: planIdentity{PlanName: "Alt Shared Plan", IssuerName: "Other Issuer", PlanIDType: "hios", PlanID: "SHARED-1", PlanMarketType: "group"}},
	}}
	projection, err := projectPlans(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(projection.Plans) != 2 || len(projection.PlanOutputs) != 2 {
		t.Fatalf("projection=%+v", projection)
	}
}

func TestSearchTextUsesOuterASCIITrimAndLowercase(t *testing.T) {
	got, err := searchText(planIdentity{
		PlanName: "  Gold\t", IssuerName: " Issuer ", PlanIDType: "hios", PlanID: " ABC ", PlanMarketType: "group",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "gold issuer hios abc group" {
		t.Fatalf("search text=%q", got)
	}
}
