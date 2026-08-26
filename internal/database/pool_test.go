package database

import "testing"

func TestMaxConnsForRole(t *testing.T) {
	t.Parallel()
	for role, want := range map[string]int32{
		"control":  ControlMaxConns,
		"mrf":      MRFMaxConns,
		"consumer": ConsumerMaxConns,
		"unknown":  OperatorMaxConns,
	} {
		if got := MaxConnsForRole(role); got != want {
			t.Errorf("MaxConnsForRole(%q) = %d, want %d", role, got, want)
		}
	}
}
