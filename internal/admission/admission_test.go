package admission

import "testing"

func TestTargetFormattingAndParsing(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		kind  string
		count *int64
		want  string
	}{
		{name: "unset", kind: TargetUnset, want: TargetUnset},
		{name: "all", kind: TargetAll, want: TargetAll},
		{name: "numeric", kind: TargetNumeric, count: ptr(3), want: "3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseTarget(tc.kind, tc.count)
			if err != nil {
				t.Fatal(err)
			}
			if FormatTarget(got) != tc.want {
				t.Fatalf("got %q", FormatTarget(got))
			}
		})
	}
}

func TestTargetTransitionIsCumulative(t *testing.T) {
	t.Parallel()
	if err := validateTargetTransition(Target{Kind: TargetNumeric, Count: 3}, Target{Kind: TargetNumeric, Count: 2}); err == nil {
		t.Fatal("decrease accepted")
	}
	if err := validateTargetTransition(Target{Kind: TargetNumeric, Count: 3}, Target{Kind: TargetAll}); err != nil {
		t.Fatal(err)
	}
	if err := validateTargetTransition(Target{Kind: TargetAll}, Target{Kind: TargetNumeric, Count: 4}); err == nil {
		t.Fatal("all reverted to numeric")
	}
}

func ptr(n int64) *int64 { return &n }
