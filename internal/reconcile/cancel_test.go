package reconcile

import "testing"

func TestControlNotifyIsMRFCancel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		payload string
		want    bool
	}{
		{payload: `{"action":"cancel","job_id":9,"queue":"mrf_download"}`, want: true},
		{payload: `{"action":"cancel","job_id":9,"queue":"mrf_parse"}`, want: true},
		{payload: `{"action":"cancel","job_id":9,"queue":"toc_download"}`, want: true},
		{payload: `{"action":"cancel","job_id":9,"queue":"toc_parse"}`, want: false},
		{payload: `{"action":"pause","queue":"mrf_download"}`, want: false},
		{payload: `{`, want: false},
	}
	for _, tc := range cases {
		if got := controlNotifyIsMRFCancel(tc.payload); got != tc.want {
			t.Fatalf("%s: got %v", tc.payload, got)
		}
	}
}
