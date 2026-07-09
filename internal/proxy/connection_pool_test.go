package proxy

import "testing"

func TestGetAppNameForTestID_TruncatesToPostgresLimit(t *testing.T) {
	longID := stringsRepeat("x", 80)
	got := getAppNameForTestID(longID)
	if len(got) != 63 {
		t.Fatalf("len = %d, want 63", len(got))
	}
	if got != "pgrollback-"+stringsRepeat("x", 52) {
		t.Fatalf("unexpected truncation: %q", got)
	}
}

func stringsRepeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}
