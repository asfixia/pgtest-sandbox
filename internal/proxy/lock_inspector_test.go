package proxy

import "testing"

func TestIsPgrollbackApplicationName(t *testing.T) {
	tests := []struct {
		app  string
		want bool
	}{
		{"", false},
		{"pgAdmin 4", false},
		{"pgrollback-lock-inspector", true},
		{"pgrollback_default", true},
		{"pgrollback-mytest", true},
		{"pgrollback_mytest", true},
		{"pgrollback-proxy", true},
	}
	for _, tt := range tests {
		if got := IsPgrollbackApplicationName(tt.app); got != tt.want {
			t.Errorf("IsPgrollbackApplicationName(%q) = %v, want %v", tt.app, got, tt.want)
		}
	}
}

func TestTestIDFromPgrollbackAppName(t *testing.T) {
	tests := []struct {
		app  string
		want string
	}{
		{"pgrollback-abc", "abc"},
		{"pgrollback_foo", "foo"},
		{"pgrollback_default", "default"},
		{"pgrollback-lock-inspector", "lock-inspector"},
		{"pgAdmin 4", ""},
	}
	for _, tt := range tests {
		if got := testIDFromPgrollbackAppName(tt.app); got != tt.want {
			t.Errorf("testIDFromPgrollbackAppName(%q) = %q, want %q", tt.app, got, tt.want)
		}
	}
}
