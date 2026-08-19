package proxy

import (
	"context"
	"testing"
	"time"

	"pgrollback/internal/config"
	"pgrollback/internal/testutil"
)

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

// TestLookupSurvivesClosedConnBetweenQueries verifies that invalidating the inspector
// connection after the first catalog query does not panic on the second query (regression
// for stale *pgx.Conn pointer reuse after invalidateConn).
func TestLookupSurvivesClosedConnBetweenQueries(t *testing.T) {
	host, port, database, user, password := postgresFromConfigOrFail(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	li := newLockInspector(host, port, database, user, password)
	li.mu.Lock()
	conn, err := li.ensureConnLocked(ctx)
	li.mu.Unlock()
	if err != nil {
		t.Fatalf("postgres not available for lock inspector test: %v", err)
	}

	// Simulate fillFromBlockingLocks error path: close conn and clear li.conn.
	li.mu.Lock()
	li.invalidateConn()
	staleConn := conn
	li.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("lookup panicked after conn invalidate (stale conn %p): %v", staleConn, r)
		}
	}()

	_ = staleConn // was the bug trigger: second query used this closed pointer
	li.lookup(context.Background(), []string{"pgrollback-parque-cafeeiro"})
}

// postgresFromConfigOrFail loads Postgres settings from PGROLLBACK_CONFIG (or default
// config/pgrollback.yaml), with POSTGRES_* env overrides — same as the rest of the test suite.
func postgresFromConfigOrFail(t *testing.T) (host string, port int, database, user, password string) {
	t.Helper()
	path := testutil.ConfigPath()
	cfg, err := config.LoadConfig(path)
	if err != nil {
		t.Fatalf("load config %s: %v", path, err)
	}
	p := cfg.Postgres
	if p.Host == "" || p.Port == 0 || p.Database == "" || p.User == "" {
		t.Fatalf("postgres settings incomplete in config %s", path)
	}
	return p.Host, p.Port, p.Database, p.User, p.Password
}

func TestEnsureConnLockedReconnectsAfterInvalidate(t *testing.T) {
	host, port, database, user, password := postgresFromConfigOrFail(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	li := newLockInspector(host, port, database, user, password)
	li.mu.Lock()
	_, err := li.ensureConnLocked(ctx)
	if err != nil {
		li.mu.Unlock()
		t.Fatalf("postgres not available: %v", err)
	}
	li.invalidateConn()
	conn, err := li.ensureConnLocked(ctx)
	li.mu.Unlock()
	if err != nil {
		t.Fatalf("ensureConnLocked after invalidate: %v", err)
	}
	if conn == nil {
		t.Fatal("expected live connection after reconnect")
	}
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("reconnected conn ping: %v", err)
	}
}
