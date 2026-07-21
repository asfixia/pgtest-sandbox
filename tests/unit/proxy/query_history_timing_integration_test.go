package tstproxy

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"pgrollback/internal/proxy"
)

// parseLoggedQueryDuration parses the duration string stored in query history
// (time.Duration.String(), e.g. "100ms", "10.5s").
func parseLoggedQueryDuration(t *testing.T, raw string) time.Duration {
	t.Helper()
	if raw == "" {
		t.Fatal("logged duration is empty")
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("parse logged duration %q: %v", raw, err)
	}
	return d
}

func lastHistoryEntryForMarker(h []proxy.QueryHistoryEntry, marker string) (proxy.QueryHistoryEntry, bool) {
	for i := len(h) - 1; i >= 0; i-- {
		if strings.Contains(h[i].Query, marker) {
			return h[i], true
		}
	}
	return proxy.QueryHistoryEntry{}, false
}

// waitForQueryHistoryComplete polls until the history entry for marker is finished (not Running, Duration set).
// ExecContext can return before the proxy finalizes the history entry (defer after ReadyForQuery is sent).
func waitForQueryHistoryComplete(
	t *testing.T,
	sess *proxy.TestSession,
	marker string,
	timeout time.Duration,
) proxy.QueryHistoryEntry {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entry, ok := lastHistoryEntryForMarker(sess.GUIQueryHistory(), marker)
		if ok && !entry.Running && entry.Duration != "" {
			return entry
		}
		time.Sleep(5 * time.Millisecond)
	}
	hist := sess.GUIQueryHistory()
	entry, ok := lastHistoryEntryForMarker(hist, marker)
	if !ok {
		t.Fatalf("history missing entry with marker %q after %v; got %#v", marker, timeout, hist)
	}
	t.Fatalf(
		"history entry for %q not finalized after %v: Running=%v Duration=%q; entry=%#v",
		marker, timeout, entry.Running, entry.Duration, entry,
	)
	return proxy.QueryHistoryEntry{}
}

func assertLoggedDurationMatchesPgSleep(
	t *testing.T,
	entry proxy.QueryHistoryEntry,
	lastQueryDuration string,
	sleep time.Duration,
	minLogged, maxLogged time.Duration,
) {
	t.Helper()
	if entry.Running {
		t.Fatal("query still marked Running after pg_sleep completed")
	}
	if entry.Duration == "" {
		t.Fatal("logged Duration is empty after pg_sleep completed")
	}
	if lastQueryDuration != entry.Duration {
		t.Fatalf("GetLastQueryDuration() = %q, want %q (last history entry)", lastQueryDuration, entry.Duration)
	}

	logged := parseLoggedQueryDuration(t, entry.Duration)
	if logged < minLogged || logged > maxLogged {
		t.Fatalf(
			"logged duration %v (%q) outside [%v, %v] for pg_sleep(%v)",
			logged, entry.Duration, minLogged, maxLogged, sleep,
		)
	}
}

func assertWallElapsedInRange(t *testing.T, elapsed, sleep, minWall, maxWall time.Duration) {
	t.Helper()
	if elapsed < minWall || elapsed > maxWall {
		t.Fatalf(
			"wall clock %v outside [%v, %v] for pg_sleep(%v)",
			elapsed.Round(time.Millisecond), minWall, maxWall, sleep,
		)
	}
}

// TestProxyGUIQueryHistoryDurationPgSleep runs pg_sleep through the proxy and checks that the
// duration recorded in query history (GUI log) is close to the sleep interval.
//
// Package budget: tests/unit/proxy already spends ~10s+ on other PostgreSQL integration tests;
// the 10s subtest needs the go test -timeout for that package to be well above 20s (see test.bat).
func TestProxyGUIQueryHistoryDurationPgSleep(t *testing.T) {
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return
	}
	if !isPostgreSQLAvailable(t, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password) {
		t.Skipf("Skipping test - PostgreSQL is not available at %s:%d", cfg.Postgres.Host, cfg.Postgres.Port)
		return
	}

	cases := []struct {
		name         string
		sleep        time.Duration
		queryTimeout time.Duration
		minLogged    time.Duration
		maxLogged    time.Duration
		minWall      time.Duration
		maxWall      time.Duration
	}{
		{
			name:         "short_pg_sleep_100ms",
			sleep:        100 * time.Millisecond,
			queryTimeout: 10 * time.Second,
			minLogged:    70 * time.Millisecond,
			maxLogged:    800 * time.Millisecond,
			minWall:      70 * time.Millisecond,
			maxWall:      800 * time.Millisecond,
		},
		{
			name:         "pg_sleep_1s_simple_query",
			sleep:        time.Second,
			queryTimeout: 10 * time.Second,
			minLogged:    time.Second,
			maxLogged:    1300 * time.Millisecond,
			minWall:      time.Second,
			maxWall:      1300 * time.Millisecond,
		},
		{
			name:         "pg_sleep_10s",
			sleep:        10 * time.Second,
			queryTimeout: 25 * time.Second,
			minLogged:    9 * time.Second,
			maxLogged:    12 * time.Second,
			minWall:      9 * time.Second,
			maxWall:      12 * time.Second,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testID := "gui_hist_timing_" + tc.name
			db, ctx, server, cleanup := connectToProxyForTestWithServer(t, testID)
			defer cleanup()
			if db == nil || server == nil {
				return
			}

			sess := server.PgRollback.GetSession(testID)
			if sess == nil {
				t.Fatal("expected session after connect")
			}

			marker := "pg_sleep_timing_" + tc.name
			sleepSeconds := tc.sleep.Seconds()
			query := fmt.Sprintf("SELECT pg_sleep(%g) -- %s", sleepSeconds, marker)

			qctx, cancel := context.WithTimeout(ctx, tc.queryTimeout)
			defer cancel()

			wallStart := time.Now()
			if _, err := db.ExecContext(qctx, query); err != nil {
				t.Fatalf("pg_sleep query: %v", err)
			}
			wallElapsed := time.Since(wallStart)

			assertWallElapsedInRange(t, wallElapsed, tc.sleep, tc.minWall, tc.maxWall)

			entry := waitForQueryHistoryComplete(t, sess, marker, 3*time.Second)
			if !strings.Contains(entry.Query, "pg_sleep") {
				t.Fatalf("history entry query = %q, want pg_sleep", entry.Query)
			}

			assertLoggedDurationMatchesPgSleep(
				t, entry, sess.GetLastQueryDuration(), tc.sleep, tc.minLogged, tc.maxLogged,
			)

			t.Logf(
				"pg_sleep(%v): wall=%v logged=%q (bounds wall [%v,%v] logged [%v,%v])",
				sleepSeconds, wallElapsed.Round(time.Millisecond), entry.Duration,
				tc.minWall, tc.maxWall, tc.minLogged, tc.maxLogged,
			)
		})
	}
}

// TestProxyGUIQueryHistoryDBProxyDurationSplit verifies the DB-vs-proxy-overhead breakdown added
// alongside query timing: for a query that is almost entirely spent waiting on PostgreSQL
// (pg_sleep), DBDuration should track the total closely and ProxyDuration should be small - the
// sanity check that DB time is measuring the real backend round trip, not just mirroring the total.
func TestProxyGUIQueryHistoryDBProxyDurationSplit(t *testing.T) {
	cfg := getConfigForProxyTest(t)
	if cfg == nil {
		return
	}
	if !isPostgreSQLAvailable(t, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.User, cfg.Postgres.Password) {
		t.Skipf("Skipping test - PostgreSQL is not available at %s:%d", cfg.Postgres.Host, cfg.Postgres.Port)
		return
	}

	testID := "gui_hist_dbproxy_split"
	db, ctx, server, cleanup := connectToProxyForTestWithServer(t, testID)
	defer cleanup()
	if db == nil || server == nil {
		return
	}

	sess := server.PgRollback.GetSession(testID)
	if sess == nil {
		t.Fatal("expected session after connect")
	}

	marker := "pg_sleep_dbproxy_split"
	query := fmt.Sprintf("SELECT pg_sleep(1) -- %s", marker)

	qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := db.ExecContext(qctx, query); err != nil {
		t.Fatalf("pg_sleep query: %v", err)
	}

	entry := waitForQueryHistoryComplete(t, sess, marker, 3*time.Second)

	if entry.DBDuration == "" {
		t.Fatal("DBDuration is empty, want a tracked value for a direct pg_sleep query")
	}
	if entry.ProxyDuration == "" {
		t.Fatal("ProxyDuration is empty, want a tracked value for a direct pg_sleep query")
	}

	total := parseLoggedQueryDuration(t, entry.Duration)
	dbTime := parseLoggedQueryDuration(t, entry.DBDuration)
	proxyTime := parseLoggedQueryDuration(t, entry.ProxyDuration)

	if dbTime+proxyTime != total {
		t.Errorf("DBDuration + ProxyDuration = %v, want exactly Duration = %v", dbTime+proxyTime, total)
	}
	// pg_sleep(1) spends essentially all its time waiting on PostgreSQL, so DB time should
	// dominate and proxy overhead should be a small fraction of the 1s sleep.
	if dbTime < 900*time.Millisecond {
		t.Errorf("DBDuration = %v, want close to the 1s pg_sleep (proxy overhead should be small)", dbTime)
	}
	if proxyTime > 100*time.Millisecond {
		t.Errorf("ProxyDuration = %v, want < 100ms of proxy overhead for a single pg_sleep query", proxyTime)
	}

	t.Logf("pg_sleep(1): total=%v db=%v proxy=%v", total, dbTime, proxyTime)
}
