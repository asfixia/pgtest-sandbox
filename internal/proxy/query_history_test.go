package proxy

import (
	"context"
	"testing"
	"time"

	sqlpkg "pgrollback/pkg/sql"
)

// newTestSessionDB returns a realSessionDB with no real connection (nil conn/tx), suitable for query history unit tests.
func newTestSessionDB() *realSessionDB {
	return newSessionDB(nil, nil, context.Background())
}

// --- isInternalNoiseQuery ---

func TestIsInternalNoiseQuery_Empty(t *testing.T) {
	if !isInternalNoiseQuery("") {
		t.Error("empty string should be noise")
	}
	if !isInternalNoiseQuery("   ") {
		t.Error("whitespace-only should be noise")
	}
}

func TestIsInternalNoiseQuery_Deallocate(t *testing.T) {
	cases := []struct {
		query string
		noise bool
	}{
		{"DEALLOCATE", true},
		{"deallocate", true},
		{"DEALLOCATE pdo_stmt_00000001", true},
		{"DEALLOCATE ALL", true},
		{"  DEALLOCATE pdo_stmt_00000001  ", true},
		{"deallocate\tpdo_stmt_00000001", true},
		{"SELECT 1", false},
		{"DEALLOCATES", false}, // not a real deallocate
	}
	for _, c := range cases {
		got := isInternalNoiseQuery(c.query)
		if got != c.noise {
			t.Errorf("isInternalNoiseQuery(%q) = %v, want %v", c.query, got, c.noise)
		}
	}
}

func TestIsInternalNoiseQuery_ReleaseSavepointIsNotNoise(t *testing.T) {
	// RELEASE SAVEPOINT is a real user query, not noise
	cases := []string{
		"RELEASE SAVEPOINT pgrollback_v_1",
		"release savepoint pgrollback_v_42",
		"RELEASE SAVEPOINT user_sp_1",
		"SAVEPOINT pgrollback_v_1",
		"ROLLBACK TO SAVEPOINT pgrollback_v_1",
	}
	for _, q := range cases {
		if isInternalNoiseQuery(q) {
			t.Errorf("isInternalNoiseQuery(%q) = true, want false", q)
		}
	}
}

func TestIsInternalNoiseQuery_RegularQueries(t *testing.T) {
	regular := []string{
		"SELECT 1",
		"INSERT INTO foo VALUES (1)",
		"UPDATE foo SET bar = 1",
		"DELETE FROM foo",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"SAVEPOINT pgrollback_v_1",
		"set search_path to \"public\"",
	}
	for _, q := range regular {
		if isInternalNoiseQuery(q) {
			t.Errorf("isInternalNoiseQuery(%q) = true, want false", q)
		}
	}
}

// --- SetLastQuery ---

func TestSetLastQuery_Basic(t *testing.T) {
	db := newTestSessionDB()
	db.Gui.SetLastQuery("SELECT 1")
	if got := db.Gui.GetLastQuery(); got != "SELECT 1" {
		t.Errorf("GetLastQuery() = %q, want %q", got, "SELECT 1")
	}
}

func TestSetLastQuery_SkipsNoise(t *testing.T) {
	db := newTestSessionDB()
	db.Gui.SetLastQuery("SELECT 1")
	db.Gui.SetLastQuery("DEALLOCATE pdo_stmt_00000001")
	if got := db.Gui.GetLastQuery(); got != "SELECT 1" {
		t.Errorf("after noise, GetLastQuery() = %q, want %q", got, "SELECT 1")
	}
	hist := db.Gui.GetQueryHistory()
	if len(hist) != 1 {
		t.Errorf("history len = %d, want 1", len(hist))
	}
}

func TestSetLastQuery_SkipsEmpty(t *testing.T) {
	db := newTestSessionDB()
	db.Gui.SetLastQuery("SELECT 1")
	db.Gui.SetLastQuery("")
	if got := db.Gui.GetLastQuery(); got != "SELECT 1" {
		t.Errorf("after empty, GetLastQuery() = %q, want %q", got, "SELECT 1")
	}
}

// TestSetLastQuery_ReturnValueGatesFinalization is the regression test for a bug where a query's
// logged duration could be silently replaced by an unrelated query's timing: SetLastQuery returns
// false for noise (e.g. DEALLOCATE) without appending an entry, but call sites used to defer
// UpdateLastQueryHistoryDuration unconditionally, which finalizes whatever is currently the last
// history entry. In a session shared by multiple connections, a fast DEALLOCATE from one
// connection could finalize a slow, still-running query's entry from another connection with the
// DEALLOCATE's own (tiny) timing - e.g. "SELECT pg_sleep(10)" showing ~2ms instead of ~10s. Call
// sites must call UpdateLastQueryHistoryDuration only when SetLastQuery (or SetLastQueryWithParams)
// returned true; this test verifies that guarded pattern actually protects the entry.
func TestSetLastQuery_ReturnValueGatesFinalization(t *testing.T) {
	db := newTestSessionDB()

	logged := db.Gui.SetLastQuery("SELECT pg_sleep(10)")
	if !logged {
		t.Fatal("SetLastQuery(real query) = false, want true")
	}

	// A second, unrelated connection on the same shared session sends a routine DEALLOCATE while
	// the slow query above is still in flight. Per contract, callers must not finalize when this
	// returns false.
	noiseLogged := db.Gui.SetLastQuery("DEALLOCATE pdo_stmt_00000001")
	if noiseLogged {
		t.Fatal("SetLastQuery(DEALLOCATE) = true, want false (noise)")
	}
	// Correct call-site behavior: skip finalization entirely since noiseLogged is false.

	hist := db.Gui.GetQueryHistory()
	if len(hist) != 1 {
		t.Fatalf("history len = %d, want 1 (DEALLOCATE must not append its own entry)", len(hist))
	}
	if !hist[0].Running {
		t.Error("Running = false after unrelated DEALLOCATE was skipped, want true (pg_sleep still in flight)")
	}
	if hist[0].Duration != "" {
		t.Errorf("Duration = %q after unrelated DEALLOCATE was skipped, want empty (must not be stomped)", hist[0].Duration)
	}

	// The real query now completes; only its own finalize call should ever touch its entry.
	db.Gui.UpdateLastQueryHistoryDuration(10*time.Second, 10*time.Second)
	hist = db.Gui.GetQueryHistory()
	if hist[0].Duration != "10s" {
		t.Errorf("Duration = %q, want %q (the real pg_sleep completion, not the DEALLOCATE's)", hist[0].Duration, "10s")
	}
}

// --- Running state ---

// TestSetLastQuery_MarksRunning verifies a query is logged as Running the moment it starts,
// before any result is known, so the GUI can show it as in-flight (and detect a stale/stuck one).
func TestSetLastQuery_MarksRunning(t *testing.T) {
	db := newTestSessionDB()
	db.Gui.SetLastQuery("SELECT pg_sleep(10)")
	hist := db.Gui.GetQueryHistory()
	if len(hist) != 1 {
		t.Fatalf("history len = %d, want 1", len(hist))
	}
	if !hist[0].Running {
		t.Error("Running = false right after SetLastQuery, want true")
	}
	if hist[0].Duration != "" {
		t.Errorf("Duration = %q right after SetLastQuery, want empty", hist[0].Duration)
	}
}

// TestUpdateLastQueryHistoryDuration_ClearsRunning verifies the state flips to "finished" on
// completion. Callers use defer for this call so it also fires on an error return, which is the
// regression case: a failed query must not stay stuck showing as Running forever.
func TestUpdateLastQueryHistoryDuration_ClearsRunning(t *testing.T) {
	db := newTestSessionDB()
	db.Gui.SetLastQuery("SELECT 1")
	db.Gui.UpdateLastQueryHistoryDuration(5*time.Millisecond, 0)
	hist := db.Gui.GetQueryHistory()
	if len(hist) != 1 {
		t.Fatalf("history len = %d, want 1", len(hist))
	}
	if hist[0].Running {
		t.Error("Running = true after UpdateLastQueryHistoryDuration, want false")
	}
	if hist[0].Duration != "5ms" {
		t.Errorf("Duration = %q, want %q", hist[0].Duration, "5ms")
	}
}

// TestUpdateLastQueryHistoryDuration_SplitsDBAndProxyTime is the regression test for the
// DB-vs-proxy-overhead breakdown shown in the GUI: DBDuration is exactly what was passed in, and
// ProxyDuration is the remainder (total - DB), so the two together always sum back to Duration.
func TestUpdateLastQueryHistoryDuration_SplitsDBAndProxyTime(t *testing.T) {
	db := newTestSessionDB()
	db.Gui.SetLastQuery("SELECT 1")
	db.Gui.UpdateLastQueryHistoryDuration(10*time.Millisecond, 7*time.Millisecond)
	hist := db.Gui.GetQueryHistory()
	if len(hist) != 1 {
		t.Fatalf("history len = %d, want 1", len(hist))
	}
	if hist[0].Duration != "10ms" {
		t.Errorf("Duration = %q, want %q", hist[0].Duration, "10ms")
	}
	if hist[0].DBDuration != "7ms" {
		t.Errorf("DBDuration = %q, want %q", hist[0].DBDuration, "7ms")
	}
	if hist[0].ProxyDuration != "3ms" {
		t.Errorf("ProxyDuration = %q, want %q (Duration - DBDuration)", hist[0].ProxyDuration, "3ms")
	}
}

// TestUpdateLastQueryHistoryDuration_UntrackedDBTimeLeavesFieldsEmpty verifies the dbElapsed<=0
// sentinel (used by call sites that cannot cleanly attribute DB time, e.g. a composite
// multi-statement batch delegated to per-command sub-entries) reports "not tracked" rather than a
// misleading 0ms DB / 100% proxy-overhead split.
func TestUpdateLastQueryHistoryDuration_UntrackedDBTimeLeavesFieldsEmpty(t *testing.T) {
	db := newTestSessionDB()
	db.Gui.SetLastQuery("SELECT 1")
	db.Gui.UpdateLastQueryHistoryDuration(10*time.Millisecond, 0)
	hist := db.Gui.GetQueryHistory()
	if hist[0].DBDuration != "" {
		t.Errorf("DBDuration = %q, want empty (untracked)", hist[0].DBDuration)
	}
	if hist[0].ProxyDuration != "" {
		t.Errorf("ProxyDuration = %q, want empty (untracked)", hist[0].ProxyDuration)
	}
}

// --- Query history ordering ---

func TestQueryHistory_ExecutionOrder(t *testing.T) {
	db := newTestSessionDB()
	queries := []string{"SELECT 1", "SELECT 2", "SELECT 3", "SELECT 4", "SELECT 5"}
	for _, q := range queries {
		db.Gui.SetLastQuery(q)
		time.Sleep(time.Millisecond) // ensure timestamps differ
	}
	hist := db.Gui.GetQueryHistory()
	if len(hist) != len(queries) {
		t.Fatalf("history len = %d, want %d", len(hist), len(queries))
	}
	for i, entry := range hist {
		if entry.Query != queries[i] {
			t.Errorf("hist[%d].Query = %q, want %q", i, entry.Query, queries[i])
		}
	}
	// Timestamps should be non-decreasing
	for i := 1; i < len(hist); i++ {
		if hist[i].At.Before(hist[i-1].At) {
			t.Errorf("hist[%d].At (%v) is before hist[%d].At (%v)", i, hist[i].At, i-1, hist[i-1].At)
		}
	}
}

func TestQueryHistory_HasTimestamp(t *testing.T) {
	db := newTestSessionDB()
	before := time.Now()
	db.Gui.SetLastQuery("SELECT 1")
	after := time.Now()
	hist := db.Gui.GetQueryHistory()
	if len(hist) != 1 {
		t.Fatalf("history len = %d, want 1", len(hist))
	}
	if hist[0].At.Before(before) || hist[0].At.After(after) {
		t.Errorf("timestamp %v not between %v and %v", hist[0].At, before, after)
	}
}

// --- Max history ---

func TestQueryHistory_MaxLimit(t *testing.T) {
	db := newTestSessionDB()
	for i := 0; i < maxQueryHistory+20; i++ {
		db.Gui.SetLastQuery("SELECT " + time.Now().String())
	}
	hist := db.Gui.GetQueryHistory()
	if len(hist) != maxQueryHistory {
		t.Errorf("history len = %d, want %d (max)", len(hist), maxQueryHistory)
	}
}

func TestQueryHistory_MaxPreservesNewest(t *testing.T) {
	db := newTestSessionDB()
	for i := 0; i < maxQueryHistory+5; i++ {
		db.Gui.SetLastQuery("Q" + time.Now().String())
	}
	db.Gui.SetLastQuery("LAST_QUERY")
	hist := db.Gui.GetQueryHistory()
	last := hist[len(hist)-1]
	if last.Query != "LAST_QUERY" {
		t.Errorf("last entry = %q, want %q", last.Query, "LAST_QUERY")
	}
}

// --- ClearQueryHistory ---

func TestClearQueryHistory(t *testing.T) {
	db := newTestSessionDB()
	db.Gui.SetLastQuery("SELECT 1")
	db.Gui.SetLastQuery("SELECT 2")
	db.Gui.ClearQueryHistory()
	if got := db.Gui.GetLastQuery(); got != "" {
		t.Errorf("after clear, GetLastQuery() = %q, want empty", got)
	}
	hist := db.Gui.GetQueryHistory()
	if hist != nil {
		t.Errorf("after clear, GetQueryHistory() = %v, want nil", hist)
	}
}

func TestClearLastQuery(t *testing.T) {
	db := newTestSessionDB()
	db.Gui.SetLastQuery("SELECT 1")
	db.Gui.ClearLastQuery()
	if got := db.Gui.GetLastQuery(); got != "" {
		t.Errorf("after ClearLastQuery, got = %q, want empty", got)
	}
	hist := db.Gui.GetQueryHistory()
	if len(hist) != 0 {
		t.Errorf("history len = %d, want 0 after ClearLastQuery", len(hist))
	}
}

// --- GetQueryHistory returns copy ---

func TestQueryHistory_ReturnsCopy(t *testing.T) {
	db := newTestSessionDB()
	db.Gui.SetLastQuery("SELECT 1")
	hist1 := db.Gui.GetQueryHistory()
	hist1[0].Query = "MODIFIED"
	hist2 := db.Gui.GetQueryHistory()
	if hist2[0].Query == "MODIFIED" {
		t.Error("modifying returned slice should not affect internal state")
	}
}

// --- SubstituteParams (via sql package) ---

func TestSubstituteParams_Basic(t *testing.T) {
	got := sqlpkg.SubstituteParams("SELECT $1, $2", []any{"hello", int32(42)}, "")
	want := "SELECT 'hello', 42"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestSubstituteParams_HighIndexFirst(t *testing.T) {
	// $10 should not be confused with $1
	args := make([]any, 10)
	for i := range args {
		args[i] = i + 1
	}
	got := sqlpkg.SubstituteParams("$1 $10", args, "")
	want := "1 10"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestSubstituteParams_Nil(t *testing.T) {
	got := sqlpkg.SubstituteParams("SELECT $1", []any{nil}, "")
	want := "SELECT NULL"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestSubstituteParams_NoArgs(t *testing.T) {
	got := sqlpkg.SubstituteParams("SELECT 1", nil, "")
	if got != "SELECT 1" {
		t.Errorf("got %q, want %q", got, "SELECT 1")
	}
}

// --- SetLastQueryWithParams ---

func TestSetLastQueryWithParams_Substitutes(t *testing.T) {
	db := newTestSessionDB()
	db.SetLastQueryWithParams("UPDATE foo SET bar = $1 WHERE id = $2", []any{"value", int32(123)}, "")
	got := db.Gui.GetLastQuery()
	want := "UPDATE foo SET bar = 'value' WHERE id = 123"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestSetLastQueryWithParams_NoArgs(t *testing.T) {
	db := newTestSessionDB()
	db.SetLastQueryWithParams("SELECT 1", nil, "")
	if got := db.Gui.GetLastQuery(); got != "SELECT 1" {
		t.Errorf("got %q, want %q", got, "SELECT 1")
	}
}

func TestSetLastQueryWithParams_EmptyArgs(t *testing.T) {
	db := newTestSessionDB()
	db.SetLastQueryWithParams("SELECT 1", []any{}, "")
	if got := db.Gui.GetLastQuery(); got != "SELECT 1" {
		t.Errorf("got %q, want %q", got, "SELECT 1")
	}
}

func TestSetLastQueryWithParams_SkipsNoise(t *testing.T) {
	db := newTestSessionDB()
	db.Gui.SetLastQuery("SELECT 1")
	db.SetLastQueryWithParams("DEALLOCATE pdo_stmt_00000001", []any{"ignored"}, "")
	if got := db.Gui.GetLastQuery(); got != "SELECT 1" {
		t.Errorf("noise should be skipped, got %q", got)
	}
}

// --- HasOpenUserTransaction ---

func TestHasOpenUserTransaction(t *testing.T) {
	db := newTestSessionDB()
	if db.HasOpenUserTransaction() {
		t.Error("new session should not have open user transaction")
	}
	// Simulate a user BEGIN
	if err := db.ClaimOpenTransaction(ConnectionID(1)); err != nil {
		t.Fatal(err)
	}
	if !db.HasOpenUserTransaction() {
		t.Error("after ClaimOpenTransaction, should have open user transaction")
	}
	// Release
	db.ReleaseOpenTransaction(ConnectionID(1))
	if db.HasOpenUserTransaction() {
		t.Error("after ReleaseOpenTransaction, should not have open user transaction")
	}
}
