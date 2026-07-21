package proxy

import (
	"context"
	"strings"
	"testing"
	"time"

	"pgrollback/internal/proxy/gui"
)

// TestSessionProviderAdapter_GetSessions_DoesNotBlockOnRunningQuery is the regression test for
// the bug that made the GUI freeze while a query was running: GetSessions() (and therefore the
// SSE snapshot/poll fallback) used to call HasOpenUserTransaction(), which RLock'd the same
// mutex that SafeQuery/SafeExec/SafeExecTCL hold write-locked for the whole duration of a query
// (via LockRun). GetSessions must return promptly even while that lock is held.
func TestSessionProviderAdapter_GetSessions_DoesNotBlockOnRunningQuery(t *testing.T) {
	p := NewPgRollback("127.0.0.1", 1, "db", "u", "p", time.Minute, time.Hour, 0)
	id := testIDFor(t)
	sess := registerTestSessionForGUI(t, p, id)
	a := newAdapterWithRollback(p)

	sess.DB.LockRun() // simulate a long-running query holding the main lock
	defer sess.DB.UnlockRun()

	done := make(chan []gui.SessionInfo, 1)
	go func() {
		done <- a.GetSessions()
	}()

	select {
	case list := <-done:
		if len(list) != 1 || list[0].TestID != id {
			t.Fatalf("GetSessions() = %+v, want one session %q", list, id)
		}
	case <-time.After(time.Second):
		t.Fatal("GetSessions() blocked while a query held the main lock (regression)")
	}
}

// registerTestSessionForGUI inserts a session with an in-memory DB (no PostgreSQL) so
// sessionProviderAdapter and DestroySession can be exercised in unit tests.
func registerTestSessionForGUI(t *testing.T, p *PgRollback, testID string) *TestSession {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ts := &TestSession{
		DB:           newTestSessionDB(),
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
		ctx:          ctx,
		cancel:       cancel,
	}
	p.mu.Lock()
	p.SessionsByTestID[testID] = ts
	p.mu.Unlock()
	return ts
}

func testIDFor(t *testing.T) string {
	return "gui-adapter-" + strings.ReplaceAll(t.Name(), "/", "_")
}

func newAdapterWithRollback(p *PgRollback) *sessionProviderAdapter {
	return &sessionProviderAdapter{s: &Server{PgRollback: p}}
}

// TestSessionProviderAdapter_GetSessions_QueryHistoryIncreasesWithEachRecordedQuery
// ensures one recorded query yields one history entry in the JSON/API view (same data the GUI polls).
func TestSessionProviderAdapter_GetSessions_QueryHistoryIncreasesWithEachRecordedQuery(t *testing.T) {
	p := NewPgRollback("127.0.0.1", 1, "db", "u", "p", time.Minute, time.Hour, 0)
	id := testIDFor(t)
	registerTestSessionForGUI(t, p, id)
	a := newAdapterWithRollback(p)

	initial := a.GetSessions()
	if len(initial) != 1 || initial[0].TestID != id {
		t.Fatalf("GetSessions() = %+v, want one session %q", initial, id)
	}
	if len(initial[0].QueryHistory) != 0 {
		t.Fatalf("initial QueryHistory len = %d, want 0", len(initial[0].QueryHistory))
	}

	sess := p.GetSession(id)
	if sess == nil || sess.DB == nil {
		t.Fatal("expected session with DB")
	}
	sess.DB.Gui.SetLastQuery("SELECT 1")

	list := a.GetSessions()
	if len(list) != 1 {
		t.Fatalf("len = %d, want 1", len(list))
	}
	if len(list[0].QueryHistory) != 1 {
		t.Fatalf("after one SetLastQuery, QueryHistory len = %d, want 1", len(list[0].QueryHistory))
	}
	if list[0].QueryHistory[0].Query != "SELECT 1" {
		t.Errorf("QueryHistory[0].Query = %q, want %q", list[0].QueryHistory[0].Query, "SELECT 1")
	}
	if list[0].LastQuery != "SELECT 1" {
		t.Errorf("LastQuery = %q, want %q", list[0].LastQuery, "SELECT 1")
	}

	sess.DB.Gui.SetLastQuery("SELECT 2")
	list = a.GetSessions()
	if len(list[0].QueryHistory) != 2 {
		t.Fatalf("after second query, QueryHistory len = %d, want 2", len(list[0].QueryHistory))
	}
}

// TestSessionProviderAdapter_ClearHistory_ClearsQueryHistoryInGetSessions verifies the clear-log
// API path empties history for the next GET /api/sessions poll.
func TestSessionProviderAdapter_ClearHistory_ClearsQueryHistoryInGetSessions(t *testing.T) {
	p := NewPgRollback("127.0.0.1", 1, "db", "u", "p", time.Minute, time.Hour, 0)
	id := testIDFor(t)
	registerTestSessionForGUI(t, p, id)
	a := newAdapterWithRollback(p)

	sess := p.GetSession(id)
	sess.DB.Gui.SetLastQuery("SELECT a")
	sess.DB.Gui.SetLastQuery("SELECT b")

	if err := a.ClearHistory(id); err != nil {
		t.Fatalf("ClearHistory: %v", err)
	}

	list := a.GetSessions()
	if len(list) != 1 {
		t.Fatalf("len = %d, want 1", len(list))
	}
	if len(list[0].QueryHistory) != 0 {
		t.Errorf("after ClearHistory, QueryHistory len = %d, want 0", len(list[0].QueryHistory))
	}
	if list[0].LastQuery != "" {
		t.Errorf("after ClearHistory, LastQuery = %q, want empty", list[0].LastQuery)
	}
}

// TestSessionProviderAdapter_GetSessions_RunningReflectsInFlightQuery verifies the Running flag
// (used by the GUI to show "(running…)" and detect a stale query) goes true the moment a query
// is logged and false once it finishes, both at the QueryHistory entry and SessionInfo level.
func TestSessionProviderAdapter_GetSessions_RunningReflectsInFlightQuery(t *testing.T) {
	p := NewPgRollback("127.0.0.1", 1, "db", "u", "p", time.Minute, time.Hour, 0)
	id := testIDFor(t)
	sess := registerTestSessionForGUI(t, p, id)
	a := newAdapterWithRollback(p)

	sess.DB.Gui.SetLastQuery("SELECT pg_sleep(10)")
	list := a.GetSessions()
	if len(list) != 1 {
		t.Fatalf("len = %d, want 1", len(list))
	}
	if !list[0].Running {
		t.Error("SessionInfo.Running = false while query in flight, want true")
	}
	if len(list[0].QueryHistory) != 1 || !list[0].QueryHistory[0].Running {
		t.Errorf("QueryHistory[0].Running = %v, want true", list[0].QueryHistory)
	}

	sess.DB.Gui.UpdateLastQueryHistoryDuration(5*time.Millisecond, 0)
	list = a.GetSessions()
	if list[0].Running {
		t.Error("SessionInfo.Running = true after query finished, want false")
	}
	if list[0].QueryHistory[0].Running {
		t.Error("QueryHistory[0].Running = true after query finished, want false")
	}
}

// TestSessionProviderAdapter_DestroySession_SessionNoLongerAppearsInGetSessions verifies disconnect/close
// removes the session from the list the GUI renders.
func TestSessionProviderAdapter_DestroySession_SessionNoLongerAppearsInGetSessions(t *testing.T) {
	p := NewPgRollback("127.0.0.1", 1, "db", "u", "p", time.Minute, time.Hour, 0)
	id := testIDFor(t)
	registerTestSessionForGUI(t, p, id)
	a := newAdapterWithRollback(p)

	p.GetSession(id).DB.Gui.SetLastQuery("SELECT stay-until-disconnect")

	if len(a.GetSessions()) != 1 {
		t.Fatal("expected one session before destroy")
	}

	if err := a.DestroySession(id); err != nil {
		t.Fatalf("DestroySession: %v", err)
	}

	if p.GetSession(id) != nil {
		t.Error("GetSession should be nil after DestroySession")
	}
	list := a.GetSessions()
	if len(list) != 0 {
		t.Errorf("GetSessions() len = %d, want 0 (session must not appear after disconnect)", len(list))
	}
}
