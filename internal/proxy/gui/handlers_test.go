package gui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// mockProvider implements SessionProvider for testing.
type mockProvider struct {
	sessions       []SessionInfo
	destroyed      []string
	clearedHistory []string
	destroyErr     error
	clearErr       error
}

func (m *mockProvider) GetSessions() []SessionInfo {
	return m.sessions
}

func (m *mockProvider) DestroySession(testID string) error {
	m.destroyed = append(m.destroyed, testID)
	return m.destroyErr
}

func (m *mockProvider) ClearHistory(testID string) error {
	m.clearedHistory = append(m.clearedHistory, testID)
	return m.clearErr
}

func (m *mockProvider) DestroyAllSessions() (int, error) {
	n := len(m.sessions)
	for _, s := range m.sessions {
		m.destroyed = append(m.destroyed, s.TestID)
	}
	m.sessions = nil
	return n, nil
}

// --- GET /api/sessions ---

func TestHandleAPISessions_ReturnsJSON(t *testing.T) {
	provider := &mockProvider{
		sessions: []SessionInfo{
			{TestID: "test-1", InTransaction: true, LastQuery: "SELECT 1", QueryHistory: []QueryHistoryItem{{Query: "SELECT 1", At: "2026-01-01T00:00:00Z"}}},
			{TestID: "test-2", InTransaction: false, LastQuery: "INSERT INTO foo VALUES (1)"},
		},
	}
	mux := NewMux(provider)
	req := httptest.NewRequest(http.MethodGet, "/api/sessions", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var got []SessionInfo
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
}

func TestHandleAPISessions_EmptyList(t *testing.T) {
	provider := &mockProvider{sessions: []SessionInfo{}}
	mux := NewMux(provider)
	req := httptest.NewRequest(http.MethodGet, "/api/sessions", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var got []SessionInfo
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
}

func TestHandleAPISessions_MethodNotAllowed(t *testing.T) {
	provider := &mockProvider{}
	mux := NewMux(provider)
	req := httptest.NewRequest(http.MethodPost, "/api/sessions", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

// --- POST /api/sessions/close ---

func TestHandleAPISessionsClose_JSON(t *testing.T) {
	provider := &mockProvider{}
	mux := NewMux(provider)
	body := `{"test_id":"test-abc"}`
	req := httptest.NewRequest(http.MethodPost, "/api/sessions/close", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if len(provider.destroyed) != 1 || provider.destroyed[0] != "test-abc" {
		t.Errorf("destroyed = %v, want [test-abc]", provider.destroyed)
	}
}

func TestHandleAPISessionsClose_MissingTestID(t *testing.T) {
	provider := &mockProvider{}
	mux := NewMux(provider)
	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/api/sessions/close", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleAPISessionsClose_NotFound(t *testing.T) {
	provider := &mockProvider{destroyErr: fmt.Errorf("not found")}
	mux := NewMux(provider)
	body := `{"test_id":"no-such"}`
	req := httptest.NewRequest(http.MethodPost, "/api/sessions/close", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleAPISessionsClose_MethodNotAllowed(t *testing.T) {
	provider := &mockProvider{}
	mux := NewMux(provider)
	req := httptest.NewRequest(http.MethodGet, "/api/sessions/close", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

// --- POST /api/sessions/clear-history ---

func TestHandleAPIClearHistory_JSON(t *testing.T) {
	provider := &mockProvider{}
	mux := NewMux(provider)
	body := `{"test_id":"test-xyz"}`
	req := httptest.NewRequest(http.MethodPost, "/api/sessions/clear-history", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if len(provider.clearedHistory) != 1 || provider.clearedHistory[0] != "test-xyz" {
		t.Errorf("clearedHistory = %v, want [test-xyz]", provider.clearedHistory)
	}
}

func TestHandleAPIClearHistory_MissingTestID(t *testing.T) {
	provider := &mockProvider{}
	mux := NewMux(provider)
	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/api/sessions/clear-history", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleAPIClearHistory_NotFound(t *testing.T) {
	provider := &mockProvider{clearErr: fmt.Errorf("session not found")}
	mux := NewMux(provider)
	body := `{"test_id":"no-such"}`
	req := httptest.NewRequest(http.MethodPost, "/api/sessions/clear-history", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleAPIClearHistory_MethodNotAllowed(t *testing.T) {
	provider := &mockProvider{}
	mux := NewMux(provider)
	req := httptest.NewRequest(http.MethodGet, "/api/sessions/clear-history", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

// --- POST /api/sessions/rollback-all ---

func TestHandleAPISessionsRollbackAll_ReturnsDestroyedCount(t *testing.T) {
	provider := &mockProvider{
		sessions: []SessionInfo{
			{TestID: "a", InTransaction: true},
			{TestID: "b", InTransaction: false},
		},
	}
	mux := NewMux(provider)
	req := httptest.NewRequest(http.MethodPost, "/api/sessions/rollback-all", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var out struct {
		Destroyed int `json:"destroyed"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Destroyed != 2 {
		t.Errorf("destroyed = %d, want 2", out.Destroyed)
	}
	if len(provider.destroyed) != 2 {
		t.Errorf("provider.destroyed length = %d, want 2", len(provider.destroyed))
	}
}

func TestHandleAPISessionsRollbackAll_EmptySessions(t *testing.T) {
	provider := &mockProvider{sessions: nil}
	mux := NewMux(provider)
	req := httptest.NewRequest(http.MethodPost, "/api/sessions/rollback-all", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var out struct {
		Destroyed int `json:"destroyed"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Destroyed != 0 {
		t.Errorf("destroyed = %d, want 0", out.Destroyed)
	}
}

// --- Sessions JSON shape ---

func TestSessionInfo_JSONShape(t *testing.T) {
	info := SessionInfo{
		TestID:        "test-1",
		InTransaction: true,
		LastQuery:     "SELECT 1",
		QueryHistory: []QueryHistoryItem{
			{Query: "SELECT 1", At: "2026-01-01T00:00:00Z"},
		},
	}
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	// Check that JSON keys match the expected shape
	for _, key := range []string{`"test_id"`, `"in_transaction"`, `"last_query"`, `"query_history"`, `"query"`, `"at"`} {
		if !strings.Contains(s, key) {
			t.Errorf("JSON missing key %s: %s", key, s)
		}
	}
	// Should NOT contain "level"
	if strings.Contains(s, `"level"`) {
		t.Errorf("JSON should not contain 'level': %s", s)
	}
}

// --- GUI home ---

func TestServeHome_RootReturnsHTML(t *testing.T) {
	provider := &mockProvider{}
	mux := NewMux(provider)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "<!DOCTYPE html>") {
		t.Error("body should contain HTML doctype")
	}
	if !strings.Contains(body, "PGTest") {
		t.Error("body should contain PGTest title")
	}
}

func TestServeHome_GuiPathReturnsHTML(t *testing.T) {
	provider := &mockProvider{}
	mux := NewMux(provider)
	for _, path := range []string{"/gui", "/gui/"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s: status = %d, want %d", path, rec.Code, http.StatusOK)
		}
	}
}

func TestServeHome_UnknownPath404(t *testing.T) {
	provider := &mockProvider{}
	mux := NewMux(provider)
	req := httptest.NewRequest(http.MethodGet, "/unknown", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// --- HTML generation ---

func TestHTML_ContainsAPIBase(t *testing.T) {
	html := HTML()
	if strings.Contains(html, "__API_BASE__") {
		t.Error("HTML() should not contain the placeholder __API_BASE__")
	}
	if !strings.Contains(html, "/api/sessions") {
		t.Error("HTML() should contain /api/sessions")
	}
}

func TestHTMLWithBase_CustomBase(t *testing.T) {
	html := HTMLWithBase("/myprefix")
	if strings.Contains(html, "__API_BASE__") {
		t.Error("HTMLWithBase should not contain placeholder")
	}
	if !strings.Contains(html, "/myprefix/api/sessions") {
		t.Error("HTMLWithBase('/myprefix') should contain /myprefix/api/sessions")
	}
}

func TestHTMLWithBase_EmptyBase(t *testing.T) {
	html := HTMLWithBase("")
	if !strings.Contains(html, "/api/sessions") {
		t.Error("HTMLWithBase('') should contain /api/sessions")
	}
}

func TestHTMLWithBase_SlashBase(t *testing.T) {
	html := HTMLWithBase("/")
	if !strings.Contains(html, "/api/sessions") {
		t.Error("HTMLWithBase('/') should contain /api/sessions")
	}
}
