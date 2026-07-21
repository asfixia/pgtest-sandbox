package proxy

import (
	"strings"
	"time"

	sqlpkg "pgrollback/pkg/sql"
)

const maxQueryHistory = 100

// QueryHistoryEntry is one item in the session's query history (for GUI and internal storage).
type QueryHistoryEntry struct {
	Query    string
	At       time.Time
	Duration string // total wall-clock time the proxy spent on this query, e.g. "12.345ms"; set when query completes
	// DBDuration is the portion of Duration spent in the actual round trip(s) to the real
	// PostgreSQL backend. Empty when not tracked for this query (e.g. a composite multi-statement
	// batch whose sub-commands are logged as their own entries - see SafeForwardMultipleCommandsToDB).
	DBDuration string
	// ProxyDuration is Duration minus DBDuration: time the proxy itself spent on this query
	// (interception, protocol handling, GUI logging) that isn't a PostgreSQL round trip. Computed
	// once in UpdateLastQueryHistoryDuration so the GUI never has to parse/subtract duration
	// strings itself. Empty whenever DBDuration is empty.
	ProxyDuration string
	Running       bool // true from the moment the query is logged until UpdateLastQueryHistoryDuration runs (success or error)
}

// isInternalNoiseQuery returns true for standard driver/internal queries we don't want in the GUI history.
// - DEALLOCATE [name]: sent by many drivers after each prepared statement use (expected protocol cleanup).
func isInternalNoiseQuery(query string) bool {
	q := strings.TrimSpace(query)
	if q == "" {
		return true
	}
	stmts, err := sqlpkg.ParseStatements(q)
	if err != nil || len(stmts) == 0 || stmts[0].Stmt == nil {
		uq := strings.ToUpper(q)
		return strings.HasPrefix(uq, "DEALLOCATE") && (len(uq) == 10 || (len(uq) > 10 && (uq[10] == ' ' || uq[10] == '\t')))
	}
	return sqlpkg.IsDeallocateNoise(stmts[0].Stmt)
}

// SetLastQuery appends the query to the session's query history (max maxQueryHistory), marked
// Running until UpdateLastQueryHistoryDuration runs. Internal noise queries (e.g. DEALLOCATE
// from the driver) are not recorded. Returns whether an entry was appended: callers MUST skip
// the matching UpdateLastQueryHistoryDuration call when this is false, since that call blindly
// finalizes whatever is currently the last entry - if this query logged nothing (e.g. a
// DEALLOCATE from one connection while another connection's real query is still the last entry
// in this shared session's history), calling it anyway stomps that unrelated entry's duration.
func (g *guiState) SetLastQuery(query string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if isInternalNoiseQuery(query) {
		return false
	}
	g.queryHistory = append(g.queryHistory, QueryHistoryEntry{Query: query, At: time.Now(), Duration: "", Running: true})
	if len(g.queryHistory) > maxQueryHistory {
		g.queryHistory = g.queryHistory[1:]
	}
	return true
}

// SetLastQueryWithParams stores the query with $1, $2, ... substituted by the given args (for extended protocol).
// connLabel is optional (e.g. connection remote address) and is prepended in the stored query for GUI.
// Returns whether an entry was appended (see SetLastQuery) - callers must use this to decide
// whether to finalize a matching UpdateLastQueryHistoryDuration call.
func (d *realSessionDB) SetLastQueryWithParams(query string, args []any, connLabel string) bool {
	if len(args) == 0 {
		return d.Gui.SetLastQuery(query)
	}
	resolved := sqlpkg.SubstituteParams(query, args, connLabel)
	return d.Gui.SetLastQuery(resolved)
}

// GetQueryHistory returns a copy of the last executed queries with timestamps (oldest first), at most maxQueryHistory.
func (g *guiState) GetQueryHistory() []QueryHistoryEntry {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if len(g.queryHistory) == 0 {
		return nil
	}
	out := make([]QueryHistoryEntry, len(g.queryHistory))
	copy(out, g.queryHistory)
	return out
}

// GetLastQueryDuration returns the duration of the last query in history (for GUI "last query" column), or "" if none.
func (g *guiState) GetLastQueryDuration() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if len(g.queryHistory) == 0 {
		return ""
	}
	return g.queryHistory[len(g.queryHistory)-1].Duration
}

// UpdateLastQueryHistoryDuration sets the total and DB-only duration of the most recently
// appended query and clears Running. Call exactly once after the query finishes, on every exit
// path (success or error) — callers use defer for this so a failed query never gets stuck showing
// as Running.
//
// dbElapsed is the portion of elapsed spent in the actual round trip(s) to PostgreSQL; pass 0 when
// that isn't tracked for this call site (see DBDuration on QueryHistoryEntry), which leaves
// DBDuration/ProxyDuration empty rather than reporting a misleading split.
func (g *guiState) UpdateLastQueryHistoryDuration(elapsed, dbElapsed time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.queryHistory) == 0 {
		return
	}
	last := &g.queryHistory[len(g.queryHistory)-1]
	last.Running = false
	if elapsed == 0 {
		last.Duration = ""
	} else {
		last.Duration = elapsed.String()
	}
	if dbElapsed <= 0 {
		last.DBDuration = ""
		last.ProxyDuration = ""
		return
	}
	last.DBDuration = dbElapsed.String()
	last.ProxyDuration = (elapsed - dbElapsed).String()
}

// ClearLastQuery removes the last query from history so GetLastQuery() returns "" or the previous query.
func (g *guiState) ClearLastQuery() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.queryHistory) > 0 {
		g.queryHistory = g.queryHistory[:len(g.queryHistory)-1]
	}
}

// ClearQueryHistory clears the query history (called when session is closed or via GUI).
func (g *guiState) ClearQueryHistory() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.queryHistory = nil
}
