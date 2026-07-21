package gui

// QueryHistoryItem is one entry in the session's query history (with timestamp and duration for display).
type QueryHistoryItem struct {
	Query    string `json:"query"`
	At       string `json:"at"`       // RFC3339 or similar for display; also the query's start time while Running
	Duration string `json:"duration"` // total time the proxy spent handling this query, e.g. "12.345ms"; empty while Running
	// DBDuration is the portion of Duration spent in the actual round trip(s) to the real
	// PostgreSQL backend; ProxyDuration is Duration minus DBDuration (proxy-side overhead: query
	// interception, protocol handling, GUI logging). Both empty when not tracked for this entry
	// (e.g. a composite multi-statement batch - see DBDuration on proxy.QueryHistoryEntry).
	DBDuration    string `json:"db_duration,omitempty"`
	ProxyDuration string `json:"proxy_duration,omitempty"`
	Running       bool   `json:"running"` // true from the moment the query is logged until it finishes (success or error)
}

// LockStatus describes whether a session backend is waiting on a PostgreSQL lock and who blocks it.
// Populated via read-only catalog queries (pg_locks / pg_stat_activity) on a separate inspector connection.
type LockStatus struct {
	WaitingOnLock          bool   `json:"waiting_on_lock"`
	WaitEventType          string `json:"wait_event_type,omitempty"`
	WaitEvent              string `json:"wait_event,omitempty"`
	LockedRelation         string `json:"locked_relation,omitempty"`
	LockMode               string `json:"lock_mode,omitempty"`
	BlockerPID             int32  `json:"blocker_pid,omitempty"`
	BlockerApplicationName string `json:"blocker_application_name,omitempty"`
	BlockerIsPgrollback    bool   `json:"blocker_is_pgrollback"`
	BlockerTestID          string `json:"blocker_test_id,omitempty"` // set when BlockerIsPgrollback is true
	BlockerQuerySnippet    string `json:"blocker_query_snippet,omitempty"`
}

// SessionInfo is the JSON shape for one session in the GUI API.
type SessionInfo struct {
	TestID            string             `json:"test_id"`
	InTransaction     bool               `json:"in_transaction"`     // true if session has an active (open) transaction
	LockStatus        *LockStatus        `json:"lock_status,omitempty"`
	LastQuery         string             `json:"last_query"`
	LastQueryDuration string             `json:"last_query_duration"` // e.g. "12.345ms" for GUI display
	Running           bool               `json:"running"`             // true while the most recent query is still executing
	QueryHistory      []QueryHistoryItem `json:"query_history"`       // last executed queries (oldest first), max 100
}

// SessionProvider supplies session data and close for the GUI. Implemented by the proxy.
type SessionProvider interface {
	GetSessions() []SessionInfo
	DestroySession(testID string) error
	ClearHistory(testID string) error
	// DestroyAllSessions disconnects all clients (rollback all sessions). Returns count destroyed.
	DestroyAllSessions() (int, error)
	// Subscribe registers for session/query lifecycle events (for the SSE stream). Call the
	// returned unsubscribe func exactly once when the subscriber goes away.
	Subscribe() (<-chan Event, func())
}
