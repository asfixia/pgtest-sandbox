package gui

// QueryHistoryItem is one entry in the session's query history (with timestamp and duration for display).
type QueryHistoryItem struct {
	Query    string `json:"query"`
	At       string `json:"at"`       // RFC3339 or similar for display; also the query's start time while Running
	Duration string `json:"duration"` // execution time e.g. "12.345ms"; empty while Running
	Running  bool   `json:"running"`  // true from the moment the query is logged until it finishes (success or error)
}

// SessionInfo is the JSON shape for one session in the GUI API.
type SessionInfo struct {
	TestID            string             `json:"test_id"`
	InTransaction     bool               `json:"in_transaction"`     // true if session has an active (open) transaction
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
