package gui

// QueryHistoryItem is one entry in the session's query history (with timestamp for display).
type QueryHistoryItem struct {
	Query string `json:"query"`
	At    string `json:"at"` // RFC3339 or similar for display
}

// SessionInfo is the JSON shape for one session in the GUI API.
type SessionInfo struct {
	TestID        string             `json:"test_id"`
	InTransaction bool               `json:"in_transaction"` // true if session has an active (open) transaction
	LastQuery     string             `json:"last_query"`
	QueryHistory  []QueryHistoryItem `json:"query_history"`  // last executed queries (oldest first), max 100
}

// SessionProvider supplies session data and close for the GUI. Implemented by the proxy.
type SessionProvider interface {
	GetSessions() []SessionInfo
	DestroySession(testID string) error
	ClearHistory(testID string) error
	// DestroyAllSessions disconnects all clients (rollback all sessions). Returns count destroyed.
	DestroyAllSessions() (int, error)
}
