package proxy

import (
	"fmt"
	"net/http"

	"pgrollback/internal/proxy/gui"
)

// sessionProviderAdapter adapts *Server to gui.SessionProvider so the GUI package does not import proxy.
type sessionProviderAdapter struct {
	s *Server
}

func (a *sessionProviderAdapter) GetSessions() []gui.SessionInfo {
	sessions := a.s.PgRollback.GetAllSessions()
	list := make([]gui.SessionInfo, 0, len(sessions))
	for testID := range sessions {
		if info, ok := a.s.PgRollback.sessionInfoWithoutLockStatus(testID); ok {
			list = append(list, info)
		}
	}
	a.s.PgRollback.enrichSessionsLockStatus(list)
	return list
}

// Subscribe forwards to the PgRollback event hub so the SSE handler can stream session/query
// lifecycle events without polling.
func (a *sessionProviderAdapter) Subscribe() (<-chan gui.Event, func()) {
	return a.s.PgRollback.Events.Subscribe(32)
}

func (a *sessionProviderAdapter) DestroySession(testID string) error {
	return a.s.PgRollback.DestroySession(testID)
}

func (a *sessionProviderAdapter) ClearHistory(testID string) error {
	session := a.s.PgRollback.GetSession(testID)
	if session == nil {
		return fmt.Errorf("session not found")
	}
	if session.DB != nil {
		session.DB.Gui.ClearQueryHistory()
	}
	a.s.PgRollback.PublishSessionUpdate(testID)
	return nil
}

func (a *sessionProviderAdapter) DestroyAllSessions() (int, error) {
	sessions := a.s.PgRollback.GetAllSessions()
	n := 0
	for testID, session := range sessions {
		session.Cancel() // unblock any in-flight query so DestroySession can acquire locks
		if err := a.s.PgRollback.DestroySession(testID); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// guiMux returns the HTTP handler for the GUI (same-port: /, /gui, /gui/, /api/...).
func guiMux(server *Server) http.Handler {
	return gui.NewMux(&sessionProviderAdapter{s: server})
}

// StartGUIServer starts the GUI HTTP server on a separate port (backward compatibility).
// Prefer same-port GUI via NewServer(..., true) so the GUI is at http://host:port/gui.
func StartGUIServer(server *Server, host string, port int) (stop func(), err error) {
	return gui.StartGUIServer(&sessionProviderAdapter{s: server}, host, port)
}
