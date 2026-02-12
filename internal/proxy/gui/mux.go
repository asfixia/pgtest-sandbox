package gui

import (
	"net/http"
)

// NewMux returns an HTTP handler that serves the GUI (sessions table, settings, API).
// Use the same handler for both same-port and standalone GUI.
func NewMux(provider SessionProvider) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", serveHome)
	mux.HandleFunc("/gui", serveHome)
	mux.HandleFunc("/gui/", serveHome)
	mux.HandleFunc("/api/sessions", handleAPISessions(provider))
	mux.HandleFunc("/api/sessions/close", handleAPISessionsClose(provider))
	mux.HandleFunc("/api/sessions/clear-history", handleAPISessionsClearHistory(provider))
	mux.HandleFunc("/api/sessions/rollback-all", handleAPISessionsRollbackAll(provider))
	mux.HandleFunc("/api/config", handleAPIConfigGet)
	mux.HandleFunc("/api/config/save", handleAPIConfigSave)
	return mux
}

func serveHome(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	if p != "/" && p != "/gui" && p != "/gui/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(HTML()))
}
