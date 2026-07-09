package gui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"pgrollback/internal/config"
)

const sseHeartbeatInterval = 15 * time.Second

// handleAPISessionsStream serves the session/query event stream over SSE. It writes an initial
// "snapshot" frame so the client can paint immediately, then forwards events published via
// provider.Subscribe() as they happen (query started/finished, session created/destroyed, etc.)
// instead of the client polling GET /api/sessions on a timer.
func handleAPISessionsStream(provider SessionProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)

		writeEvent := func(e Event) bool {
			data, err := json.Marshal(e)
			if err != nil {
				return true
			}
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.Type, data); err != nil {
				return false
			}
			flusher.Flush()
			return true
		}

		if !writeEvent(Event{Type: EventSnapshot, Sessions: provider.GetSessions()}) {
			return
		}

		ch, unsubscribe := provider.Subscribe()
		defer unsubscribe()

		ticker := time.NewTicker(sseHeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case e := <-ch:
				if !writeEvent(e) {
					return
				}
			case <-ticker.C:
				if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
}

// ConfigResponse is the config returned by GET /api/config.
// PostgresConnectionStringMasked is always derived from the same in-memory postgres settings as the proxy (via config.PostgresConnStringMasked), not stored separately.
type ConfigResponse struct {
	ConfigPath                     string         `json:"config_path"`
	Config                         *config.Config `json:"config"`
	PostgresConnectionStringMasked string         `json:"postgres_connection_string_masked"`
}

func handleAPISessions(provider SessionProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		list := provider.GetSessions()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(list)
	}
}

func handleAPISessionsClose(provider SessionProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var testID string
		if ct := r.Header.Get("Content-Type"); strings.Contains(ct, "application/json") {
			var body struct {
				TestID string `json:"test_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "invalid JSON", http.StatusBadRequest)
				return
			}
			testID = body.TestID
		} else {
			testID = r.URL.Query().Get("test_id")
			if testID == "" {
				testID = r.FormValue("test_id")
			}
		}
		if testID == "" {
			http.Error(w, "test_id required", http.StatusBadRequest)
			return
		}
		if err := provider.DestroySession(testID); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}
}

func handleAPISessionsClearHistory(provider SessionProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var testID string
		if ct := r.Header.Get("Content-Type"); strings.Contains(ct, "application/json") {
			var body struct {
				TestID string `json:"test_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "invalid JSON", http.StatusBadRequest)
				return
			}
			testID = body.TestID
		} else {
			testID = r.URL.Query().Get("test_id")
			if testID == "" {
				testID = r.FormValue("test_id")
			}
		}
		if testID == "" {
			http.Error(w, "test_id required", http.StatusBadRequest)
			return
		}
		if err := provider.ClearHistory(testID); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}
}

func handleAPISessionsRollbackAll(provider SessionProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		n, err := provider.DestroyAllSessions()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int{"destroyed": n})
	}
}

func handleAPISessionsDisconnectAll(provider SessionProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		n, err := provider.DestroyAllSessions()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int{"destroyed": n})
	}
}

func handleAPIConfigGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	cfg, ok := config.GetCfgIfSet()
	if !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "config not initialized"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ConfigResponse{
		// Use EffectiveConfigPath so the UI always sees the path it will
		// use when saving (even if the file didn't exist at startup).
		ConfigPath:                     config.EffectiveConfigPath(),
		Config:                         config.ConfigForAPI(cfg),
		PostgresConnectionStringMasked: config.PostgresConnStringMasked(&cfg.Postgres),
	})
}

func handleAPIConfigSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var payload struct {
		Config     *config.Config `json:"config"`
		ConfigPath string         `json:"config_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if payload.Config == nil {
		http.Error(w, "config required", http.StatusBadRequest)
		return
	}
	// Determine which path to save to: user-provided or default.
	path := strings.TrimSpace(payload.ConfigPath)
	if path == "" {
		path = config.EffectiveConfigPath()
	}
	config.SetConfigPath(path)
	if err := config.UpdateAndSave(payload.Config); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}
