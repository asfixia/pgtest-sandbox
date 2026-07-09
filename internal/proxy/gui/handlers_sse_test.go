package gui

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// readSSEFrame reads one "event: <type>\ndata: <json>\n\n" frame (skipping blank/comment lines
// such as heartbeats) from r.
func readSSEFrame(t *testing.T, r *bufio.Reader) (eventType, data string) {
	t.Helper()
	var line string
	for {
		l, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read event line: %v", err)
		}
		l = strings.TrimRight(l, "\r\n")
		if l == "" || strings.HasPrefix(l, ":") {
			continue // blank separator or heartbeat comment
		}
		line = l
		break
	}
	if !strings.HasPrefix(line, "event: ") {
		t.Fatalf("expected 'event: ' line, got %q", line)
	}
	eventType = strings.TrimPrefix(line, "event: ")

	dataLine, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read data line: %v", err)
	}
	dataLine = strings.TrimRight(dataLine, "\r\n")
	if !strings.HasPrefix(dataLine, "data: ") {
		t.Fatalf("expected 'data: ' line, got %q", dataLine)
	}
	data = strings.TrimPrefix(dataLine, "data: ")
	return eventType, data
}

func TestHandleAPISessionsStream_SendsSnapshotThenPublishedEvent(t *testing.T) {
	hub := NewHub()
	provider := &mockProvider{
		sessions: []SessionInfo{{TestID: "t1"}},
		hub:      hub,
	}
	srv := httptest.NewServer(NewMux(provider))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/sessions/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", resp.Header.Get("Content-Type"))
	}

	r := bufio.NewReader(resp.Body)
	eventType, data := readSSEFrame(t, r)
	if eventType != string(EventSnapshot) {
		t.Fatalf("first event type = %q, want %q", eventType, EventSnapshot)
	}
	if !strings.Contains(data, `"t1"`) {
		t.Errorf("snapshot data = %q, want it to contain session t1", data)
	}

	// Give the handler goroutine time to reach provider.Subscribe() (right after the snapshot
	// write/flush above) before publishing, so the event below isn't published to zero subscribers.
	time.Sleep(50 * time.Millisecond)
	hub.Publish(Event{Type: EventSessionUpdate, Session: &SessionInfo{TestID: "t2", LastQuery: "SELECT 1"}})

	eventType, data = readSSEFrame(t, r)
	if eventType != string(EventSessionUpdate) {
		t.Fatalf("second event type = %q, want %q", eventType, EventSessionUpdate)
	}
	if !strings.Contains(data, `"t2"`) || !strings.Contains(data, "SELECT 1") {
		t.Errorf("session_update data = %q, want it to contain t2/SELECT 1", data)
	}
}

func TestHandleAPISessionsStream_DisconnectUnsubscribes(t *testing.T) {
	hub := NewHub()
	provider := &mockProvider{hub: hub}
	srv := httptest.NewServer(NewMux(provider))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/sessions/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(resp.Body)
	readSSEFrame(t, r) // initial snapshot

	cancel()
	resp.Body.Close()
	time.Sleep(50 * time.Millisecond)

	hub.mu.Lock()
	n := len(hub.subs)
	hub.mu.Unlock()
	if n != 0 {
		t.Errorf("subscriber count after disconnect = %d, want 0", n)
	}
}
