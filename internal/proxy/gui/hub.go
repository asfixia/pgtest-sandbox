package gui

import "sync"

// EventType identifies the shape of an Event's payload.
type EventType string

const (
	// EventSnapshot carries the full session list (sent on subscribe, and on
	// structural changes: session created/destroyed, history cleared in bulk).
	EventSnapshot EventType = "snapshot"
	// EventSessionUpdate carries a single changed session (query started/finished).
	EventSessionUpdate EventType = "session_update"
)

// Event is one message pushed to GUI subscribers (e.g. over SSE).
type Event struct {
	Type     EventType     `json:"type"`
	Session  *SessionInfo  `json:"session,omitempty"`
	Sessions []SessionInfo `json:"sessions,omitempty"`
}

// Hub is a simple in-memory pub/sub broadcaster for Events. The zero value is not usable;
// construct with NewHub. Publish never blocks: a slow/full subscriber simply misses events
// (the GUI resyncs periodically via a full snapshot, so this is safe to drop).
type Hub struct {
	mu   sync.Mutex
	subs map[int]chan Event
	next int
}

// NewHub creates an empty Hub.
func NewHub() *Hub {
	return &Hub{subs: make(map[int]chan Event)}
}

// Subscribe registers a new subscriber with the given channel buffer size and returns the
// channel to receive events on plus an unsubscribe function. Call unsubscribe exactly once
// when done (e.g. when the SSE client disconnects).
func (h *Hub) Subscribe(buf int) (<-chan Event, func()) {
	h.mu.Lock()
	id := h.next
	h.next++
	ch := make(chan Event, buf)
	h.subs[id] = ch
	h.mu.Unlock()

	unsubscribe := func() {
		h.mu.Lock()
		delete(h.subs, id)
		h.mu.Unlock()
	}
	return ch, unsubscribe
}

// Publish sends e to every current subscriber without blocking; subscribers whose buffer
// is full do not receive e (dropped, not queued).
func (h *Hub) Publish(e Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.subs {
		select {
		case ch <- e:
		default:
		}
	}
}
