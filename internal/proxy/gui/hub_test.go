package gui

import (
	"testing"
	"time"
)

func TestHub_PublishDeliversToSubscriber(t *testing.T) {
	h := NewHub()
	ch, unsubscribe := h.Subscribe(1)
	defer unsubscribe()

	h.Publish(Event{Type: EventSessionUpdate, Session: &SessionInfo{TestID: "t1"}})

	select {
	case e := <-ch:
		if e.Type != EventSessionUpdate || e.Session == nil || e.Session.TestID != "t1" {
			t.Errorf("got %+v, want EventSessionUpdate for t1", e)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestHub_PublishFansOutToAllSubscribers(t *testing.T) {
	h := NewHub()
	ch1, unsub1 := h.Subscribe(1)
	defer unsub1()
	ch2, unsub2 := h.Subscribe(1)
	defer unsub2()

	h.Publish(Event{Type: EventSnapshot})

	for _, ch := range []<-chan Event{ch1, ch2} {
		select {
		case e := <-ch:
			if e.Type != EventSnapshot {
				t.Errorf("got %+v, want EventSnapshot", e)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for event on one of the subscribers")
		}
	}
}

func TestHub_UnsubscribeStopsDelivery(t *testing.T) {
	h := NewHub()
	ch, unsubscribe := h.Subscribe(1)
	unsubscribe()

	h.Publish(Event{Type: EventSnapshot})

	select {
	case e, ok := <-ch:
		if ok {
			t.Errorf("expected no delivery after unsubscribe, got %+v", e)
		}
	case <-time.After(50 * time.Millisecond):
		// No event arrived, as expected.
	}
}

// TestHub_PublishDoesNotBlockOnFullSubscriber is the regression test for the design constraint
// that Publish must never block the caller (the proxy's hot query path calls it inline): a
// subscriber that never drains its channel must not slow down or hang Publish.
func TestHub_PublishDoesNotBlockOnFullSubscriber(t *testing.T) {
	h := NewHub()
	_, unsubscribe := h.Subscribe(1) // buffer of 1, never drained
	defer unsubscribe()

	h.Publish(Event{Type: EventSnapshot}) // fills the buffer

	done := make(chan struct{})
	go func() {
		h.Publish(Event{Type: EventSnapshot}) // would block on a blocking-send implementation
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish blocked on a full subscriber channel")
	}
}

func TestHub_SubscribeIsolatesSubscribers(t *testing.T) {
	h := NewHub()
	ch1, unsub1 := h.Subscribe(2)
	defer unsub1()

	// Subscribe and immediately unsubscribe a second listener; it must not affect the first.
	_, unsub2 := h.Subscribe(2)
	unsub2()

	h.Publish(Event{Type: EventSnapshot})

	select {
	case <-ch1:
	case <-time.After(time.Second):
		t.Fatal("first subscriber did not receive event after second unsubscribed")
	}
}
