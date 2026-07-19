package events

import (
	"testing"
	"time"
)

func TestHubPublishSubscribe(t *testing.T) {
	h := NewHub()
	ch, cancel := h.Subscribe()
	defer cancel()

	if h.Subscribers() != 1 {
		t.Fatalf("subscribers = %d, want 1", h.Subscribers())
	}

	h.Publish(Event{Type: "job_start", Slot: "A"})
	select {
	case e := <-ch:
		if e.Type != "job_start" || e.Slot != "A" {
			t.Fatalf("event = %+v", e)
		}
		if e.At.IsZero() {
			t.Fatal("publish should stamp At")
		}
	case <-time.After(time.Second):
		t.Fatal("no event received")
	}
}

func TestHubCancelUnsubscribes(t *testing.T) {
	h := NewHub()
	ch, cancel := h.Subscribe()
	cancel()
	if h.Subscribers() != 0 {
		t.Fatalf("subscribers after cancel = %d, want 0", h.Subscribers())
	}
	if _, ok := <-ch; ok {
		t.Fatal("channel should be closed after cancel")
	}
	// Cancel is idempotent and Publish to no subscribers is a no-op.
	cancel()
	h.Publish(Event{Type: "x"})
}

func TestHubDoesNotBlockOnSlowSubscriber(t *testing.T) {
	h := NewHub()
	_, cancel := h.Subscribe() // never drained
	defer cancel()
	// Far more than the buffer; Publish must not block.
	done := make(chan struct{})
	go func() {
		for range 1000 {
			h.Publish(Event{Type: "progress"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a slow subscriber")
	}
}
