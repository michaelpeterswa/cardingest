// Package events is a tiny in-process publish/subscribe hub used to stream live
// ingest events (job start/progress/complete, per-slot status) from the app to
// SSE subscribers in the web layer. Keeping it in its own package avoids an
// app<->web import cycle.
package events

import (
	"sync"
	"time"
)

// Event is a single live update. Data is an arbitrary JSON-serialisable payload.
type Event struct {
	Type string    `json:"type"` // job_start | progress | job_complete | job_error | status
	Slot string    `json:"slot,omitempty"`
	Data any       `json:"data,omitempty"`
	At   time.Time `json:"at"`
}

// Hub fans events out to all current subscribers. Publish never blocks: a
// subscriber whose buffer is full drops the event rather than stalling ingest.
type Hub struct {
	mu   sync.Mutex
	subs map[chan Event]struct{}
	now  func() time.Time
}

func NewHub() *Hub {
	return &Hub{subs: make(map[chan Event]struct{}), now: time.Now}
}

// Subscribe returns a channel of events and a cancel func that unsubscribes and
// closes the channel. The channel is buffered so brief consumer slowness does
// not drop events.
func (h *Hub) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, 64)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.subs, ch)
			h.mu.Unlock()
			close(ch)
		})
	}
	return ch, cancel
}

// Publish delivers e to every subscriber (best-effort, non-blocking).
func (h *Hub) Publish(e Event) {
	if e.At.IsZero() {
		e.At = h.now()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- e:
		default: // drop for a slow subscriber
		}
	}
}

// Subscribers reports the current subscriber count (for diagnostics/tests).
func (h *Hub) Subscribers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}
