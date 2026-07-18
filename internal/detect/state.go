package detect

import (
	"time"

	"github.com/michaelpeterswa/cardingest/internal/card"
)

// tracker turns a stream of presence snapshots into insert/remove events. It is
// pure (no I/O, no clock of its own) so the debounce behaviour is exhaustively
// unit-testable on any platform.
//
// Debounce rule: a device must be observed in two consecutive snapshots before
// its insertion is reported (guards against acting on a half-enumerated device
// or insertion bounce). Removal is reported immediately on first absence.
type tracker struct {
	present map[card.Slot]card.DeviceInfo // emitted as inserted, awaiting removal
	pending map[card.Slot]card.DeviceInfo // seen once, awaiting a confirming poll
}

func newTracker() *tracker {
	return &tracker{
		present: map[card.Slot]card.DeviceInfo{},
		pending: map[card.Slot]card.DeviceInfo{},
	}
}

// reconcile diffs snap (currently observed devices, keyed by slot) against the
// committed state and returns the events to emit. Removals precede insertions
// for a slot whose card was swapped between polls.
func (t *tracker) reconcile(snap map[card.Slot]card.DeviceInfo, now time.Time) []Event {
	var events []Event

	// Removals: a committed slot whose device vanished or whose serial changed.
	for slot, dev := range t.present {
		cur, ok := snap[slot]
		if !ok || cur.Serial != dev.Serial {
			events = append(events, Event{Type: EventRemoved, Slot: slot, Device: dev, At: now})
			delete(t.present, slot)
		}
	}

	// Insertions: confirm a candidate seen in the previous snapshot, or record
	// a new candidate for next time.
	nextPending := make(map[card.Slot]card.DeviceInfo, len(snap))
	for slot, dev := range snap {
		if p, ok := t.present[slot]; ok && p.Serial == dev.Serial {
			continue // already present and emitted
		}
		if prev, ok := t.pending[slot]; ok && prev.Serial == dev.Serial {
			events = append(events, Event{Type: EventInserted, Slot: slot, Device: dev, At: now})
			t.present[slot] = dev
			continue
		}
		nextPending[slot] = dev // first sighting (or a different serial)
	}
	t.pending = nextPending

	return events
}
