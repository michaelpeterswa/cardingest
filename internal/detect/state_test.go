package detect

import (
	"testing"
	"time"

	"github.com/michaelpeterswa/cardingest/internal/card"
)

func dev(serial string) card.DeviceInfo {
	return card.DeviceInfo{DevPath: "/dev/sd" + serial, Serial: serial}
}

func snap(pairs ...any) map[card.Slot]card.DeviceInfo {
	m := map[card.Slot]card.DeviceInfo{}
	for i := 0; i+1 < len(pairs); i += 2 {
		m[card.Slot(pairs[i].(string))] = dev(pairs[i+1].(string))
	}
	return m
}

// summarize renders events as "slot:type" for compact assertions.
func summarize(evs []Event) []string {
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		out = append(out, string(e.Slot)+":"+e.Type.String())
	}
	return out
}

func eq(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	// Order within a single reconcile is map-iteration dependent for distinct
	// slots, so compare as a multiset.
	counts := map[string]int{}
	for _, g := range got {
		counts[g]++
	}
	for _, w := range want {
		counts[w]--
	}
	for k, v := range counts {
		if v != 0 {
			t.Fatalf("events = %v, want %v (mismatch on %q)", got, want, k)
		}
	}
}

func TestTrackerDebouncesInsert(t *testing.T) {
	now := time.Unix(0, 0)
	trk := newTracker()

	// First sighting: no event yet (debounce).
	eq(t, summarize(trk.reconcile(snap("A", "card1"), now)), nil)
	// Second consecutive sighting: inserted.
	eq(t, summarize(trk.reconcile(snap("A", "card1"), now)), []string{"A:inserted"})
	// Steady state: nothing.
	eq(t, summarize(trk.reconcile(snap("A", "card1"), now)), nil)
}

func TestTrackerBounceDoesNotInsert(t *testing.T) {
	now := time.Unix(0, 0)
	trk := newTracker()

	eq(t, summarize(trk.reconcile(snap("A", "card1"), now)), nil) // seen once
	eq(t, summarize(trk.reconcile(snap(), now)), nil)             // gone before confirm
	eq(t, summarize(trk.reconcile(snap(), now)), nil)             // still gone, no phantom insert
}

func TestTrackerRemoveIsImmediate(t *testing.T) {
	now := time.Unix(0, 0)
	trk := newTracker()

	trk.reconcile(snap("A", "card1"), now)
	trk.reconcile(snap("A", "card1"), now) // inserted
	eq(t, summarize(trk.reconcile(snap(), now)), []string{"A:removed"})
}

func TestTrackerSwapEmitsRemoveThenInsert(t *testing.T) {
	now := time.Unix(0, 0)
	trk := newTracker()

	trk.reconcile(snap("A", "card1"), now)
	trk.reconcile(snap("A", "card1"), now) // card1 inserted

	// A different serial appears in the same slot: card1 removed immediately,
	// card2 becomes a fresh candidate (needs a confirming poll).
	eq(t, summarize(trk.reconcile(snap("A", "card2"), now)), []string{"A:removed"})
	eq(t, summarize(trk.reconcile(snap("A", "card2"), now)), []string{"A:inserted"})
}

func TestTrackerTwoSlotsIndependent(t *testing.T) {
	now := time.Unix(0, 0)
	trk := newTracker()

	trk.reconcile(snap("A", "a", "B", "b"), now)
	eq(t, summarize(trk.reconcile(snap("A", "a", "B", "b"), now)),
		[]string{"A:inserted", "B:inserted"})

	// Remove only A.
	eq(t, summarize(trk.reconcile(snap("B", "b"), now)), []string{"A:removed"})
}
