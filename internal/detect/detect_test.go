package detect

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/michaelpeterswa/cardingest/internal/card"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeEnum returns a fixed snapshot every poll, exercising the portable
// poll+debounce path on any OS.
type fakeEnum struct{ snap map[card.Slot]card.DeviceInfo }

func (f fakeEnum) enumerate(context.Context) (map[card.Slot]card.DeviceInfo, error) {
	return f.snap, nil
}

func recv(t *testing.T, ch <-chan Event) Event {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before event")
		}
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return Event{}
	}
}

func TestPollDetectorEmitsInsertAfterDebounce(t *testing.T) {
	enum := fakeEnum{snap: map[card.Slot]card.DeviceInfo{
		card.SlotA: {DevPath: "/dev/sdb", Serial: "card1"},
	}}
	d := newPollDetector(enum, Config{PollInterval: time.Millisecond, Now: time.Now}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := d.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	ev := recv(t, ch)
	if ev.Type != EventInserted || ev.Slot != card.SlotA || ev.Device.Serial != "card1" {
		t.Fatalf("unexpected event: %+v", ev)
	}
}

func TestPollDetectorWatchTwiceFails(t *testing.T) {
	d := newPollDetector(fakeEnum{}, Config{PollInterval: time.Millisecond, Now: time.Now}, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := d.Watch(ctx); err != nil {
		t.Fatalf("first Watch: %v", err)
	}
	if _, err := d.Watch(ctx); !errors.Is(err, ErrAlreadyWatching) {
		t.Fatalf("second Watch err = %v, want ErrAlreadyWatching", err)
	}
}

func TestMockInsertRemove(t *testing.T) {
	d, err := New(Config{Mock: true, Now: time.Now}, testLogger())
	if err != nil {
		t.Fatalf("New mock: %v", err)
	}
	mock := d.(*MockDetector)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := mock.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	if err := mock.Insert(card.SlotA, t.TempDir()); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if ev := recv(t, ch); ev.Type != EventInserted || ev.Slot != card.SlotA {
		t.Fatalf("insert event = %+v", ev)
	}

	// Re-inserting an occupied slot is rejected.
	if err := mock.Insert(card.SlotA, t.TempDir()); !errors.Is(err, ErrSlotOccupied) {
		t.Fatalf("double insert err = %v, want ErrSlotOccupied", err)
	}

	if err := mock.Remove(card.SlotA); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if ev := recv(t, ch); ev.Type != EventRemoved || ev.Slot != card.SlotA {
		t.Fatalf("remove event = %+v", ev)
	}

	// Removing an empty slot is rejected.
	if err := mock.Remove(card.SlotA); !errors.Is(err, ErrSlotEmpty) {
		t.Fatalf("empty remove err = %v, want ErrSlotEmpty", err)
	}
}

func TestMockInsertNoDirNoFixture(t *testing.T) {
	d, _ := New(Config{Mock: true, Now: time.Now}, testLogger())
	mock := d.(*MockDetector)
	if err := mock.Insert(card.SlotA, ""); !errors.Is(err, ErrNoFixtureDir) {
		t.Fatalf("err = %v, want ErrNoFixtureDir", err)
	}
}
