package detect

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/michaelpeterswa/cardingest/internal/card"
)

// MockDetector simulates a reader for development and tests. A "card" is a
// directory on the host: Insert reports it as present, and the fake mounter
// (mounter package) mounts that same directory. Insertion is driven explicitly
// via Insert/Remove (or the /api/v1/dev/* endpoints) rather than by polling.
type MockDetector struct {
	log        *slog.Logger
	fixtureDir string
	now        func() time.Time

	mu       sync.Mutex
	present  map[card.Slot]card.DeviceInfo
	in       chan Event
	watching bool
}

func newMock(cfg Config, log *slog.Logger) *MockDetector {
	return &MockDetector{
		log:        log,
		fixtureDir: cfg.FixtureDir,
		now:        cfg.Now,
		present:    map[card.Slot]card.DeviceInfo{},
		in:         make(chan Event, 32),
	}
}

func (m *MockDetector) Watch(ctx context.Context) (<-chan Event, error) {
	m.mu.Lock()
	if m.watching {
		m.mu.Unlock()
		return nil, ErrAlreadyWatching
	}
	m.watching = true
	m.mu.Unlock()

	out := make(chan Event)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-m.in:
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// Insert simulates inserting a card sourced from contentDir into slot. If
// contentDir is empty it defaults to <FixtureDir>/<slot>.
func (m *MockDetector) Insert(slot card.Slot, contentDir string) error {
	if contentDir == "" {
		if m.fixtureDir == "" {
			return ErrNoFixtureDir
		}
		contentDir = filepath.Join(m.fixtureDir, string(slot))
	}

	m.mu.Lock()
	if _, ok := m.present[slot]; ok {
		m.mu.Unlock()
		return fmt.Errorf("slot %q: %w", slot, ErrSlotOccupied)
	}
	dev := card.DeviceInfo{
		DevPath:   contentDir,
		ByPath:    "mock:" + string(slot),
		Serial:    filepath.Base(contentDir),
		VendorID:  "mock",
		ProductID: "mock",
	}
	m.present[slot] = dev
	m.mu.Unlock()

	m.log.Info("mock: card inserted", slog.String("slot", string(slot)), slog.String("dir", contentDir))
	m.in <- Event{Type: EventInserted, Slot: slot, Device: dev, At: m.now()}
	return nil
}

// Remove simulates removing the card from slot.
func (m *MockDetector) Remove(slot card.Slot) error {
	m.mu.Lock()
	dev, ok := m.present[slot]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("slot %q: %w", slot, ErrSlotEmpty)
	}
	delete(m.present, slot)
	m.mu.Unlock()

	m.log.Info("mock: card removed", slog.String("slot", string(slot)))
	m.in <- Event{Type: EventRemoved, Slot: slot, Device: dev, At: m.now()}
	return nil
}

func (m *MockDetector) Close() error { return nil }
