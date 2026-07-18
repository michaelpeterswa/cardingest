// Package detect watches the reader for card insertion and removal.
//
// The Detector interface is the hardware-abstraction boundary: portable logic
// (the presence-diff state machine and the poll loop) lives here and is unit
// tested on any OS, while the actual device enumeration is either the real
// Linux sysfs implementation (detect_linux.go) or the mock (mock.go).
package detect

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/michaelpeterswa/cardingest/internal/card"
)

// EventType distinguishes insertion from removal.
type EventType int

const (
	EventInserted EventType = iota
	EventRemoved
)

func (t EventType) String() string {
	switch t {
	case EventInserted:
		return "inserted"
	case EventRemoved:
		return "removed"
	default:
		return "unknown"
	}
}

// Event is a physical presence change for one slot.
type Event struct {
	Type   EventType
	Slot   card.Slot
	Device card.DeviceInfo
	At     time.Time
}

// Detector streams card insert/remove events until its context is cancelled.
type Detector interface {
	// Watch returns a channel of events. It may be called once; the channel is
	// closed when ctx is cancelled.
	Watch(ctx context.Context) (<-chan Event, error)
	Close() error
}

// Config configures a Detector. Mock selects the fixture-backed detector;
// otherwise the real Linux detector is built (and fails on non-Linux).
type Config struct {
	Mock         bool
	USBIDs       []string // "vendor:product" allowlist (real mode)
	PollInterval time.Duration
	FixtureDir   string // default card source for mock inserts

	// Now is injectable for tests; defaults to time.Now.
	Now func() time.Time
}

// ErrRealModeUnsupported is returned when the real detector is requested on a
// platform without the Linux hardware layer (e.g. macOS development).
var ErrRealModeUnsupported = errors.New("detect: real reader mode is only supported on linux")

// ErrAlreadyWatching is returned if Watch is called more than once.
var ErrAlreadyWatching = errors.New("detect: Watch already called")

// Mock-trigger errors, returned by MockDetector.Insert/Remove and mapped to
// HTTP status codes by the web layer.
var (
	ErrSlotOccupied = errors.New("detect: slot already occupied")
	ErrSlotEmpty    = errors.New("detect: slot is empty")
	ErrNoFixtureDir = errors.New("detect: no dir given and no fixture dir configured")
)

// New builds a Detector for the given configuration.
func New(cfg Config, log *slog.Logger) (Detector, error) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.Mock {
		return newMock(cfg, log), nil
	}
	return newReal(cfg, log)
}
