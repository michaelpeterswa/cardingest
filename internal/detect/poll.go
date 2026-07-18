package detect

import (
	"context"
	"log/slog"
	"time"

	"github.com/michaelpeterswa/cardingest/internal/card"
)

// enumerator lists the matching devices currently present, keyed by slot. The
// real Linux implementation reads sysfs; tests supply a fake. Keeping this
// interface portable lets the whole poll+debounce path be tested on macOS.
type enumerator interface {
	enumerate(ctx context.Context) (map[card.Slot]card.DeviceInfo, error)
}

// pollDetector drives an enumerator on a ticker and feeds the results through
// the debounce tracker. It implements Detector and is platform-independent.
type pollDetector struct {
	enum     enumerator
	interval time.Duration
	now      func() time.Time
	log      *slog.Logger

	watching bool
}

func newPollDetector(enum enumerator, cfg Config, log *slog.Logger) *pollDetector {
	return &pollDetector{
		enum:     enum,
		interval: cfg.PollInterval,
		now:      cfg.Now,
		log:      log,
	}
}

func (d *pollDetector) Watch(ctx context.Context) (<-chan Event, error) {
	if d.watching {
		return nil, ErrAlreadyWatching
	}
	d.watching = true

	out := make(chan Event)
	go d.run(ctx, out)
	return out, nil
}

func (d *pollDetector) run(ctx context.Context, out chan<- Event) {
	defer close(out)

	trk := newTracker()
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snap, err := d.enum.enumerate(ctx)
			if err != nil {
				d.log.Warn("device enumeration failed", slog.String("error", err.Error()))
				continue
			}
			for _, ev := range trk.reconcile(snap, d.now()) {
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (d *pollDetector) Close() error { return nil }
