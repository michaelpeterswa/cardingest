// Package notify sends completion/error notifications with job statistics via
// pluggable backends, with per-event toggles.
//
// The only backend today is the pulsar-notification-pipeline writer (pulsar.go).
// Adding another (ntfy, webhook, SMTP, ...) is a new file implementing Notifier
// plus one case in construct(); nothing else changes. Notifiers are wired from
// the YAML policy config by Build.
package notify

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/michaelpeterswa/cardingest/internal/config"
)

// EventType is the ingest lifecycle point a notification fires on.
type EventType string

const (
	EventStart    EventType = "start"
	EventComplete EventType = "complete"
	EventError    EventType = "error"
)

// Notification carries the job summary delivered to a backend. Stats fields are
// populated for EventComplete; Err is set for EventError.
type Notification struct {
	Event   EventType
	Slot    string
	Message string

	Copied      int
	Deduped     int
	Skipped     int
	Erased      int
	BytesCopied int64
	Duration    time.Duration
	ByCategory  map[string]int

	Err string
}

// Notifier delivers notifications.
type Notifier interface {
	Notify(ctx context.Context, n Notification) error
}

// Noop discards notifications.
type Noop struct{}

func (Noop) Notify(context.Context, Notification) error { return nil }

// Logger logs notifications via slog; the default in development.
type Logger struct{ Log *slog.Logger }

func (l Logger) Notify(_ context.Context, n Notification) error {
	l.Log.Info("notify",
		slog.String("event", string(n.Event)),
		slog.String("slot", n.Slot),
		slog.String("message", n.Message),
		slog.Int("copied", n.Copied),
		slog.Int("skipped", n.Skipped),
		slog.Int("erased", n.Erased),
	)
	return nil
}

// Multi fans a notification out to several notifiers, collecting their errors.
type Multi []Notifier

func (m Multi) Notify(ctx context.Context, n Notification) error {
	var errs []error
	for _, nf := range m {
		if err := nf.Notify(ctx, n); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// filtered forwards only the events in its set (empty set = all events).
type filtered struct {
	inner  Notifier
	events map[EventType]bool
}

func (f filtered) Notify(ctx context.Context, n Notification) error {
	if len(f.events) > 0 && !f.events[n.Event] {
		return nil
	}
	return f.inner.Notify(ctx, n)
}

// Build constructs the configured notifiers. With no config it returns a Logger
// (dev-friendly). Construction failures are returned as a joined error but do
// not prevent the successfully-built notifiers from being used — ingest must
// never be blocked by a misconfigured notifier.
func Build(cfgs []config.Notifier, log *slog.Logger) (Notifier, error) {
	if len(cfgs) == 0 {
		return Logger{Log: log}, nil
	}

	var ns Multi
	var errs []error
	for i, c := range cfgs {
		nf, err := construct(c)
		if err != nil {
			errs = append(errs, fmt.Errorf("notifier %d (%q): %w", i, c.Type, err))
			continue
		}
		ns = append(ns, withEvents(nf, c.On))
	}

	if len(ns) == 0 {
		return Noop{}, errors.Join(errs...)
	}
	return ns, errors.Join(errs...)
}

// construct builds a single backend. Add new backends here.
func construct(c config.Notifier) (Notifier, error) {
	switch strings.ToLower(strings.TrimSpace(c.Type)) {
	case "pulsar":
		return newPulsar(c)
	default:
		return nil, fmt.Errorf("unknown notifier type (supported: pulsar)")
	}
}

func withEvents(n Notifier, on []string) Notifier {
	if len(on) == 0 {
		return n
	}
	set := make(map[EventType]bool, len(on))
	for _, e := range on {
		set[EventType(strings.ToLower(strings.TrimSpace(e)))] = true
	}
	return filtered{inner: n, events: set}
}
