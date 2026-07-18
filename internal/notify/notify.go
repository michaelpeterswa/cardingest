// Package notify sends completion/error notifications with job statistics via
// pluggable backends (ntfy, generic webhook, SMTP), with per-event toggles.
//
// Milestone 1 stub: the Notifier interface plus a Noop and a slog-logging
// implementation. The real backends land in milestone 4.
package notify

import (
	"context"
	"log/slog"
)

// EventType is the ingest lifecycle point a notification fires on.
type EventType string

const (
	EventStart    EventType = "start"
	EventComplete EventType = "complete"
	EventError    EventType = "error"
)

// Notification carries the job summary delivered to a backend.
type Notification struct {
	Event   EventType
	Slot    string
	Message string
	// Stats (files kept/skipped, bytes, duration, per-category) land in M4.
}

// Notifier delivers notifications.
type Notifier interface {
	Notify(ctx context.Context, n Notification) error
}

// Noop discards notifications.
type Noop struct{}

func (Noop) Notify(context.Context, Notification) error { return nil }

// Logger logs notifications via slog; useful in development.
type Logger struct{ Log *slog.Logger }

func (l Logger) Notify(_ context.Context, n Notification) error {
	l.Log.Info("notify",
		slog.String("event", string(n.Event)),
		slog.String("slot", n.Slot),
		slog.String("message", n.Message),
	)
	return nil
}
