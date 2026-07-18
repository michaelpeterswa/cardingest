// Package app is the orchestrator: it consumes detector events and drives one
// ingest worker per slot through the mount → scan → unmount lifecycle.
//
// Milestone 1 runs the scan-only pipeline, so the card stays read-only for the
// whole job and no remount/erase happens yet. The worker structure is built to
// host the full copy → verify → remount → erase sequence in later milestones.
package app

import (
	"context"
	"path/filepath"
	"sync"

	"log/slog"

	"github.com/michaelpeterswa/cardingest/internal/card"
	"github.com/michaelpeterswa/cardingest/internal/detect"
	"github.com/michaelpeterswa/cardingest/internal/mounter"
	"github.com/michaelpeterswa/cardingest/internal/notify"
	"github.com/michaelpeterswa/cardingest/internal/pipeline"
)

// Runner processes one mounted card. *pipeline.Pipeline satisfies it; tests
// inject their own.
type Runner interface {
	Run(ctx context.Context, in pipeline.Input) (pipeline.Result, error)
}

// App wires the ingest pipeline together.
type App struct {
	det       detect.Detector
	mnt       mounter.Mounter
	pipe      Runner
	notifier  notify.Notifier
	mountRoot string
	log       *slog.Logger

	mu      sync.Mutex
	workers map[card.Slot]context.CancelFunc
	wg      sync.WaitGroup
}

// Config holds the app's dependencies.
type Config struct {
	Detector  detect.Detector
	Mounter   mounter.Mounter
	Pipeline  Runner
	Notifier  notify.Notifier
	MountRoot string
	Log       *slog.Logger
}

func New(cfg Config) *App {
	return &App{
		det:       cfg.Detector,
		mnt:       cfg.Mounter,
		pipe:      cfg.Pipeline,
		notifier:  cfg.Notifier,
		mountRoot: cfg.MountRoot,
		log:       cfg.Log,
		workers:   map[card.Slot]context.CancelFunc{},
	}
}

// Run watches for cards and ingests them until ctx is cancelled. It waits for
// in-flight workers to finish before returning.
func (a *App) Run(ctx context.Context) error {
	events, err := a.det.Watch(ctx)
	if err != nil {
		return err
	}
	a.log.Info("cardingest orchestrator started")

	for {
		select {
		case <-ctx.Done():
			a.shutdown()
			return nil
		case ev, ok := <-events:
			if !ok {
				a.shutdown()
				return nil
			}
			a.handle(ctx, ev)
		}
	}
}

func (a *App) handle(ctx context.Context, ev detect.Event) {
	switch ev.Type {
	case detect.EventInserted:
		a.log.Info("detect: card inserted",
			slog.String("slot", string(ev.Slot)),
			slog.String("serial", ev.Device.Serial))
		a.startWorker(ctx, ev)
	case detect.EventRemoved:
		a.log.Info("detect: card removed", slog.String("slot", string(ev.Slot)))
		a.stopWorker(ev.Slot)
	}
}

func (a *App) startWorker(parent context.Context, ev detect.Event) {
	a.mu.Lock()
	if _, busy := a.workers[ev.Slot]; busy {
		a.mu.Unlock()
		a.log.Warn("slot busy, ignoring insert", slog.String("slot", string(ev.Slot)))
		return
	}
	wctx, cancel := context.WithCancel(parent)
	a.workers[ev.Slot] = cancel
	a.wg.Add(1)
	a.mu.Unlock()

	go func() {
		defer a.wg.Done()
		defer a.finishWorker(ev.Slot)
		a.ingest(wctx, ev)
	}()
}

func (a *App) finishWorker(slot card.Slot) {
	a.mu.Lock()
	if cancel, ok := a.workers[slot]; ok {
		cancel()
		delete(a.workers, slot)
	}
	a.mu.Unlock()
}

func (a *App) stopWorker(slot card.Slot) {
	a.mu.Lock()
	if cancel, ok := a.workers[slot]; ok {
		cancel() // finishWorker (in the worker goroutine) removes the entry
	}
	a.mu.Unlock()
}

func (a *App) shutdown() {
	a.mu.Lock()
	for _, cancel := range a.workers {
		cancel()
	}
	a.mu.Unlock()
	a.wg.Wait()
}

// ingest runs the per-card lifecycle. Unmount/eject always run, even on error or
// cancellation, using a background context so teardown is not skipped.
func (a *App) ingest(ctx context.Context, ev detect.Event) {
	slot := string(ev.Slot)
	a.notify(ctx, notify.EventStart, slot, "ingest started")

	target := filepath.Join(a.mountRoot, slot)
	m, err := a.mnt.Mount(ctx, ev.Device.DevPath, target, mounter.Options{
		ReadOnly: true,
		NoExec:   true,
		NoSuid:   true,
	})
	if err != nil {
		a.log.Error("mount failed", slog.String("slot", slot), slog.String("error", err.Error()))
		a.notify(ctx, notify.EventError, slot, "mount failed: "+err.Error())
		return
	}
	defer func() {
		bg := context.Background()
		if err := a.mnt.Unmount(bg, m); err != nil {
			a.log.Error("unmount failed", slog.String("slot", slot), slog.String("error", err.Error()))
		}
		if err := a.mnt.Eject(bg, ev.Device.DevPath); err != nil {
			a.log.Error("eject failed", slog.String("slot", slot), slog.String("error", err.Error()))
		}
	}()

	res, err := a.pipe.Run(ctx, pipeline.Input{
		Slot:   ev.Slot,
		Serial: ev.Device.Serial,
		FS:     m.FS,
	})
	if err != nil {
		a.log.Error("pipeline failed", slog.String("slot", slot), slog.String("error", err.Error()))
		a.notify(ctx, notify.EventError, slot, "pipeline failed: "+err.Error())
		return
	}

	a.log.Info("ingest complete",
		slog.String("slot", slot),
		slog.Int("files", len(res.Files)),
		slog.Int64("bytes", res.Bytes))
	a.notify(ctx, notify.EventComplete, slot, "ingest complete")
}

func (a *App) notify(ctx context.Context, ev notify.EventType, slot, msg string) {
	if err := a.notifier.Notify(ctx, notify.Notification{Event: ev, Slot: slot, Message: msg}); err != nil {
		a.log.Warn("notify failed", slog.String("error", err.Error()))
	}
}
