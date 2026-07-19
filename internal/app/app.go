// Package app is the orchestrator: it consumes detector events and drives one
// ingest worker per slot through the full lifecycle — mount read-only, copy and
// verify to the destination, then (only if verification passed and policy asks)
// remount read-write, erase the ingested files, sync, and eject.
package app

import (
	"context"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"log/slog"

	"github.com/michaelpeterswa/cardingest/internal/card"
	"github.com/michaelpeterswa/cardingest/internal/detect"
	"github.com/michaelpeterswa/cardingest/internal/events"
	"github.com/michaelpeterswa/cardingest/internal/mounter"
	"github.com/michaelpeterswa/cardingest/internal/notify"
	"github.com/michaelpeterswa/cardingest/internal/pipeline"
	"github.com/michaelpeterswa/cardingest/internal/store"
	"github.com/spf13/afero"
)

// Ingestor copies+verifies a mounted card and erases ingested files afterward.
// *pipeline.Pipeline satisfies it; tests inject their own.
type Ingestor interface {
	Ingest(ctx context.Context, in pipeline.Input) (pipeline.Result, error)
	Erase(ctx context.Context, cardFS afero.Fs, paths []string) error
}

// JobRecorder persists ingest jobs for history/stats. *store.SQLite satisfies
// it; it is optional (nil = no recording).
type JobRecorder interface {
	StartJob(ctx context.Context, slot, serial string) (int64, error)
	FinishJob(ctx context.Context, id int64, status store.JobStatus, j store.Job) error
}

// ErasePolicy controls post-verification erasure and ejection.
type ErasePolicy struct {
	EraseIngested bool // erase copied+verified files from the card
	EraseSkipped  bool // also erase rule-skipped files (opt-in, default off)
	EjectWhenDone bool
}

// SlotState is the live state of one reader slot, exposed via the status API.
type SlotState struct {
	Slot  string `json:"slot"`
	State string `json:"state"` // idle | ingesting
	JobID int64  `json:"jobId,omitempty"`
}

// App wires the ingest pipeline together.
type App struct {
	det       detect.Detector
	mnt       mounter.Mounter
	pipe      Ingestor
	notifier  notify.Notifier
	mountRoot string
	policy    ErasePolicy
	hub       *events.Hub // optional live-event publisher
	jobs      JobRecorder // optional job history recorder
	log       *slog.Logger

	mu       sync.Mutex
	workers  map[card.Slot]context.CancelFunc
	statuses map[card.Slot]SlotState
	wg       sync.WaitGroup
}

// Config holds the app's dependencies.
type Config struct {
	Detector  detect.Detector
	Mounter   mounter.Mounter
	Pipeline  Ingestor
	Notifier  notify.Notifier
	MountRoot string
	Policy    ErasePolicy
	Events    *events.Hub
	Jobs      JobRecorder
	Log       *slog.Logger
}

func New(cfg Config) *App {
	return &App{
		det:       cfg.Detector,
		mnt:       cfg.Mounter,
		pipe:      cfg.Pipeline,
		notifier:  cfg.Notifier,
		mountRoot: cfg.MountRoot,
		policy:    cfg.Policy,
		hub:       cfg.Events,
		jobs:      cfg.Jobs,
		log:       cfg.Log,
		workers:   map[card.Slot]context.CancelFunc{},
		statuses:  map[card.Slot]SlotState{},
	}
}

// Status returns a snapshot of every slot's live state, ordered by slot.
func (a *App) Status() []SlotState {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]SlotState, 0, len(a.statuses))
	for _, s := range a.statuses {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slot < out[j].Slot })
	return out
}

// Run watches for cards and ingests them until ctx is cancelled. It waits for
// in-flight workers to finish before returning.
func (a *App) Run(ctx context.Context) error {
	evc, err := a.det.Watch(ctx)
	if err != nil {
		return err
	}
	a.log.Info("cardingest orchestrator started")

	for {
		select {
		case <-ctx.Done():
			a.shutdown()
			return nil
		case ev, ok := <-evc:
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
	start := time.Now()

	jobID := a.startJob(slot, ev.Device.Serial)
	a.setStatus(ev.Slot, "ingesting", jobID)
	defer a.setStatus(ev.Slot, "idle", 0) // returns slot to idle on any exit

	a.publish("job_start", slot, map[string]any{"jobId": jobID, "serial": ev.Device.Serial})
	a.emit(ctx, notify.Notification{Event: notify.EventStart, Slot: slot, Message: "ingest started"})

	target := filepath.Join(a.mountRoot, slot)
	m, err := a.mnt.Mount(ctx, ev.Device.DevPath, target, mounter.Options{
		ReadOnly: true,
		NoExec:   true,
		NoSuid:   true,
	})
	if err != nil {
		a.failJob(ctx, jobID, slot, "mount failed", err)
		return
	}
	// Unmount (and eject if asked) always runs, even on error/cancel; a
	// background context ensures teardown is not skipped.
	defer func() {
		bg := context.Background()
		if err := a.mnt.Unmount(bg, m); err != nil {
			a.log.Error("unmount failed", slog.String("slot", slot), slog.String("error", err.Error()))
		}
		if a.policy.EjectWhenDone {
			if err := a.mnt.Eject(bg, ev.Device.DevPath); err != nil {
				a.log.Error("eject failed", slog.String("slot", slot), slog.String("error", err.Error()))
			}
		}
	}()

	// Copy + verify. On any error the card is left intact (invariant #5).
	res, err := a.pipe.Ingest(ctx, pipeline.Input{
		Slot:   ev.Slot,
		Serial: ev.Device.Serial,
		CardFS: m.FS,
	})
	if err != nil {
		a.failJob(ctx, jobID, slot, "ingest failed", err)
		return
	}

	// Decide what may be erased. Verified files are safe by construction;
	// skipped files only if explicitly opted in.
	var toErase []string
	if a.policy.EraseIngested {
		toErase = append(toErase, res.Verified...)
	}
	if a.policy.EraseSkipped {
		toErase = append(toErase, res.SkippedPath...)
	}

	if len(toErase) > 0 {
		if err := a.mnt.Remount(ctx, m, mounter.Options{ReadOnly: false, NoExec: true, NoSuid: true}); err != nil {
			a.failJob(ctx, jobID, slot, "remount failed", err)
			return
		}
		if err := a.pipe.Erase(ctx, m.FS, toErase); err != nil {
			// Copies are verified and safe; a partial erase is a card-side
			// problem worth surfacing but not data loss. The job still completes.
			a.log.Error("erase incomplete", slog.String("slot", slot), slog.String("error", err.Error()))
			a.emitError(ctx, slot, "erase incomplete", err)
		}
		if err := a.mnt.Sync(ctx, m); err != nil {
			a.log.Error("sync failed", slog.String("slot", slot), slog.String("error", err.Error()))
		}
	}

	dur := time.Since(start)
	a.finishJob(jobID, store.JobComplete, store.Job{
		Copied: res.Copied, Deduped: res.Deduped, Skipped: res.Skipped,
		Erased: len(toErase), Bytes: res.BytesCopied,
	})
	a.log.Info("ingest complete",
		slog.String("slot", slot),
		slog.Int("copied", res.Copied),
		slog.Int("deduped", res.Deduped),
		slog.Int("skipped", res.Skipped),
		slog.Int("erased", len(toErase)),
		slog.Int64("bytes", res.BytesCopied))
	a.emit(ctx, notify.Notification{
		Event:       notify.EventComplete,
		Slot:        slot,
		Message:     "ingest complete",
		Copied:      res.Copied,
		Deduped:     res.Deduped,
		Skipped:     res.Skipped,
		Erased:      len(toErase),
		BytesCopied: res.BytesCopied,
		Duration:    dur,
		ByCategory:  res.ByCategory,
	})
	a.publish("job_complete", slot, map[string]any{
		"jobId": jobID, "copied": res.Copied, "deduped": res.Deduped,
		"skipped": res.Skipped, "erased": len(toErase),
		"bytes": res.BytesCopied, "durationMs": dur.Milliseconds(),
	})
}

// failJob records an error outcome and emits the matching notifications/events.
func (a *App) failJob(ctx context.Context, jobID int64, slot, msg string, cause error) {
	a.log.Error(msg, slog.String("slot", slot), slog.String("error", cause.Error()))
	a.finishJob(jobID, store.JobError, store.Job{Err: cause.Error()})
	a.emitError(ctx, slot, msg, cause)
	a.publish("job_error", slot, map[string]any{"jobId": jobID, "message": msg, "error": cause.Error()})
}

func (a *App) startJob(slot, serial string) int64 {
	if a.jobs == nil {
		return 0
	}
	id, err := a.jobs.StartJob(context.Background(), slot, serial)
	if err != nil {
		a.log.Warn("record job start failed", slog.String("error", err.Error()))
		return 0
	}
	return id
}

func (a *App) finishJob(id int64, status store.JobStatus, j store.Job) {
	if a.jobs == nil || id == 0 {
		return
	}
	if err := a.jobs.FinishJob(context.Background(), id, status, j); err != nil {
		a.log.Warn("record job finish failed", slog.String("error", err.Error()))
	}
}

func (a *App) publish(typ, slot string, data any) {
	if a.hub == nil {
		return
	}
	a.hub.Publish(events.Event{Type: typ, Slot: slot, Data: data})
}

func (a *App) setStatus(slot card.Slot, state string, jobID int64) {
	a.mu.Lock()
	a.statuses[slot] = SlotState{Slot: string(slot), State: state, JobID: jobID}
	a.mu.Unlock()
	a.publish("status", string(slot), a.Status())
}

func (a *App) emit(ctx context.Context, n notify.Notification) {
	if err := a.notifier.Notify(ctx, n); err != nil {
		a.log.Warn("notify failed", slog.String("error", err.Error()))
	}
}

func (a *App) emitError(ctx context.Context, slot, msg string, cause error) {
	a.emit(ctx, notify.Notification{
		Event:   notify.EventError,
		Slot:    slot,
		Message: msg,
		Err:     cause.Error(),
	})
}
