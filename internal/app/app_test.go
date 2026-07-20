package app

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/michaelpeterswa/cardingest/internal/card"
	"github.com/michaelpeterswa/cardingest/internal/detect"
	"github.com/michaelpeterswa/cardingest/internal/mounter"
	"github.com/michaelpeterswa/cardingest/internal/notify"
	"github.com/michaelpeterswa/cardingest/internal/pipeline"
	"github.com/michaelpeterswa/cardingest/internal/store"
	"github.com/spf13/afero"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// recordingNotifier captures the events fired for a slot and signals when a
// terminal (complete/error) event arrives.
type recordingNotifier struct {
	mu     sync.Mutex
	events []notify.EventType
	done   chan struct{}
	once   sync.Once
}

func newRecordingNotifier() *recordingNotifier {
	return &recordingNotifier{done: make(chan struct{})}
}

func (n *recordingNotifier) Notify(_ context.Context, note notify.Notification) error {
	n.mu.Lock()
	n.events = append(n.events, note.Event)
	n.mu.Unlock()
	if note.Event == notify.EventComplete || note.Event == notify.EventError {
		n.once.Do(func() { close(n.done) })
	}
	return nil
}

func (n *recordingNotifier) last() notify.EventType {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.events) == 0 {
		return ""
	}
	return n.events[len(n.events)-1]
}

// spyMounter wraps a real mounter to observe orchestration: whether a rw
// remount happened, and which files survive on the medium just before unmount
// (i.e. after erase). This lets the test verify erase acted on the mounted card
// without depending on the fake's private temp copy.
type spyMounter struct {
	inner mounter.Mounter

	mu          sync.Mutex
	remountedRW bool
	ejected     bool
	surviving   []string
}

func (s *spyMounter) Mount(ctx context.Context, dev, target string, o mounter.Options) (*mounter.Mount, error) {
	return s.inner.Mount(ctx, dev, target, o)
}
func (s *spyMounter) Remount(ctx context.Context, m *mounter.Mount, o mounter.Options) error {
	if !o.ReadOnly {
		s.mu.Lock()
		s.remountedRW = true
		s.mu.Unlock()
	}
	return s.inner.Remount(ctx, m, o)
}
func (s *spyMounter) Unmount(ctx context.Context, m *mounter.Mount) error {
	_ = afero.Walk(m.FS, "", func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			s.mu.Lock()
			s.surviving = append(s.surviving, p)
			s.mu.Unlock()
		}
		return nil
	})
	return s.inner.Unmount(ctx, m)
}
func (s *spyMounter) Sync(ctx context.Context, m *mounter.Mount) error { return s.inner.Sync(ctx, m) }
func (s *spyMounter) Eject(ctx context.Context, dev string) error {
	s.mu.Lock()
	s.ejected = true
	s.mu.Unlock()
	return s.inner.Eject(ctx, dev)
}

// makeCard writes a two-file fixture card and returns its path.
func makeCard(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	sub := filepath.Join(dir, "DCIM", "100MSDCF")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"DSC0001.ARW", "DSC0001.JPG"} {
		if err := os.WriteFile(filepath.Join(sub, name), []byte("data-"+name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// TestIngestMockEndToEnd is the milestone-2 acceptance test: a mock card
// insertion drives mount(ro) → copy+verify → remount(rw) → erase entirely on
// the host, landing files on an in-memory destination and clearing the card.
func TestIngestMockEndToEnd(t *testing.T) {
	log := testLogger()
	dest := afero.NewMemMapFs()
	sqlite, err := store.OpenSQLite(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	defer func() { _ = sqlite.Close() }()

	pipe := pipeline.New(pipeline.Deps{
		Routes: []pipeline.Route{{Category: "photos", Exts: []string{".arw", ".jpg"}, Dest: dest, Layout: "{category}/{date}"}},
		Store:  sqlite,
		DateFn: func(afero.Fs, card.FileEntry) time.Time { return time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC) },
		Log:    log,
	})

	det, _ := detect.New(detect.Config{Mock: true, Now: time.Now}, log)
	mock := det.(*detect.MockDetector)
	fakeMnt, _ := mounter.New(mounter.Config{Mock: true}, log)
	mnt := &spyMounter{inner: fakeMnt}

	notifier := newRecordingNotifier()
	a := New(Config{
		Detector:  det,
		Mounter:   mnt,
		Pipeline:  pipe,
		Notifier:  notifier,
		MountRoot: t.TempDir(),
		Policy:    ErasePolicy{EraseIngested: true, EjectWhenDone: true},
		Log:       log,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	cardDir := makeCard(t)
	if err := mock.Insert(card.SlotA, cardDir); err != nil {
		t.Fatalf("insert: %v", err)
	}

	select {
	case <-notifier.done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ingest")
	}
	if got := notifier.last(); got != notify.EventComplete {
		t.Fatalf("terminal event = %q, want complete", got)
	}

	// Files landed on the destination.
	for _, name := range []string{"DSC0001.ARW", "DSC0001.JPG"} {
		if ok, _ := afero.Exists(dest, "photos/2026-07-18/"+name); !ok {
			t.Fatalf("expected landed file photos/2026-07-18/%s", name)
		}
	}

	// Cancel and wait for the worker (including its deferred unmount/eject) to
	// fully finish before inspecting teardown state.
	cancel()
	select {
	case <-runErr:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	// Orchestration: the card was remounted rw, erased, and ejected. No files
	// survived on the medium at unmount time (both were ingested + erased).
	mnt.mu.Lock()
	remounted, ejected, surviving := mnt.remountedRW, mnt.ejected, mnt.surviving
	mnt.mu.Unlock()
	if !remounted {
		t.Fatal("card was not remounted read-write for erase")
	}
	if !ejected {
		t.Fatal("card was not ejected")
	}
	if len(surviving) != 0 {
		t.Fatalf("files survived on card after erase: %v", surviving)
	}
}
