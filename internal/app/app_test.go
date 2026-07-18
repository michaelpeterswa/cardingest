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
	"github.com/spf13/afero"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// recordingRunner captures what the worker handed the pipeline: the scanned
// files and whether the mount was read-only during scan.
type recordingRunner struct {
	mu       sync.Mutex
	files    []string
	readOnly bool
	done     chan struct{}
}

func newRecordingRunner() *recordingRunner {
	return &recordingRunner{done: make(chan struct{})}
}

func (r *recordingRunner) Run(_ context.Context, in pipeline.Input) (pipeline.Result, error) {
	var res pipeline.Result
	_ = afero.Walk(in.FS, "", func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		r.files = append(r.files, path)
		res.Files = append(res.Files, card.FileEntry{Path: path, Size: info.Size()})
		return nil
	})

	// Safety invariant #1: the card is read-only during ingest. A write must fail.
	writeErr := afero.WriteFile(in.FS, "___probe", []byte("x"), 0o644)

	r.mu.Lock()
	r.readOnly = writeErr != nil
	r.mu.Unlock()

	close(r.done)
	return res, nil
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
		if err := os.WriteFile(filepath.Join(sub, name), []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// TestIngestMockEndToEnd is the milestone-1 acceptance test: a mock card
// insertion drives mount(ro) → scan → unmount/eject entirely on the host, no
// hardware. It runs on any OS.
func TestIngestMockEndToEnd(t *testing.T) {
	log := testLogger()

	det, err := detect.New(detect.Config{Mock: true, Now: time.Now}, log)
	if err != nil {
		t.Fatalf("detector: %v", err)
	}
	mock := det.(*detect.MockDetector)

	mnt, err := mounter.New(mounter.Config{Mock: true}, log)
	if err != nil {
		t.Fatalf("mounter: %v", err)
	}

	runner := newRecordingRunner()
	a := New(Config{
		Detector:  det,
		Mounter:   mnt,
		Pipeline:  runner,
		Notifier:  notify.Noop{},
		MountRoot: t.TempDir(),
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
	case <-runner.done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ingest to run")
	}

	runner.mu.Lock()
	defer runner.mu.Unlock()
	if len(runner.files) != 2 {
		t.Fatalf("scanned files = %v, want 2", runner.files)
	}
	if !runner.readOnly {
		t.Fatal("mount was not read-only during scan")
	}

	// Clean shutdown.
	cancel()
	select {
	case <-runErr:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
