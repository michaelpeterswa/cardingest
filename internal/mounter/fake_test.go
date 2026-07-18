package mounter

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// makeCard writes a small fixture "card" directory and returns its path.
func makeCard(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "DCIM"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "DCIM", "IMG_0001.JPG"), []byte("jpeg"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "DCIM", "IMG_0002.ARW"), []byte("raw!"), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestFakeMountLifecycle(t *testing.T) {
	ctx := context.Background()
	cardDir := makeCard(t)
	m := newFake(Config{}, testLogger())

	mount, err := m.Mount(ctx, cardDir, "/ignored", Options{ReadOnly: true, NoExec: true, NoSuid: true})
	if err != nil {
		t.Fatalf("Mount: %v", err)
	}
	if !mount.ReadOnly() {
		t.Fatal("mount should be read-only")
	}

	// The fixture is visible through the afero FS.
	got, err := afero.ReadFile(mount.FS, "DCIM/IMG_0001.JPG")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "jpeg" {
		t.Fatalf("content = %q", got)
	}

	// Read-only mount rejects writes.
	if err := afero.WriteFile(mount.FS, "DCIM/new.txt", []byte("x"), 0o644); err == nil {
		t.Fatal("write to read-only mount should fail")
	}

	// Remount rw, then erase a specific file only.
	if err := m.Remount(ctx, mount, Options{ReadOnly: false, NoExec: true, NoSuid: true}); err != nil {
		t.Fatalf("Remount: %v", err)
	}
	if mount.ReadOnly() {
		t.Fatal("mount should be writable after remount")
	}
	if err := mount.FS.Remove("DCIM/IMG_0001.JPG"); err != nil {
		t.Fatalf("erase: %v", err)
	}
	if _, err := mount.FS.Stat("DCIM/IMG_0001.JPG"); !os.IsNotExist(err) {
		t.Fatalf("erased file still present: %v", err)
	}
	// The other file is untouched.
	if _, err := mount.FS.Stat("DCIM/IMG_0002.ARW"); err != nil {
		t.Fatalf("non-erased file missing: %v", err)
	}

	// The fixture on disk is never modified (we operate on a copy).
	if _, err := os.Stat(filepath.Join(cardDir, "DCIM", "IMG_0001.JPG")); err != nil {
		t.Fatalf("fixture was harmed: %v", err)
	}

	// Unmount removes the temp copy.
	target := mount.Target
	if err := m.Unmount(ctx, mount); err != nil {
		t.Fatalf("Unmount: %v", err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("temp mount dir not cleaned up: %v", err)
	}
}

func TestFakeMountRejectsNonDir(t *testing.T) {
	m := newFake(Config{}, testLogger())
	f := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Mount(context.Background(), f, "/ignored", Options{ReadOnly: true}); err == nil {
		t.Fatal("mounting a non-directory should fail")
	}
}
