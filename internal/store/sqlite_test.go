package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestSQLiteSeenMarkVerified(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "cardingest.db") // nested dir is created
	s, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer s.Close()

	seen, err := s.Seen(ctx, "deadbeef")
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Fatal("hash should not be seen initially")
	}

	if err := s.MarkVerified(ctx, "deadbeef"); err != nil {
		t.Fatalf("MarkVerified: %v", err)
	}
	// Idempotent: marking twice is fine.
	if err := s.MarkVerified(ctx, "deadbeef"); err != nil {
		t.Fatalf("MarkVerified twice: %v", err)
	}

	seen, err = s.Seen(ctx, "deadbeef")
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if !seen {
		t.Fatal("hash should be seen after MarkVerified")
	}
}

func TestSQLitePersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cardingest.db")

	s1, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s1.MarkVerified(ctx, "abc123"); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	seen, err := s2.Seen(ctx, "abc123")
	if err != nil {
		t.Fatalf("seen: %v", err)
	}
	if !seen {
		t.Fatal("hash index did not persist across reopen")
	}
}
