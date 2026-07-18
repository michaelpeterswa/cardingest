package config

import (
	"path/filepath"
	"testing"
)

func TestStoreMissingFileIsEmpty(t *testing.T) {
	s, err := NewStore(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if got := s.Get(); len(got.Reader.USBIDs) != 0 || got.Destination.Path != "" {
		t.Fatalf("missing file should yield empty File, got %+v", got)
	}
}

func TestStoreSaveLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	s, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	want := File{
		Destination: Destination{Path: "/data/dest", Layout: "{category}/{date}", DateSource: "exif_then_mtime"},
		Categories:  map[string][]string{"photos": {".arw", ".jpg"}},
		Reader:      Reader{USBIDs: []string{"05dc:b054"}},
		Card:        CardPolicy{EraseIngested: true, EjectWhenDone: true},
	}
	if err := s.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// A fresh store reading the same path sees the saved content.
	s2, err := NewStore(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got := s2.Get()
	if got.Destination.Path != want.Destination.Path {
		t.Fatalf("destination.path = %q, want %q", got.Destination.Path, want.Destination.Path)
	}
	if len(got.Reader.USBIDs) != 1 || got.Reader.USBIDs[0] != "05dc:b054" {
		t.Fatalf("reader.usb_ids = %v", got.Reader.USBIDs)
	}
	if !got.Card.EraseIngested || !got.Card.EjectWhenDone {
		t.Fatalf("card policy round-trip failed: %+v", got.Card)
	}
}
