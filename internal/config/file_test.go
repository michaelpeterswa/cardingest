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

func TestStoreRulesRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	s, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	want := File{
		Rules: []Rule{
			{Name: "skip thumbnails", Action: "skip", Match: &RuleMatch{
				Ext: []string{".thm"}, PathGlob: "**/THMBNL/**"}},
			{Name: "skip tiny", Action: "skip", Match: &RuleMatch{MaxSize: "100KB"}},
			{Name: "keep media", Action: "keep", Match: &RuleMatch{Ext: []string{".arw"}}},
			{Default: "skip"},
		},
	}
	if err := s.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	s2, err := NewStore(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got := s2.Get().Rules
	if len(got) != 4 {
		t.Fatalf("rules len = %d, want 4", len(got))
	}
	if got[0].Name != "skip thumbnails" || got[0].Match == nil ||
		got[0].Match.PathGlob != "**/THMBNL/**" || got[0].Match.Ext[0] != ".thm" {
		t.Fatalf("rule 0 round-trip failed: %+v", got[0])
	}
	if got[1].Match.MaxSize != "100KB" {
		t.Fatalf("rule 1 max_size = %q", got[1].Match.MaxSize)
	}
	if got[3].Default != "skip" {
		t.Fatalf("default entry round-trip failed: %+v", got[3])
	}
}
