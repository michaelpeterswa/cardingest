package pipeline

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/michaelpeterswa/cardingest/internal/card"
	"github.com/michaelpeterswa/cardingest/internal/config"
	"github.com/michaelpeterswa/cardingest/internal/rules"
	"github.com/michaelpeterswa/cardingest/internal/store"
	"github.com/spf13/afero"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

var fixedDate = time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)

func newTestPipeline(dest afero.Fs, st store.Store) *Pipeline {
	return New(Deps{
		Dest:       dest,
		Store:      st,
		Categories: map[string][]string{"photos": {".arw", ".jpg"}, "video": {".mp4"}},
		Layout:     "{category}/{date}",
		DateFn:     func(card.FileEntry) time.Time { return fixedDate },
		Log:        testLogger(),
	})
}

func writeCard(t *testing.T, fs afero.Fs, files map[string]string) {
	t.Helper()
	for p, content := range files {
		if err := afero.WriteFile(fs, p, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}
}

func TestIngestCopiesVerifiesAndFolders(t *testing.T) {
	ctx := context.Background()
	cardFS := afero.NewMemMapFs()
	dest := afero.NewMemMapFs()
	writeCard(t, cardFS, map[string]string{
		"DCIM/A.ARW": "raw-bytes",
		"DCIM/A.JPG": "jpeg-bytes",
		"CLIP/C.MP4": "movie-bytes",
		"notes.txt":  "uncategorized -> skipped",
	})

	p := newTestPipeline(dest, store.Noop{})
	res, err := p.Ingest(ctx, Input{Slot: card.SlotA, Serial: "card1", CardFS: cardFS})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	if res.Copied != 3 || res.Skipped != 1 || res.Deduped != 0 {
		t.Fatalf("counts: copied=%d skipped=%d deduped=%d", res.Copied, res.Skipped, res.Deduped)
	}
	if len(res.Verified) != 3 {
		t.Fatalf("verified=%v, want 3", res.Verified)
	}

	// Files landed under {category}/{date}/name, contents intact.
	for path, want := range map[string]string{
		"photos/2026-07-18/A.ARW": "raw-bytes",
		"photos/2026-07-18/A.JPG": "jpeg-bytes",
		"video/2026-07-18/C.MP4":  "movie-bytes",
	} {
		got, err := afero.ReadFile(dest, path)
		if err != nil {
			t.Fatalf("expected landed file %s: %v", path, err)
		}
		if string(got) != want {
			t.Fatalf("%s content = %q, want %q", path, got, want)
		}
	}

	// The uncategorized file was never copied.
	if ok, _ := afero.Exists(dest, "notes.txt"); ok {
		t.Fatal("skipped file should not be copied")
	}
	// No temp files left behind.
	if ok, _ := afero.DirExists(dest, tmpDir); ok {
		if entries, _ := afero.ReadDir(dest, tmpDir); len(entries) != 0 {
			t.Fatalf("temp dir not empty: %d entries", len(entries))
		}
	}
}

func TestIngestDedupesSeenContent(t *testing.T) {
	ctx := context.Background()
	sqlite, err := store.OpenSQLite(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	defer func() { _ = sqlite.Close() }()

	dest := afero.NewMemMapFs()
	p := newTestPipeline(dest, sqlite)

	// First card lands the file and records its hash.
	card1 := afero.NewMemMapFs()
	writeCard(t, card1, map[string]string{"DCIM/A.ARW": "identical-content"})
	if _, err := p.Ingest(ctx, Input{Slot: card.SlotA, Serial: "c1", CardFS: card1}); err != nil {
		t.Fatalf("first ingest: %v", err)
	}

	// Second card with the SAME content under a different name: deduped, not
	// re-landed, but still marked safe to erase.
	card2 := afero.NewMemMapFs()
	writeCard(t, card2, map[string]string{"DCIM/B.ARW": "identical-content"})
	res, err := p.Ingest(ctx, Input{Slot: card.SlotB, Serial: "c2", CardFS: card2})
	if err != nil {
		t.Fatalf("second ingest: %v", err)
	}
	if res.Deduped != 1 || res.Copied != 0 {
		t.Fatalf("expected dedupe: copied=%d deduped=%d", res.Copied, res.Deduped)
	}
	if len(res.Verified) != 1 || res.Verified[0] != "DCIM/B.ARW" {
		t.Fatalf("verified=%v, want [DCIM/B.ARW]", res.Verified)
	}
	// The duplicate was not landed under B's name.
	if ok, _ := afero.Exists(dest, "photos/2026-07-18/B.ARW"); ok {
		t.Fatal("deduped content should not be re-landed")
	}
}

func TestIngestHonorsRules(t *testing.T) {
	ctx := context.Background()
	eng, err := rules.Compile([]config.Rule{
		{Name: "skip text", Action: "skip", Match: &config.RuleMatch{Ext: []string{".jpg"}}},
		{Default: "keep"},
	})
	if err != nil {
		t.Fatalf("compile rules: %v", err)
	}

	cardFS := afero.NewMemMapFs()
	writeCard(t, cardFS, map[string]string{
		"DCIM/A.ARW": "raw",
		"DCIM/A.JPG": "jpeg", // skipped by rule
	})
	dest := afero.NewMemMapFs()
	p := New(Deps{
		Dest:       dest,
		Store:      store.Noop{},
		Rules:      eng,
		Categories: map[string][]string{"photos": {".arw", ".jpg"}},
		Layout:     "{category}/{date}",
		DateFn:     func(card.FileEntry) time.Time { return fixedDate },
		Log:        testLogger(),
	})

	res, err := p.Ingest(ctx, Input{Slot: card.SlotA, Serial: "c", CardFS: cardFS})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.Copied != 1 || res.Skipped != 1 {
		t.Fatalf("copied=%d skipped=%d, want 1/1", res.Copied, res.Skipped)
	}
	if len(res.SkippedPath) != 1 || res.SkippedPath[0] != "DCIM/A.JPG" {
		t.Fatalf("skipped paths = %v", res.SkippedPath)
	}
	if ok, _ := afero.Exists(dest, "photos/2026-07-18/A.JPG"); ok {
		t.Fatal("rule-skipped file should not be copied")
	}
	if ok, _ := afero.Exists(dest, "photos/2026-07-18/A.ARW"); !ok {
		t.Fatal("kept file should be copied")
	}
}

func TestEraseRemovesOnlyListedPaths(t *testing.T) {
	ctx := context.Background()
	cardFS := afero.NewMemMapFs()
	writeCard(t, cardFS, map[string]string{
		"DCIM/keep.ARW":  "k",
		"DCIM/erase.ARW": "e",
	})

	p := newTestPipeline(afero.NewMemMapFs(), store.Noop{})
	if err := p.Erase(ctx, cardFS, []string{"DCIM/erase.ARW"}); err != nil {
		t.Fatalf("Erase: %v", err)
	}

	if ok, _ := afero.Exists(cardFS, "DCIM/erase.ARW"); ok {
		t.Fatal("listed file was not erased")
	}
	if ok, _ := afero.Exists(cardFS, "DCIM/keep.ARW"); !ok {
		t.Fatal("unlisted file was wrongly erased")
	}
}
