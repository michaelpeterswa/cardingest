// Package pipeline runs the per-card ingest: scan → rules → copy → verify (NAS
// read-back) → erase.
//
// Safety model (see INSTRUCTIONS.md invariants): the card is read during Ingest
// only; files are copied to a temp path on the destination, verified by hashing
// the landed bytes back off the destination, and only then atomically renamed
// into place and recorded in the hash index. Any destination I/O error or hash
// mismatch aborts the job (returns an error) so the caller never erases a card
// whose copies aren't provably safe. Erase is a separate, explicit step the
// caller invokes after remounting the card read-write.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/michaelpeterswa/cardingest/internal/card"
	"github.com/michaelpeterswa/cardingest/internal/rules"
	"github.com/michaelpeterswa/cardingest/internal/store"
	"github.com/spf13/afero"
)

// tmpDir is where in-flight copies land on the destination before verification.
const tmpDir = ".cardingest-tmp"

// Deps are the pipeline's collaborators, fixed for the process lifetime.
type Deps struct {
	Dest       afero.Fs                       // destination (NAS) root
	Store      store.Store                    // verified-hash index
	Rules      rules.Engine                   // keep/skip decision
	Categories map[string][]string            // category -> extensions ("*" = catch-all)
	Layout     string                         // e.g. "{category}/{date}"
	DateFn     func(card.FileEntry) time.Time // foldering date for a file
	Log        *slog.Logger
}

// Pipeline ingests cards. Safe for concurrent use across slots.
type Pipeline struct {
	deps Deps
	seq  atomic.Uint64 // unique temp-file suffixes across concurrent slots
}

func New(deps Deps) *Pipeline {
	if deps.Layout == "" {
		deps.Layout = "{category}/{date}"
	}
	if deps.DateFn == nil {
		deps.DateFn = func(f card.FileEntry) time.Time { return f.ModTime }
	}
	if deps.Rules == nil {
		deps.Rules = rules.KeepAll{}
	}
	return &Pipeline{deps: deps}
}

// Input is one card to ingest.
type Input struct {
	Slot   card.Slot
	Serial string
	CardFS afero.Fs // the mounted card, read-only during Ingest
}

// Result summarizes an ingest run.
type Result struct {
	Copied      int            // newly copied + verified
	Deduped     int            // content already in the index; not re-copied
	Skipped     int            // rule-skipped or uncategorized
	BytesCopied int64          // bytes written for Copied files
	ByCategory  map[string]int // landed count per category
	Verified    []string       // card-relative paths safe to erase (copied+deduped)
	SkippedPath []string       // card-relative paths that were skipped
}

// Ingest scans the card, copies kept files to the destination, and verifies
// each by reading it back. The card is never modified here.
func (p *Pipeline) Ingest(ctx context.Context, in Input) (Result, error) {
	res := Result{ByCategory: map[string]int{}}

	entries, err := scan(in.CardFS)
	if err != nil {
		return res, fmt.Errorf("scan card: %w", err)
	}

	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return res, err
		}

		if p.deps.Rules.Decide(e) == rules.Skip {
			res.Skipped++
			res.SkippedPath = append(res.SkippedPath, e.Path)
			continue
		}
		category, ok := categorize(e.Path, p.deps.Categories)
		if !ok {
			res.Skipped++
			res.SkippedPath = append(res.SkippedPath, e.Path)
			continue
		}

		copied, err := p.copyAndVerify(ctx, in, e, category)
		if err != nil {
			// Destination unreachable / corrupt: abort before any erase so the
			// card is left intact (invariant #5). Nothing copied so far has been
			// erased; already-verified content is deduped on the retry.
			return res, fmt.Errorf("ingest %s: %w", e.Path, err)
		}
		if copied {
			res.Copied++
			res.BytesCopied += e.Size
		} else {
			res.Deduped++
		}
		res.ByCategory[category]++
		res.Verified = append(res.Verified, e.Path)
	}

	p.deps.Log.Info("pipeline: ingest complete",
		slog.String("slot", string(in.Slot)),
		slog.String("serial", in.Serial),
		slog.Int("copied", res.Copied),
		slog.Int("deduped", res.Deduped),
		slog.Int("skipped", res.Skipped),
		slog.Int64("bytes", res.BytesCopied),
	)
	return res, nil
}

// copyAndVerify streams one file card→destination temp while hashing, dedupes
// against the index, verifies by reading the landed bytes back, then atomically
// renames into place. Returns whether a copy was actually written (false =
// deduped).
func (p *Pipeline) copyAndVerify(ctx context.Context, in Input, e card.FileEntry, category string) (copied bool, err error) {
	if err := p.deps.Dest.MkdirAll(tmpDir, 0o755); err != nil {
		return false, fmt.Errorf("prepare temp dir: %w", err)
	}
	tmpPath := path.Join(tmpDir, fmt.Sprintf("ingest-%s-%d", in.Slot, p.seq.Add(1)))

	// Single-pass copy with streaming hash.
	hash, err := p.streamCopy(in.CardFS, e.Path, tmpPath)
	if err != nil {
		_ = p.deps.Dest.Remove(tmpPath)
		return false, err
	}

	// Dedupe: content already verified in a prior job. Erase from the card
	// without re-landing it.
	seen, err := p.deps.Store.Seen(ctx, hash)
	if err != nil {
		_ = p.deps.Dest.Remove(tmpPath)
		return false, fmt.Errorf("hash index: %w", err)
	}
	if seen {
		_ = p.deps.Dest.Remove(tmpPath)
		return false, nil
	}

	// Verify by hashing the landed bytes off the destination. A successful
	// write syscall is not sufficient over NFS.
	landedHash, err := hashFile(p.deps.Dest, tmpPath)
	if err != nil {
		_ = p.deps.Dest.Remove(tmpPath)
		return false, fmt.Errorf("read-back: %w", err)
	}
	if landedHash != hash {
		_ = p.deps.Dest.Remove(tmpPath)
		return false, fmt.Errorf("verify mismatch: card=%s dest=%s", hash, landedHash)
	}

	// Atomically move into place.
	finalPath := p.destPath(category, p.deps.DateFn(e), filepath.Base(e.Path))
	if err := p.deps.Dest.MkdirAll(path.Dir(finalPath), 0o755); err != nil {
		_ = p.deps.Dest.Remove(tmpPath)
		return false, fmt.Errorf("create dest dir: %w", err)
	}
	if err := p.deps.Dest.Rename(tmpPath, finalPath); err != nil {
		_ = p.deps.Dest.Remove(tmpPath)
		return false, fmt.Errorf("place file: %w", err)
	}

	if err := p.deps.Store.MarkVerified(ctx, hash); err != nil {
		return false, fmt.Errorf("record hash: %w", err)
	}
	return true, nil
}

// streamCopy copies src (on cardFS) to dstPath (on Dest), returning the content
// hash computed during the copy.
func (p *Pipeline) streamCopy(cardFS afero.Fs, srcPath, dstPath string) (string, error) {
	src, err := cardFS.Open(srcPath)
	if err != nil {
		return "", fmt.Errorf("open card file: %w", err)
	}
	defer func() { _ = src.Close() }()

	dst, err := p.deps.Dest.Create(dstPath)
	if err != nil {
		return "", fmt.Errorf("create dest temp: %w", err)
	}

	h := xxhash.New()
	if _, err := io.Copy(dst, io.TeeReader(src, h)); err != nil {
		_ = dst.Close()
		return "", fmt.Errorf("copy: %w", err)
	}
	if err := dst.Close(); err != nil {
		return "", fmt.Errorf("flush dest temp: %w", err)
	}
	return fmt.Sprintf("%016x", h.Sum64()), nil
}

// Erase deletes the given card-relative paths from cardFS. Targeted deletes
// only — never directories, never recursive (invariant #3). The caller must
// have remounted the card read-write first.
func (p *Pipeline) Erase(_ context.Context, cardFS afero.Fs, paths []string) error {
	var errs []error
	for _, rel := range paths {
		if err := cardFS.Remove(rel); err != nil {
			errs = append(errs, fmt.Errorf("erase %s: %w", rel, err))
		}
	}
	return errors.Join(errs...)
}

func (p *Pipeline) destPath(category string, date time.Time, name string) string {
	rel := strings.ReplaceAll(p.deps.Layout, "{category}", category)
	rel = strings.ReplaceAll(rel, "{date}", date.Format("2006-01-02"))
	return path.Join(rel, name)
}

// scan walks the card filesystem into a stable file list.
func scan(fs afero.Fs) ([]card.FileEntry, error) {
	var entries []card.FileEntry
	err := afero.Walk(fs, "", func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		entries = append(entries, card.FileEntry{Path: p, Size: info.Size(), ModTime: info.ModTime()})
		return nil
	})
	return entries, err
}

// hashFile computes the xxhash of a file on fs.
func hashFile(fs afero.Fs, p string) (string, error) {
	f, err := fs.Open(p)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	h := xxhash.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%016x", h.Sum64()), nil
}

// categorize maps a file to a category by extension. An explicit extension match
// wins; otherwise a category listing "*" catches everything. Returns false when
// nothing matches (the file is skipped).
func categorize(p string, cats map[string][]string) (string, bool) {
	ext := strings.ToLower(filepath.Ext(p))
	catchAll := ""
	for cat, exts := range cats {
		for _, e := range exts {
			if e == "*" {
				catchAll = cat
				continue
			}
			if strings.ToLower(e) == ext {
				return cat, true
			}
		}
	}
	if catchAll != "" {
		return catchAll, true
	}
	return "", false
}
