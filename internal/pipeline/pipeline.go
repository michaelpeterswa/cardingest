// Package pipeline runs the per-card ingest work: scan → rules → copy → verify
// (NAS read-back) → erase.
//
// Milestone 1 implements the scan stage only: it walks the mounted card through
// the afero filesystem and reports what it found. Copy/verify/erase and the
// SQLite hash index land in milestone 2. The Run signature is intended to
// remain stable as those stages fill in.
package pipeline

import (
	"context"
	"log/slog"
	"os"

	"github.com/michaelpeterswa/cardingest/internal/card"
	"github.com/spf13/afero"
)

// Input is one card to ingest.
type Input struct {
	Slot   card.Slot
	Serial string
	FS     afero.Fs // the mounted card, rooted at its mount point
}

// Result summarizes an ingest run.
type Result struct {
	Files []card.FileEntry
	Bytes int64
}

// Pipeline processes cards. It is safe for concurrent use across slots.
type Pipeline struct {
	log *slog.Logger
}

func New(log *slog.Logger) *Pipeline {
	return &Pipeline{log: log}
}

// Run currently scans the card and returns the file inventory. Later milestones
// extend this to copy, verify, and erase.
func (p *Pipeline) Run(ctx context.Context, in Input) (Result, error) {
	var res Result

	err := afero.Walk(in.FS, "", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res.Files = append(res.Files, card.FileEntry{
			Path:    path,
			Size:    info.Size(),
			ModTime: info.ModTime(),
		})
		res.Bytes += info.Size()
		return nil
	})
	if err != nil {
		return res, err
	}

	p.log.Info("pipeline: scanned card",
		slog.String("slot", string(in.Slot)),
		slog.String("serial", in.Serial),
		slog.Int("files", len(res.Files)),
		slog.Int64("bytes", res.Bytes),
	)
	return res, nil
}
