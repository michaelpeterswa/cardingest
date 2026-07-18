// Package exifdate extracts the capture date used to folder ingested files:
// EXIF DateTimeOriginal for stills, the MP4/QuickTime creation atom for video,
// falling back to file mtime. (Dates drive foldering only, never rules.)
//
// Milestone 1 stub: only the mtime fallback is implemented. EXIF and MP4 atom
// parsing land alongside the copy stage in milestone 2.
package exifdate

import (
	"time"

	"github.com/michaelpeterswa/cardingest/internal/card"
)

// Source describes where a date came from, for logging/diagnostics.
type Source int

const (
	SourceMtime Source = iota
	SourceEXIF
	SourceMP4
)

// DateOf returns the capture date for a file. For now it always uses mtime.
func DateOf(f card.FileEntry) (time.Time, Source) {
	return f.ModTime, SourceMtime
}
