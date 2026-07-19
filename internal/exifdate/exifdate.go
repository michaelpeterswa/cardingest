// Package exifdate extracts the capture date used to folder ingested files:
// EXIF DateTimeOriginal for stills, the MP4/QuickTime creation atom for video,
// falling back to file mtime. (Dates drive foldering only, never rules — exFAT
// mtimes are unreliable, but they remain the last-resort fallback.)
package exifdate

import (
	"path/filepath"
	"strings"
	"time"

	"github.com/michaelpeterswa/cardingest/internal/card"
	"github.com/rwcarlsen/goexif/exif"
	"github.com/spf13/afero"
)

// Source describes where a date came from, for logging/diagnostics.
type Source int

const (
	SourceMtime Source = iota
	SourceEXIF
	SourceMP4
)

func (s Source) String() string {
	switch s {
	case SourceEXIF:
		return "exif"
	case SourceMP4:
		return "mp4"
	default:
		return "mtime"
	}
}

var (
	stillExts = map[string]bool{
		".jpg": true, ".jpeg": true, ".arw": true, ".dng": true,
		".tif": true, ".tiff": true, ".cr2": true, ".cr3": true, ".nef": true,
	}
	videoExts = map[string]bool{
		".mp4": true, ".mov": true, ".m4v": true,
	}
)

// DateOf returns the capture date for a file on fs, preferring embedded
// metadata (EXIF for stills, the mvhd atom for video) and falling back to the
// file's mtime. It never returns an error: any parse failure falls back.
func DateOf(fs afero.Fs, e card.FileEntry) (time.Time, Source) {
	ext := strings.ToLower(filepath.Ext(e.Path))
	switch {
	case stillExts[ext]:
		if t, ok := exifDate(fs, e.Path); ok {
			return t, SourceEXIF
		}
	case videoExts[ext]:
		if t, ok := mp4CreationDate(fs, e.Path); ok {
			return t, SourceMP4
		}
	}
	return e.ModTime, SourceMtime
}

// exifDate reads EXIF DateTimeOriginal (falling back to DateTime) from a still.
func exifDate(fs afero.Fs, path string) (time.Time, bool) {
	f, err := fs.Open(path)
	if err != nil {
		return time.Time{}, false
	}
	defer func() { _ = f.Close() }()

	x, err := exif.Decode(f)
	if err != nil {
		return time.Time{}, false
	}
	t, err := x.DateTime() // DateTimeOriginal, then DateTime
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
