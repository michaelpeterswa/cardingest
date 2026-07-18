package rules

import (
	"path/filepath"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/michaelpeterswa/cardingest/internal/card"
)

// matcher is a compiled set of AND-ed match criteria. A zero matcher (no
// criteria) matches every file.
type matcher struct {
	exts    map[string]bool // lowercased extensions incl. dot; empty = any
	glob    string          // doublestar pattern; "" = any
	maxSize int64
	hasMax  bool
	minSize int64
	hasMin  bool
}

func (m matcher) matches(f card.FileEntry) bool {
	if len(m.exts) > 0 && !m.exts[strings.ToLower(filepath.Ext(f.Path))] {
		return false
	}
	if m.glob != "" {
		// Pattern validated at compile time, so any error here means no match.
		if ok, err := doublestar.Match(m.glob, normalizeSlash(f.Path)); err != nil || !ok {
			return false
		}
	}
	if m.hasMax && f.Size > m.maxSize {
		return false
	}
	if m.hasMin && f.Size < m.minSize {
		return false
	}
	return true
}

// normalizeSlash converts OS path separators to forward slashes so globs are
// portable regardless of where the card was scanned.
func normalizeSlash(p string) string {
	if filepath.Separator == '/' {
		return p
	}
	return strings.ReplaceAll(p, string(filepath.Separator), "/")
}
