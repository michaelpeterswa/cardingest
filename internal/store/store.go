// Package store persists the verified-hash index (and, later, jobs and per-file
// records) in SQLite. The hash index gives idempotence: re-inserting a
// half-ingested card resumes rather than re-copying, and previously-seen
// content dedupes.
//
// The SQLite backend uses the pure-Go modernc.org/sqlite driver so the binary
// still builds with CGO_ENABLED=0 for the distroless image.
package store

import "context"

// Store records verified file hashes for idempotence/dedupe.
type Store interface {
	// Seen reports whether a file with this content hash has already been
	// verified into the destination.
	Seen(ctx context.Context, hash string) (bool, error)
	// MarkVerified records a hash as verified-and-landed.
	MarkVerified(ctx context.Context, hash string) error
	Close() error
}

// Noop is a stateless Store that never reports anything as seen. Useful in
// tests and when idempotence is not desired.
type Noop struct{}

func (Noop) Seen(context.Context, string) (bool, error) { return false, nil }
func (Noop) MarkVerified(context.Context, string) error { return nil }
func (Noop) Close() error                               { return nil }
