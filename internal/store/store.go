// Package store persists jobs, per-file records, and the verified-hash index in
// SQLite. The hash index gives idempotence: re-inserting a half-ingested card
// resumes rather than re-copying, and previously-seen content dedupes.
//
// Milestone 1 stub: an in-memory Noop implementing the interface so the app can
// be wired. The SQLite implementation lands in milestone 2.
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

// Noop is a stateless Store that never reports anything as seen.
type Noop struct{}

func (Noop) Seen(context.Context, string) (bool, error) { return false, nil }
func (Noop) MarkVerified(context.Context, string) error { return nil }
func (Noop) Close() error                               { return nil }
