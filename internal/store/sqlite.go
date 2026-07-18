package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite" // pure-Go driver, registered as "sqlite"
)

// SQLite is a Store backed by a SQLite database file.
type SQLite struct {
	db *sql.DB
}

// OpenSQLite opens (creating if needed) the database at path and applies the
// schema. Parent directories are created.
func OpenSQLite(path string) (*SQLite, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create state dir: %w", err)
		}
	}

	// _pragma busy_timeout avoids "database is locked" under concurrent slots.
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}

	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS verified_hashes (
			hash       TEXT PRIMARY KEY,
			created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
		)`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}

	return &SQLite{db: db}, nil
}

func (s *SQLite) Seen(ctx context.Context, hash string) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM verified_hashes WHERE hash = ?`, hash).Scan(&one)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("query hash: %w", err)
	default:
		return true, nil
	}
}

func (s *SQLite) MarkVerified(ctx context.Context, hash string) error {
	if _, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO verified_hashes (hash) VALUES (?)`, hash); err != nil {
		return fmt.Errorf("mark verified: %w", err)
	}
	return nil
}

func (s *SQLite) Close() error { return s.db.Close() }
