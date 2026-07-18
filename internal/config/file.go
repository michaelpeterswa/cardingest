package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"gopkg.in/yaml.v3"
)

// File is the runtime ingest policy, loaded from the YAML at CONFIG_PATH and
// round-tripped by the web UI. Unlike Config (env), it is hot-editable.
//
// The full schema is defined in INSTRUCTIONS.md. Milestone 1 only reads
// Reader.USBIDs and Destination.Path, but the whole document is modelled so the
// rules editor can load and save it without losing fields.
type File struct {
	Destination Destination         `yaml:"destination"`
	Categories  map[string][]string `yaml:"categories"`
	Rules       []Rule              `yaml:"rules"`
	Card        CardPolicy          `yaml:"card"`
	Reader      Reader              `yaml:"reader"`
	Notify      []Notifier          `yaml:"notify"`
}

type Destination struct {
	Path       string `yaml:"path"`
	Layout     string `yaml:"layout"`
	DateSource string `yaml:"date_source"`
}

// Rule is one ordered match/action entry. The final entry in the spec's YAML is
// a bare `{default: skip}`; Default is set for that form and Name/Match/Action
// are empty.
type Rule struct {
	Name    string     `yaml:"name,omitempty"`
	Match   *RuleMatch `yaml:"match,omitempty"`
	Action  string     `yaml:"action,omitempty"`
	Default string     `yaml:"default,omitempty"`
}

type RuleMatch struct {
	Ext      []string `yaml:"ext,omitempty"`
	PathGlob string   `yaml:"path_glob,omitempty"`
	MaxSize  string   `yaml:"max_size,omitempty"` // human size, e.g. "100KB"
	MinSize  string   `yaml:"min_size,omitempty"`
}

type CardPolicy struct {
	EraseIngested bool `yaml:"erase_ingested"`
	EraseSkipped  bool `yaml:"erase_skipped"`
	EjectWhenDone bool `yaml:"eject_when_done"`
}

type Reader struct {
	USBIDs []string `yaml:"usb_ids"` // "vendor:product" allowlist, lowercase hex
}

type Notifier struct {
	Type string   `yaml:"type"` // ntfy | webhook | smtp
	URL  string   `yaml:"url,omitempty"`
	On   []string `yaml:"on,omitempty"` // start | complete | error
}

// Store loads, holds, and persists the YAML policy File. It is safe for
// concurrent use: the ingest workers read snapshots while the UI saves edits.
type Store struct {
	path string

	mu   sync.RWMutex
	file File
}

// NewStore loads the policy file at path. A missing file is not an error: the
// store starts with a zero-valued File so a fresh deployment can be configured
// through the UI.
func NewStore(path string) (*Store, error) {
	s := &Store{path: path}
	if err := s.Load(); err != nil {
		return nil, err
	}
	return s, nil
}

// Load re-reads the file from disk, replacing the in-memory snapshot.
func (s *Store) Load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			s.mu.Lock()
			s.file = File{}
			s.mu.Unlock()
			return nil
		}
		return fmt.Errorf("read config %s: %w", s.path, err)
	}

	var f File
	if err := yaml.Unmarshal(data, &f); err != nil {
		return fmt.Errorf("parse config %s: %w", s.path, err)
	}

	s.mu.Lock()
	s.file = f
	s.mu.Unlock()
	return nil
}

// Get returns a copy of the current policy snapshot.
func (s *Store) Get() File {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.file
}

// Save atomically persists f (write to a temp file in the same directory, then
// rename) and updates the in-memory snapshot.
func (s *Store) Save(f File) error {
	data, err := yaml.Marshal(f)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".config-*.yaml.tmp")
	if err != nil {
		return fmt.Errorf("create temp config: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp config: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp config: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("rename temp config: %w", err)
	}

	s.mu.Lock()
	s.file = f
	s.mu.Unlock()
	return nil
}
