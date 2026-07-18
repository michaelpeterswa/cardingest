// Package rules decides keep vs skip for each scanned file by applying the
// ordered rule list from the YAML policy config (first match wins).
//
// Milestone 1 stub: the real ordered-rule engine lands in milestone 3. KeepAll
// exists so the pipeline can be wired now.
package rules

import "github.com/michaelpeterswa/cardingest/internal/card"

// Action is the decision for a file.
type Action int

const (
	Keep Action = iota
	Skip
)

// Engine decides what to do with a scanned file.
type Engine interface {
	Decide(f card.FileEntry) Action
}

// KeepAll keeps every file. Placeholder until the config-driven engine exists.
type KeepAll struct{}

func (KeepAll) Decide(card.FileEntry) Action { return Keep }
