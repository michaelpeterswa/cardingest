// Package rules decides keep vs skip for each scanned file by applying the
// ordered rule list from the YAML policy config (first match wins).
//
// A rule matches when *all* of its specified criteria match (ext AND path_glob
// AND size bounds); to express alternatives, write separate rules. Evaluation
// stops at the first matching rule and returns its action. If no rule matches,
// the engine's default action is used.
package rules

import (
	"github.com/michaelpeterswa/cardingest/internal/card"
)

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

// KeepAll keeps every file. Used when no rules are configured.
type KeepAll struct{}

func (KeepAll) Decide(card.FileEntry) Action { return Keep }

// engine is the ordered first-match-wins rule evaluator.
type engine struct {
	rules      []compiledRule
	defaultAct Action
}

type compiledRule struct {
	match  matcher
	action Action
}

func (e *engine) Decide(f card.FileEntry) Action {
	for _, r := range e.rules {
		if r.match.matches(f) {
			return r.action
		}
	}
	return e.defaultAct
}
