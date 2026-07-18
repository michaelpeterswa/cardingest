package rules

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/michaelpeterswa/cardingest/internal/config"
)

// Compile turns the YAML policy rules into an Engine.
//
// Semantics: rules are evaluated in order, first match wins. A bare
// `- default: <action>` entry sets the fallback for files that match no rule.
// If no rules are configured at all, everything is kept (KeepAll) and the
// categorizer decides foldering. If rules are present but no default entry is
// given, unmatched files are skipped (the conservative choice).
func Compile(cfgRules []config.Rule) (Engine, error) {
	if len(cfgRules) == 0 {
		return KeepAll{}, nil
	}

	e := &engine{defaultAct: Skip}
	for i, r := range cfgRules {
		if r.Default != "" {
			act, err := parseAction(r.Default)
			if err != nil {
				return nil, fmt.Errorf("rule %d (default): %w", i, err)
			}
			e.defaultAct = act
			continue
		}

		act, err := parseAction(r.Action)
		if err != nil {
			return nil, fmt.Errorf("rule %d (%q): %w", i, r.Name, err)
		}
		m, err := compileMatch(r.Match)
		if err != nil {
			return nil, fmt.Errorf("rule %d (%q): %w", i, r.Name, err)
		}
		e.rules = append(e.rules, compiledRule{match: m, action: act})
	}
	return e, nil
}

func parseAction(s string) (Action, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "keep":
		return Keep, nil
	case "skip":
		return Skip, nil
	default:
		return Skip, fmt.Errorf("unknown action %q (want keep|skip)", s)
	}
}

func compileMatch(rm *config.RuleMatch) (matcher, error) {
	var m matcher
	if rm == nil {
		return m, nil // matches everything
	}

	if len(rm.Ext) > 0 {
		m.exts = make(map[string]bool, len(rm.Ext))
		for _, e := range rm.Ext {
			m.exts[strings.ToLower(strings.TrimSpace(e))] = true
		}
	}
	if rm.PathGlob != "" {
		if !doublestar.ValidatePattern(rm.PathGlob) {
			return m, fmt.Errorf("invalid path_glob %q", rm.PathGlob)
		}
		m.glob = rm.PathGlob
	}
	if rm.MaxSize != "" {
		n, err := parseSize(rm.MaxSize)
		if err != nil {
			return m, err
		}
		m.maxSize, m.hasMax = n, true
	}
	if rm.MinSize != "" {
		n, err := parseSize(rm.MinSize)
		if err != nil {
			return m, err
		}
		m.minSize, m.hasMin = n, true
	}
	return m, nil
}

// parseSize parses a human-readable byte size such as "100KB", "4MiB", "512".
// Decimal units (KB/MB/GB/TB) are powers of 1000; binary units (KiB/MiB/GiB/TiB)
// are powers of 1024. A bare number is bytes.
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}

	i := 0
	for i < len(s) && (s[i] == '.' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	num, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, fmt.Errorf("bad size %q: %w", s, err)
	}

	var mult int64
	switch strings.ToUpper(strings.TrimSpace(s[i:])) {
	case "", "B":
		mult = 1
	case "K", "KB":
		mult = 1000
	case "M", "MB":
		mult = 1000 * 1000
	case "G", "GB":
		mult = 1000 * 1000 * 1000
	case "T", "TB":
		mult = 1000 * 1000 * 1000 * 1000
	case "KIB":
		mult = 1 << 10
	case "MIB":
		mult = 1 << 20
	case "GIB":
		mult = 1 << 30
	case "TIB":
		mult = 1 << 40
	default:
		return 0, fmt.Errorf("unknown size unit in %q", s)
	}
	return int64(num * float64(mult)), nil
}
