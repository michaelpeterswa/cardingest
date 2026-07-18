package rules

import (
	"testing"

	"github.com/michaelpeterswa/cardingest/internal/card"
	"github.com/michaelpeterswa/cardingest/internal/config"
)

// specRules mirrors the ordered rules in INSTRUCTIONS.md.
func specRules() []config.Rule {
	return []config.Rule{
		{Name: "skip thumbnails", Action: "skip", Match: &config.RuleMatch{
			Ext: []string{".thm"}, PathGlob: "**/THMBNL/**"}},
		{Name: "skip tiny files", Action: "skip", Match: &config.RuleMatch{MaxSize: "100KB"}},
		{Name: "keep media", Action: "keep", Match: &config.RuleMatch{
			Ext: []string{".arw", ".dng", ".jpg", ".mp4", ".mov"}}},
		{Default: "skip"},
	}
}

func TestEngineSpecRules(t *testing.T) {
	eng, err := Compile(specRules())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	const big = 200 * 1024 // above the 100KB tiny threshold
	cases := []struct {
		name string
		file card.FileEntry
		want Action
	}{
		{"thumbnail in THMBNL", card.FileEntry{Path: "DCIM/THMBNL/x.THM", Size: 500}, Skip},
		{"thm outside THMBNL is tiny", card.FileEntry{Path: "DCIM/x.THM", Size: 500}, Skip},
		{"large raw kept", card.FileEntry{Path: "DCIM/big.ARW", Size: big}, Keep},
		{"large jpg kept", card.FileEntry{Path: "DCIM/big.JPG", Size: big}, Keep},
		{"small raw is tiny", card.FileEntry{Path: "DCIM/small.ARW", Size: 50}, Skip},
		{"large unknown -> default skip", card.FileEntry{Path: "DOCS/readme.txt", Size: big}, Skip},
	}
	for _, c := range cases {
		if got := eng.Decide(c.file); got != c.want {
			t.Errorf("%s: Decide=%v want %v", c.name, got, c.want)
		}
	}
}

func TestCompileEmptyRulesKeepsAll(t *testing.T) {
	eng, err := Compile(nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if _, ok := eng.(KeepAll); !ok {
		t.Fatalf("empty rules should compile to KeepAll, got %T", eng)
	}
	if eng.Decide(card.FileEntry{Path: "anything.xyz", Size: 1}) != Keep {
		t.Fatal("KeepAll should keep everything")
	}
}

func TestDefaultWithoutDefaultEntryIsSkip(t *testing.T) {
	eng, err := Compile([]config.Rule{
		{Name: "keep raw", Action: "keep", Match: &config.RuleMatch{Ext: []string{".arw"}}},
	})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if got := eng.Decide(card.FileEntry{Path: "x.txt", Size: 10}); got != Skip {
		t.Fatalf("unmatched file default = %v, want Skip", got)
	}
}

func TestCompileErrors(t *testing.T) {
	if _, err := Compile([]config.Rule{{Name: "bad", Action: "purge"}}); err == nil {
		t.Fatal("expected error for unknown action")
	}
	if _, err := Compile([]config.Rule{{Name: "bad", Action: "keep",
		Match: &config.RuleMatch{MaxSize: "10 bananas"}}}); err == nil {
		t.Fatal("expected error for bad size")
	}
}

func TestParseSize(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"512", 512},
		{"100KB", 100_000},
		{"1.5MB", 1_500_000},
		{"4MiB", 4 * 1024 * 1024},
		{"1GiB", 1024 * 1024 * 1024},
		{"2B", 2},
	}
	for _, c := range cases {
		got, err := parseSize(c.in)
		if err != nil {
			t.Errorf("parseSize(%q) error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseSize(%q) = %d, want %d", c.in, got, c.want)
		}
	}
	for _, bad := range []string{"", "abc", "10XB", "KB"} {
		if _, err := parseSize(bad); err == nil {
			t.Errorf("parseSize(%q) should error", bad)
		}
	}
}
