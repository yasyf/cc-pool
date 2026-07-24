package overlay

import (
	"path"
	"slices"
	"strings"
	"testing"
)

// TestPrivateClassificationDisjoint pins the credential-leak invariant across every
// classification category: the shared carve-out set (SharedTopLevel) and the private
// set (PrivateEntry or the carveOutPrivate leak guard) never overlap — a name in both
// would cross plain claude state into an account projection. Each case also asserts the
// concrete verdict so a dropped arm (e.g. carveOutPrivate losing its glob match) is
// caught, not just a lucky still-disjoint result.
func TestPrivateClassificationDisjoint(t *testing.T) {
	cases := []struct {
		name        string
		desc        string
		wantShared  bool
		wantPrivate bool
	}{
		{name: "daemon", desc: "excluded dir", wantPrivate: true},
		{name: ".credentials.json", desc: "exact private", wantPrivate: true},
		{name: "remote-settings.json", desc: "exact private", wantPrivate: true},
		{name: ".claude.json.tmp.abcd", desc: "prefix private (atomic-write temp sibling)", wantPrivate: true},
		{name: ".credentials.json~", desc: "bare-prefix gap-class private", wantPrivate: true},
		{name: "session.lock", desc: "glob-pattern private", wantPrivate: true},
		{name: "foo.lock", desc: "glob-pattern private", wantPrivate: true},
		{name: "SESSION.LOCK", desc: "case-alias of a glob match (leak guard only)", wantPrivate: true},
		{name: ".Credentials.json", desc: "case-alias of exact private", wantPrivate: true},
		{name: "Backups", desc: "case-alias of excluded dir", wantPrivate: true},
		{name: "projects", desc: "shared carve-out dir", wantShared: true},
		{name: "statsig", desc: "shared carve-out dir", wantShared: true},
		{name: "history.jsonl", desc: "shared carve-out file", wantShared: true},
		{name: "lockfile", desc: "near-miss: no .lock suffix stays shared", wantShared: true},
		{name: "mcp-needs-auth.json", desc: "near-miss: genuinely different file stays shared", wantShared: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shared := SharedTopLevel(tc.name)
			private := PrivateEntry(tc.name) || carveOutPrivate(tc.name)
			if shared && private {
				t.Fatalf("%s (%s): SharedTopLevel AND private — the carve-out set must stay disjoint from private classification", tc.name, tc.desc)
			}
			if shared != tc.wantShared {
				t.Errorf("SharedTopLevel(%q) = %v, want %v (%s)", tc.name, shared, tc.wantShared, tc.desc)
			}
			if private != tc.wantPrivate {
				t.Errorf("PrivateEntry||carveOutPrivate(%q) = %v, want %v (%s)", tc.name, private, tc.wantPrivate, tc.desc)
			}
		})
	}
}

func TestSkipPrefixesContainOnlyCurrentSourceArtifacts(t *testing.T) {
	if len(SkipPrefixes) != 1 || SkipPrefixes[0] != "._" {
		t.Fatalf("SkipPrefixes = %v, want only AppleDouble sidecars", SkipPrefixes)
	}
	for _, name := range []string{".fuse_hidden.ccpool.orphan", ".nfs.orphan"} {
		if !SharedTopLevel(name) {
			t.Errorf("SharedTopLevel(%q) = false, want ordinary current source content", name)
		}
	}
}

// TestPrivatePatternsValid fails loud on a typo'd PrivatePatterns entry: PrivateEntry
// and carveOutPrivate discard path.Match's error (a glob is a compile-time constant),
// so a malformed pattern would silently match nothing — this guard surfaces the
// ErrBadPattern the production path swallows.
func TestPrivatePatternsValid(t *testing.T) {
	for _, p := range PrivatePatterns {
		if _, err := path.Match(p, "sample.name"); err != nil {
			t.Errorf("PrivatePatterns entry %q is not a valid path.Match glob: %v", p, err)
		}
	}
}

func TestCacheFileClassification(t *testing.T) {
	cases := []struct {
		name        string
		wantPrivate bool
		wantShared  bool
	}{
		{name: "stats-cache.json", wantPrivate: true},
		{name: "stats-cache.json.tmp.abcd", wantPrivate: true},
		{name: "policy-limits.json", wantPrivate: true},
		{name: "policy-limits.json.tmp.abcd", wantPrivate: true},
		{name: "readout-cost-cache.json", wantShared: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := PrivateEntry(tc.name); got != tc.wantPrivate {
				t.Errorf("PrivateEntry(%q) = %v, want %v", tc.name, got, tc.wantPrivate)
			}
			if got := carveOutPrivate(tc.name); got != tc.wantPrivate {
				t.Errorf("carveOutPrivate(%q) = %v, want %v", tc.name, got, tc.wantPrivate)
			}
			if got := SharedTopLevel(tc.name); got != tc.wantShared {
				t.Errorf("SharedTopLevel(%q) = %v, want %v", tc.name, got, tc.wantShared)
			}
		})
	}
}

func TestPrivateStagingFamiliesAreExactTopLevelAtomicTemporaryPrefixes(t *testing.T) {
	want := []string{
		".claude.json.tmp.",
		"settings.json.tmp.",
		".credentials.json.tmp.",
		".last-update-result.json.tmp.",
		"remote-settings.json.tmp.",
		"mcp-needs-auth-cache.json.tmp.",
		"stats-cache.json.tmp.",
		"policy-limits.json.tmp.",
	}
	if !slices.Equal(PrivateStagingPrefixes, want) {
		t.Fatalf("PrivateStagingPrefixes = %v, want %v", PrivateStagingPrefixes, want)
	}
	for _, prefix := range want {
		if !PrivateStagingEntry(prefix+"A1B2") || !PrivateStagingEntry(strings.ToUpper(prefix)+"A1B2") {
			t.Errorf("private staging prefix %q did not match exact ASCII case-insensitive family", prefix)
		}
		if PrivateStagingEntry(strings.TrimSuffix(prefix, ".")) {
			t.Errorf("private staging prefix %q matched without the terminal separator", prefix)
		}
	}
	for _, name := range []string{
		".claude.json", "settings.json", ".credentials.json", ".last-update-result.json",
		"remote-settings.json", "mcp-needs-auth-cache.json", "stats-cache.json", "policy-limits.json",
		".storage-write.lock", ".oauth_refresh.lock",
	} {
		if PrivateStagingEntry(name) {
			t.Errorf("PrivateStagingEntry(%q) = true, want namespace or lock", name)
		}
	}
}
