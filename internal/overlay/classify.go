// Package overlay holds Claude-specific merge and classification policy.
package overlay

import (
	"path"
	"strings"
)

const (
	claudeJSONName = ".claude.json"
	settingsName   = "settings.json"
)

// ExcludedEntries are top-level ~/.claude entries that must NOT be shared across
// accounts; each becomes a private, empty per-account directory instead:
//
//   - daemon:  claude's PID-keyed worker supervisor; sharing makes two sessions
//     fight over one supervisor.
//   - ide:     per-process IDE lock/socket files; a pooled session must not
//     advertise on another account's IDE registry.
//   - backups: claude's rotating .claude.json backups; sharing leaks one
//     account's backups into another's restore prompt.
var ExcludedEntries = map[string]bool{
	"daemon":  true,
	"ide":     true,
	"backups": true,
}

// SharedEntries are canonical top-level entries projected into every tenant.
// plans is both a shared physical subtree and the path injected into each
// tenant's synthetic settings.json. Disjoint from ExcludedEntries and
// PrivateEntry.
var SharedEntries = map[string]bool{
	"plans": true,
}

// SkipEntries are top-level OS artifacts omitted from every tenant projection.
var SkipEntries = map[string]bool{
	".DS_Store": true,
}

// SkipPrefixes are top-level AppleDouble sidecar prefixes omitted from every
// tenant projection.
var SkipPrefixes = []string{"._"}

// PrivatePatterns are top-level (path.Match, no Separator) glob patterns whose
// matches are per-account private — case-sensitive in PrivateEntry, case-
// insensitive in PrivateTopLevel. The complete set is part of the source
// authority declaration digest, so policy changes fence the durable fleet.
var PrivatePatterns = []string{"*.lock"}

// PrivateEntry reports whether a top-level entry name is per-account private:
//
//   - the ExcludedEntries dirs;
//   - .claude.json and its atomic-write temp siblings (.claude.json.tmp.XXXX);
//   - .credentials.json and siblings: sharing would let a pool account adopt and
//     rotate plain claude's login, which the pool must never do;
//   - .last-update-result.json, remote-settings.json, mcp-needs-auth-cache.json,
//     stats-cache.json, and policy-limits.json
//     (and temp siblings): claude rewrites these atomically. Keeping each family
//     in one tenant preserves replace identity and prevents account state from
//     crossing projections. mcp-needs-auth-cache.json is per-account MCP auth
//     state — sharing it would cross one account's server auth prompts into another's.
//   - .storage-write.lock, .oauth_refresh.lock: claude's mkdir/rmdir lock dirs.
//     Kept per-account so lock creation and removal target the same private source.
//   - any case-sensitive PrivatePatterns glob match (e.g. *.lock).
func PrivateEntry(name string) bool {
	if ExcludedEntries[name] || hasPrefix(name, PrivateSourcePrefixes, false) {
		return true
	}
	for _, p := range PrivatePatterns {
		if ok, _ := path.Match(p, name); ok {
			return true
		}
	}
	return false
}

// PrivateTopLevel reports whether a top-level account object belongs in the
// private FuseKit backing tree, including case-insensitive leak guards.
func PrivateTopLevel(name string) bool {
	return PrivateEntry(name) || carveOutPrivate(name)
}

// SharedTopLevel reports whether a top-level canonical entry belongs in every
// tenant projection. carveOutPrivate keeps shared and private source policy disjoint.
func SharedTopLevel(name string) bool {
	if PrivateTopLevel(name) || SkipEntries[name] || name == claudeJSONName || name == settingsName {
		return false
	}
	for _, p := range SkipPrefixes {
		if strings.HasPrefix(name, p) {
			return false
		}
	}
	return true
}

// PrivateSourcePrefixes are top-level name prefixes routed to the per-account source,
// matched by HasPrefix so each covers its exact name and every atomic-write temp/lock sibling
// — keeping claude's tmp→rename commit (.claude.json.tmp.XXXX → .claude.json) on
// one filesystem. MUST stay in sync with PrivateEntry's file-family arms: together
// they are the one private-name policy.
var PrivateSourcePrefixes = []string{
	".claude.json",
	"settings.json.tmp.",
	".credentials.json",
	".last-update-result",
	"remote-settings.json",
	"mcp-needs-auth-cache.json",
	"stats-cache.json",
	"policy-limits.json",
	".storage-write.lock",
	".oauth_refresh.lock",
}

// PrivateStagingPrefixes are the exact top-level atomic-write families that
// File Provider creates outside the public namespace until promotion.
var PrivateStagingPrefixes = []string{
	".claude.json.tmp.",
	"settings.json.tmp.",
	".credentials.json.tmp.",
	".last-update-result.json.tmp.",
	"remote-settings.json.tmp.",
	"mcp-needs-auth-cache.json.tmp.",
	"stats-cache.json.tmp.",
	"policy-limits.json.tmp.",
}

// PrivateLockPrefixes are private source locks, never File Provider staging.
var PrivateLockPrefixes = []string{".storage-write.lock", ".oauth_refresh.lock"}

// PrivateStagingEntry reports whether a top-level name belongs to an exact
// atomic-write staging family. Matching folds ASCII letters only.
func PrivateStagingEntry(name string) bool {
	return hasPrefix(name, PrivateStagingPrefixes, true)
}

// carveOutPrivate bars a name from shared projection beyond PrivateEntry:
// bare PrivateSourcePrefixes matches, case
// variants (the default APFS base is case-insensitive, so ".Credentials.json" IS
// the live credential file), and case-insensitive PrivatePatterns matches (so
// SESSION.LOCK never crosses tenants). Barring is always safe: an uncertain name
// remains private.
func carveOutPrivate(name string) bool {
	lower := strings.ToLower(name)
	if ExcludedEntries[lower] {
		return true
	}
	if hasPrefix(name, PrivateSourcePrefixes, true) {
		return true
	}
	for _, p := range PrivatePatterns {
		if ok, _ := path.Match(p, lower); ok {
			return true
		}
	}
	return false
}

func hasPrefix(value string, prefixes []string, foldASCII bool) bool {
	for _, prefix := range prefixes {
		if (!foldASCII && strings.HasPrefix(value, prefix)) ||
			(foldASCII && hasASCIIFoldPrefix(value, prefix)) {
			return true
		}
	}
	return false
}

func hasASCIIFoldPrefix(value, prefix string) bool {
	if len(value) < len(prefix) {
		return false
	}
	for index := range len(prefix) {
		left, right := value[index], prefix[index]
		if left >= 'A' && left <= 'Z' {
			left += 'a' - 'A'
		}
		if right >= 'A' && right <= 'Z' {
			right += 'a' - 'A'
		}
		if left != right {
			return false
		}
	}
	return true
}
