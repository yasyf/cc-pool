// Package overlay holds cc-pool's mirror-specific overlay code: the ~/.claude
// entry classification (ExcludedEntries, SharedEntries, SkipEntries,
// SkipPrefixes, PrivateEntry) that builds the fusekit/overlay Spec, the fuse
// mirror, and the detached mount-holder host (fuse-tagged) driven by
// fusekit/overlay's
// RemoteFuseProvider. The overlay abstraction (Backend, Provider, selection,
// migration) lives in github.com/yasyf/fusekit/overlay.
package overlay

import "strings"

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

// SharedEntries are top-level entries shared across all accounts even when
// ~/.claude lacks them yet: claude writes them lazily into $CLAUDE_CONFIG_DIR, so
// without pre-creating them in the base and linking they would scatter as real
// per-account dirs. plans is the motivating case. Disjoint from ExcludedEntries
// and PrivateEntry.
//
// plans is the physical share both the symlink mechanism (acct-NN/plans →
// ~/.claude/plans) and the fuse base-absent fallback rely on, so it must stay.
// Fuse accounts additionally report the canonical path via an injected
// settings.json plansDirectory (fuse_settings.go) — an additive reporting layer,
// not a replacement for this share.
var SharedEntries = map[string]bool{
	"plans": true,
}

// SkipEntries are top-level entries never linked or mirrored (OS cruft).
var SkipEntries = map[string]bool{
	".DS_Store": true,
}

// SkipPrefixes are top-level name prefixes skipped exactly like SkipEntries
// (fkoverlay.Spec.Skipped matches by HasPrefix): AppleDouble "._*" sidecar
// litter, plus ".fuse_hidden*" and ".nfs.*" silly-rename litter left by crashed
// old-provider fuse-t/NFS mounts — ignored and cleaned by conversion and sweeps,
// never linked, moved, or carved out into ~/.claude, but NOT blocked from
// creation through the mount.
var SkipPrefixes = []string{"._", ".fuse_hidden", ".nfs."}

// PrivateEntry reports whether a top-level entry name is per-account private:
//
//   - the ExcludedEntries dirs;
//   - .claude.json and its atomic-write temp siblings (.claude.json.tmp.XXXX);
//   - .credentials.json and siblings: sharing would let a pool account adopt and
//     rotate plain claude's login, which the pool must never do;
//   - .last-update-result.json, remote-settings.json, and mcp-needs-auth-cache.json
//     (and temp siblings): claude rewrites these atomically, clobbering the overlay
//     symlink Sync would otherwise refuse to relink. mcp-needs-auth-cache.json is
//     per-account MCP auth state — sharing it would cross one account's server
//     auth prompts into another's.
func PrivateEntry(name string) bool {
	return ExcludedEntries[name] ||
		name == ".claude.json" || strings.HasPrefix(name, ".claude.json.") ||
		name == ".credentials.json" || strings.HasPrefix(name, ".credentials.json.") ||
		strings.HasPrefix(name, ".last-update-result") ||
		name == "remote-settings.json" || strings.HasPrefix(name, "remote-settings.json.") ||
		name == "mcp-needs-auth-cache.json" || strings.HasPrefix(name, "mcp-needs-auth-cache.json.")
}

// sharedTopLevel reports whether a top-level base entry is carved out as a live
// symlink into ~/.claude: everything except private names, skipped litter, the two
// synthetic documents, and the probe. Mirrors the symlink provider's link loop
// (fkoverlay.Spec.Skipped/IsPrivate), except the private rejection is strictly
// wider: carveOutPrivate re-applies the holder's own bare-prefix private-redirect
// match, keeping the manifest's symlink set provably disjoint from the set the
// holder routes to the per-account private root. A name in both would serve the
// symlink — the holder consults the manifest before the private redirect — handing
// a pool session plain claude's file.
func sharedTopLevel(name string) bool {
	if PrivateEntry(name) || carveOutPrivate(name) || SkipEntries[name] || name == claudeJSONName || name == settingsName || name == ProbeFileName {
		return false
	}
	for _, p := range SkipPrefixes {
		if strings.HasPrefix(name, p) {
			return false
		}
	}
	return true
}

// PrivatePrefixes are the top-level name prefixes the shared holder routes to the
// per-account private root rather than the shared base ("source" mode), matched by
// HasPrefix so each covers its exact name and every atomic-write temp/lock sibling
// — keeping claude's tmp→rename commit (.claude.json.tmp.XXXX → .claude.json) on
// one filesystem. MUST stay in sync with PrivateEntry's file-family arms: together
// they are the one private-name policy.
var PrivatePrefixes = []string{
	".claude.json",
	".credentials.json",
	".last-update-result",
	"remote-settings.json",
	"mcp-needs-auth-cache.json",
}

// carveOutPrivate reports whether a top-level name is barred from the shared
// carve-out beyond PrivateEntry's dot-anchored families:
//
//   - any bare PrivatePrefixes match — the holder's own private-redirect test —
//     so a gap-class sibling like ".credentials.json~" (an editor backup of plain
//     claude's credential file) can never be both private-routed and symlinked;
//   - any case variant of a private family or excluded dir, because the default
//     APFS base resolves names case-insensitively: ".Credentials.json" there IS
//     plain claude's live credential file.
//
// Barring a name here is always safe — it falls back to a per-account
// passthrough or private entry, never a symlink into base.
func carveOutPrivate(name string) bool {
	if ExcludedEntries[strings.ToLower(name)] {
		return true
	}
	for _, p := range PrivatePrefixes {
		if len(name) >= len(p) && strings.EqualFold(name[:len(p)], p) {
			return true
		}
	}
	return false
}
