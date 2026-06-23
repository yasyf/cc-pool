// Package overlay holds cc-pool's mirror-specific overlay code: the
// per-consumer CLASSIFICATION (ExcludedEntries, SharedEntries, SkipEntries,
// PrivateEntry) that builds the fusekit/overlay Spec, plus the fuse MIRROR and
// the detached mount-holder host (fuse-tagged) that fusekit/overlay's
// RemoteFuseProvider drives over its socket. The overlay ABSTRACTION — the
// Backend type, the symlink/fuse Provider interface, provider selection, and
// the conversion/migration primitives — now lives in
// github.com/yasyf/fusekit/overlay; cc-pool is a blind consumer of it.
//
// A small set of ~/.claude entries is held back from sharing because they are
// instance-local runtime state that would conflict across concurrent sessions;
// see ExcludedEntries and PrivateEntry. Held-back files stay per-account, but
// .claude.json's shareable top-level keys (everything outside
// ClaudeJSONPrivateKeys, plus the per-project approval keys
// ClaudeJSONSharedProjectKeys carves out of "projects") still propagate: one-way
// base→account at launch under symlink (pool.MergeBaseClaudeJSON), two-way under
// fuse (live merged read view plus shareable-key write-through to ~/.claude.json).
package overlay

import "strings"

// ExcludedEntries are top-level ~/.claude entries that must NOT be shared
// across accounts. Each excluded entry becomes a private, empty per-account
// directory instead.
//
//   - daemon:  Claude Code's own PID-keyed worker supervisor (daemon/roster.json
//     records a supervisorPid + worker registry). Sharing it makes two sessions
//     fight over one supervisor.
//   - ide:     per-process IDE lock/socket files; a pooled session must not
//     advertise itself on another account's IDE registry.
//   - backups: claude's rotating backups of $CONFIG_DIR/.claude.json. Sharing
//     it surfaces one account's config backups inside another's restore prompt
//     (cross-account contamination) and commingles every account's backups.
var ExcludedEntries = map[string]bool{
	"daemon":  true,
	"ide":     true,
	"backups": true,
}

// SharedEntries are top-level entries that must be shared across all accounts
// even when ~/.claude does not contain them yet. claude writes these lazily into
// $CLAUDE_CONFIG_DIR (the account dir itself), so without proactively creating
// them in the base and linking them they would be born as real per-account dirs
// and scatter. plans (plan-mode plans) is the motivating case. Disjoint from
// ExcludedEntries / PrivateEntry.
//
// SharedEntries["plans"] is the underlying physical share — the symlink-account
// mechanism (acct-NN/plans → ~/.claude/plans) and the fuse base-absent fallback
// both rely on it, so it must stay. On top of it, fuse accounts ADDITIONALLY get
// the canonical ~/.claude/plans path REPORTED to the session via an injected
// settings.json plansDirectory (settingsView, fuse_settings.go), so a pooled
// claude writes and reports the shared path directly rather than its per-account
// $CONFIG_DIR/plans. The injection is an additive reporting layer, not a
// replacement for this physical share.
var SharedEntries = map[string]bool{
	"plans": true,
}

// SkipEntries are never linked or mirrored (noise / OS cruft). Exported so the
// pool can read it when building the fusekit/overlay Spec.
var SkipEntries = map[string]bool{
	".DS_Store": true,
}

// PrivateEntry reports whether a top-level entry name is per-account private:
// the excluded dirs above, plus claude's primary state file .claude.json and
// its atomic-write temp files (.claude.json.tmp.XXXX) — the file itself is
// never linked or mirrored, but only its ClaudeJSONPrivateKeys (identity like
// oauthAccount, per-account state) are truly private — and even there,
// "projects" carves out the per-project ClaudeJSONSharedProjectKeys, which
// cross both ways; every other key propagates between base and account
// (symlink: one-way base-wins launch merge, pool.MergeBaseClaudeJSON; fuse:
// live merged view with shareable keys written through to ~/.claude.json). Plus .credentials.json (and its
// temp/lock siblings), claude's plaintext credential
// store — the OAuth token blob it writes to $CONFIG_DIR/.credentials.json when
// the macOS Keychain is unavailable (e.g. a headless SSH session). Sharing it
// would symlink plain claude's live credential into a pool account, so
// `claude /login` would adopt plain claude's login and a refresh would mutate
// it — the exact thing the pool must never do. Plus .last-update-result.json
// (claude's auto-update result, instance-local), which claude rewrites
// atomically — replacing the overlay's symlink with a real file that Sync would
// otherwise refuse to relink on every poll. Plus remote-settings.json (and its
// atomic-write temp siblings), claude's cached per-subscription settings
// fetched from claude.ai — per-account state claude writes directly into
// $CONFIG_DIR with the same atomic-rewrite symlink-clobbering mode as
// .last-update-result.json.
func PrivateEntry(name string) bool {
	return ExcludedEntries[name] ||
		name == ".claude.json" || strings.HasPrefix(name, ".claude.json.") ||
		name == ".credentials.json" || strings.HasPrefix(name, ".credentials.json.") ||
		strings.HasPrefix(name, ".last-update-result") ||
		name == "remote-settings.json" || strings.HasPrefix(name, "remote-settings.json.")
}
