// Package overlay makes a pool account dir present the live contents of
// ~/.claude with writes shared straight back, so a pooled session sees the same
// projects/skills/settings as plain `claude`. Two interchangeable providers:
//
//   - symlink (default + always-available fallback): symlink each top-level
//     entry of ~/.claude into the account dir.
//   - fuse (preferred when fuse-t is installed; built with -tags fuse): an
//     in-process passthrough mirror mounted via fuse-t.
//
// Both yield the same observable result. A small set of entries is held back
// from sharing because they are instance-local runtime state that would
// conflict across concurrent sessions; see ExcludedEntries and PrivateEntry.
// Held-back files stay per-account, but .claude.json's shareable top-level
// keys (everything outside ClaudeJSONPrivateKeys, plus the per-project
// approval keys ClaudeJSONSharedProjectKeys carves out of "projects") still
// propagate: one-way base→account at launch under symlink
// (pool.MergeBaseClaudeJSON), two-way under fuse (live merged read view plus
// shareable-key write-through to ~/.claude.json).
package overlay

import "strings"

// Kind identifies an overlay provider.
type Kind string

const (
	KindSymlink Kind = "symlink"
	KindFuse    Kind = "fuse"
)

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

// skipEntries are never linked or mirrored (noise / OS cruft).
var skipEntries = map[string]bool{
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

// Provider establishes and maintains an overlay of base at accountDir.
type Provider interface {
	Kind() Kind

	// Setup makes accountDir reflect base. Idempotent.
	Setup(base, accountDir string) error

	// Sync re-asserts the overlay, picking up new top-level entries in base
	// and repairing drift. Idempotent. Safe to call repeatedly.
	Sync(base, accountDir string) error

	// Health returns nil if the overlay is intact, else a descriptive error.
	Health(base, accountDir string) error

	// Teardown removes the overlay from accountDir. It must never touch base.
	Teardown(base, accountDir string) error

	// PrivateRoot returns the directory where account-local (private) files
	// physically live. For the symlink provider that is accountDir itself; for
	// fuse it is the private backing dir beside the mountpoint. Writing there
	// is correct whether or not a mount is currently up.
	PrivateRoot(accountDir string) string
}

// FuseBuilt reports whether this binary includes the fuse provider at all.
func FuseBuilt() bool { return fuseBuilt }
