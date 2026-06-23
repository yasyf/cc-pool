// Package pool owns the canonical filesystem layout, account model, and
// per-account lifecycle helpers for cc-pool.
//
// Two distinct trees exist and must not be confused:
//
//   - ~/.claude      The canonical Claude Code config dir: plain `claude`'s
//     home and the shared overlay base. NEVER moved, never
//     registered as a pool account — the pool never touches plain
//     claude's credential or login identity. Its sibling state
//     file ~/.claude.json IS read (seeding, every launch merge)
//     and, under the fuse overlay, written through with shareable
//     keys; overlay.ClaudeJSONPrivateKeys never cross either way,
//     save the per-project overlay.ClaudeJSONSharedProjectKeys
//     carved out of "projects".
//   - ~/.cc-pool/    cc-pool's OWN state (sqlite db, daemon socket, logs),
//     plus accounts/ holding the pool account dirs
//     (acct-01, acct-02, ...). Each account dir is a real,
//     unique path so it gets its own Keychain item.
package pool

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/fusekit/state"
)

// stateDir is cc-pool's private per-user state tree (~/.cc-pool). The layout
// mechanics — home resolution, the dir join, idempotent creation, and the
// atomic status-mirror write — live in fusekit/state; the leaf names below are
// cc-pool's own.
var stateDir = state.Dir{App: "cc-pool"}

// Home returns the current user's home directory.
func Home() (string, error) {
	return state.Home()
}

func mustHome() string {
	h, err := Home()
	if err != nil {
		panic(err)
	}
	return h
}

// ClaudeDir is the canonical Claude config dir (~/.claude): plain `claude`'s
// home and the shared overlay base — never a pool account.
func ClaudeDir() string {
	return filepath.Join(mustHome(), ".claude")
}

// ClaudeJSONPath is plain claude's primary state file (~/.claude.json — in
// $HOME, NOT inside ~/.claude). With CLAUDE_CONFIG_DIR set, claude reads and
// writes $CONFIG_DIR/.claude.json instead. The pool reads it at add time
// (seedClaudeJSON, so new accounts inherit onboarding state and settings) and
// at every symlink-arm launch (MergeBaseClaudeJSON, shareable keys in, base
// wins); the fuse merged view writes shareable keys back through to it. Keys
// in overlay.ClaudeJSONPrivateKeys never cross in either direction, except
// the per-project overlay.ClaudeJSONSharedProjectKeys inside "projects".
func ClaudeJSONPath() string {
	return filepath.Join(mustHome(), ".claude.json")
}

// AccountsDir is the parent of all account dirs (~/.cc-pool/accounts).
func AccountsDir() string {
	return filepath.Join(StateDir(), "accounts")
}

// StateDir is cc-pool's own private state directory (~/.cc-pool).
func StateDir() string {
	return stateDir.Root()
}

// DBPath is the sqlite database path.
func DBPath() string {
	return stateDir.Path("pool.db")
}

// SocketPath is the daemon's unix socket path.
func SocketPath() string {
	return stateDir.Path("daemon.sock")
}

// LogPath is the daemon log path.
func LogPath() string {
	return stateDir.Path("daemon.log")
}

// MountsSocketPath is the mount-holder's unix socket path.
func MountsSocketPath() string {
	return stateDir.Path("mounts.sock")
}

// MountHolderLogPath is the mount-holder log path.
func MountHolderLogPath() string {
	return stateDir.Path("mount-holder.log")
}

// HolderBinDir is the stable, non-versioned directory under which the mount
// holder binary is materialized as a copy (~/.cc-pool/bin), passed to fusekit
// as mountd.Spawn.StableExecDir. macOS tccd keys the one-time "Network Volumes"
// grant on the holder's resolved path, and Homebrew installs each version at a
// new Cellar path; spawning from this fixed copy (whose embedded Developer-ID
// requirement survives the byte copy) keeps the grant across versions instead
// of re-prompting every upgrade.
func HolderBinDir() string {
	return stateDir.Path("bin")
}

// StatusSnapshotPath is the daemon's on-disk status mirror
// (~/.cc-pool/status.json), rewritten atomically after every completed poll
// for out-of-process readers like the Notification Center widget.
func StatusSnapshotPath() string {
	return stateDir.Path("status.json")
}

// AccountDirName is the directory basename for account index n (n >= 1).
func AccountDirName(n int) string {
	return fmt.Sprintf("acct-%02d", n)
}

// AccountDir returns the config-dir path for account index n (n >= 1).
//
// The returned path is exactly the string ccp emits for CLAUDE_CONFIG_DIR and
// the string we hash for the per-dir Keychain service name; the two MUST stay
// byte-identical, so do not realpath or normalize divergently elsewhere.
func AccountDir(n int) string {
	if n < 1 {
		panic(fmt.Sprintf("AccountDir(%d): account indexes start at 1", n))
	}
	return filepath.Join(AccountsDir(), AccountDirName(n))
}

// EnsureStateDir creates ~/.cc-pool with 0700 perms if missing.
func EnsureStateDir() error {
	return stateDir.Ensure()
}

// EnsureAccountsDir creates ~/.cc-pool/accounts with 0700 perms if missing.
func EnsureAccountsDir() error {
	return os.MkdirAll(AccountsDir(), 0o700)
}
