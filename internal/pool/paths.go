// Package pool owns the canonical filesystem layout, account model, and
// per-account lifecycle helpers for cc-pool.
//
// Two distinct trees exist and must not be confused:
//
//   - ~/.claude      Canonical Claude Code config dir: plain `claude`'s home
//     and the shared overlay base. NEVER moved or registered as a pool
//     account; the pool never touches plain claude's credential or login
//     identity.
//   - ~/.cc-pool/    cc-pool's own state (sqlite db, daemon socket, logs) plus
//     accounts/ holding the pool account dirs (acct-01, acct-02, ...). Each
//     account dir is a unique path so it gets its own Keychain item.
package pool

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/fusekit/state"
)

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

// ClaudeJSONPath is plain claude's primary state file ~/.claude.json — in
// $HOME, NOT inside ~/.claude.
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

// BridgeSocketPath is the daemon's content.BridgeServer data socket
// (~/.cc-pool/bridge.sock): the daemon binds it, the shared holder dials it to
// fetch cc-pool's synthetic entries (merged .claude.json, injected
// settings.json).
func BridgeSocketPath() string {
	return stateDir.Path("bridge.sock")
}

// MountHolderLogPath is the dev-spawned holder's log path; the production
// fusekit-holder cask owns its own.
func MountHolderLogPath() string {
	return stateDir.Path("mount-holder.log")
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
