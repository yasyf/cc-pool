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
)

// Home returns the current user's home directory.
func Home() (string, error) {
	return os.UserHomeDir()
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

// AccountsDir is the stable presentation root exported through CLAUDE_CONFIG_DIR.
func AccountsDir() string {
	return filepath.Join(StateDir(), "accounts")
}

// StateDir is cc-pool's own private state directory (~/.cc-pool).
func StateDir() string {
	return filepath.Join(mustHome(), ".cc-pool")
}

func statePath(elements ...string) string {
	return filepath.Join(append([]string{StateDir()}, elements...)...)
}

// FuseKitRuntimeDir is the consumer-owned private holder runtime directory.
func FuseKitRuntimeDir() string { return statePath("fusekit") }

// FuseKitSocketPath is the exact persistent holder session socket.
func FuseKitSocketPath() string { return filepath.Join(FuseKitRuntimeDir(), "fusekit.sock") }

// FuseKitProcessStorePath is the signed holder's durable native-child and worker identity store.
func FuseKitProcessStorePath() string { return filepath.Join(FuseKitRuntimeDir(), "processes.json") }

// DisposableWorkerStorePath is daemonkit's durable process-group identity store.
func DisposableWorkerStorePath() string { return statePath("workers.json") }

// HostSyncWorkerStorePath is the host-sync child's independent durable process ledger.
func HostSyncWorkerStorePath() string { return statePath("hostsync-workers-v1.json") }

// FuseKitBackingRoot is the private source root behind every account presentation.
func FuseKitBackingRoot() string { return filepath.Join(FuseKitRuntimeDir(), "backing") }

// FuseKitPresentationRoot is the stable native mount root for every account tenant.
func FuseKitPresentationRoot() string { return AccountsDir() }

// AccountPresentationDir is one account's direct child in the native root.
func AccountPresentationDir(id int) string {
	return filepath.Join(FuseKitPresentationRoot(), AccountDirName(id))
}

// AccountBackingDir is one account's private FuseKit source directory.
func AccountBackingDir(id int) string {
	return filepath.Join(FuseKitBackingRoot(), AccountDirName(id))
}

// DBPath is the v1 runtime sqlite database path.
func DBPath() string {
	return statePath("pool-v1.db")
}

// SocketPath is the daemon's unix socket path.
func SocketPath() string {
	return statePath("daemon.sock")
}

// LogPath is the daemon log path.
func LogPath() string {
	return statePath("daemon.log")
}

// SyncDir is cc-pool's host-sync state dir (~/.cc-pool/sync): the secretless
// CRDT registry plus the per-account fsnotify stamp dirs.
func SyncDir() string {
	return statePath("sync")
}

// SyncSocketPath is the daemon's synckit consumer socket
// (~/.cc-pool/sync.sock), a second socket beside daemon.sock.
func SyncSocketPath() string {
	return statePath("sync.sock")
}

// SyncStampsDir is the parent of the per-account stamp dirs synckitd watches
// (~/.cc-pool/sync/stamps).
func SyncStampsDir() string {
	return filepath.Join(SyncDir(), "stamps")
}

// SyncRegistryPath is the secretless host-sync CRDT registry
// (~/.cc-pool/sync/registry.json).
func SyncRegistryPath() string {
	return filepath.Join(SyncDir(), "registry.json")
}

// SyncRegistryLockPath is the flock serializing registry read-modify-write
// cycles across the daemon and CLI lifecycle hooks.
func SyncRegistryLockPath() string {
	return filepath.Join(SyncDir(), "registry.lock")
}

// WidgetAppPath is the CCPoolStatus app bundle path (the cask's default
// appdir): the Notification Center widget host and the File Provider
// companion app.
func WidgetAppPath() string {
	return "/Applications/CCPoolStatus.app"
}

// WidgetAppBinaryPath is the Mach-O inside the CCPoolStatus app bundle.
func WidgetAppBinaryPath() string {
	return filepath.Join(WidgetAppPath(), "Contents", "MacOS", "CCPoolStatus")
}

// StatusSnapshotPath is the daemon's on-disk status mirror
// (~/.cc-pool/status.json), rewritten atomically after every completed poll
// for out-of-process readers like the Notification Center widget.
func StatusSnapshotPath() string {
	return statePath("status.json")
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
	return os.MkdirAll(StateDir(), 0o700)
}

// EnsureAccountsDir creates ~/.cc-pool/accounts with 0700 perms if missing.
func EnsureAccountsDir() error {
	return os.MkdirAll(AccountsDir(), 0o700)
}
