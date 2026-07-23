// Package pool owns the canonical filesystem layout, account model, and
// per-account lifecycle helpers for cc-pool.
//
// Two distinct trees exist and must not be confused:
//
//   - ~/.claude      Canonical Claude Code config dir: plain `claude`'s home
//     and the shared source base. NEVER moved or registered as a pool
//     account; the pool never touches plain claude's credential or login
//     identity.
//   - ~/.cc-pool/    cc-pool's private state and FuseKit source backing. Public
//     account config dirs are File Provider roots selected by macOS.
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
// home and the shared source base — never a pool account.
func ClaudeDir() string {
	return filepath.Join(mustHome(), ".claude")
}

// ClaudeJSONPath is plain claude's primary state file ~/.claude.json — in
// $HOME, NOT inside ~/.claude.
func ClaudeJSONPath() string {
	return filepath.Join(mustHome(), ".claude.json")
}

// StateDir is cc-pool's own private state directory (~/.cc-pool).
func StateDir() string {
	return filepath.Join(mustHome(), ".cc-pool")
}

func statePath(elements ...string) string {
	return filepath.Join(append([]string{StateDir()}, elements...)...)
}

// FuseKitRuntimeDir is the consumer-owned private FuseKit runtime directory.
func FuseKitRuntimeDir() string { return statePath("fusekit") }

// FuseKitSocketPath is the exact persistent runtime session socket.
func FuseKitSocketPath() string { return filepath.Join(FuseKitRuntimeDir(), "fusekit.sock") }

// DaemonServiceStatePath is the cc-pool LaunchAgent controller's durable desired state.
func DaemonServiceStatePath() string { return statePath("daemon-services.db") }

// DaemonServiceProcessStorePath is the daemon service controller's worker and stop-authority ledger.
func DaemonServiceProcessStorePath() string { return statePath("daemon-service-processes.db") }

// DisposableWorkerStorePath is daemonkit's durable process-group identity store.
func DisposableWorkerStorePath() string { return statePath("workers.json") }

// HostSyncWorkerStorePath is the host-sync child's independent durable process ledger.
func HostSyncWorkerStorePath() string { return statePath("hostsync-workers-v1.json") }

// FuseKitBackingRoot is the private source root behind every account presentation.
func FuseKitBackingRoot() string { return filepath.Join(FuseKitRuntimeDir(), "backing") }

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

// WidgetAppDir is the fixed per-user application root.
func WidgetAppDir() string {
	return filepath.Join(mustHome(), "Applications")
}

// WidgetAppPath is the fixed per-user CCPoolStatus app bundle path: the
// Notification Center widget host and File Provider companion app.
func WidgetAppPath() string {
	return filepath.Join(WidgetAppDir(), "CCPoolStatus.app")
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

// EnsureStateDir creates ~/.cc-pool with 0700 perms if missing.
func EnsureStateDir() error {
	return os.MkdirAll(StateDir(), 0o700)
}
