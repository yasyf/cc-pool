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
	"strings"

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

// MuxRootDir is the single native fuse-t mount (~/.cc-pool/mnt) that serves
// every fuse account as a logical subtree MuxRootDir()/<acct-NN>. One native
// mount, one go-nfsv4 helper for the whole pool. Each account dir is a
// fail-closed bridge symlink into its subtree; the account dir string itself
// never changes (it is still hashed byte-for-byte into the Keychain service name
// and matched exactly by procscan's CLAUDE_CONFIG_DIR scan).
func MuxRootDir() string {
	return stateDir.Path("mnt")
}

// ConfigDirForMount translates a holder-reported mount dir back to the account
// ConfigDir the daemon keys everything on — the SINGLE wire→ConfigDir
// translation. A mux subtree (a direct child of MuxRootDir()) maps to
// AccountsDir()/<basename>; any other dir — a legacy per-account mount whose
// served path IS the ConfigDir — passes through unchanged. Pure string function:
// it never touches the filesystem, so it is safe on the holder cache hot path.
func ConfigDirForMount(mountDir string) string {
	if filepath.Dir(mountDir) == MuxRootDir() {
		return filepath.Join(AccountsDir(), filepath.Base(mountDir))
	}
	return mountDir
}

// IsBridgeSymlink reports whether dir is a mux bridge symlink pointing into the
// shared mux root — the fuse-mux overlay's account-dir stand-in. It reads the
// link with os.Readlink (which never traverses INTO the target, so it cannot
// hang on a wedged mount) and checks the target is a child of MuxRootDir(). A
// symlink-row file operation must never follow it — moving files through it
// writes into the live mirror — so convertToFuse and HealStrandedPrivate refuse
// when it is present, and the daemon uses it to tell a migrated mux account (a
// bridge symlink) from a legacy per-dir mount (a real dir).
func IsBridgeSymlink(dir string) bool {
	target, err := os.Readlink(dir)
	if err != nil {
		return false
	}
	return strings.HasPrefix(target, MuxRootDir()+string(os.PathSeparator))
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

// FPExtensionBundleID is the File Provider extension's bundle identifier,
// handed to pluginkit by fusekit's FileProviderAvailable gate.
const FPExtensionBundleID = "com.yasyf.cc-pool.status.fileprovider"

// AppGroupID is the App Group container shared by the CCPoolStatus host app,
// the File Provider extension, and the daemon. It must equal
// $(TeamIdentifierPrefix)ccp in widget/project.yml; release.yml asserts the
// built appex's NSExtensionFileProviderDocumentGroup matches this constant.
const AppGroupID = "SXKCTF23Q2.ccp"

// WidgetAppPath is the CCPoolStatus app bundle path (the cask's default
// appdir): the Notification Center widget host and the File Provider
// companion app.
func WidgetAppPath() string {
	return "/Applications/CCPoolStatus.app"
}

// FPControlSocketPath is the File Provider control socket
// (~/.cc-pool/domains.sock): the CCPoolStatus app binds it, the daemon dials
// it to register/remove/signal domains.
func FPControlSocketPath() string {
	return stateDir.Path("domains.sock")
}

// FPBridgeSocketPath is the File Provider data socket: the daemon binds it,
// the sandboxed extension dials it for computed content. It lives in the App
// Group container — the one location BOTH sides may touch: the sandboxed
// appex gets blanket container access (file temp-exceptions do not cover
// AF_UNIX connect, a network-outbound operation), and the daemon is an
// entitled group member. Transient bind failures (daemon restart drain races)
// self-heal via the FP bridge serve-retry loop. The short leaf keeps the path
// under the 104-byte sun_path limit.
func FPBridgeSocketPath() string {
	return filepath.Join(mustHome(), "Library", "Group Containers", AppGroupID, "b.sock")
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
