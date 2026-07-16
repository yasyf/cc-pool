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
	"strconv"
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

// StableBinDir is the version-independent directory the daemon re-execs itself
// from (~/.cc-pool/bin). TCC keys a bare executable's app-group-container grants
// by resolved path, so re-execing from here keeps grants alive across the
// per-version Homebrew keg paths a `brew upgrade` churns through.
func StableBinDir() string {
	return filepath.Join(StateDir(), "bin")
}

// MuxRootDir is the single native fuse-t mount (~/.cc-pool/mnt) serving every
// fuse account as a logical subtree MuxRootDir()/<acct-NN>; each account dir is
// a fail-closed bridge symlink into its subtree.
func MuxRootDir() string {
	return stateDir.Path("mnt")
}

// ConfigDirForMount translates a holder-reported mount dir back to the account
// ConfigDir the daemon keys on — the SINGLE wire→ConfigDir translation. Pure
// string function; never touches the filesystem.
func ConfigDirForMount(mountDir string) string {
	if filepath.Dir(mountDir) == MuxRootDir() {
		return filepath.Join(AccountsDir(), filepath.Base(mountDir))
	}
	return mountDir
}

// DirKind classifies what occupies an account config-dir path, judged by
// Lstat/Readlink alone so it never traverses a possibly-dead mount or a
// materializing domain.
type DirKind int

const (
	// DirReal is a real directory (any non-symlink Lstat result).
	DirReal DirKind = iota
	// DirAbsent is a path that does not exist, or is otherwise unstattable.
	DirAbsent
	// DirMuxBridge is a symlink into the shared fuse mux root (MuxRootDir).
	DirMuxBridge
	// DirFPBridge is a symlink into the File Provider CloudStorage root
	// (FPCloudStorageDir).
	DirFPBridge
	// DirForeignLink is any other symlink, including one whose target is
	// unreadable.
	DirForeignLink
)

// ClassifyAccountDir reports what occupies an account config dir path, by
// Lstat/Readlink ONLY — never a stat through a possibly-dead mount. The second
// result is the symlink target ("" unless dir is a link with a readable target).
// Readlink never traverses INTO the target, so it cannot hang on a wedged mount;
// callers must never follow a bridge link — moving files through it writes into
// the live mirror or domain.
func ClassifyAccountDir(dir string) (DirKind, string) {
	fi, err := os.Lstat(dir)
	if err != nil {
		return DirAbsent, ""
	}
	if fi.Mode()&os.ModeSymlink == 0 {
		return DirReal, ""
	}
	target, err := os.Readlink(dir)
	if err != nil {
		return DirForeignLink, ""
	}
	switch {
	case strings.HasPrefix(target, MuxRootDir()+string(os.PathSeparator)):
		return DirMuxBridge, target
	case strings.HasPrefix(target, FPCloudStorageDir()+string(os.PathSeparator)):
		return DirFPBridge, target
	default:
		return DirForeignLink, target
	}
}

// IsBridgeSymlink reports whether dir is a mux bridge symlink into MuxRootDir().
// It is deliberately blind to a File Provider domain bridge (that reads false);
// see ClassifyAccountDir for the full set. Callers must never follow the link —
// moving files through it writes into the live mirror.
func IsBridgeSymlink(dir string) bool {
	kind, _ := ClassifyAccountDir(dir)
	return kind == DirMuxBridge
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

// SyncDir is cc-pool's host-sync state dir (~/.cc-pool/sync): the secretless
// CRDT registry plus the per-account fsnotify stamp dirs.
func SyncDir() string {
	return stateDir.Path("sync")
}

// SyncSocketPath is the daemon's synckit consumer socket
// (~/.cc-pool/sync.sock), a second socket beside daemon.sock.
func SyncSocketPath() string {
	return stateDir.Path("sync.sock")
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

// WidgetAppBinaryPath is the Mach-O inside the CCPoolStatus app bundle.
func WidgetAppBinaryPath() string {
	return filepath.Join(WidgetAppPath(), "Contents", "MacOS", "CCPoolStatus")
}

// WidgetAppexBinaryPath is the Mach-O inside the Notification Center widget
// appex in the CCPoolStatus bundle. The daemon and doctor match live
// processes' exec paths against it to find an appex a cask upgrade left
// running a replaced binary.
func WidgetAppexBinaryPath() string {
	return filepath.Join(WidgetAppPath(),
		"Contents", "PlugIns", "CCPoolStatusWidget.appex", "Contents", "MacOS", "CCPoolStatusWidget")
}

// FPControlSocketPath is the File Provider control socket
// (~/.cc-pool/domains.sock): the CCPoolStatus app binds it, the daemon dials
// it to register/remove/signal domains.
func FPControlSocketPath() string {
	return stateDir.Path("domains.sock")
}

// FPBridgeSocketLeaf is the File Provider data socket's filename inside the
// app-group container. Kept short so the container-relative sun_path stays within
// AF_UNIX's 104-byte limit — see ccn doc f71e9b1.
const FPBridgeSocketLeaf = "b.sock"

// FPBridgeSocketPath is a HAND-BUILT ~/Library/Group Containers/<group>/b.sock
// path for display and diagnostics ONLY. The daemon does not bind here: it
// resolves the container through -[NSFileManager
// containerURLForSecurityApplicationGroupIdentifier:] (fusekit/appgroup) so its
// access is prompt-free, then joins FPBridgeSocketLeaf onto the resolved dir. On
// disk both point at the same file, so a CLI dial to this advisory path still
// reaches the daemon's socket; the fixed suffix stays short enough for sun_path
// (it leaves room for $HOME — see ccn doc f71e9b1).
func FPBridgeSocketPath() string {
	return filepath.Join(mustHome(), "Library", "Group Containers", AppGroupID, FPBridgeSocketLeaf)
}

// DaemonBinaryPath is the daemon .app bundle's main executable inside a Homebrew
// install: <brew prefix>/libexec/CCPoolDaemon.app/Contents/MacOS/cc-pool. The
// bundle's app-group entitlement + embedded Developer ID profile give the daemon
// prompt-free group-container access — the durable replacement for the stable-path
// re-exec. The brew prefix is two dirs up from the running binary (invoked via the
// <prefix>/bin/cc-pool symlink). Callers stat the result: a missing bundle
// (source/HEAD builds) is the expected signal to fall back to the running binary.
func DaemonBinaryPath() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve executable: %w", err)
	}
	prefix := filepath.Dir(filepath.Dir(exe))
	return filepath.Join(prefix, "libexec", "CCPoolDaemon.app", "Contents", "MacOS", "cc-pool"), nil
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

// FPDomainFolderPrefix is the ~/Library/CloudStorage folder-name prefix macOS
// gives a cc-pool File Provider domain. It must match widget/project.yml
// PRODUCT_NAME (the domain display name derives from the app's product name).
const FPDomainFolderPrefix = "CCPoolStatus-"

// FPCloudStorageDir is macOS's per-user File Provider mount parent
// (~/Library/CloudStorage); each registered domain surfaces there as
// FPDomainFolderPrefix + the domain's account dir name.
func FPCloudStorageDir() string {
	return filepath.Join(mustHome(), "Library", "CloudStorage")
}

// ParseFPDomainFolder extracts the account index from a ~/Library/CloudStorage
// File Provider folder name (FPDomainFolderPrefix + AccountDirName). ok is false
// for any name whose suffix does not round-trip AccountDirName(n) exactly — an
// unpadded "acct-1", junk, or a foreign app's folder — so a non-pool folder is
// never mistaken for a pool domain.
func ParseFPDomainFolder(name string) (int, bool) {
	rest, ok := strings.CutPrefix(name, FPDomainFolderPrefix)
	if !ok {
		return 0, false
	}
	digits := strings.TrimPrefix(rest, "acct-")
	n, err := strconv.Atoi(digits)
	if err != nil || n < 1 || AccountDirName(n) != rest {
		return 0, false
	}
	return n, true
}

// EnsureStateDir creates ~/.cc-pool with 0700 perms if missing.
func EnsureStateDir() error {
	return stateDir.Ensure()
}

// EnsureAccountsDir creates ~/.cc-pool/accounts with 0700 perms if missing.
func EnsureAccountsDir() error {
	return os.MkdirAll(AccountsDir(), 0o700)
}
