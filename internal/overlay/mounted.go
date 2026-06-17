package overlay

import "github.com/yasyf/fusekit"

// Mounted reports whether dir is currently a mountpoint. It wraps
// fusekit.Mounted, which on darwin reads the kernel's cached mount table with
// Getfsstat(MNT_NOWAIT) and checks dir for membership: MNT_NOWAIT returns the
// in-kernel snapshot without refreshing any filesystem, so the call cannot block
// — unlike an lstat of the mountpoint, which on a fuse-t mirror resolves INTO
// the NFS-backed fs (a GETATTR) and hangs forever on a wedged mount.
//
// Invariant #3 (AGENTS.md): Mounted is a read-only predicate and never
// realpaths, normalizes, stores, or hashes the account dir itself (fusekit
// resolves only dir's PARENT to match the kernel's firmlinked spelling, never
// dir).
func Mounted(dir string) bool { return fusekit.Mounted(dir) }
