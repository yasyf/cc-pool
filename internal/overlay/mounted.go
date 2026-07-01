package overlay

import "github.com/yasyf/fusekit"

// Mounted reports whether dir is a mountpoint. It wraps fusekit.Mounted, which on
// darwin reads the kernel mount table via Getfsstat(MNT_NOWAIT) — a non-blocking
// snapshot, unlike an lstat that resolves INTO a wedged fuse-t/NFS mirror and
// hangs forever.
//
// Invariant #3 (AGENTS.md): a read-only predicate that never realpaths,
// normalizes, stores, or hashes the account dir (fusekit resolves only dir's
// PARENT).
func Mounted(dir string) bool { return fusekit.Mounted(dir) }
