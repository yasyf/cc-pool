package overlay

import "github.com/yasyf/fusekit"

// This file holds the overlay package's thin re-exports of fusekit's untagged
// direct force-unmount primitive. It compiles in every build variant: the daemon
// (pure-Go default included) force-unmounts a dead holder's orphaned fuse
// carcasses ITSELF, without routing through the holder, the moment the holder
// dies — a wedged NFS carcass must never linger. The per-dir StatProbes join, the
// unmountFn syscall seam, and the timeout all live in fusekit now.

// ErrForceUnmountTimeout means a forced unmount syscall did not return within
// fusekit's bound: the carcass is so wedged the kernel will not even complete
// MNT_FORCE in time. It is an ALIAS of fusekit.ErrForceUnmountTimeout so
// errors.Is identity holds across the boundary.
var ErrForceUnmountTimeout = fusekit.ErrForceUnmountTimeout

// ForceUnmount force-unmounts dir directly (unix.Unmount(MNT_FORCE) on darwin),
// bounded by fusekit. It does NOT contact the mount holder: the daemon calls it
// on a holder's orphaned carcasses the moment the holder dies, when the dead
// holder can no longer perform the unmount itself. The syscall runs in a per-dir
// StatProbes goroutine behind the bound because a wedged NFS carcass can make the
// unmount block forever; repeated re-issues against the same carcass share that
// one parked goroutine. Returns nil on a clean unmount, the wrapped syscall error,
// or ErrForceUnmountTimeout when the bound elapses. It wraps fusekit.ForceUnmount.
func ForceUnmount(dir string) error { return fusekit.ForceUnmount(dir) }
