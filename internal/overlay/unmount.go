package overlay

import "github.com/yasyf/fusekit"

// ErrForceUnmountTimeout means a forced unmount did not return within fusekit's
// bound. It aliases fusekit.ErrForceUnmountTimeout so errors.Is identity holds
// across the boundary.
var ErrForceUnmountTimeout = fusekit.ErrForceUnmountTimeout

// ForceUnmount force-unmounts dir directly (unix.Unmount(MNT_FORCE) on darwin),
// bounded by fusekit; it does NOT contact the mount holder — the daemon calls it
// on a dead holder's orphaned carcasses. A wedged NFS carcass can block the
// unmount forever, so it runs behind fusekit's bound and returns
// ErrForceUnmountTimeout when the bound elapses.
func ForceUnmount(dir string) error { return fusekit.ForceUnmount(dir) }
