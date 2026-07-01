package overlay

import "github.com/yasyf/fusekit"

// StatProbes bounds wedge-prone kernel stats. fuse-t's NFS backend has no
// soft/timeout mount options, so a stat through a wedged mirror can block
// indefinitely; Do runs each stat in its own goroutine behind a timeout, and
// concurrent callers for one key JOIN the in-flight probe instead of stacking
// stuck goroutines.
type StatProbes[V any] = fusekit.StatProbes[V]

// MountAlive reports whether accountDir currently mirrors base, comparing a stat
// of base itself (always exists) seen through the mountpoint.
func MountAlive(base, accountDir string) bool { return fusekit.MountAlive(base, accountDir) }

// MountAliveWithin is MountAlive bounded by fusekit's stat-probe timeout: a
// probe that does not answer within the bound reads NOT alive, since a mirror
// that cannot answer the stat is exactly the dead-or-wedged mount to flag.
func MountAliveWithin(base, accountDir string) bool {
	return fusekit.MountAliveWithin(base, accountDir)
}
