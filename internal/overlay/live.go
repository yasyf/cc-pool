package overlay

import "github.com/yasyf/fusekit"

// This file holds the overlay package's thin re-exports of fusekit's untagged
// mount-liveness primitives. They compile in every build variant: a non-fuse
// binary (and the process that does not host mounts) must still be able to
// observe whether an account dir is a live mirror of base. The bounded-stat
// machinery (the StatProbes join, the mountAliveFn seam, the timeouts) lives in
// fusekit now; overlay keeps only the names its daemon/cli/pool callers import.

// StatProbes bounds wedge-prone kernel stats. It is an alias to fusekit's
// implementation: the overlay package's own probe instances (deepProbes) and the
// daemon/cli callers that hold a StatProbes share a single bounded-stat type
// rather than a redeclared twin. fuse-t's NFS backend has no soft/timeout mount
// options, so a stat through a wedged mirror can block indefinitely; Do runs each
// stat in its own goroutine behind a timeout, and concurrent callers for the same
// key JOIN the in-flight probe rather than stacking another stuck goroutine per
// caller.
type StatProbes[V any] = fusekit.StatProbes[V]

// MountAlive reports whether accountDir currently mirrors base, comparing a stat
// of base itself (always exists) seen through the mountpoint. It wraps
// fusekit.MountAlive.
func MountAlive(base, accountDir string) bool { return fusekit.MountAlive(base, accountDir) }

// MountAliveWithin reports MountAlive(base, accountDir) bounded by fusekit's
// stat-probe timeout. A probe that does not answer within the bound reads NOT
// alive — every liveness caller fails the same direction: a mirror that cannot
// answer the stat is exactly the dead-or-wedged mount the check exists to flag.
// It wraps fusekit.MountAliveWithin.
func MountAliveWithin(base, accountDir string) bool { return fusekit.MountAliveWithin(base, accountDir) }
