package overlay

import "github.com/yasyf/fusekit"

// This file holds the overlay package's re-exports of fusekit's untagged fuse
// failure-mode sentinels, which callers classify without string matching (the
// mount-holder server maps them onto wire error classes). They are ALIASES, not
// fresh errors.New values: errors.Is identity must hold across the
// fusekit/overlay/mountd boundary, so an error wrapped with fusekit.ErrMountNotLive
// in the holder (or fusekit/mountd.RemoteHost) still matches overlay.ErrMountNotLive
// in daemon/server.go's healFuse taxonomy. Redeclaring them would silently break
// every errors.Is. They compile in every build variant so a non-fuse binary can
// still classify errors that crossed a process boundary.
var (
	// ErrFuseUnavailable means the binary was built with fuse support but the
	// fuse runtime could not be brought up (cgofuse failed to dlopen libfuse-t).
	// Distinct from mountd.ErrCannotHost, the pure-build refusal.
	ErrFuseUnavailable = fusekit.ErrFuseUnavailable

	// ErrMountNotLive means a fuse mount was issued but never came live in a
	// process that has NOT yet hosted any live mount — on macOS almost always
	// the one-time "Network Volumes" TCC grant. fusekit.Mount (which
	// FuseProvider.Setup delegates to) wraps its mount-timeout error with it only
	// while the grant is unproven; once any mount in the process has come live,
	// timeouts wrap ErrMountTimeout instead.
	ErrMountNotLive = fusekit.ErrMountNotLive

	// ErrMountTimeout means a fuse mount timed out in a process that has
	// ALREADY hosted a live mount, so the macOS "Network Volumes" TCC grant is
	// proven and this is transient fuse-t slowness — never the TCC condition.
	// Callers retry; they must never convert the provider or surface TCC
	// guidance for it. Honest gap: a grant revoked mid-process still reads as
	// this — established NFS mounts survive revocation, there is no public TCC
	// query API for Network Volumes, and attempting a mount is the only
	// observable — and a holder restart resets the deduction.
	ErrMountTimeout = fusekit.ErrMountTimeout

	// ErrMountFailed means a fuse mount was rejected outright — the holder's
	// serving goroutine exited before the mount came live (fuse-t not installed
	// or loadable, the kernel refusing the mount, a bad CGOFUSE_LIBFUSE_PATH).
	// It is NEVER the "Network Volumes" TCC grant (a pending grant keeps the
	// mount call blocked, surfacing as ErrMountNotLive), so the daemon must not
	// retry it for the grant — it retreats the row to symlink instead.
	ErrMountFailed = fusekit.ErrMountFailed

	// ErrUnmountWedged means an unmount did not take: the dir is still a live
	// mountpoint and must not be treated as torn down (RemoveAll through it
	// would reach the backing ~/.claude). fusekit's bounded teardown wraps its
	// refusal with it.
	ErrUnmountWedged = fusekit.ErrUnmountWedged
)
