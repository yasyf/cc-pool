package overlay

import "github.com/yasyf/fusekit"

// Aliases, never redeclarations: errors.Is identity must hold across the
// fusekit/overlay/mountd process boundary.
var (
	// ErrFuseUnavailable means a fuse-built binary could not bring up the fuse
	// runtime (cgofuse failed to dlopen libfuse-t), unlike the pure-build
	// mountd.ErrCannotHost refusal.
	ErrFuseUnavailable = fusekit.ErrFuseUnavailable

	// ErrMountNotLive means a fuse mount never came live in a process that has
	// not yet hosted one — on macOS almost always the one-time volume-access
	// grant. After any live mount, timeouts wrap ErrMountTimeout instead.
	ErrMountNotLive = fusekit.ErrMountNotLive

	// ErrMountTimeout means a fuse mount timed out in a process that already
	// hosted a live mount: the grant is proven, so this is transient fuse-t
	// slowness — callers retry, never convert the provider or surface TCC
	// guidance. Known gap: a grant revoked mid-process still reads as this
	// (established mounts survive revocation; no public TCC query API) until
	// a holder restart resets the deduction.
	ErrMountTimeout = fusekit.ErrMountTimeout

	// ErrMountFailed means a fuse mount was rejected outright — the serving
	// goroutine exited before the mount came live. Never the volume-access
	// grant (a pending grant blocks, surfacing as ErrMountNotLive), so the
	// daemon retreats the row to symlink instead of retrying.
	ErrMountFailed = fusekit.ErrMountFailed

	// ErrUnmountWedged means an unmount did not take: the dir is still a live
	// mountpoint — RemoveAll through it would reach the backing ~/.claude.
	ErrUnmountWedged = fusekit.ErrUnmountWedged

	// ErrLivenessTimeout means a bounded liveness stat did not answer in time:
	// unresponsive but NOT proven dead (the holder may be CPU-saturated), so
	// supervision debounces it instead of remounting — a definitive dead
	// reading answers fast and stays a plain error.
	ErrLivenessTimeout = fusekit.ErrLivenessTimeout
)
