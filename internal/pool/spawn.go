package pool

import (
	"time"

	"github.com/yasyf/fusekit/mountd"
)

// cannotHostHint is the user-facing guidance appended to mountd.ErrCannotHost
// when a pure (non-fuse) build is asked to host or spawn a mount holder. It
// points at the one-step setup command; the rendered error reads "this binary
// cannot host fuse mounts: run `ccp fuse enable` …".
const cannotHostHint = "run `ccp fuse enable` to install fuse-t and switch to the live-mirror build"

// SpawnHolder ensures a detached `cc-pool mount-holder --socket <socket>` is
// serving socket, auto-spawning one (in its own session) and waiting up to
// timeout for its socket. It is cc-pool's seam over fusekit's generic
// mountd.Spawn: it owns the holder argv (the hidden mount-holder subcommand)
// and the pure-build brew hint. A running holder is usable by any build, so
// only the spawn path needs the fuse build; a pure build refuses with
// mountd.ErrCannotHost (carrying the hint), never wrapped in
// ErrHolderUnavailable. The signature matches the daemon's spawnHolder seam so
// the daemon's spawn default is a one-line swap.
func SpawnHolder(socket, logPath string, timeout time.Duration) error {
	return mountd.Spawn{
		Socket:         socket,
		LogPath:        logPath,
		Args:           []string{"mount-holder", "--socket", socket},
		Timeout:        timeout,
		CannotHostHint: cannotHostHint,
		StableExecDir:  HolderBinDir(),
	}.EnsureRunning()
}
