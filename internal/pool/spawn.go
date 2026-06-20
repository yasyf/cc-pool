package pool

import (
	"time"

	"github.com/yasyf/fusekit/mountd"
)

// cannotHostHint is the user-facing guidance appended to mountd.ErrCannotHost
// when a pure (non-fuse) build is asked to host or spawn a mount holder. It is
// cc-pool's brew text, lifted verbatim from the pre-fusekit EnsureRunning
// refusal so the install guidance is preserved; the rendered error reads
// "this binary cannot host fuse mounts: install fuse-t …".
const cannotHostHint = "install fuse-t (brew install macos-fuse-t/cask/fuse-t) then brew reinstall cc-pool to get the fuse build"

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
