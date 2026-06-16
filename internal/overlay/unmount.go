package overlay

import (
	"errors"
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

// This file holds the untagged direct force-unmount primitive. It compiles in
// every build variant: the daemon (pure-Go default included) force-unmounts a
// dead holder's orphaned fuse carcasses ITSELF, without routing through the
// holder, the moment the holder dies — a wedged NFS carcass must never linger.

// ErrForceUnmountTimeout means a forced unmount syscall did not return within
// the bound: the carcass is so wedged the kernel will not even complete
// MNT_FORCE in time. The syscall runs inside a per-dir StatProbes join, so
// repeated force-unmounts of the same wedged carcass share the single parked
// goroutine (it exits if the kernel ever answers) — a single wedged carcass
// parks at most one goroutine, never the caller, no matter how many ticks
// re-issue against it.
var ErrForceUnmountTimeout = errors.New("forced unmount did not return in time")

// forceUnmountTimeout bounds one ForceUnmount syscall. A var, not a const, so
// tests can shrink it.
var forceUnmountTimeout = 5 * time.Second

// unmountFn seams the force-unmount syscall so ForceUnmount is unit-testable
// without a real mount. Tests swap it and restore via t.Cleanup. Production:
// unix.Unmount.
var unmountFn = unix.Unmount

// forceUnmountProbes joins concurrent and repeated force-unmounts per dir. Its
// own StatProbes instance, never shared with the stat or deep probes
// (aliveProbes/deepProbes): a parked MNT_FORCE against a
// permanently-wedged carcass must never block — or be answered by — a liveness
// stat behind its join, and vice versa. The join is what makes the
// at-most-one-parked-goroutine-per-carcass contract true: the daemon re-issues
// ForceUnmount against the same wedged dir every supervision tick
// (forceUnmountOrphans) and every breaker window (escalateWedgedRow), and each
// re-issue shares the single already-parked goroutine instead of spawning
// another, so a carcass the kernel will never MNT_FORCE cannot leak goroutines
// per tick.
var forceUnmountProbes StatProbes[error]

// ForceUnmount force-unmounts dir directly via unix.Unmount(MNT_FORCE), bounded
// by forceUnmountTimeout. It does NOT contact the mount holder: the daemon
// calls it on a holder's orphaned carcasses the moment the holder dies, when
// the dead holder can no longer perform the unmount itself. The syscall runs in
// a per-dir StatProbes goroutine behind the bound because a wedged NFS carcass
// can make unix.Unmount block forever in uninterruptible wait — that must never
// hang the daemon's supervise goroutine, and repeated re-issues against the
// same carcass must share that one parked goroutine rather than each spawning
// one (see forceUnmountProbes). The probe goroutine exits if the syscall ever
// returns, even past the bound. Returns nil on a clean unmount, the wrapped
// syscall error, or ErrForceUnmountTimeout when the bound elapses.
func ForceUnmount(dir string) error {
	err, ok := forceUnmountProbes.Do(dir, forceUnmountTimeout, func() error {
		return unmountFn(dir, unix.MNT_FORCE)
	})
	if !ok {
		return fmt.Errorf("%w: %s", ErrForceUnmountTimeout, dir)
	}
	if err != nil {
		return fmt.Errorf("force unmount %s: %w", dir, err)
	}
	return nil
}
