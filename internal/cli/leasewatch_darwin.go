//go:build darwin

package cli

import (
	"errors"

	"golang.org/x/sys/unix"
)

// realSessionLeader returns the calling process's session-leader pid (getsid(0)):
// the terminal tab's shell in every `ccp select`/`ccp env` invocation shape,
// including a `$(…)` command-substitution subshell whose immediate parent dies at
// once. The lease agent watches THIS pid, not getppid().
func realSessionLeader() (int, error) {
	sid, err := unix.Getsid(0)
	if err != nil {
		return 0, err
	}
	return sid, nil
}

// realProcStartTime reads pid's start-time stamp (start-second * 1e6 + microsecond)
// via sysctl KERN_PROC_PID — a stable per-process identity that changes on pid
// reuse. A gone pid maps to ErrNoProc.
func realProcStartTime(pid int) (int64, error) {
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		if errors.Is(err, unix.ESRCH) || errors.Is(err, unix.ENOENT) {
			return 0, ErrNoProc
		}
		return 0, err
	}
	tv := kp.Proc.P_starttime
	return tv.Sec*1_000_000 + int64(tv.Usec), nil
}

// kqueueWaiter blocks on an EVFILT_PROC / NOTE_EXIT registration; it works on a
// non-child, same-UID pid with no entitlements.
type kqueueWaiter struct{ kq int }

func (w *kqueueWaiter) Wait() error {
	var ev [1]unix.Kevent_t
	for {
		n, err := unix.Kevent(w.kq, nil, ev[:], nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if n > 0 {
			return nil // NOTE_EXIT delivered
		}
	}
}

// Drain polls the kqueue with a zero timeout for an already-queued NOTE_EXIT. A
// pending exit returns true (and consumes the one-shot event; the agent fails rather
// than Wait); no pending exit returns false and leaves the watch armed for Wait.
func (w *kqueueWaiter) Drain() (bool, error) {
	var ev [1]unix.Kevent_t
	var zero unix.Timespec // zero timeout ⇒ non-blocking
	for {
		n, err := unix.Kevent(w.kq, nil, ev[:], &zero)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return false, err
		}
		return n > 0, nil
	}
}

func (w *kqueueWaiter) Close() error { return unix.Close(w.kq) }

// realRegisterProcExit registers a one-shot EVFILT_PROC/NOTE_EXIT watch on pid.
// ESRCH at register (the pid already gone) maps to ErrNoProc so the agent
// releases immediately instead of blocking forever.
func realRegisterProcExit(pid int) (procWaiter, error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, err
	}
	ev := unix.Kevent_t{
		// #nosec G115 -- the caller validates pid is greater than one before registering the watch.
		Ident:  uint64(pid),
		Filter: unix.EVFILT_PROC,
		Flags:  unix.EV_ADD | unix.EV_ONESHOT,
		Fflags: unix.NOTE_EXIT,
	}
	if _, err := unix.Kevent(kq, []unix.Kevent_t{ev}, nil, nil); err != nil {
		_ = unix.Close(kq)
		if errors.Is(err, unix.ESRCH) {
			return nil, ErrNoProc
		}
		return nil, err
	}
	return &kqueueWaiter{kq: kq}, nil
}
