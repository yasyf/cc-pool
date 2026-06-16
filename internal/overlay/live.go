package overlay

import (
	"os"
	"path/filepath"
	"sync"
	"time"
)

// This file holds untagged mount-liveness checks. They compile in every build
// variant: a non-fuse binary (and, once the mount-holder lands, the process
// that does not host mounts) must still be able to observe whether an account
// dir is a live mirror of base.

// StatProbes bounds wedge-prone kernel stats. fuse-t's NFS backend has no
// soft/timeout mount options, so a stat through a wedged mirror can block
// indefinitely; Do runs each stat in its own goroutine behind a timeout, and
// concurrent callers for the same key JOIN the in-flight probe (sharing its
// verdict when it answers in time) rather than stacking another stuck
// goroutine per caller. The probe goroutine's exit is the stat returning; for
// a truly wedged mount that is never — exactly the condition the bound exists
// to contain, and why the goroutine is deliberately untracked.
type StatProbes[V any] struct {
	mu       sync.Mutex
	inflight map[string]*statProbe[V]
}

// statProbe is one in-flight stat; v is valid once done closes.
type statProbe[V any] struct {
	done chan struct{}
	v    V
}

// Do runs stat keyed by key, returning its verdict and ok=true, or the zero V
// and ok=false when it does not answer within timeout. The caller chooses the
// fail direction for a timed-out probe: liveness checks read dead, teardown
// verifications read still-mounted.
func (p *StatProbes[V]) Do(key string, timeout time.Duration, stat func() V) (V, bool) {
	p.mu.Lock()
	pr, ok := p.inflight[key]
	if !ok {
		if p.inflight == nil {
			p.inflight = map[string]*statProbe[V]{}
		}
		pr = &statProbe[V]{done: make(chan struct{})}
		p.inflight[key] = pr
		go func() {
			v := stat()
			p.mu.Lock()
			pr.v = v
			delete(p.inflight, key)
			p.mu.Unlock()
			close(pr.done)
		}()
	}
	p.mu.Unlock()
	select {
	case <-pr.done:
		return pr.v, true
	case <-time.After(timeout):
		var zero V
		return zero, false
	}
}

// Inflight reports the probes currently running. Tests drain wedged probes
// against it before restoring the stat seams the probe goroutines read.
func (p *StatProbes[V]) Inflight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.inflight)
}

// statProbeTimeout bounds the overlay package's own wedge-prone kernel stats.
// Its sole remaining user is aliveProbes via MountAliveWithin: FuseProvider.
// Teardown's post-unmount check is now the non-blocking Mounted (a cached
// Getfsstat read that cannot wedge), which needs no bound. A var, not a const,
// so tests can shrink it.
var statProbeTimeout = 2 * time.Second

// MountAlive reports whether accountDir currently mirrors base. It compares a
// stat of base itself (always exists) seen through the mountpoint.
func MountAlive(base, accountDir string) bool {
	fi, err := os.Stat(accountDir)
	if err != nil || !fi.IsDir() {
		return false
	}
	// The mount is "live" if the dir is backed by a fuse fs; a cheap proxy is
	// that reading it does not error and base's own entries are visible.
	entries, err := os.ReadDir(base)
	if err != nil || len(entries) == 0 {
		return err == nil
	}
	_, err = os.Lstat(filepath.Join(accountDir, entries[0].Name()))
	return err == nil
}

// mountAliveFn seams MountAlive so waitMounted and MountAliveWithin are
// unit-testable without the fuse build tag. Tests swap it and restore via
// t.Cleanup.
var mountAliveFn = MountAlive

// aliveProbes joins the package's own bounded mount-liveness probes. Its own
// StatProbes instance, keyed by the account dir but never shared with the deep
// probes: a read-through MountAlive verdict must never join (or be answered by)
// a deep-read probe of the same dir.
var aliveProbes StatProbes[bool]

// MountAliveWithin reports MountAlive(base, accountDir) bounded by the
// package's stat-probe timeout. A probe that does not answer within the bound
// reads NOT alive — there is no ok return, because every
// liveness caller fails the same direction: a mirror that cannot answer a 2s
// stat is exactly the dead-or-wedged mount the check exists to flag, and
// reading it as alive would let a wedged mirror pass for healthy.
func MountAliveWithin(base, accountDir string) bool {
	alive, ok := aliveProbes.Do(accountDir, statProbeTimeout, func() bool {
		return mountAliveFn(base, accountDir)
	})
	return ok && alive
}

// mountPollInterval is waitMounted's probe cadence. A var, not a const, so
// tests can shrink it.
var mountPollInterval = 100 * time.Millisecond

// waitMounted polls until base's contents are visible through accountDir.
// Probe-first, then deadline, then sleep: the ordering guarantees one final
// probe at/after the deadline, so a mount that lands while the last sleep
// straddles the deadline is kept rather than reported dead (and a timeout of
// zero probes exactly once).
func waitMounted(base, accountDir string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if mountAliveFn(base, accountDir) {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(mountPollInterval)
	}
}
