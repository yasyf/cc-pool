package daemon

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/fusekit/mountd"
	"github.com/yasyf/fusekit/proc"
)

// holderPolicy adapts the cc-pool daemon to proc.Policy and the Supervisor's
// child-control callbacks: the generic state machine (revive / spare / replace,
// spawn backoff, crash-loop breaker, version-skew settle) is proc.Supervisor,
// and every cc-pool-specific judgement (what "ready" means, when a replace is
// safe, what retreat does, how a holder is shut down / waited out / reaped /
// reconciled) lives behind these hooks.
//
// Only the supervise goroutine (s.superviseHolder's loop, or the tests' direct
// s.sup.Tick / Replace calls) drives these hooks, single-threaded, so the
// policy's bookkeeping carries no lock — the same discipline the old supervisor
// struct held.
type holderPolicy struct {
	s *Server

	// degradedStreak counts CONSECUTIVE degraded polls for the degradedStrikes
	// debounce (relocated from the old handleDegraded). Reset to 0 by any
	// non-degraded poll, so a one-off List blip never reaches the act threshold
	// and triggers proc's first-tick force-replace.
	degradedStreak int

	// replaceRows is the fuse rows the cleared ReplaceSafe gate holds claims on,
	// stashed for the Reconcile remount/release. capturedPID / capturedPIDErr is
	// the holder's identity resolved at gate time, so a later peer-gated Kill
	// lands only on this exact process — never a successor that bound the socket
	// in between.
	replaceRows    []store.Account
	capturedPID    int
	capturedPIDErr error

	// carriedSnapshot / rowDirsSnapshot are the dead holder's carried pre-row
	// bases and account-row dir set, snapshotted in the ChildDied reconcile
	// (which fires BEFORE the respawn, while the cache still holds them) for the
	// Respawned reconcile to consume — by which point verifySpawned's refresh has
	// overwritten the cache with the fresh holder's empty List. ChildDied stashes;
	// Respawned reads-and-clears.
	carriedSnapshot map[string]string
	rowDirsSnapshot map[string]bool

	// lastSpawnErr / lastDefer / lastConvergeVer back the once-per-transition
	// operator logs proc does not own: a spawn-failure text, a skew-replace
	// deferral reason, and the reverse-skew converge guidance (logged once per
	// distinct spawnedSkew version cc-pool re-derives from the post-spawn poll).
	lastSpawnErr    string
	lastDefer       string
	lastConvergeVer string

	// loggedWedgedAlive dedups the once-per-episode "alive but unresponsive" log
	// on the PeerAlive spare path (proc spares a wedged-alive holder silently);
	// reset on recovery (a reachable Probe) or genuine death (PeerAlive false), so
	// each fresh wedged-alive episode logs exactly once.
	loggedWedgedAlive bool
}

// Probe polls the holder (holder.refresh = Client.Poll) and distills the cache
// into a proc.Verdict, applying the degraded debounce: a degraded poll is only
// reported as Degraded once it has been seen degradedStrikes consecutive times,
// so a single transient List blip reports a non-degraded, reachable verdict at
// the known version (proc routes it to the healthy-settled branch and never
// replaces) and is waited out with no destructive action.
func (p *holderPolicy) Probe() proc.Verdict {
	p.s.holder.refresh(p.s.holderClient())
	healthy, degraded, ver := p.s.holder.viewState()
	switch {
	case degraded:
		p.degradedStreak++
		if p.degradedStreak < degradedStrikes {
			// Not yet debounced: a reachable verdict at the known version. proc
			// routes it to the healthy-settled branch and never replaces; the cache
			// still reads degraded so superviseTick's heal gate skips it this tick.
			return proc.Verdict{Reachable: true, Version: ver}
		}
		// Debounced: a real degraded holder. proc force-replaces iff skewed.
		return proc.Verdict{Reachable: true, Degraded: true, Version: ver}
	case healthy:
		p.degradedStreak = 0
		p.loggedWedgedAlive = false
		return proc.Verdict{Reachable: true, Version: ver}
	default:
		p.degradedStreak = 0
		return proc.Verdict{Reachable: false}
	}
}

// PeerAlive reports whether the holder socket still has a live peer — the
// dead(revive)-vs-wedged(spare) meltdown gate proc consults before every
// destructive arm. A live peer is the alive-but-unresponsive case proc spares
// silently; cc-pool logs it once per episode here (proc owns no callback for the
// spare). A no-peer result is the genuine death: it ends the episode (resetting
// the dedup) so the next wedged-alive episode logs afresh.
func (p *holderPolicy) PeerAlive() bool {
	alive := p.s.peerAliveOn(p.s.holderSocket)
	if alive {
		if !p.loggedWedgedAlive {
			p.loggedWedgedAlive = true
			p.s.log.Printf("mount holder alive but unresponsive; preserving its mirrors (recovers, or the spawn-fail breaker retreats them)")
		}
		return true
	}
	// Genuinely dead (no peer): end any wedged-alive episode so the NEXT one (e.g.
	// a respawned holder that wedges again) logs once more, rather than being
	// suppressed by a stale flag.
	p.loggedWedgedAlive = false
	return false
}

// ReplaceSafe runs cc-pool's idle/claim gate (skewReplaceGate). A non-"" reason
// defers (no claims held), logged once per distinct reason. On "" the gate
// cleared with the replace claims HELD on every fuse row: the rows are stashed
// for the Reconcile remount/release, and the holder's identity is captured NOW
// (before asking it to exit) so a later peer-gated Kill lands only on this exact
// process.
func (p *holderPolicy) ReplaceSafe(ctx context.Context, force bool) string {
	_, ver := p.s.holder.view()
	fuse, reason := p.s.skewReplaceGate(ctx, force)
	if reason != "" {
		if reason != p.lastDefer {
			p.lastDefer = reason
			p.s.log.Printf("deferring replacement of version-skewed mount holder (%s): %s", ver, reason)
		}
		return reason
	}
	p.lastDefer = ""
	p.s.log.Printf("replacing version-skewed mount holder (%s) with %s", ver, version.String())
	p.replaceRows = fuse
	p.capturedPID, p.capturedPIDErr = p.s.peerPIDOf(p.s.holderSocket)
	return ""
}

// Retreat is the crash-loop breaker action: fall every fuse row back to the
// always-available symlink overlay (fallbackCrashLoopedRows).
func (p *holderPolicy) Retreat(ctx context.Context, reason string) {
	fuse, err := p.s.fuseAccounts()
	if err != nil {
		p.s.log.Printf("holder supervision retreat: list accounts: %v", err)
		return
	}
	p.s.log.Printf("mount holder unrecoverable (%s); retreating fuse rows to symlink", reason)
	p.s.fallbackCrashLoopedRows(ctx, fuse)
}

// Shutdown asks the holder to step down for a graceful replace. The holder
// sweeps its mounts BEFORE the Shutdown reply lands, so the cache stops vouching
// either way — an errored RPC is outcome-unknown, not nothing-happened; proc
// waits the holder out (WaitGone) before deciding. The mountd seam returns
// (failed, err); only the err drives proc's routing.
func (p *holderPolicy) Shutdown(ctx context.Context) error {
	cl := p.s.holderClient()
	failed, err := cl.Shutdown()
	p.s.holder.markUnhealthy()
	if err == nil && len(failed) > 0 {
		p.s.log.Printf("skewed holder reported %d dir(s) that would not unmount", len(failed))
	}
	return err
}

// WaitGone reports whether the retiring holder released its socket within d.
func (p *holderPolicy) WaitGone(ctx context.Context, d time.Duration) bool {
	return p.s.holderClient().WaitGoneContext(ctx, d)
}

// Kill is the peer-gated SIGKILL escape hatch (force-Replace reap only). It
// kills only the pid captured at gate time; an uncaptured identity refuses
// before the seam, and mountd.ErrUnreachable (the peer vanished between
// WaitGone's last probe and now) is translated to proc.ErrChildUnavailable so
// proc's reapWedged reads it as "nothing to kill, socket free" and proceeds.
func (p *holderPolicy) Kill() (int, error) {
	if p.capturedPIDErr != nil {
		p.s.log.Printf("skewed holder wedged after shutdown, but its pid was not captured at gate time (%v); not killing; retrying next tick", p.capturedPIDErr)
		return 0, fmt.Errorf("holder pid not captured at gate time: %w", p.capturedPIDErr)
	}
	pid, err := p.s.killPeerPid(p.s.holderSocket, p.capturedPID)
	if errors.Is(err, mountd.ErrUnreachable) {
		return 0, fmt.Errorf("%w: %w", proc.ErrChildUnavailable, err)
	}
	if err != nil {
		p.s.log.Printf("skewed holder wedged after shutdown; kill socket peer: %v; retrying next tick", err)
		return 0, err
	}
	p.s.log.Printf("skewed holder wedged after shutdown; killed socket peer pid %d", pid)
	return pid, nil
}

// Reconcile dispatches proc's transition events to cc-pool's re-establishment:
//   - ChildDied (the genuine no-peer death): force-unmount the dead holder's
//     orphaned carcasses before the respawn (the kill-9 whole-machine hazard),
//     and stash the carry / row-dir snapshot for the Respawned remount.
//   - Respawned: remount the fuse rows + the dead holder's pre-row carried dirs.
//   - ReplaceSucceeded: remount under the HELD replace claims, then release them.
//   - ReplaceAborted: release the held claims, no remount.
func (p *holderPolicy) Reconcile(ctx context.Context, ev proc.ReconcileEvent) {
	switch ev.Kind {
	case proc.ChildDied:
		// proc fires ChildDied only on the genuine-death (no-peer) path (contract 1:
		// an alive-but-wedged holder is spared), BEFORE the respawn, while
		// carriedBases still holds the dead holder's registry — snapshot it (and the
		// row-dir set) here for the Respawned reconcile, which runs after
		// verifySpawned's refresh wiped the cache.
		p.s.log.Printf("mount holder unreachable")
		// The carried pre-row bases come from the in-memory registry (NO store read),
		// and a dead holder's mounts are always dead carcasses. Force-unmount THEIR
		// carcasses (the kill-9 whole-machine hazard) FIRST and unconditionally — a
		// transient store read must never gate carcass clearing, so even the fuse-row
		// listing below failing cannot skip the carried set.
		carry := p.s.holder.carriedBases()
		p.s.forceUnmountOrphans(orphanDirs(nil, carry))
		fuse, err := p.s.fuseAccounts()
		if err != nil {
			// The fuse-row set is unknown (store read failed), but the carried
			// carcasses are already cleared. Snapshot (carry, nil) so Respawned
			// remounts the carried dirs and never reads a stale prior-episode set.
			p.s.log.Printf("holder supervision: fuse accounts: %v", err)
			p.carriedSnapshot, p.rowDirsSnapshot = carry, nil
			return
		}
		// Now the fuse set is known, clear its row carcasses too (carried dirs are
		// disjoint from row dirs, so this never re-touches an already-cleared one).
		p.s.forceUnmountOrphans(orphanDirs(fuse, nil))
		all, err := p.s.m.Store.ListAccounts()
		if err != nil {
			// Carcasses are cleared; the row-dir set is unknown, so snapshot a nil
			// set — Respawned then remounts the carried dirs without suppressing any
			// as account rows, and never reads a stale prior-episode set.
			p.s.log.Printf("holder supervision: list accounts: %v", err)
			p.carriedSnapshot, p.rowDirsSnapshot = carry, nil
			return
		}
		rowDirs := make(map[string]bool, len(all))
		for _, a := range all {
			rowDirs[a.ConfigDir] = true
		}
		p.carriedSnapshot, p.rowDirsSnapshot = carry, rowDirs

	case proc.Respawned:
		// Read-and-clear the ChildDied stash FIRST, before any fallible call. A
		// Respawned can fire with NO preceding ChildDied (a wedged-alive holder that
		// recovers when its own spawn-verify finally answers — see proc.Respawned's
		// contract), so an early return below must never leave a stale snapshot for a
		// later episode to consume.
		carry, rowDirs := p.carriedSnapshot, p.rowDirsSnapshot
		p.carriedSnapshot, p.rowDirsSnapshot = nil, nil
		fuse, err := p.s.fuseAccounts()
		if err != nil {
			p.s.log.Printf("holder supervision: list accounts: %v", err)
			return
		}
		p.s.log.Printf("mount holder respawned")
		p.s.remountFuseRows(ctx, fuse)
		p.s.remountCarriedDirs(ctx, rowDirs, carry)

	case proc.ReplaceSucceeded:
		p.s.remountReplacedRows(ctx, p.replaceRows)
		p.s.log.Printf("mount holder replaced at %s", version.String())
		p.s.endReplace(accountIDs(p.replaceRows))
		p.replaceRows = nil

	case proc.ReplaceAborted:
		p.s.endReplace(accountIDs(p.replaceRows))
		p.replaceRows = nil
	}
}

// onSpawnError surfaces a proc-booked spawn/verify failure on the status wire
// (HolderStatus.SpawnError) and logs it once per distinct error text — the
// surface proc itself does not own. proc passes the post-increment attempt count
// and the next-retry floor, so the log carries the "attempt N, next in W" detail.
// Wired to proc.Supervisor.OnSpawnError.
func (p *holderPolicy) onSpawnError(err error, attempt int, nextRetry time.Time) {
	p.s.holder.recordSpawnError(err.Error())
	if err.Error() != p.lastSpawnErr {
		p.lastSpawnErr = err.Error()
		p.s.log.Printf("spawn mount holder failed (attempt %d, next in %s): %v", attempt, time.Until(nextRetry).Round(time.Second), err)
	}
}

// noteSettledVersion reconstructs the cc-pool status/log surfaces a healthy
// settle clears or guides on — proc owns the spawnedSkew bookkeeping privately
// and never calls back at settle, so cc-pool re-derives them from the post-poll
// version: a successful settle clears the surfaced spawn error, and a settle at
// a NEW reverse-skew version (our own spawn produced a newer binary's version)
// logs the converge guidance once per distinct version. Called from
// superviseTick on the healthy, not-degraded, version-known path.
func (p *holderPolicy) noteSettledVersion(ver string) {
	if p.lastSpawnErr != "" {
		p.lastSpawnErr = ""
		p.s.holder.recordSpawnError("")
	}
	switch {
	case ver == "" || ver == version.String():
		// A genuine settle at our version: the skewed holder we may have been
		// deferring is gone. Clear BOTH dedups so a future converge note and a future
		// skew-deferral each log afresh — reset only on a real settle here, never on
		// the forward-skew default arm below (where a deferral is still in flight).
		p.lastConvergeVer = ""
		p.lastDefer = ""
	case ver == p.s.sup.SpawnedSkew():
		// A TRUE reverse-skew steady state: our OWN spawn settled at a newer on-disk
		// version (an upgrade swapped the binary under this running daemon). proc
		// treats that as steady and never re-replaces it — a settled holder, not one
		// we are deferring — so clear the deferral dedup and guide the operator to
		// restart to converge, once per distinct version.
		p.lastDefer = ""
		if ver != p.lastConvergeVer {
			p.lastConvergeVer = ver
			p.s.log.Printf("spawned mount holder reports %s but this daemon is %s (binary swapped by an upgrade?); serving as-is — restart the daemon to converge", ver, version.String())
		}
	default:
		// A forward-skew holder proc is actively trying to replace (its converge just
		// deferred on a busy pool): this is NOT a settle, so leave lastDefer intact —
		// the once-per-reason deferral dedup must hold across ticks. Emit no converge
		// guidance (proc IS converging it); reset only the converge dedup so a later
		// genuine reverse-skew still logs.
		p.lastConvergeVer = ""
	}
}
