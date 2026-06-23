package daemon

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/mountd"
	"github.com/yasyf/fusekit/proc"
	"github.com/yasyf/fusekit/version"
)

const (
	// defaultSuperviseInterval is the holder supervision cadence: a crashed
	// holder is respawned and its mirrors remounted in ~10s instead of the
	// scheduler's ~3.5-minute poll.
	defaultSuperviseInterval = 10 * time.Second

	// spawnBackoffBase/spawnBackoffCap bound the respawn backoff: consecutive
	// spawn failures double the wait 10s → 20s → … capped at 10 minutes.
	spawnBackoffBase = 10 * time.Second
	spawnBackoffCap  = 10 * time.Minute

	// remountBackoffBase/remountBackoffCap bound the per-row remount backoff
	// for fuse rows the holder cannot vouch for (see retryUnvouchedFuseRows):
	// consecutive failed heals double the wait 10s → 20s → … capped at 2
	// minutes. The cap sits deliberately under the 180s scheduler period —
	// supervision must never be the slower recovery path.
	remountBackoffBase = 10 * time.Second
	remountBackoffCap  = 2 * time.Minute

	// defaultHolderGoneWait bounds waiting for a retiring holder to release
	// its socket after acking Shutdown — the holder's own sweep runs under a
	// 60s op deadline and the client's Shutdown timeout is 65s, so this sits
	// just above both.
	defaultHolderGoneWait = 70 * time.Second
)

// remountBreakerThreshold is the circuit-breaker limit on retryUnvouchedFuseRows:
// after this many CONSECUTIVE wedged/never-recovering heal failures (mount-up
// timeouts, a wedged unmount in the way — never a TCC block, which is a clean
// not-mounted state), the loop stops retrying the row forever and escalates
// (escalateWedgedRow). A mount that never comes up churns indefinitely and can
// keep re-wedging the kernel — the whole-machine hazard the kill-9 holder-death
// incident exposed — so the row is forced down and converted to symlink instead
// of retried into eternity.
const remountBreakerThreshold = 5

// tccBreakerThreshold bounds how long the daemon waits for the macOS "Network
// Volumes" grant before giving up and retreating the row to symlink. A TCC
// block is a clean not-mounted state (so it never trips remountBreakerThreshold
// — see rowRetryState.hazard), but it must NOT retry forever: a machine where
// the grant can never land would otherwise churn doomed mounts and leave every
// account unusable. After this many CONSECUTIVE TCC-blocked heals — with the
// per-row backoff (remountBackoff, capped at 2m) the span is ~4-5 minutes,
// ample for an attentive human to grant — the row escalates to the
// always-available symlink overlay (escalateTCCBlockedRow). Set ABOVE
// remountBreakerThreshold: a TCC block is a benign wait, not the kernel hazard a
// wedged mount is, so it earns a longer grace. The startup capability probe
// (reconcileOverlays) already retreats a hard-failing pool in one pass, so this
// per-row grace only governs the genuinely-pending case.
const tccBreakerThreshold = 6

// reviveBreakerThreshold is the holder-level circuit breaker. After this many
// CONSECUTIVE holder deaths (each force-unmounting EVERY mount and losing
// in-flight writes — see reviveHolder) without the holder ever returning at
// THIS daemon's version — a stuck old holder we cannot replace under live
// sessions, or fuse-t/NFS gone unavailable so a holder will not spawn at all —
// the supervisor stops reviving it and falls every fuse row back to the
// always-available symlink overlay (retreatAllFuseRows). Lower than
// remountBreakerThreshold because a holder-level loop churns the WHOLE pool
// each cycle, so its data-loss blast radius warrants a faster retreat. A clean
// holder restart only reaches 1 (the next settled tick at our version resets
// it), so this never false-trips on a normal respawn or a one-off death.
const reviveBreakerThreshold = 3

// reviveHazardWindow bounds what counts as a CONSECUTIVE death for the
// crash-loop breaker: two deaths farther apart than this start a fresh cluster
// (reviveHazard resets to 1). Without it, a holder that settles healthy at a
// spawnedSkew version between deaths (which deliberately does NOT reset the
// count — see reviveHazard) would accumulate unrelated, far-apart transient
// deaths over its whole lifetime and eventually demote a perfectly healthy
// pool. A real crash loop re-dies every few minutes, well inside this window;
// genuinely occasional deaths fall outside it and never accumulate.
const reviveHazardWindow = 30 * time.Minute

// degradedStrikes debounces the degraded verdict (holder alive but its List
// failed): a single transient List blip under load must not trigger a
// destructive forced converge or a needless heal sweep, so superviseTick acts
// on a degraded holder only after this many CONSECUTIVE degraded ticks. Two —
// the same one-transient-blip tolerance as deepWedgeStrikes — so a persistently
// degraded skewed holder (the serial-List incident) still converges on the next
// tick, while a one-off List timeout is waited out.
const degradedStrikes = 2

// forceUnmount force-unmounts an orphaned fuse carcass directly, without
// routing through the (possibly dead) holder; seamed so tests assert the calls
// without real mounts. Production: overlay.ForceUnmount (bounded).
var forceUnmount = overlay.ForceUnmount

// rowRetryState is one fuse row's remount-backoff bookkeeping in s.rowRetry,
// the cc-pool-owned half of supervision state that stayed in the daemon when
// the generic state machine moved to proc.Supervisor.
type rowRetryState struct {
	failures int       // consecutive failed heal attempts (drives the backoff window)
	retryAt  time.Time // backoff: earliest next heal attempt
	// hazard counts CONSECUTIVE wedged/never-recovering heal failures for the
	// circuit breaker (remountBreakerThreshold). It is advanced only by hazard
	// outcomes (healRetry/healFallback) and reset by a successful mount or a
	// healTCCBlocked — a TCC block is a clean not-mounted state, so it backs off
	// (via failures) but never counts toward the wedged breaker.
	hazard int
	// tccBlocks counts CONSECUTIVE healTCCBlocked outcomes for the TCC grace
	// breaker (tccBreakerThreshold): a grant that never lands must not retry
	// forever. Advanced only by advanceTCCRetry and reset by any non-TCC outcome
	// (a successful mount drops the whole ledger entry; a wedged/DB failure zeroes
	// it via advanceRowRetry), so an alternating TCC/wedge row reaches neither
	// breaker.
	tccBlocks int
}

// buildSupervisor constructs the holder supervisor and its cc-pool policy
// adapter. Called once on the supervise goroutine before its loop starts (so
// that goroutine stays the sole mutator of supervisor state); tests that drive
// s.sup.Tick directly call it from their setup. The generic mechanism — revive
// under spawn backoff, the crash-loop breaker, the version-skew settle — is
// proc.Supervisor's; every cc-pool judgement and child-control effect is wired
// through the holderPolicy, which implements the full proc.Policy. The Spawn's
// Override routes the actual bring-up through s.spawn (the injectable seam), so
// proc drives the lifecycle without exec'ing a child itself — production
// delegates to pool.SpawnHolder, tests to the canned-holder recorder.
func (s *Server) buildSupervisor() {
	p := &holderPolicy{s: s}
	s.policy = p
	// proc's per-leg WaitGone/reap wait is Supervisor.GoneWait; cc-pool's gone-wait
	// for a retiring holder (holderGoneWait, tests shrink it) drives it. The actual
	// bring-up timeout lives in the Override seam (s.spawnIfServing ->
	// pool.SpawnHolder), so proc's come-up Spawn.Timeout is never exercised.
	goneWait := s.holderGoneWait
	if goneWait <= 0 {
		goneWait = defaultHolderGoneWait
	}
	s.sup = &proc.Supervisor{
		Spawn: proc.Spawn{
			Socket:    s.holderSocket,
			Available: func() bool { return mountd.NewClient(s.holderSocket).Available() },
			CanHost:   func() error { return nil },
			Override:  s.spawnIfServing,
		},
		MyVersion:     version.String(),
		Policy:        p,
		OnSpawnError:  p.onSpawnError,
		GoneWait:      goneWait,
		HazardWindow:  reviveHazardWindow,
		SpawnBackoff:  proc.Backoff{Base: spawnBackoffBase, Cap: spawnBackoffCap},
		ReviveBreaker: reviveBreakerThreshold,
	}
	// Fail loud at wire time if a Required field is missing, rather than
	// nil-panicking deep inside a revive or replace.
	if err := s.sup.Validate(); err != nil {
		panic(fmt.Sprintf("daemon: holder supervisor misconfigured: %v", err))
	}
}

// superviseHolder watches the detached mount holder until ctx is cancelled:
// the generic state machine (revive a dead holder under spawn backoff and the
// crash-loop breaker, spare an alive-but-wedged one, replace a version-skewed
// one once the claim gate clears) is proc.Supervisor.Tick; superviseTick wraps
// it with cc-pool's steady-state heal. proc owns no ticker of its own — the
// consumer drives the loop, precisely because the heal-after-tick coupling
// belongs in cc-pool. Started after the startup reconcile so it never races the
// initial mounts.
func (s *Server) superviseHolder(ctx context.Context) {
	interval := s.superviseInterval
	if interval <= 0 {
		interval = defaultSuperviseInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.superviseTick(ctx)
		}
	}
}

// superviseTick runs one supervision pass: the generic state machine
// (proc.Supervisor.Tick — revive / spare / replace, spawn backoff, crash-loop
// breaker, version-skew settle) followed by the cc-pool steady-state heal,
// gated on the post-tick cache reading healthy and not-degraded — the same
// outcomes the old hand-rolled tick healed under (a settled holder, or a
// deferred skew whose rows must still reach the remount breaker). A degraded
// or unreachable post-tick cache skips the heal: Tick already routed it
// (force-converge / revive), and a row under a still-degraded or dead holder
// has no live mirror to vouch for this tick. The reverse-skew converge guidance
// and the cleared-on-settle spawn-error surface — which proc owns privately —
// are reconstructed here from the post-tick version.
func (s *Server) superviseTick(ctx context.Context) {
	if s.sup == nil {
		// Production builds the supervisor in serve before the loop; tests drive
		// superviseTick directly after their setup, so build it lazily on the
		// first tick — by which point holderSocket / holderGoneWait / the seams
		// are at their test values.
		s.buildSupervisor()
	}
	s.sup.Tick(ctx)
	healthy, degraded, ver := s.holder.viewState()
	if healthy && !degraded {
		s.policy.noteSettledVersion(ver)
		s.retryUnvouchedFuseRows(ctx)
	}
}

// forceUnmountOrphans force-unmounts every dir in dirs that is currently a
// mountpoint, directly via the bounded overlay.ForceUnmount — bypassing the
// (dead) holder entirely. A dead holder's mirrors are ALWAYS dead carcasses
// (the cache dropped them and reviveHolder remounts them fresh), so this loses
// nothing and removes the whole-machine hazard a wedged NFS carcass becomes
// (the kill-9 incident: lsof/stat on the box then block forever on it). Yes it
// yanks fds from any live session still on a dir — the mirror is already
// wedged and a relaunch is the fix, consistent with the held-dead remount
// policy. Each unmount is bounded inside overlay.ForceUnmount so a carcass the
// kernel will not even MNT_FORCE cannot hang the supervise goroutine.
// Mountpoint membership uses the bounded overlayMounted seam (a probe that
// cannot answer reads still-mounted, so a wedged dir is unmounted rather than
// skipped); a dir provably not a mountpoint is skipped.
func (s *Server) forceUnmountOrphans(dirs []string) {
	for _, dir := range dirs {
		if !overlayMounted(dir) {
			continue
		}
		if err := forceUnmount(dir); err != nil {
			s.log.Printf("force-unmount orphaned carcass %s: %v", dir, err)
			continue
		}
		s.log.Printf("force-unmounted orphaned mount %s after holder death", dir)
	}
}

// orphanDirs is the deduplicated set of mountpoints a dead holder may have left
// wedged: every fuse row's ConfigDir plus every carried pre-row mount dir
// (a `ccp add` mid-login the holder served before its account row landed).
func orphanDirs(fuse []store.Account, carry map[string]string) []string {
	seen := make(map[string]bool, len(fuse)+len(carry))
	dirs := make([]string, 0, len(fuse)+len(carry))
	add := func(d string) {
		if d == "" || seen[d] {
			return
		}
		seen[d] = true
		dirs = append(dirs, d)
	}
	for _, a := range fuse {
		add(a.ConfigDir)
	}
	for dir := range carry {
		add(dir)
	}
	return dirs
}

// skewReplaceGate evaluates every leg of the idle gate, returning the fuse
// rows for the remount and the first blocking reason — "" means clear, and
// means the replace claims are HELD on every fuse row (the caller must
// endReplace). force (the degraded-skew converge) bypasses ONLY the busy and
// uptime legs — a degraded holder is already failing the sessions, so a clean
// swap beats leaving them on it; every claim-safety leg below still holds. The
// legs, in claim-first order (the convertAccount pattern: claim, then scan — so
// nothing can land between a clean scan and the holder's sweep):
//   - this build must be able to spawn the replacement: never stop a holder
//     we cannot succeed;
//   - daemon uptime ≥ reservationTTL (skipped under force): a freshly-started
//     daemon's reservation map is empty while a ≤30s-old select may not have
//     exec'd its claude yet;
//   - beginReplace claims every fuse row — refusing on any live reservation,
//     any mid-poll account, or ANY in-flight conversion (a symlink→fuse
//     migrate is about to Mount through the holder being retired; its row
//     only flips at the end, so it is invisible to per-fuse-row checks);
//   - the fuse set is re-listed under the claims: a conversion that completed
//     between the listing and the claim could have flipped a row either way,
//     leaving an unclaimed fuse row (selectable mid-sweep) or a claimed
//     symlink row; a changed set defers one tick;
//   - every dir the holder serves must have an account row: a pre-row dir is a
//     `ccp add` mid-login (its row lands at FinalizeAdd), invisible to the
//     claims, and the sweep would yank its mirror under the login. Under force
//     the served set comes from the surviving bases registry, since a degraded
//     holder's mounts map is nil;
//   - the session scan must succeed — fail closed — and zero sessions on every
//     fuse row's dir AND every dir the holder serves (kernel truth, covering
//     mounts whose rows were deleted while a teardown was refused). BOTH skipped
//     under force.
func (s *Server) skewReplaceGate(ctx context.Context, force bool) (fuse []store.Account, reason string) {
	if !s.canSpawnHolder() {
		return nil, "this build cannot spawn a replacement holder (no fuse support)"
	}
	if !force {
		if up := time.Since(s.startedAt); up < reservationTTL {
			return nil, fmt.Sprintf("daemon up only %s; a pre-restart select may not be visible yet", up.Round(time.Second))
		}
	}
	fuse, err := s.fuseAccounts()
	if err != nil {
		return nil, fmt.Sprintf("list accounts: %v", err)
	}
	ids := accountIDs(fuse)
	if reason := s.beginReplace(ids); reason != "" {
		return nil, reason
	}
	bail := func(why string) ([]store.Account, string) {
		s.endReplace(ids)
		return nil, why
	}
	fresh, err := s.fuseAccounts()
	if err != nil {
		return bail(fmt.Sprintf("re-list accounts: %v", err))
	}
	if !sameAccountIDs(fuse, fresh) {
		return bail("the fuse account set changed mid-gate")
	}
	all, err := s.m.Store.ListAccounts()
	if err != nil {
		return bail(fmt.Sprintf("list account dirs: %v", err))
	}
	rowDirs := make(map[string]bool, len(all))
	for _, a := range all {
		rowDirs[a.ConfigDir] = true
	}
	dirs := make(map[string]bool, len(fuse))
	for _, a := range fuse {
		dirs[a.ConfigDir] = true
	}
	// The holder-served dirs the pre-row guard checks. A forced converge runs
	// against a DEGRADED holder whose mounts map is nil, so mountDirs() is empty
	// — the surviving bases registry (carriedBases) is the only record of what it
	// serves, including a `ccp add` mid-login mount with no account row yet.
	served := s.holder.mountDirs()
	if force {
		for dir := range s.holder.carriedBases() {
			served = append(served, dir)
		}
	}
	for _, dir := range served {
		// A holder-served dir with no account row is a `ccp add` mid-login:
		// the row only lands at FinalizeAdd, so the replace claims cannot see
		// it and the sweep would yank the mirror under the login. Defer.
		if !rowDirs[dir] {
			return bail(fmt.Sprintf("holder serves %s with no account row (an add may be in flight)", dir))
		}
		dirs[dir] = true
	}
	if force {
		// A forced converge replaces a degraded skewed holder despite live
		// sessions: it is already failing their mirrors, so a clean swap onto our
		// version is strictly better than leaving them on it. Only the session
		// scan and its zero-sessions gate are skipped — every other claim
		// (beginReplace, the re-list, the pre-row guard above) still holds.
		return fuse, ""
	}
	sessions, err := s.scan(ctx)
	if err != nil {
		return bail(fmt.Sprintf("session scan: %v", err))
	}
	for dir := range dirs {
		if n := procscan.CountByConfigDir(sessions, dir); n > 0 {
			return bail(fmt.Sprintf("%d live session(s) on %s", n, dir))
		}
	}
	return fuse, ""
}

// remountFuseRows heals every fuse row the holder cache cannot vouch for,
// each under the scheduler's poll-claim discipline so supervision never races
// a poll or conversion on the same account. A claimed account is skipped, not
// raced — its owner leaves it consistent, and a later revive or poll
// re-checks. The row is re-read under the claim (the caller's list aged
// across the spawn I/O) so a row converted in the gap is left alone. Used by
// reviveHolder; the skew replace remounts under its own claims instead (see
// remountReplacedRows).
func (s *Server) remountFuseRows(ctx context.Context, accts []store.Account) {
	for _, a := range accts {
		if ctx.Err() != nil {
			return
		}
		if s.holder.ready(a.ConfigDir) {
			continue
		}
		if !s.beginPoll(a.ID) {
			s.log.Printf("acct-%02d busy; deferring its remount to the next supervision tick", a.ID)
			continue
		}
		fresh, err := s.m.Store.GetAccount(a.ID)
		switch {
		case err != nil:
			s.log.Printf("acct-%02d re-read row before remount: %v", a.ID, err)
		case fresh.OverlayKind == string(overlay.KindFuse):
			s.healFuse(ctx, fresh)
		}
		s.endPoll(a.ID)
	}
}

// retryUnvouchedFuseRows is the steady-state heal loop: on every supervision
// tick with a healthy settled holder, each fuse row the holder cache cannot
// vouch for — a remount that failed during a revive, a TCC-blocked row
// waiting on its grant, or a mirror the holder reports present-but-dead,
// deep-wedged or plain dead — is retried under per-account exponential backoff
// (remountBackoffBase doubling to remountBackoffCap, deliberately under the
// 180s scheduler period). Each attempt runs under the scheduler's poll-claim
// discipline: a claimed account is skipped WITHOUT advancing its backoff —
// skipping is not failing — and its row is re-read under the claim so a row
// converted in the gap is left alone. A successful heal (by this loop or
// anyone else — ready() covers mounts established by any path) deletes the
// row's ledger entry; rows that left the fuse set are pruned after the pass.
// reviveHolder's one-shot remountFuseRows stays separate: a revived holder's
// rows get one immediate sweep there, and the first steady tick afterwards
// lands here ~10s later for anything it could not finish.
func (s *Server) retryUnvouchedFuseRows(ctx context.Context) {
	fuse, err := s.fuseAccounts()
	if err != nil {
		s.log.Printf("holder supervision: list accounts: %v", err)
		return
	}
	// One session scan per pass — but only when there are fuse rows to heal or
	// probe: the periodic in-use probe gates on a live session, and the
	// held-dead log line wants the count too. A symlink-only or empty pool
	// never scans. A failed scan is non-fatal — the gate then reads zero
	// sessions and skips probing (the caution direction), and the held-dead
	// line reports 0.
	var sessions []procscan.Session
	if len(fuse) > 0 {
		ses, serr := s.scan(ctx)
		if serr != nil {
			s.log.Printf("holder supervision: session scan: %v", serr)
		}
		sessions = ses
	}
	now := time.Now()
	inPass := make(map[int]bool, len(fuse))
	for _, a := range fuse {
		inPass[a.ID] = true
		if ctx.Err() != nil {
			return
		}
		// Periodic in-use deep probe: only a mount the holder still vouches for
		// (shallow-live) AND backing at least one live session AND not probed
		// within the interval AND not mid-conversion. A fresh wedge flips the
		// verdict here so the ready() check just below sends the row to heal in
		// this same pass. IDLE mounts (no live session) are never probed — that
		// is the whole point of moving the probe into the daemon; an idle
		// mount's wedge is caught at select time instead (handleSelect). The
		// probe is a bounded read (deepProbe, 5s; concurrent same-dir probes
		// join one in-flight read), so it runs without the poll claim.
		if s.holder.shallowLive(a.ConfigDir) &&
			procscan.CountByConfigDir(sessions, a.ConfigDir) > 0 &&
			s.holder.dueForDeepProbe(a.ConfigDir, now, deepProbeInterval) &&
			!s.isConverting(a.ID) {
			if msg := s.holder.recordDeep(a.ConfigDir, deepProbe(a.ConfigDir)); msg != "" {
				s.log.Printf("%s", msg)
			}
		}
		if s.holder.ready(a.ConfigDir) {
			delete(s.rowRetry, a.ID)
			continue
		}
		if now.Before(s.rowRetry[a.ID].retryAt) {
			continue
		}
		if !s.beginPoll(a.ID) {
			continue // skip-don't-race; the owner leaves it consistent
		}
		fresh, err := s.m.Store.GetAccount(a.ID)
		switch {
		case err != nil:
			s.log.Printf("acct-%02d re-read row before remount: %v", a.ID, err)
			// A DB re-read failure is not a mount hazard; back off, never trip
			// the breaker on it.
			s.advanceRowRetry(a.ID, false)
		case fresh.OverlayKind != string(overlay.KindFuse):
			// Converted while this pass's listing aged: its owner left it
			// consistent, and a non-fuse row needs no remount ledger.
			delete(s.rowRetry, a.ID)
		default:
			if dead, wedged := s.holder.heldDead(a.ConfigDir); dead {
				// The deep-probe verdict picks the copy: a deep wedge serves
				// metadata but hangs reads, while a plain-dead registered mirror
				// (an out-of-band `umount -f` or a dead fuse-t worker) fails
				// reads outright. The relaunch guidance holds in both shapes —
				// sessions on the old mirror are orphaned by the remount either
				// way.
				desc := "dead mirror (fails reads outright; unmounted out of band or its fuse worker died?)"
				if wedged {
					desc = "wedged mirror (serves metadata but hangs reads)"
				}
				s.log.Printf("acct-%02d %s; remounting under %d live session(s) — relaunch them",
					a.ID, desc, procscan.CountByConfigDir(sessions, a.ConfigDir))
			}
			switch s.healFuse(ctx, fresh) {
			case healMounted:
				delete(s.rowRetry, a.ID)
			case healTCCBlocked:
				// A clean not-mounted state waiting on the macOS "Network
				// Volumes" grant: back off and give the grant a bounded grace
				// window. It never counts toward the wedged breaker (it is not a
				// kernel hazard), but it must NOT retry forever — a grant that can
				// never land would churn doomed mounts and leave the row unusable
				// — so once the consecutive TCC-block count crosses
				// tccBreakerThreshold, retreat the row to symlink.
				if s.advanceTCCRetry(a.ID) >= tccBreakerThreshold {
					s.escalateTCCBlockedRow(fresh)
				}
			default:
				// healRetry/healFallback: the mirror is not up and we keep
				// trying. Count it as a hazard; once the consecutive count hits
				// the breaker threshold, stop churning and escalate.
				if s.advanceRowRetry(a.ID, true) >= remountBreakerThreshold {
					s.escalateWedgedRow(fresh)
				}
			}
		}
		s.endPoll(a.ID)
	}
	for id := range s.rowRetry {
		if !inPass[id] {
			delete(s.rowRetry, id)
		}
	}
}

// advanceRowRetry books one failed heal attempt against account id's remount
// ledger: the failure count grows and the next attempt waits out the doubled
// window. hazard reports whether the failure was a wedged/never-recovering
// mount (healRetry/healFallback) — those advance the breaker's consecutive
// hazard count; a non-hazard failure (a TCC block or a DB re-read error)
// resets it, since the breaker only ever escalates a genuinely stuck mirror.
// Either way it also resets the TCC grace count, because a non-TCC failure
// breaks any consecutive-TCC run. Returns the post-update hazard count for the
// wedged-breaker check.
func (s *Server) advanceRowRetry(id int, hazard bool) int {
	if s.rowRetry == nil {
		s.rowRetry = make(map[int]rowRetryState)
	}
	st := s.rowRetry[id]
	st.failures++
	if hazard {
		st.hazard++
	} else {
		st.hazard = 0
	}
	// A non-TCC outcome breaks any consecutive-TCC run, so the TCC grace breaker
	// only ever fires on a genuinely stuck-pending grant.
	st.tccBlocks = 0
	st.retryAt = time.Now().Add(proc.Backoff{Base: remountBackoffBase, Cap: remountBackoffCap}.After(st.failures))
	s.rowRetry[id] = st
	return st.hazard
}

// advanceTCCRetry books one TCC-blocked heal against account id's ledger: it
// backs off like advanceRowRetry (failures++ drives the doubling window) but
// advances the TCC grace counter instead of the wedged-hazard one, and resets
// the wedged hazard (a TCC block is a clean not-mounted state, never a wedge).
// Returns the post-update consecutive TCC-block count for the tccBreakerThreshold
// check — the wait-for-the-grant grace is bounded, not infinite.
func (s *Server) advanceTCCRetry(id int) int {
	if s.rowRetry == nil {
		s.rowRetry = make(map[int]rowRetryState)
	}
	st := s.rowRetry[id]
	st.failures++
	st.hazard = 0
	st.tccBlocks++
	st.retryAt = time.Now().Add(proc.Backoff{Base: remountBackoffBase, Cap: remountBackoffCap}.After(st.failures))
	s.rowRetry[id] = st
	return st.tccBlocks
}

// convertRowToSymlink is the single fuse→symlink retreat primitive every
// hazard/dead-end path shares (the wedged breaker, the TCC grace breaker, the
// holder crash-loop retreat, and the startup capability gate). The caller holds
// a convert claim (beginConvert standalone, or beginConvertUnderPoll under a
// held poll). It re-reads the row (a conversion that landed in the claim gap is
// left alone), force-unmounts any standing mount — ABORTING the retreat if the
// unmount wedges, because ConvertOverlay's Teardown would then re-spawn the very
// holder being retreated from (the wedged-carcass churn the kill-9 incident
// exposed) — converts the row, drops the holder-cache vouch and any retry-ledger
// entry, and logs announce. Returns whether the row ended up off fuse. The
// idle/live-session gate is deliberately skipped, distinct from the idle-gated
// fallbackToSymlink: every caller is remediating a hazard or a dead-end (a
// never-recovering wedge, a grant that will not land, a crash-looped holder, a
// machine that cannot fuse), where relaunch — not deferral — is the fix.
func (s *Server) convertRowToSymlink(a store.Account, announce string) bool {
	fresh, err := s.m.Store.GetAccount(a.ID)
	switch {
	case err != nil:
		s.log.Printf("acct-%02d symlink retreat: re-read row: %v", a.ID, err)
		return false
	case fresh.OverlayKind != string(overlay.KindFuse):
		delete(s.rowRetry, a.ID) // already converted by another path: drop any stale ledger entry
		return true
	}
	s.log.Printf("%s", announce)
	if overlayMounted(fresh.ConfigDir) {
		if err := forceUnmount(fresh.ConfigDir); err != nil {
			// The forced unmount wedged. Do NOT proceed into ConvertOverlay: its
			// Teardown would see the dir still mounted and re-spawn the holder
			// being retreated from. Leave the row fuse; a dir whose unmount the
			// kernel refuses cannot be safely symlinked anyway.
			s.log.Printf("acct-%02d symlink retreat: force-unmount %s wedged; leaving fuse: %v", a.ID, fresh.ConfigDir, err)
			return false
		}
	}
	if _, err := s.m.ConvertOverlay(fresh, overlay.KindSymlink); err != nil {
		s.log.Printf("acct-%02d symlink retreat: convert to symlink: %v", a.ID, err)
		return false
	}
	s.holder.noteUnmounted(fresh.ConfigDir)
	delete(s.rowRetry, a.ID)
	return true
}

// escalateRowToSymlink is the poll-held entry to convertRowToSymlink: the
// steady-state breakers (escalateWedgedRow, escalateTCCBlockedRow) hold the
// account's poll claim, so it takes the converting claim with
// beginConvertUnderPoll — refused over a pending select, leaving the row armed
// to re-fire next windowed tick. Returns whether the row converted.
func (s *Server) escalateRowToSymlink(a store.Account, announce string) bool {
	if !s.beginConvertUnderPoll(a.ID) {
		s.log.Printf("acct-%02d symlink retreat deferred: reserved by a pending select", a.ID)
		return false
	}
	defer s.endConvert(a.ID)
	return s.convertRowToSymlink(a, announce)
}

// escalateWedgedRow is the wedged-mount circuit breaker (remountBreakerThreshold
// consecutive wedged/never-recovering heals). Retrying forever lets a wedged NFS
// carcass linger and re-wedge the kernel — the whole-machine hazard the kill-9
// incident exposed — so it retreats the row to symlink via escalateRowToSymlink.
// Caller holds the account's poll claim.
func (s *Server) escalateWedgedRow(a store.Account) {
	announce := fmt.Sprintf("acct-%02d fuse mount never recovered after %d consecutive attempts; force-unmounting and falling back to symlink — relaunch any sessions on it",
		a.ID, remountBreakerThreshold)
	if s.escalateRowToSymlink(a, announce) {
		s.log.Printf("acct-%02d fell back to symlink after exhausting fuse remount attempts", a.ID)
	}
}

// escalateTCCBlockedRow is the TCC grace breaker (tccBreakerThreshold consecutive
// TCC-blocked heals): the macOS "Network Volumes" grant never landed within the
// grace window, so the daemon stops waiting and retreats the row to symlink so
// the account is usable again. It clears the stale process-wide TCC guidance
// only on a real conversion (tccErr is one string — clearing it in the shared
// body, or on a deferred/failed conversion, would wipe a concurrent wedged row's
// still-valid guidance). Caller holds the account's poll claim.
func (s *Server) escalateTCCBlockedRow(a store.Account) {
	announce := fmt.Sprintf("acct-%02d \"Network Volumes\" grant never landed after %d attempts; falling back to symlink — `ccp migrate --to fuse` re-promotes once fuse-t can mount here",
		a.ID, tccBreakerThreshold)
	if s.escalateRowToSymlink(a, announce) {
		s.holder.recordTCC("")
		s.log.Printf("acct-%02d fell back to symlink after the \"Network Volumes\" grant did not land", a.ID)
	}
}

// retreatAllFuseRows retreats every fuse account to the always-available symlink
// overlay, each under its own standalone convert claim (beginConvert — the
// callers hold no poll claim) so a select, a scheduler poll, or another
// conversion cannot interleave; a row that cannot be claimed or fails to convert
// is left for a later pass. reason names the pool-wide condition (logged per
// row). It is the standalone, whole-pool analog of escalateRowToSymlink: the
// holder crash-loop breaker (Policy.Retreat) and the startup capability gate
// (reconcileOverlays) both use it when fuse is unusable for the WHOLE pool, not
// just one row. Per-row conversion (including the wedged-unmount abort) is
// convertRowToSymlink, shared with the breakers.
func (s *Server) retreatAllFuseRows(ctx context.Context, fuse []store.Account, reason string) {
	if len(fuse) == 0 {
		return // a symlink-only pool: nothing to retreat
	}
	for _, a := range fuse {
		if ctx.Err() != nil {
			return
		}
		if !s.beginConvert(a.ID) {
			s.log.Printf("acct-%02d symlink retreat deferred: reserved, polling, or converting", a.ID)
			continue
		}
		announce := fmt.Sprintf("acct-%02d %s; falling back to symlink — relaunch any sessions on it", a.ID, reason)
		if s.convertRowToSymlink(a, announce) {
			s.log.Printf("acct-%02d fell back to symlink (%s)", a.ID, reason)
		}
		s.endConvert(a.ID)
	}
}

// remountReplacedRows heals every fuse row after a holder replacement, under
// the replace's already-held converting claims — never beginPoll: those
// claims, taken at gate time, are what kept selects and conversions off these
// dirs through the old holder's sweep, and they hold until the caller's
// endReplace. The rows are stable under them (the gate verified the set and
// the replacing fence blocks new conversions), so no re-read is needed.
func (s *Server) remountReplacedRows(ctx context.Context, accts []store.Account) {
	for _, a := range accts {
		if ctx.Err() != nil {
			return
		}
		if s.holder.ready(a.ConfigDir) {
			continue
		}
		s.healFuse(ctx, a)
	}
}

// remountCarriedDirs remounts a dead holder's pre-row mounts: dirs its
// registry served that no account row names — a `ccp add` mid-login, whose
// row only lands at FinalizeAdd. Dropping them would strand the add (its
// mount died with the holder and nothing else knows the dir exists). The
// bases come from carriedBases' snapshot of the dead holder's registry; a
// dir that has since gained a row is left to remountFuseRows' claim
// discipline. Carcasses clear through the provider's foreign-mount contract,
// exactly like mountFuse.
func (s *Server) remountCarriedDirs(ctx context.Context, rowDirs map[string]bool, carry map[string]string) {
	for dir, base := range carry {
		if ctx.Err() != nil {
			return
		}
		if rowDirs[dir] || s.holder.ready(dir) {
			continue
		}
		prov := s.overlayFor(overlay.KindFuse)
		if prov.Kind() != overlay.KindFuse {
			return
		}
		err := prov.Setup(base, dir)
		if errors.Is(err, mountd.ErrForeignMount) || errors.Is(err, mountd.ErrBaseMismatch) {
			if terr := prov.Teardown(base, dir); terr != nil {
				s.log.Printf("pre-row mount %s: clear carcass: %v", dir, terr)
				continue
			}
			err = prov.Setup(base, dir)
		}
		if err != nil {
			s.log.Printf("remount pre-row mount %s: %v", dir, err)
			continue
		}
		s.holder.noteMounted(dir)
		s.log.Printf("remounted pre-row mount %s (in-flight add)", dir)
	}
}

// fuseAccounts lists the fuse-kind account rows.
func (s *Server) fuseAccounts() ([]store.Account, error) {
	accts, err := s.m.Store.ListAccounts()
	if err != nil {
		return nil, err
	}
	fuse := make([]store.Account, 0, len(accts))
	for _, a := range accts {
		if a.OverlayKind == string(overlay.KindFuse) {
			fuse = append(fuse, a)
		}
	}
	return fuse, nil
}

// accountIDs extracts the row ids, in order.
func accountIDs(accts []store.Account) []int {
	ids := make([]int, len(accts))
	for i, a := range accts {
		ids[i] = a.ID
	}
	return ids
}

// sameAccountIDs reports whether two account lists name the same ids in the
// same order (both sides come from the same ordered store query).
func sameAccountIDs(a, b []store.Account) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID {
			return false
		}
	}
	return true
}

// canSpawnHolder reports whether this daemon can spawn a mount holder at all:
// an injected seam vouches for itself; the real spawn needs the fuse build.
func (s *Server) canSpawnHolder() bool {
	return s.spawnHolder != nil || overlay.FuseBuilt()
}

// spawn starts a mount holder on the daemon's holder socket through the seam;
// nil means pool.SpawnHolder.
func (s *Server) spawn() error {
	if s.spawnHolder != nil {
		return s.spawnHolder(s.holderSocket, s.holderLog, mountd.DefaultSpawnTimeout)
	}
	return pool.SpawnHolder(s.holderSocket, s.holderLog, mountd.DefaultSpawnTimeout)
}

// spawnIfServing is the proc.Spawn.Override: it spawns a holder only when this
// build CAN host one and there is something for it to serve — at least one fuse
// row, or a mount this holder previously served (orphaned carcasses warrant a
// respawn even with no fuse row left). A build that cannot host fuse, or an empty
// pool with no history, refuses the spawn with an ErrSkipSpawn-wrapping sentinel
// that proc treats as a benign no-op (no backoff, no crash-loop breaker, no
// surfaced spawn error), never actually starting a child.
func (s *Server) spawnIfServing() error {
	if !s.canSpawnHolder() {
		// A pure-Go build carrying inherited fuse rows cannot host a holder: there
		// is nothing it can do, so skip the spawn benignly rather than surface a
		// spawn failure or eventually retreat the rows on the breaker.
		return fmt.Errorf("this build cannot host a mount holder: %w", proc.ErrSkipSpawn)
	}
	fuse, err := s.fuseAccounts()
	if err != nil {
		return fmt.Errorf("spawn mount holder: list accounts: %w", err)
	}
	if len(fuse) == 0 && !s.holder.hadMounts() {
		return errNothingToServe
	}
	return s.spawn()
}

// errNothingToServe is spawnIfServing's refusal when the pool has no fuse row and
// no mount history — there is nothing for a holder to serve. It wraps
// proc.ErrSkipSpawn so the Supervisor treats it as a benign no-op (no backoff, no
// crash-loop breaker advance, no surfaced spawn error), never starting a child.
var errNothingToServe = fmt.Errorf("no fuse rows or mount history; nothing for a mount holder to serve: %w", proc.ErrSkipSpawn)

// killPeerPid force-terminates the process holding socket through the seam,
// but only when its peer pid matches wantPID; nil means mountd.Client.KillPeer
// (peer credentials resolved and matched in one dial, never a name match).
func (s *Server) killPeerPid(socket string, wantPID int) (int, error) {
	if s.killHolderPeer != nil {
		return s.killHolderPeer(socket, wantPID)
	}
	return mountd.NewClient(socket).KillPeer(wantPID)
}

// peerPIDOf resolves the pid holding socket through the seam; nil means
// mountd.Client.PeerPID.
func (s *Server) peerPIDOf(socket string) (int, error) {
	if s.peerPID != nil {
		return s.peerPID(socket)
	}
	return mountd.NewClient(socket).PeerPID()
}

// peerAliveOn reports whether socket currently has a live peer, through the
// seam; nil means mountd.Client.PeerAlive. It is the reviveHolder gate that
// distinguishes a genuinely dead holder (no peer → revive) from one that is
// alive but unresponsive (peer present → defer, never force-unmount).
func (s *Server) peerAliveOn(socket string) bool {
	if s.peerAlive != nil {
		return s.peerAlive(socket)
	}
	return mountd.NewClient(socket).PeerAlive()
}
