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
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/proc"
)

const (
	// defaultHealInterval is the steady-state heal cadence: a fuse row the shared
	// holder cannot vouch for is re-registered (or retreated to symlink) in ~10s
	// instead of waiting for the scheduler's ~3.5-minute poll.
	defaultHealInterval = 10 * time.Second

	// remountBackoffBase/remountBackoffCap bound the per-row remount backoff
	// for fuse rows the holder cannot vouch for (see retryUnvouchedFuseRows):
	// consecutive failed heals double the wait 10s → 20s → … capped at 2
	// minutes. The cap sits deliberately under the 180s scheduler period — the
	// heal must never be the slower recovery path.
	remountBackoffBase = 10 * time.Second
	remountBackoffCap  = 2 * time.Minute
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

// forceUnmount force-unmounts an orphaned fuse carcass directly, without
// routing through the (possibly dead) holder; seamed so tests assert the calls
// without real mounts. Production: overlay.ForceUnmount (bounded).
var forceUnmount = overlay.ForceUnmount

// rowRetryState is one fuse row's remount-backoff bookkeeping in s.rowRetry,
// driving the steady-state heal loop's per-row backoff and circuit breakers.
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

// healFuseRows runs the steady-state heal loop on a ticker until ctx is
// cancelled. Each pass refreshes the shared-holder cache, then re-registers (or
// retreats to symlink) every fuse row the holder cannot vouch for. cc-pool no
// longer owns the holder's lifecycle — the signed fusekit-holder cask is
// launchd-managed and shared across tenants, and the provider's Setup lazily
// (re)spawns it via the cask ExecPath — so there is no holder supervisor here,
// only the per-account mount-health net (backoff, breakers, deep-probe wedge
// detection, symlink-retreat). Started after the startup reconcile so it never
// races the initial mounts.
func (s *Server) healFuseRows(ctx context.Context) {
	interval := s.healInterval
	if interval <= 0 {
		interval = defaultHealInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Refresh first: the heal ticker runs far faster than the scheduler
			// poll, so without a refresh it would act on a stale cache.
			s.holder.refresh(s.holderClient())
			s.retryUnvouchedFuseRows(ctx)
			s.logContentHealth()
		}
	}
}

// logContentHealth surfaces the content source's last merged/served-read and base
// write-through failures (the in-process mirror's healthErr, now over the bridge),
// logging only on a change so a persistent failure is not repeated every tick.
func (s *Server) logContentHealth() {
	if s.contentSource == nil {
		return
	}
	msg := ""
	if err := s.contentSource.HealthErrors(); err != nil {
		msg = err.Error()
	}
	if msg != s.lastContentHealth {
		s.lastContentHealth = msg
		if msg != "" {
			s.log.Printf("content source health: %s", msg)
		}
	}
}

// retryUnvouchedFuseRows is the steady-state heal loop body: each fuse row the
// holder cache cannot vouch for — a remount that failed earlier, a TCC-blocked
// row waiting on its grant, or a mirror the holder reports present-but-dead,
// deep-wedged or plain dead — is retried under per-account exponential backoff
// (remountBackoffBase doubling to remountBackoffCap, deliberately under the
// 180s scheduler period). Each attempt runs under the scheduler's poll-claim
// discipline: a claimed account is skipped WITHOUT advancing its backoff —
// skipping is not failing — and its row is re-read under the claim so a row
// converted in the gap is left alone. A successful heal (by this loop or
// anyone else — ready() covers mounts established by any path) deletes the
// row's ledger entry; rows that left the fuse set are pruned after the pass.
func (s *Server) retryUnvouchedFuseRows(ctx context.Context) {
	fuse, err := s.fuseAccounts()
	if err != nil {
		s.log.Printf("heal loop: list accounts: %v", err)
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
			s.log.Printf("heal loop: session scan: %v", serr)
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
			s.holder.resetShallowDead(a.ConfigDir)
			continue
		}
		if now.Before(s.rowRetry[a.ID].retryAt) {
			continue
		}
		// Corroborate a shallow PLAIN-dead verdict before remounting: the holder
		// computes its List Live bool with the same bounded 2s stat that
		// false-negatives under load, so a present-but-!live mirror (NOT a deep
		// wedge — the periodic probe above already debounces that) may be merely
		// slow, not dead. deferShallowDead re-probes with our own bounded Health
		// and waits out a saturation blip, so one slow stat never tears down a
		// mirror serving live sessions. A definitive dead reading is not deferred.
		// It sits AFTER the backoff gate so a backed-off row never spends a ~2s
		// Health probe (nor re-arms its strike count) while waiting out a retry.
		if dead, wedged := s.holder.heldDead(a.ConfigDir); dead && !wedged && s.deferShallowDead(a) {
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
		case !fuseBackedRow(fresh.OverlayKind):
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

// deferShallowDead corroborates a holder-reported shallow plain-dead mirror
// before the heal loop remounts it, and reports whether to DEFER the remount
// this pass. It re-probes with the daemon's own bounded Health (zero RPC; same
// kernel as the holder but a distinct process), and splits the verdict the
// holder's single Live bool cannot:
//   - a live reading (nil) means the holder's List Live=false was a transient
//     under-load false negative — clear the strike and defer; the next List
//     re-vouches the mirror.
//   - a liveness TIMEOUT while the holder PROCESS is alive means slow under
//     load, not dead — record a strike and defer until shallowDeadStrikes
//     consecutive passes agree, then proceed.
//   - a definitive dead reading (no longer a mountpoint / base invisible), or a
//     timeout with no live holder peer, means proceed now: a genuinely dead
//     single mirror is healed at once, exactly as before this gate, and a dead
//     holder is left to the provider's lazy (re)spawn on the next mount, not
//     handled here.
func (s *Server) deferShallowDead(a store.Account) bool {
	prov := s.overlayForRow(a)
	if prov == nil {
		// Wrong-backend injected fake (or a nil resolution): cannot corroborate,
		// so proceed with the holder's verdict rather than deferring forever.
		s.holder.resetShallowDead(a.ConfigDir)
		return false
	}
	switch err := prov.Health(pool.ClaudeDir(), a.ConfigDir); {
	case err == nil:
		s.holder.resetShallowDead(a.ConfigDir)
		return true
	case errors.Is(err, overlay.ErrLivenessTimeout) && s.peerAliveOn(s.holderSocket):
		if s.holder.recordShallowDead(a.ConfigDir) < shallowDeadStrikes {
			return true
		}
		s.holder.resetShallowDead(a.ConfigDir)
		return false
	default:
		s.holder.resetShallowDead(a.ConfigDir)
		return false
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
// hazard/dead-end path shares (the wedged breaker, the TCC grace breaker, and
// the startup capability gate). The caller holds
// a convert claim (beginConvert standalone, or beginConvertUnderPoll under a
// held poll). It re-reads the row (a conversion that landed in the claim gap is
// left alone), force-unmounts any standing mount — ABORTING the retreat if the
// unmount wedges, because ConvertOverlay's Teardown would then re-spawn the very
// holder being retreated from (the wedged-carcass churn the kill-9 incident
// exposed) — converts the row, drops the holder-cache vouch and any retry-ledger
// entry, and logs announce. Returns whether the row ended up off fuse. The
// idle/live-session gate is deliberately skipped, distinct from the idle-gated
// fallbackToSymlink: every caller is remediating a hazard or a dead-end (a
// never-recovering wedge, a grant that will not land, a machine that cannot
// fuse), where relaunch — not deferral — is the fix.
func (s *Server) convertRowToSymlink(a store.Account, announce string) bool {
	fresh, err := s.m.Store.GetAccount(a.ID)
	switch {
	case err != nil:
		s.log.Printf("acct-%02d symlink retreat: re-read row: %v", a.ID, err)
		return false
	case !fuseBackedRow(fresh.OverlayKind):
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
	if _, err := s.m.ConvertOverlay(fresh, fkoverlay.BackendSymlink); err != nil {
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
// TCC-blocked heals): the macOS volume-access grant never landed within the
// grace window, so the daemon stops waiting and retreats the row to symlink so
// the account is usable again. It clears the stale process-wide TCC guidance
// only on a real conversion (tccErr is one string — clearing it in the shared
// body, or on a deferred/failed conversion, would wipe a concurrent wedged row's
// still-valid guidance). Caller holds the account's poll claim.
func (s *Server) escalateTCCBlockedRow(a store.Account) {
	announce := fmt.Sprintf("acct-%02d macOS volume-access grant never landed after %d attempts; falling back to symlink — `ccp migrate --to fuse` re-promotes once fuse-t can mount here",
		a.ID, tccBreakerThreshold)
	if s.escalateRowToSymlink(a, announce) {
		s.holder.recordTCC("", "")
		s.log.Printf("acct-%02d fell back to symlink after the macOS volume-access grant did not land", a.ID)
	}
}

// retreatAllFuseRows retreats every fuse account to the always-available symlink
// overlay, each under its own standalone convert claim (beginConvert — the
// callers hold no poll claim) so a select, a scheduler poll, or another
// conversion cannot interleave; a row that cannot be claimed or fails to convert
// is left for a later pass. reason names the pool-wide condition (logged per
// row). It is the standalone, whole-pool analog of escalateRowToSymlink: the
// startup capability gate (reconcileOverlays) uses it when fuse is unusable for
// the WHOLE pool, not just one row. Per-row conversion (including the
// wedged-unmount abort) is convertRowToSymlink, shared with the breakers.
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

// fuseAccounts lists the fuse-kind account rows.
func (s *Server) fuseAccounts() ([]store.Account, error) {
	accts, err := s.m.Store.ListAccounts()
	if err != nil {
		return nil, err
	}
	fuse := make([]store.Account, 0, len(accts))
	for _, a := range accts {
		if fuseBackedRow(a.OverlayKind) {
			fuse = append(fuse, a)
		}
	}
	return fuse, nil
}

// canSpawnHolder reports whether this machine can host fuse mounts via the shared
// holder — the signed fusekit-holder cask is installed, or a holder is already
// serving the shared socket. It gates the startup capability probe
// (fuseHardUnavailable). The holder's actual spawn is the provider's job
// (RemoteFuseProvider.Setup → mountd.Spawn via the cask ExecPath); cc-pool no
// longer spawns it itself.
func (s *Server) canSpawnHolder() bool {
	return pool.CanHostFuse()
}

// peerAliveOn reports whether the shared holder socket currently has a live peer,
// through the test seam; nil means mountd.Client.PeerAlive. deferShallowDead uses
// it to distinguish a saturated-but-alive holder (a slow liveness stat under load
// → wait it out) from a genuinely gone one.
func (s *Server) peerAliveOn(socket string) bool {
	if s.peerAlive != nil {
		return s.peerAlive(socket)
	}
	return mountd.NewClient(socket).PeerAlive()
}
