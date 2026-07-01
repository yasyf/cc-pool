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
	// defaultHealInterval is the steady-state heal cadence, far under the
	// scheduler's ~3.5-minute poll.
	defaultHealInterval = 10 * time.Second

	// remountBackoffBase/remountBackoffCap bound the per-row remount backoff;
	// the cap stays under the 180s scheduler period so the heal is never the
	// slower recovery path.
	remountBackoffBase = 10 * time.Second
	remountBackoffCap  = 2 * time.Minute
)

// remountBreakerThreshold is the consecutive wedged/never-recovering heal
// failures before the row stops retrying and retreats to symlink — endless
// remount churn can re-wedge the kernel (the kill-9 holder incident).
const remountBreakerThreshold = 5

// tccBreakerThreshold is the consecutive TCC-blocked heals (waiting on the
// macOS "Network Volumes" grant) before the row retreats to symlink; above
// remountBreakerThreshold since a TCC block is a benign wait, not a kernel
// hazard.
const tccBreakerThreshold = 6

// forceUnmount force-unmounts a fuse carcass without routing through the
// (possibly dead) holder; test seam.
var forceUnmount = overlay.ForceUnmount

// liveSessionGate reports whether dir backs any live claude session, via the
// bounded ps-env scan — never lsof/stat on the mount, so a wedged mirror
// cannot park it. A scan failure returns busy=true: force-unmounting a busy
// NFS mount panics the kernel (nfs_vinvalbuf2: ubc_msync failed).
func (s *Server) liveSessionGate(ctx context.Context, dir string) (busy bool, n int) {
	sessions, err := s.scan(ctx)
	if err != nil {
		s.log.Printf("live-session gate: scan for %s failed: %v; treating as busy (refusing to force-unmount)", dir, err)
		return true, 0
	}
	n = procscan.CountByConfigDir(sessions, dir)
	return n > 0, n
}

type rowRetryState struct {
	failures int
	retryAt  time.Time
	// hazard/tccBlocks count consecutive wedged / TCC-blocked heals toward
	// their breakers; each resets the other, so an alternating TCC/wedge row
	// trips neither.
	hazard    int
	tccBlocks int
}

// healFuseRows heals unvouched fuse rows until ctx is cancelled. Holder
// lifecycle belongs to the provider's Setup (the launchd-managed cask), not
// this loop; started after the startup reconcile so it never races the
// initial mounts.
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
			// Refresh first: the ticker outpaces the scheduler poll's cache refresh.
			s.holder.refresh(s.holderClient())
			s.retryUnvouchedFuseRows(ctx)
			s.logContentHealth()
		}
	}
}

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

// retryUnvouchedFuseRows retries every fuse row the holder cache cannot vouch
// for, under per-row backoff and the scheduler's poll-claim discipline.
func (s *Server) retryUnvouchedFuseRows(ctx context.Context) {
	fuse, err := s.fuseAccounts()
	if err != nil {
		s.log.Printf("heal loop: list accounts: %v", err)
		return
	}
	// A failed scan reads as zero sessions, which skips probing — the cautious
	// direction.
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
		// Deep-probe in-use vouched mounts only — an idle mount's wedge is
		// caught at select time (handleSelect). The probe is a bounded read,
		// so it needs no poll claim.
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
		// deferShallowDead sits after the backoff gate: a backed-off row must
		// not spend a Health probe nor re-arm its shallow-dead strikes.
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
			// Not a mount hazard: back off without a breaker strike.
			s.advanceRowRetry(a.ID, false)
		case !fuseBackedRow(fresh.OverlayKind):
			// Converted while the listing aged; no ledger for a non-fuse row.
			delete(s.rowRetry, a.ID)
		default:
			if dead, wedged := s.holder.heldDead(a.ConfigDir); dead {
				desc := "dead mirror (fails reads outright; unmounted out of band or its fuse worker died?)"
				if wedged {
					desc = "wedged mirror (serves metadata but hangs reads)"
				}
				n := procscan.CountByConfigDir(sessions, a.ConfigDir)
				relaunch := ""
				if n > 0 {
					relaunch = " — relaunch them"
				}
				s.log.Printf("acct-%02d %s; %d live session(s) on it%s", a.ID, desc, n, relaunch)
			}
			switch s.healFuse(ctx, fresh) {
			case healMounted:
				delete(s.rowRetry, a.ID)
			case healDeferredBusy:
				// No hazard strike: a busy mount must never reach the wedged
				// breaker, and the reset disarms a mid-countdown breaker.
				s.advanceRowRetry(a.ID, false)
			case healTCCBlocked:
				if s.advanceTCCRetry(a.ID) >= tccBreakerThreshold {
					s.escalateTCCBlockedRow(ctx, fresh)
				}
			default:
				// healRetry/healFallback: hazard outcomes.
				if s.advanceRowRetry(a.ID, true) >= remountBreakerThreshold {
					s.escalateWedgedRow(ctx, fresh)
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

// deferShallowDead reports whether to defer remounting a holder-reported
// shallow plain-dead mirror; the holder's liveness stat false-negatives under
// load, so a passing provider Health check defers the remount.
func (s *Server) deferShallowDead(a store.Account) bool {
	prov := s.overlayForRow(a)
	if prov == nil {
		// No provider to corroborate with (wrong-backend test fake): take the
		// holder's verdict rather than defer forever.
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
	st.tccBlocks = 0
	st.retryAt = time.Now().Add(proc.Backoff{Base: remountBackoffBase, Cap: remountBackoffCap}.After(st.failures))
	s.rowRetry[id] = st
	return st.hazard
}

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

// convertRowToSymlink is the shared fuse→symlink retreat primitive; the caller
// holds a convert claim. Returns whether the row ended up off fuse.
func (s *Server) convertRowToSymlink(ctx context.Context, a store.Account, announce string) bool {
	fresh, err := s.m.Store.GetAccount(a.ID)
	switch {
	case err != nil:
		s.log.Printf("acct-%02d symlink retreat: re-read row: %v", a.ID, err)
		return false
	case !fuseBackedRow(fresh.OverlayKind):
		delete(s.rowRetry, a.ID) // converted in the claim gap
		return true
	}
	s.log.Printf("%s", announce)
	if overlayMounted(fresh.ConfigDir) {
		if busy, n := s.liveSessionGate(ctx, fresh.ConfigDir); busy {
			s.log.Printf("acct-%02d symlink retreat deferred: %d live session(s) on %s — leaving fuse (force-unmounting a busy mirror panics the kernel)", a.ID, n, fresh.ConfigDir)
			return false
		}
		if err := forceUnmount(fresh.ConfigDir); err != nil {
			// Do not proceed into ConvertOverlay: its Teardown would re-spawn the
			// holder being retreated from. A dir the kernel refuses to unmount
			// cannot be safely symlinked anyway.
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

// escalateRowToSymlink is the poll-held entry to convertRowToSymlink; a claim
// refusal (pending select) leaves the breaker armed to re-fire.
func (s *Server) escalateRowToSymlink(ctx context.Context, a store.Account, announce string) bool {
	if !s.beginConvertUnderPoll(a.ID) {
		s.log.Printf("acct-%02d symlink retreat deferred: reserved by a pending select", a.ID)
		return false
	}
	defer s.endConvert(a.ID)
	return s.convertRowToSymlink(ctx, a, announce)
}

// escalateWedgedRow retreats a row that tripped remountBreakerThreshold to
// symlink. Caller holds the account's poll claim.
func (s *Server) escalateWedgedRow(ctx context.Context, a store.Account) {
	announce := fmt.Sprintf("acct-%02d fuse mount never recovered after %d consecutive attempts; force-unmounting and falling back to symlink — relaunch any sessions on it",
		a.ID, remountBreakerThreshold)
	if s.escalateRowToSymlink(ctx, a, announce) {
		s.log.Printf("acct-%02d fell back to symlink after exhausting fuse remount attempts", a.ID)
	}
}

// escalateTCCBlockedRow retreats a row that tripped tccBreakerThreshold to
// symlink; the process-wide TCC guidance is cleared only on a real conversion,
// so a deferred retreat cannot wipe a concurrent row's guidance. Caller holds
// the account's poll claim.
func (s *Server) escalateTCCBlockedRow(ctx context.Context, a store.Account) {
	announce := fmt.Sprintf("acct-%02d macOS volume-access grant never landed after %d attempts; falling back to symlink — `ccp migrate --to fuse` re-promotes once fuse-t can mount here",
		a.ID, tccBreakerThreshold)
	if s.escalateRowToSymlink(ctx, a, announce) {
		s.holder.recordTCC("", "")
		s.log.Printf("acct-%02d fell back to symlink after the macOS volume-access grant did not land", a.ID)
	}
}

// retreatAllFuseRows retreats every listed fuse account to symlink, each under
// its own standalone convert claim (callers hold no poll claim).
func (s *Server) retreatAllFuseRows(ctx context.Context, fuse []store.Account, reason string) {
	if len(fuse) == 0 {
		return
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
		if s.convertRowToSymlink(ctx, a, announce) {
			s.log.Printf("acct-%02d fell back to symlink (%s)", a.ID, reason)
		}
		s.endConvert(a.ID)
	}
}

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

// canSpawnHolder reports whether this machine can host fuse mounts; spawning
// itself is the provider's job.
func (s *Server) canSpawnHolder() bool {
	return pool.CanHostFuse()
}

// peerAliveOn reports whether socket has a live peer; s.peerAlive is a test
// seam.
func (s *Server) peerAliveOn(socket string) bool {
	if s.peerAlive != nil {
		return s.peerAlive(socket)
	}
	return mountd.NewClient(socket).PeerAlive()
}
