package daemon

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// defaultHealInterval is the steady-state heal cadence, far under the
// scheduler's ~3.5-minute poll. (Per-row remount backoff/breaker constants live
// in policies.go — the self-heal policy substrate.)
const defaultHealInterval = 10 * time.Second

// fuseRemountPolicy is the fuse-row remount policy: one shared backoff clock
// under two mutually-resetting breaker lanes — hazard (strikes, escalating via
// escalateWedgedRow) and TCC (altHits, escalating via escalateTCCBlockedRow) —
// so an alternating TCC/wedge row trips neither. Rows are keyed by account
// ConfigDir.
var fuseRemountPolicy = policies["fuse.remount"]

// remountClear drops dir's fuse.remount row: the mirror recovered, left the
// fuse backend, or finished its retreat.
func (s *Server) remountClear(dir string) {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	s.led.clear(fuseRemountPolicy, dir)
}

// remountAttempt books one remount attempt for dir on the selected breaker
// lane, returning whether a breaker has tripped. Benign deferrals (busy,
// unmitigated, a failed row re-read) book attemptNeutral: the shared clock
// keeps backing off while both lanes reset, so no breaker is reachable.
func (s *Server) remountAttempt(dir string, kind attemptKind) bool {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	return s.led.attempt(fuseRemountPolicy, dir, kind, time.Now())
}

// remountBackoffElapsed gates the next remount attempt on the backoff window
// alone, never the breakers — a breaker whose escalation was deferred (claim
// refusal, live sessions) stays armed to re-fire on the next elapsed window.
func (s *Server) remountBackoffElapsed(dir string, now time.Time) bool {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	return s.led.backoffElapsed(fuseRemountPolicy, dir, now)
}

// remountPrune drops fuse.remount rows keep rejects — accounts that left the
// fuse set while the listing aged.
func (s *Server) remountPrune(keep func(dir string) bool) {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	s.led.prune(fuseRemountPolicy, keep)
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
			// The heal tick body is the healTable: holder cache first (the ticker
			// outpaces the poll's refresh), then the companion-app ensure, the
			// fuse/FP/strand self-heal families, the orphaned-domain reap, and
			// content-source health. FP rows are probed and healed up the recovery
			// ladder here too — never through fuse.remount, since FP is a third
			// backend, not a fuse mount. Strand rows carrying crash-window wreckage
			// converge here, promoting the startup-only reconcile onto the ticker.
			s.runTable(ctx, s.newTick(ctx), healTable)
		}
	}
}

// logContentHealth logs a content-source health transition. The content.health
// row gates on s.contentSource != nil, so this never runs on a nil source.
func (s *Server) logContentHealth() {
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
// for, under per-row backoff and the scheduler's poll-claim discipline. Session
// liveness comes from the shared tick (a failed scan reads as zero sessions,
// which skips probing — the cautious direction).
func (s *Server) retryUnvouchedFuseRows(ctx context.Context, t *tick) {
	now := time.Now()
	inPass := map[string]bool{}
	completed := s.forEach(ctx, fuseRows, false, func(a store.Account) {
		inPass[a.ConfigDir] = true
		// Deep-probe in-use vouched mounts only — an idle mount's wedge is
		// caught at select time (handleSelect). The probe is a bounded read,
		// so it needs no poll claim.
		if s.holder.shallowLive(a.ConfigDir) &&
			t.sessionCount(a.ConfigDir) > 0 &&
			s.holder.dueForDeepProbe(a.ConfigDir, now, deepProbeInterval) &&
			!s.cl.held(a.ID) {
			if msg := s.holder.recordDeep(a.ConfigDir, deepProbe(a.ConfigDir)); msg != "" {
				s.log.Printf("%s", msg)
			}
		}
		if s.holder.ready(a.ConfigDir) {
			s.remountClear(a.ConfigDir)
			s.holder.resetShallowDead(a.ConfigDir)
			return
		}
		if !s.remountBackoffElapsed(a.ConfigDir, now) {
			return
		}
		// deferShallowDead sits after the backoff gate: a backed-off row must
		// not spend a Health probe nor re-arm its shallow-dead strikes.
		if dead, wedged := s.holder.heldDead(a.ConfigDir); dead && !wedged && s.deferShallowDead(a) {
			return
		}
		s.claimed(a, func() {
			fresh, err := s.m.Store.GetAccount(a.ID)
			switch {
			case err != nil:
				s.log.Printf("acct-%02d re-read row before remount: %v", a.ID, err)
				// Not a mount hazard: back off without a breaker strike.
				s.remountAttempt(a.ConfigDir, attemptNeutral)
			case !fuseBackedRow(fresh.OverlayKind):
				// Converted while the listing aged; no ledger for a non-fuse row.
				s.remountClear(a.ConfigDir)
			default:
				if dead, wedged := s.holder.heldDead(a.ConfigDir); dead {
					desc := "dead mirror (fails reads outright; unmounted out of band or its fuse worker died?)"
					if wedged {
						desc = "wedged mirror (serves metadata but hangs reads)"
					}
					n := t.sessionCount(a.ConfigDir)
					relaunch := ""
					if n > 0 {
						relaunch = " — relaunch them"
					}
					s.log.Printf("acct-%02d %s; %d live session(s) on it%s", a.ID, desc, n, relaunch)
				}
				switch s.healFuse(ctx, fresh) {
				case healMounted:
					s.remountClear(a.ConfigDir)
				case healDeferredBusy, healDeferredUnsupported:
					// attemptNeutral, no hazard strike: a busy mirror (a live session
					// holds its lease, or a graceful unmount answered EBUSY) must never
					// reach the wedged breaker, and neither must a benign wait for a cask
					// upgrade — both keep deferring, never retreating to symlink.
					s.remountAttempt(a.ConfigDir, attemptNeutral)
				case healTCCBlocked:
					if s.remountAttempt(a.ConfigDir, attemptAlt) {
						s.escalateTCCBlockedRow(ctx, fresh)
					}
				default:
					// healRetry/healFallback: hazard outcomes.
					if s.remountAttempt(a.ConfigDir, attemptPrimary) {
						s.escalateWedgedRow(ctx, fresh)
					}
				}
			}
		})
	})
	// Prune absent rows only after a full pass — a list-accounts failure or a
	// mid-iteration ctx cancellation leaves inPass partial, and the old loop
	// returned before pruning in both cases (preserving every row's ledger state).
	if completed {
		s.remountPrune(func(dir string) bool { return inPass[dir] })
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

// convertRowToSymlink is the shared fuse→symlink retreat primitive; the caller
// holds a convert claim. Returns whether the row ended up off fuse.
func (s *Server) convertRowToSymlink(ctx context.Context, a store.Account, announce string) bool {
	fresh, err := s.m.Store.GetAccount(a.ID)
	switch {
	case err != nil:
		s.log.Printf("acct-%02d symlink retreat: re-read row: %v", a.ID, err)
		return false
	case !fuseBackedRow(fresh.OverlayKind):
		s.remountClear(fresh.ConfigDir) // converted in the claim gap
		return true
	}
	s.log.Printf("%s", announce)
	// ConvertOverlay's teardown routes through the shared holder's lease-ladder
	// unmount: a held session lease answers ErrBusy (the launcher that leased the
	// dir still owns it), so the retreat defers instead of breaking a live session
	// — and a busy graceful unmount answers ErrBusy too, never a force. No
	// consumer-side session scan or force-unmount.
	if _, err := s.m.ConvertOverlay(ctx, fresh, fkoverlay.BackendSymlink); err != nil {
		if errors.Is(err, mountd.ErrBusy) {
			s.log.Printf("acct-%02d symlink retreat deferred: %s is busy (a live session holds its lease) — leaving fuse", a.ID, fresh.ConfigDir)
			return false
		}
		s.log.Printf("acct-%02d symlink retreat: convert to symlink: %v", a.ID, err)
		return false
	}
	s.holder.noteUnmounted(fresh.ConfigDir)
	s.remountClear(fresh.ConfigDir)
	return true
}

// escalateRowToSymlink is the poll-held entry to convertRowToSymlink; a claim
// refusal (pending select) leaves the breaker armed to re-fire.
func (s *Server) escalateRowToSymlink(ctx context.Context, a store.Account, announce string) bool {
	if !s.cl.ownHeld(a.ID) {
		s.log.Printf("acct-%02d symlink retreat deferred: reserved by a pending select", a.ID)
		return false
	}
	defer s.cl.disownConvert(a.ID)
	return s.convertRowToSymlink(ctx, a, announce)
}

// escalateWedgedRow handles a row that tripped remountBreakerThreshold after its
// per-poll detach/re-attach cycles (mountFuse) failed. Caller holds the account's
// poll claim. It retreats the row to symlink; the pool-wide native-mount recovery
// a wedge might need is the holder's job now (lease-gated, surfaced via health
// WedgedDirs), so the daemon never force-unmounts the shared root itself.
func (s *Server) escalateWedgedRow(ctx context.Context, a store.Account) {
	shape := "mux subtree"
	if overlayMounted(a.ConfigDir) {
		shape = "legacy fuse mount"
	}
	announce := fmt.Sprintf("acct-%02d %s never recovered after %d consecutive attempts; falling back to symlink — relaunch any sessions on it",
		a.ID, shape, remountBreakerThreshold)
	if s.escalateRowToSymlink(ctx, a, announce) {
		s.log.Printf("acct-%02d fell back to symlink after exhausting fuse remount attempts", a.ID)
	}
}

// escalateTCCBlockedRow retreats a row that tripped tccBreakerThreshold to
// symlink. Caller holds the account's poll claim.
func (s *Server) escalateTCCBlockedRow(ctx context.Context, a store.Account) {
	announce := fmt.Sprintf("acct-%02d macOS volume-access grant never landed after %d attempts; falling back to symlink — `ccp migrate --to fuse` re-promotes once fuse-t can mount here",
		a.ID, tccBreakerThreshold)
	if s.escalateRowToSymlink(ctx, a, announce) {
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
		if !s.cl.own(a.ID) {
			s.log.Printf("acct-%02d symlink retreat deferred: reserved, polling, or converting", a.ID)
			continue
		}
		announce := fmt.Sprintf("acct-%02d %s; falling back to symlink — relaunch any sessions on it", a.ID, reason)
		if s.convertRowToSymlink(ctx, a, announce) {
			s.log.Printf("acct-%02d fell back to symlink (%s)", a.ID, reason)
		}
		s.cl.disownConvert(a.ID)
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
