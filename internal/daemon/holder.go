package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// defaultHealInterval is the steady-state heal cadence, far under the
// scheduler's ~3.5-minute poll. (Per-row remount backoff/breaker constants live
// in policies.go — the self-heal policy substrate.)
const defaultHealInterval = 10 * time.Second

// forceUnmount force-unmounts a fuse carcass without routing through the
// (possibly dead) holder; test seam.
var forceUnmount = overlay.ForceUnmount

// reapOrphanedServers SIGKILLs the go-nfsv4 servers a crashed holder orphaned
// under the given roots — each killed only after its mountpoint is independently
// confirmed a carcass, so a healthy server is never touched. Test seam so unit
// tests never signal a real process.
var reapOrphanedServers = fusekit.ReapOrphanedServers

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

// unmountIdle is the sole per-dir force-unmount chokepoint: it force-unmounts dir
// only after a fresh liveSessionGate proves nothing rides it. busy=true (with the
// live-session count n) means dir was left mounted — a live session, or a scan
// failure treated as busy (fail-closed) — since force-unmounting a busy NFS
// mirror panics the kernel (nfs_vinvalbuf2). busy=false means dir was idle and
// the force-unmount was attempted; err carries its result. Callers own their
// context logging off (busy, n, err).
//
// A shared native mux root is NOT force-unmounted here: a per-dir scan cannot see
// a session on a subtree, so that path keeps its own pool-wide gate
// (sweepMuxRootIdle → anyLiveFuseSession) before its force-unmount.
func (s *Server) unmountIdle(ctx context.Context, dir string) (busy bool, n int, err error) {
	if busy, n = s.liveSessionGate(ctx, dir); busy {
		return true, n, nil
	}
	return false, n, forceUnmount(dir)
}

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
			// outpaces the poll's refresh), then the fuse/FP/strand self-heal
			// families, then content-source health. FP rows are probed and healed up
			// the recovery ladder here too — never through fuse.remount, since FP is
			// a third backend, not a fuse mount. Strand rows carrying crash-window
			// wreckage converge here, promoting the startup-only reconcile onto the
			// ticker.
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
				case healDeferredBusy, healDeferredUnmitigated:
					// attemptNeutral, no hazard strike: a busy mount must never reach
					// the wedged breaker (and the lane reset disarms a mid-countdown
					// breaker), and an unmitigated holder is a benign wait for the cask
					// upgrade — the row keeps deferring until `brew upgrade --cask
					// fusekit-holder` lands, never retreating to symlink.
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
	// A per-account teardown breaks any live session bound to the mirror, so gate
	// on it. A legacy per-dir mount (a real mountpoint) also needs a kernel
	// force-unmount, and force-unmounting a busy NFS mirror panics — so it comes
	// down only when idle. A mux subtree (a bridge symlink) is detached kernel-free
	// by ConvertOverlay's mux Teardown, but the detach still yields EIO/ENOENT on a
	// live session's open files, so it keeps the same gate — session-breaking, not
	// kernel-hazardous.
	// A legacy per-dir mount comes down through the idle chokepoint; a mux bridge
	// (mutually exclusive with a real mountpoint) is detached kernel-free by
	// ConvertOverlay below but still needs the same idle gate, since the detach
	// breaks a live session's open files.
	legacy := overlayMounted(fresh.ConfigDir)
	if legacy {
		busy, n, err := s.unmountIdle(ctx, fresh.ConfigDir)
		if busy {
			s.log.Printf("acct-%02d symlink retreat deferred: %d live session(s) on %s — leaving fuse", a.ID, n, fresh.ConfigDir)
			return false
		}
		if err != nil {
			// Do not proceed into ConvertOverlay: its Teardown would re-spawn the
			// holder being retreated from. A dir the kernel refuses to unmount
			// cannot be safely symlinked anyway.
			s.log.Printf("acct-%02d symlink retreat: force-unmount %s wedged; leaving fuse: %v", a.ID, fresh.ConfigDir, err)
			return false
		}
	} else if pool.IsBridgeSymlink(fresh.ConfigDir) {
		if busy, n := s.liveSessionGate(ctx, fresh.ConfigDir); busy {
			s.log.Printf("acct-%02d symlink retreat deferred: %d live session(s) on %s — leaving fuse", a.ID, n, fresh.ConfigDir)
			return false
		}
	}
	if _, err := s.m.ConvertOverlay(ctx, fresh, fkoverlay.BackendSymlink); err != nil {
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
// poll claim. A legacy per-dir mount retreats to symlink with a per-row
// force-unmount. For a mux subtree the choice is between native-level recovery and
// an isolated per-row retreat: a wedged native mount fails every subtree, so when
// siblings also fail (nativeMountWedged) the shared root is force-unmounted
// (pool-wide-idle-gated) and the heal loop reassembles the pool; a lone wedged
// subtree (siblings healthy) retreats just this row to symlink with a kernel-free
// detach.
func (s *Server) escalateWedgedRow(ctx context.Context, a store.Account) {
	if overlayMounted(a.ConfigDir) {
		announce := fmt.Sprintf("acct-%02d legacy fuse mount never recovered after %d consecutive attempts; force-unmounting and falling back to symlink — relaunch any sessions on it",
			a.ID, remountBreakerThreshold)
		if s.escalateRowToSymlink(ctx, a, announce) {
			s.log.Printf("acct-%02d fell back to symlink after exhausting fuse remount attempts", a.ID)
		}
		return
	}
	if s.nativeMountWedged(a) {
		s.recoverNativeMount(ctx)
		return
	}
	announce := fmt.Sprintf("acct-%02d mux subtree never recovered after %d consecutive attempts; detaching and falling back to symlink — relaunch any sessions on it",
		a.ID, remountBreakerThreshold)
	if s.escalateRowToSymlink(ctx, a, announce) {
		s.log.Printf("acct-%02d fell back to symlink after exhausting fuse remount attempts", a.ID)
	}
}

// nativeMountWedged reports whether a's wedge is at the shared native mount, not
// one subtree: a wedged native mount fails EVERY subtree, so if any sibling fuse
// row is also deep-wedged or held-dead the fault is pool-wide. A lone wedged
// subtree (siblings healthy, or no siblings) is isolated and retreats on its own.
func (s *Server) nativeMountWedged(a store.Account) bool {
	fuse, err := s.fuseAccounts()
	if err != nil {
		// Cannot enumerate siblings: prefer the milder per-row retreat over
		// force-unmounting the whole pool on a guess.
		s.log.Printf("native-wedge check: list accounts: %v", err)
		return false
	}
	for _, sib := range fuse {
		if sib.ID == a.ID {
			continue
		}
		if s.holder.deepWedged(sib.ConfigDir) {
			return true
		}
		if dead, _ := s.holder.heldDead(sib.ConfigDir); dead {
			return true
		}
	}
	return false
}

// recoverNativeMount force-unmounts the shared native mux mount to clear a
// pool-wide wedge. After the unmount each row's next idempotent Mount RPC finds
// the root dead, re-mounts once, and re-attaches — the heal loop self-reassembles
// the pool.
func (s *Server) recoverNativeMount(ctx context.Context) {
	fuse, err := s.fuseAccounts()
	if err != nil {
		s.log.Printf("native mux recovery: list accounts: %v", err)
		return
	}
	if s.sweepMuxRootIdle(ctx, fuse) {
		s.log.Printf("force-unmounted the pool-wide wedged native mux mount %s; the heal loop will re-mount and re-attach every account", pool.MuxRootDir())
	}
}

// sweepMuxRootIdle force-unmounts the shared native mux root under two gates that
// close their races the way a per-account convert closes its own: the pool-wide
// reservation gate (beginNativeRecovery refuses if any fuse account is reserved
// and blocks new reservations for the span, so a select on a healthy sibling can
// never land between the session scan and the force-unmount) and the pool-idle
// session gate (a bound session — including a pre-FinalizeAdd `ccp add` on a bridge
// symlink — defers it, since force-unmounting a busy NFS mirror panics the kernel).
// Returns whether the root came down; callers establish that the mounted root is a
// carcass (dead or foreign holder) or pool-wide wedge. After the unmount noteUnmounted
// drops every stale vouch so no select trusts the torn-down mount before the re-mount.
func (s *Server) sweepMuxRootIdle(ctx context.Context, fuse []store.Account) bool {
	if !s.cl.ownPool(fuse) {
		s.log.Printf("native mux force-unmount deferred: a select holds a reservation on a fuse account")
		return false
	}
	defer s.cl.disownPool()
	if busy, dir, n := s.anyLiveFuseSession(ctx, fuse); busy {
		s.log.Printf("native mux mount left under %d live session(s) on %s — deferring force-unmount (drops every pooled session); relaunch them", n, dir)
		return false
	}
	root := pool.MuxRootDir()
	if !overlayMounted(root) {
		return false
	}
	if err := forceUnmount(root); err != nil {
		s.log.Printf("native mux force-unmount %s: %v", root, err)
		return false
	}
	for _, a := range fuse {
		s.holder.noteUnmounted(a.ConfigDir)
	}
	return true
}

// holderOwnsMuxRoot reports whether a reachable holder actually serves the shared
// mux root — a live peer AND at least one listed subtree row whose MuxRoot is ours.
// A dead peer, or a live-but-empty holder freshly respawned over a root a dead
// predecessor left mounted, does NOT own the root, so that carcass reads unowned
// and the orphan sweep clears it. A List error cannot disprove ownership, so it
// reads owned — the cautious direction (never force-unmount a root a live holder
// may still serve).
func (s *Server) holderOwnsMuxRoot() bool {
	if !s.peerAliveOn(s.holderSocket) {
		return false
	}
	mounts, err := s.holderClient().List()
	if err != nil {
		return true
	}
	root := pool.MuxRootDir()
	for _, mi := range mounts {
		if mi.MuxRoot == root {
			return true
		}
	}
	return false
}

// anyLiveFuseSession reports whether any fuse account backs a live claude session,
// via one bounded ps-env scan. A scan failure cannot rule a session out, so it
// reads busy — the cautious direction for a pool-wide force-unmount. Busy is
// derived from the scan itself, not just the row list: a pre-FinalizeAdd `ccp add`
// session has no account row yet, but its CLAUDE_CONFIG_DIR is already a bridge
// symlink into the shared mux root, so force-unmounting the root under it is the
// same kernel-panic hazard — count any scanned session on such a bridge.
func (s *Server) anyLiveFuseSession(ctx context.Context, fuse []store.Account) (busy bool, dir string, n int) {
	sessions, err := s.scan(ctx)
	if err != nil {
		s.log.Printf("pool-wide session gate: scan failed: %v; treating as busy (refusing to force-unmount the shared mount)", err)
		return true, "", 0
	}
	for _, a := range fuse {
		if c := procscan.CountByConfigDir(sessions, a.ConfigDir); c > 0 {
			return true, a.ConfigDir, c
		}
	}
	accountsPrefix := pool.AccountsDir() + string(os.PathSeparator)
	for _, se := range sessions {
		if strings.HasPrefix(se.ConfigDir, accountsPrefix) && pool.IsBridgeSymlink(se.ConfigDir) {
			return true, se.ConfigDir, procscan.CountByConfigDir(sessions, se.ConfigDir)
		}
	}
	return false, "", 0
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
