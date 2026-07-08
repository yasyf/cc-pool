package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/content"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/proc"
	"github.com/yasyf/fusekit/version"
)

// reservationTTL suppresses re-picking an account until its claude becomes
// visible to procscan.
const reservationTTL = 30 * time.Second

// preflightTimeout bounds a best-effort preflight refresh so shutdown is never
// blocked on a slow network refresh.
const preflightTimeout = 8 * time.Second

// defaultEvictTimeout bounds how long a starting daemon waits for a
// version-skewed holder to release the socket after being told to step down.
const defaultEvictTimeout = 5 * time.Second

// overlayMounted is a test seam over overlay.Mounted, which reads the kernel
// mount table via Getfsstat — non-blocking, so it cannot wedge on a dead
// fuse-t mirror.
var overlayMounted = overlay.Mounted

// Server is the running daemon.
type Server struct {
	m            *pool.Manager
	socket       string
	holderSocket string // mount-holder socket; tests point it at a fake holder
	syncSocket   string // synckit consumer socket; tests point it into a short temp dir
	snapshot     string // status mirror path; tests point it into a temp dir
	log          *log.Logger

	// holder caches mount-holder truth (reachability, version, per-dir mount
	// liveness); refreshed at prime, reconcile, and each poll, and lazily
	// (rate-limited) on a select cache-miss.
	holder holderState

	// evictTimeout bounds the wait for a skewed holder to release the socket.
	evictTimeout time.Duration

	// fpBridgeBackoff is the FP bridge serve-loop retry delay; zero means
	// defaultFPBridgeBackoff. Tests shrink it to pin the retry-after-bind-failure
	// path.
	fpBridgeBackoff time.Duration

	// fpBridgeWait bounds startFPBridge's synchronous wait for the FP socket to
	// accept before flagging the bind consent-pending; zero means
	// defaultFPBridgeWait. Tests shrink it.
	fpBridgeWait time.Duration

	// fpConsentPending: the FP bridge bind has not completed while the daemon is
	// alive — the app-group-container TCC consent signature. Set and cleared by
	// startFPBridge's watchdog, read by handleStatus.
	fpConsentPending atomic.Bool

	// fpBridgeHardErr latches a NON-permission serve-loop failure (a genuine
	// bind error, not the TCC-parked/denied signature), so the consent-pending
	// signal is not raised for an unrelated failure. Stored by serveFPBridge.
	fpBridgeHardErr atomic.Bool

	// triggerShutdown cancels serve's context. Set once before the accept loop
	// starts; the spawning go-statement's happens-before lets handlers read it
	// unlocked.
	triggerShutdown context.CancelFunc

	// serveCtx is serve's cancellable context, captured before any worker spawns
	// so the async holder-loss sweep (scheduleHolderLostSweep) runs under the
	// daemon's lifetime and unwinds on shutdown. Read unlocked, same as
	// triggerShutdown.
	serveCtx context.Context

	// wg tracks every daemon goroutine; serve Waits on it before Run's deferred
	// m.Close() closes the database under them.
	wg sync.WaitGroup

	// cl is the account-claim discipline: select reservations plus poll, convert,
	// and pool-wide claims (see claims.go). It owns its own mutex.
	cl *claims

	// netOutage is set when a full poll sweep found every attempted account
	// failing network-class (an outage). While set, the scheduler drops to a
	// short canary cadence (nextPollDelay) and pollOnce probes only one account
	// until connectivity returns. Scheduler-goroutine-local — no lock.
	netOutage bool
	// netProbeLogSkip throttles the per-canary "network unreachable" log while an
	// outage persists: only every netProbeLogEvery-th probe logs (the rest prove
	// liveness silently) so a multi-hour outage does not spam the log. Reset to 0
	// on each outage entry. Scheduler-goroutine-local — no lock.
	netProbeLogSkip int

	// fuseGateFn is a test seam over the migrate handler's fuse-capability
	// gate; nil means the real check (CanHostFuse + probe mount).
	fuseGateFn func() (fkoverlay.Backend, string)

	// migrateBudget bounds one migrate request's conversion work; zero means
	// defaultMigrateBudget. Tests shrink it to pin the out-of-time path.
	migrateBudget time.Duration

	// scanSessions is a test seam over procscan.Scan; nil means the real scan.
	scanSessions func(context.Context) ([]procscan.Session, error)

	// pollSpacing overrides perAccountSpacing (the inter-sample delay); zero means
	// the default. Tests shrink it so a multi-account sweep does not sleep for
	// seconds per account.
	pollSpacing time.Duration

	// startedAt stamps daemon start. The skew-replace gate requires uptime ≥
	// reservationTTL: a fresh daemon's reservation map is empty while a recent
	// select may not have exec'd its claude yet.
	startedAt time.Time

	// holderLog receives a dev-spawned holder's stdout/stderr (production
	// launches the signed cask via launchd, which owns its own log).
	holderLog string

	// healInterval is the steady-state heal-loop cadence; zero means
	// defaultHealInterval. Tests shrink it.
	healInterval time.Duration

	// peerAlive is a test seam over mountd.Client.PeerAlive — true means the
	// holder socket still has a live peer (saturated-but-alive: wait it out),
	// false means gone.
	peerAlive func(socket string) bool

	// contentSource is the content.Source the daemon's BridgeServer serves to the
	// shared holder — the merged .claude.json and injected settings.json — for
	// every cc-pool mount.
	contentSource *overlay.PoolContentSource
	// lastContentHealth dedups the content-source health log; only the heal
	// goroutine touches it — no lock.
	lastContentHealth string

	// led is the self-heal ledger store shared by every ported Server-owned
	// family: the fp.domain and fuse.remount rows, plus the auth.streak and
	// ratelimit.acct / ratelimit.pool streaks (the holder cache's fuse.deepwedge
	// / fuse.shallowdead rows live in holderState.led under its mu). ledMu is the
	// enclosing serialization the ledgers type documents: rows are touched from
	// the heal tick, the scheduler poll loop, and the select/status/repair RPC
	// handlers plus migrate/strand/convert, so every s.led access takes ledMu —
	// never held across mount/Sync/re-register/bounce or usage-fetch/refresh I/O
	// (bookkeeping in, I/O out). The streaks were scheduler-goroutine-local (no
	// lock) before the fold; sharing one map with the concurrently-touched
	// heal-family rows makes ledMu mandatory on every streak access too.
	led   *ledgers
	ledMu sync.Mutex

	// fpSynth reports whether an account's synthetic .claude.json is non-empty, so
	// the wedge detector strikes a 0-byte served file only for an account that
	// genuinely has an identity. It doubles as the "FP self-heal wired" marker: nil
	// in bare test servers, so every FP reader guards on fpEnabled.
	fpSynth func(dir string) bool

	// fpBridgeReadyFn is a test seam over the FP-bridge-up precondition for probing
	// FP domains; nil means the real check (consent settled + data socket up).
	fpBridgeReadyFn func() bool

	// sync gates preemptive refreshes to the chain holder and carries the lease
	// lifecycle; nil ⇒ host sync disabled, byte-identical to a syncless build.
	sync *syncGate

	// syncPull runs one converge pull for the invalid_grant self-heal (syncHeal);
	// nil ⇒ sync disabled.
	syncPull func(ctx context.Context) error
}

// Run is the entry point for `cc-pool daemon`. It blocks until the process
// is signalled.
func Run(ctx context.Context) error {
	m, err := pool.Open()
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	// Reclaim account-index reservations whose `ccp add` died before
	// FinalizeAdd/AbandonAdd could run; one sweep per daemon start is the TTL
	// backstop for the pending-row allocator.
	if _, err := m.Store.SweepPendingAdds(time.Now().Add(-store.PendingAddTTL)); err != nil {
		return fmt.Errorf("sweep stale pending adds: %w", err)
	}

	s := &Server{
		m:             m,
		socket:        pool.SocketPath(),
		holderSocket:  mountd.DefaultHolderSocket(),
		syncSocket:    pool.SyncSocketPath(),
		holderLog:     pool.MountHolderLogPath(),
		snapshot:      pool.StatusSnapshotPath(),
		log:           log.New(os.Stderr, "[cc-pool] ", log.LstdFlags),
		evictTimeout:  defaultEvictTimeout,
		startedAt:     time.Now(),
		contentSource: overlay.NewPoolContentSource(pool.ClaudeDir(), pool.ClaudeJSONPath()),
		cl:            newClaims(),
		led:           newLedgers(),
	}
	// The FP wedge detector strikes a 0-byte served .claude.json only when the
	// account genuinely has an identity (its synth is non-empty) — resolved through
	// the same content source the bridge serves. Wiring the seam arms FP self-heal.
	s.fpSynth = s.contentSource.SynthNonEmpty
	// The convert gate proves a freshly registered domain serves before flipping the
	// row, through the SAME bounded control-op probe the heal loop uses — never a
	// through-domain read. A NoVerdict returns non-nil, so the gate rolls back rather
	// than flip an unverified row.
	m.FPProbe = func(ctx context.Context, accountDir string) error {
		probeCtx, cancel := context.WithTimeout(ctx, fpControlProbeTimeout)
		defer cancel()
		return fpDomainProbe(probeCtx, accountDir)
	}
	// Route fusekit/overlay's conflict-resolution log through s.log. A package
	// global, so assigned once before serve spawns any worker.
	fkoverlay.ResolvedConflictLogf = s.log.Printf
	return s.serve(ctx)
}

// detectClaudeVersion runs `claude --version` (best-effort) to stamp the UA.
func detectClaudeVersion(ctx context.Context) string {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "claude", "--version").Output()
	if err != nil {
		return ""
	}
	// `claude --version` emits "2.1.166 (Claude Code)".
	fields := strings.Fields(string(out))
	if len(fields) > 0 {
		return fields[0]
	}
	return ""
}

// detectAndSetUserAgent stamps the OAuth user-agent with the detected claude
// version (the ua.detect startup row).
func (s *Server) detectAndSetUserAgent(ctx context.Context) {
	oauth.SetUserAgentVersion(detectClaudeVersion(ctx))
}

func (s *Server) serve(ctx context.Context) error {
	ln, lock, err := s.listen()
	if err != nil {
		return err
	}
	// The flock is the cross-process guarantee that only this daemon may
	// stale-check, remove, bind, or unlink the socket. It must outlive the
	// listener, so this defer is registered first (runs last).
	defer func() { _ = lock.Close() }()
	// *net.UnixListener.Close unlinks the socket by path and is NOT idempotent:
	// a second Close (the late deferred one) would delete a successor daemon's
	// freshly-bound socket, so sync.Once pins the unlink to the first close. No
	// explicit os.Remove, same reason.
	var closeOnce sync.Once
	closeListener := func() { closeOnce.Do(func() { _ = ln.Close() }) }
	defer closeListener()

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// stop cancels ctx, so it doubles as the over-the-socket shutdown trigger
	// (OpShutdown). Set before the accept loop spawns any handler.
	s.triggerShutdown = stop
	// Capture ctx and arm the holder-loss sweep before any worker refreshes the
	// holder cache: markUnhealthy fires the hook the instant a crashed holder is
	// first observed unreachable while its mounts are still held.
	s.serveCtx = ctx
	s.holder.onLostWithMounts = s.scheduleHolderLostSweep

	// Host sync wires before any worker or handler can read s.sync (per-call
	// gated on the sync_enabled meta); a failure leaves this run syncless —
	// sync must never take down single-host pooling.
	if err := s.setupSync(ctx); err != nil {
		s.log.Printf("host sync disabled for this run: %v", err)
	}

	s.log.Printf("daemon %s started; socket=%s", version.String(), s.socket)

	// One startup goroutine, off the accept path so Health answers from the first
	// instant, runs the ordered startupTable strictly in order (bridges bind before
	// any mount/FP enumeration registers; holder.refresh primes the mount cache
	// before selects key on it — mountReady's lazy refresh covers the residual
	// bind→prime gap; ua.detect only stamps the OAuth UA off the pre-bind path;
	// overlays.reconcile finishes before the heal loop and scheduler, which both
	// touch fuse Setup), then starts the loops.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runTable(ctx, s.newTick(ctx), startupTable)
		// The heal loop is only the per-account mount-health net. The Add(1)
		// runs inside this already-tracked goroutine, so the counter is ≥1 and
		// cannot race a zero-counter Wait.
		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.healFuseRows(ctx) }()
		s.scheduler(ctx)
	}()

	go func() {
		<-ctx.Done()
		closeListener()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}
			// Back off on a transient accept error (e.g. EMFILE) instead of
			// busy-spinning a core until the next shutdown.
			s.log.Printf("accept: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.handle(ctx, conn) }()
	}

	s.wg.Wait()
	// Deliberately no mount teardown: the detached holder owns the fuse
	// mirrors, and they must outlive this daemon so live claude sessions keep
	// their config dirs across daemon restarts and upgrades.
	s.log.Printf("daemon stopped")
	return nil
}

// listen binds the unix socket (0600) under an exclusive flock on
// socket+".lock" that makes the stale-check/remove/bind sequence single-entrant
// across processes: a live same-version peer is refused, a version-skewed one
// evicted. The flock — held by serve for the daemon's lifetime — is never
// removed: unlinking a held lock file would let a third daemon flock a fresh
// inode and reopen the race. proc.SingleEntrant owns the sequence; the Evict
// closure, which speaks the daemon wire, is the only cc-pool-specific policy.
func (s *Server) listen() (net.Listener, *os.File, error) {
	return proc.SingleEntrant{
		Socket:  s.socket,
		Timeout: s.evictTimeout,
		Evict: func() (bool, error) {
			c := &Client{socket: s.socket}
			resp, err := c.Health()
			if err != nil {
				return false, nil // no live peer answered
			}
			if resp.Version == version.String() {
				return false, errors.New("another cc-pool daemon at the same version is already running")
			}
			if err := s.evictPeer(c, resp.Version); err != nil {
				return false, err
			}
			return true, nil // evicted (flock-holder polls; flock-less binds)
		},
	}.Listen()
}

// evictPeer tells a version-skewed peer daemon to step down (OpShutdown) and
// waits it out, hard-killing the exact socket peer if it acks but wedges.
func (s *Server) evictPeer(c *Client, ver string) error {
	s.log.Printf("evicting version-skewed daemon (%s) holding the socket", ver)
	if _, err := c.Shutdown(); err != nil {
		return fmt.Errorf("evict holder %s: %w", ver, err)
	}
	if !c.WaitGone(s.evictTimeout) {
		// Acked OpShutdown but wedged: kill the exact socket holder so we can
		// rebind, rather than exiting and leaving launchd to retry against it.
		if _, err := c.KillSocketPeer(); err != nil {
			s.log.Printf("kill socket peer: %v", err)
		}
		if !c.WaitGone(s.evictTimeout) {
			return fmt.Errorf("holder %s did not release the socket within %s", ver, s.evictTimeout)
		}
	}
	return nil
}

// handle serves one connection. ctx is the daemon's lifecycle context (bounds
// shutdown); the conn deadline independently bounds a single slow client.
func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	var req Request
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		writeResp(conn, Response{OK: false, Error: "bad request: " + err.Error()})
		return
	}
	if req.Op == OpMigrate || req.Op == OpCredMove || req.Op == OpFPRepair {
		// These ops legitimately outlive the 10s deadline (migrate: a probe
		// mount plus up to an 8s wait and a bounded rollback per account;
		// credmove: a bounded per-account lock wait; fprepair: a Teardown+Setup
		// per domain, each of which can take seconds to materialize); stay under
		// the client's 150s so the server, not a dead socket, reports the outcome.
		_ = conn.SetDeadline(time.Now().Add(140 * time.Second))
	}
	resp := s.dispatch(ctx, req)
	resp.Proto = ProtocolVersion
	writeResp(conn, resp)
}

func writeResp(conn net.Conn, r Response) {
	r.Proto = ProtocolVersion
	_ = json.NewEncoder(conn).Encode(r)
}

func (s *Server) dispatch(ctx context.Context, req Request) Response {
	switch req.Op {
	case OpHealth:
		return Response{OK: true, Version: version.String()}
	case OpStatus:
		return s.handleStatus(ctx)
	case OpSelect:
		return s.handleSelect(ctx, req)
	case OpCheckin:
		return s.handleCheckin(ctx, req)
	case OpMigrate:
		return s.handleMigrate(ctx, req)
	case OpCredMove:
		return s.handleCredMove(ctx, req)
	case OpFPRepair:
		return s.handleFPRepair(ctx, req)
	case OpShutdown:
		return s.handleShutdown()
	default:
		return Response{OK: false, Error: "unknown op: " + string(req.Op)}
	}
}

// handleShutdown replies OK, then cancels serve's context so this instance
// releases the socket — the only eviction that works on an orphan launchd no
// longer tracks. Cancelling closes the listener, not this live connection, so
// the OK reply still lands. Idempotent.
func (s *Server) handleShutdown() Response {
	s.triggerShutdown()
	return Response{OK: true, Version: version.String()}
}

// handleStatus returns scored snapshots from cached samples (no live fetch).
func (s *Server) handleStatus(ctx context.Context) Response {
	accts, err := s.statuses(ctx)
	if err != nil {
		return Response{OK: false, Error: err.Error()}
	}
	// Version lets the client detect a pre-upgrade daemon (which omits newer wire
	// fields like Components) and fall back to live sampling.
	resp := Response{OK: true, Version: version.String(), Accounts: accts, Holder: s.holder.wireStatus(), FPConsentPending: s.fpConsentPending.Load()}
	// Content-source health lives only in this process; errors.Join's newlines
	// fold to "; " so doctor renders one line.
	if s.contentSource != nil {
		if err := s.contentSource.HealthErrors(); err != nil {
			resp.ContentHealth = strings.ReplaceAll(err.Error(), "\n", "; ")
		}
	}
	resp.FPWedged = s.fpWedgedStates(accts)
	return resp
}

// fpWedgedStates lists the currently-wedged File Provider domains for the status
// wire, joining the fp state's dir-keyed verdicts to account IDs and labels. nil
// when no domain is wedged or fp state is absent (bare test servers).
func (s *Server) fpWedgedStates(accts []AccountStatus) []FPDomainState {
	if !s.fpEnabled() {
		return nil
	}
	wedges := s.fpWedgedSnapshot()
	if len(wedges) == 0 {
		return nil
	}
	byDir := make(map[string]AccountStatus, len(accts))
	for _, a := range accts {
		byDir[a.ConfigDir] = a
	}
	out := make([]FPDomainState, 0, len(wedges))
	for _, w := range wedges {
		st := FPDomainState{ConfigDir: w.Dir, RecoveryAttempts: w.Attempts, BreakerTripped: w.Tripped}
		if a, ok := byDir[w.Dir]; ok {
			st.ID = a.ID
			st.Label = a.Label
		}
		out = append(out, st)
	}
	return out
}

// statuses assembles the wire view of every account from cached samples — the
// single mapping shared by the socket status op and the on-disk snapshot.
func (s *Server) statuses(ctx context.Context) ([]AccountStatus, error) {
	snaps, err := s.m.Snapshots(ctx, false, 0)
	if err != nil {
		return nil, err
	}
	return ToStatuses(snaps), nil
}

// handleSelect picks the best available account from cached scores, applying
// short-lived reservations to avoid two selects colliding, and records a
// reservation for the winner.
func (s *Server) handleSelect(ctx context.Context, req Request) Response {
	snaps, err := s.m.Snapshots(ctx, false, 0)
	if err != nil {
		return Response{OK: false, Error: err.Error()}
	}
	if len(snaps) == 0 {
		return Response{OK: false, Error: pool.ErrNoAccounts.Error()}
	}

	if req.Account != nil {
		for _, sn := range snaps {
			if sn.Account.ID == *req.Account {
				if !s.mountReady(sn.Account) {
					switch {
					case fuseBackedRow(sn.Account.OverlayKind):
						return Response{OK: false, Error: fmt.Sprintf("acct-%02d's fuse mount is not up yet; retry shortly", sn.Account.ID)}
					case fpBackedRow(sn.Account.OverlayKind):
						return Response{OK: false, Error: fmt.Sprintf("acct-%02d's file provider domain is wedged; the daemon is recovering it — retry shortly", sn.Account.ID)}
					default:
						return Response{OK: false, Error: fmt.Sprintf("acct-%02d's dir is unexpectedly a mountpoint (wedged unmount?); see `ccp doctor` and the daemon log", sn.Account.ID)}
					}
				}
				if !s.probeWinnerReady(ctx, sn.Account) {
					return Response{OK: false, Error: fmt.Sprintf("acct-%02d's overlay is wedged; the daemon is recovering it — retry shortly", sn.Account.ID)}
				}
				if !s.cl.reserve(sn.Account.ID) {
					return Response{OK: false, Error: fmt.Sprintf("acct-%02d is migrating overlays; retry shortly", sn.Account.ID)}
				}
				if !req.NoMark && req.PID > 0 {
					if _, err := s.m.Store.OpenSession(sn.Account.ID, req.PID, sn.Account.ConfigDir, req.Cwd, time.Now()); err != nil {
						s.log.Printf("open session for acct-%02d pid %d: %v", sn.Account.ID, req.PID, err)
					}
				}
				s.recordSticky(req.Cwd, sn.Account.ID)
				// A launch here makes this host the chain's refresher.
				s.sync.claimForSelect(ctx, sn.Account)
				id := sn.Account.ID
				return Response{
					OK: true, Dir: sn.Account.ConfigDir, SelectedID: &id,
					Remaining5h: sn.Remaining5h, Remaining7d: sn.Remaining7d, HasUsage: sn.HasUsage,
					Scoped7dUtil: sn.Scoped7dUtil, Scoped7dModel: sn.Scoped7dModel,
				}
			}
		}
		return Response{OK: false, Error: fmt.Sprintf("account %d not found", *req.Account)}
	}

	// Reconcile session rows against reality before consulting the pin: a
	// claude that just exited must read as warm (bind), not live (hold), and
	// pollOnce's ~3.5-minute cadence is too coarse for a quick resume.
	if sessions, err := s.scan(ctx); err == nil {
		if _, cerr := s.m.Store.CloseDeadSessions(procscan.AlivePIDs(sessions), time.Now()); cerr != nil {
			s.log.Printf("close dead sessions: %v", cerr)
		}
	}

	// An account mid-conversion or whose mirror is not mounted yet cannot serve
	// a session — its config dir is not in a usable shape. Exclude, don't
	// penalize.
	usable := make([]pool.Snapshot, 0, len(snaps))
	for _, sn := range snaps {
		if s.cl.held(sn.Account.ID) || !s.mountReady(sn.Account) {
			continue
		}
		usable = append(usable, sn)
	}
	if len(usable) == 0 {
		soonest := soonestReset(snaps)
		resp := Response{OK: false, Error: pool.ErrMountsNotReady.Error(), NoneAvailable: true, MountsNotReady: true}
		if !soonest.IsZero() {
			resp.SoonestReset = &soonest
		}
		s.log.Printf("select: %s -> none available (all accounts migrating or unmounted)", req.Cwd)
		return resp
	}

	ranked, bySnap := s.rankWithReservations(usable)
	pin, outcome := s.m.StickyPick(req.Cwd, ranked, time.Now())
	r := pin
	fallback := false
	if outcome != pool.StickyBind {
		var ok bool
		r, ok = score.Pick(ranked)
		if !ok && !req.NoFallback {
			// Every account is exhausted (or worse): launch on the least-bad
			// exhausted one rather than refusing; the client warns loudly.
			r, ok = score.PickFallback(ranked)
			fallback = true
		}
		if !ok {
			soonest := soonestReset(snaps)
			resp := Response{OK: false, Error: pool.ErrNoneAvailable.Error(), NoneAvailable: true}
			when := "unknown"
			if !soonest.IsZero() {
				resp.SoonestReset = &soonest
				when = soonest.Format(time.RFC3339)
			}
			s.log.Printf("select: %s -> none available (soonest reset %s)", req.Cwd, when)
			return resp
		}
	}
	best := bySnap[r.AccountID]
	// Deep-probe the winner before handing it to a session — a wedge refuses
	// and the client retries onto a healthy account.
	if !s.probeWinnerReady(ctx, best.Account) {
		return Response{OK: false, Error: fmt.Sprintf("acct-%02d's overlay is wedged; the daemon is recovering it — retry shortly", best.Account.ID)}
	}
	if !req.NoMark {
		if !s.cl.reserve(best.Account.ID) {
			// A conversion claimed the winner between the filter above and
			// here — vanishingly rare; the client just retries.
			return Response{OK: false, Error: fmt.Sprintf("acct-%02d began migrating overlays mid-select; retry shortly", best.Account.ID)}
		}
		if req.PID > 0 {
			if _, err := s.m.Store.OpenSession(best.Account.ID, req.PID, best.Account.ConfigDir, req.Cwd, time.Now()); err != nil {
				s.log.Printf("open session for acct-%02d pid %d: %v", best.Account.ID, req.PID, err)
			}
		}
	}
	// Record regardless of NoMark (cache continuity is established by no-mark
	// selects too) — but never over a held pin, unless the free ranking landed
	// on the pinned account anyway, which is genuine pin activity.
	if !outcome.Held() || best.Account.ID == pin.AccountID {
		s.recordSticky(req.Cwd, best.Account.ID)
	}
	s.log.Printf("select%s: %s -> acct-%02d (score %.1f · 5h %.0f%% used · 7d %.0f%% used%s)",
		selectKind(outcome, fallback), req.Cwd, best.Account.ID,
		r.Score, best.Util5h, best.Util7d, runnerUp(ranked, r.AccountID, fallback))
	id := best.Account.ID
	// Claim holdership + a lease BEFORE the async preflight refresh below: the
	// claim is what legitimizes it under the one-holder rule.
	s.sync.claimForSelect(ctx, best.Account)
	// Best-effort preflight refresh of the winner. The Add(1) is inside an
	// already-tracked goroutine, so it cannot race a zero-counter Wait.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		pctx, cancel := context.WithTimeout(ctx, preflightTimeout)
		defer cancel()
		if err := s.m.PreflightRefresh(pctx, best.Account); err != nil {
			s.log.Printf("acct-%02d preflight refresh: %v", best.Account.ID, err)
		}
	}()
	resp := Response{
		OK: true, Dir: best.Account.ConfigDir, SelectedID: &id,
		Sticky:      outcome == pool.StickyBind,
		Remaining5h: best.Remaining5h, Remaining7d: best.Remaining7d, HasUsage: best.HasUsage,
		ExhaustedFallback: fallback, ExtraEnabled: best.ExtraEnabled,
		Scoped7dUtil: best.Scoped7dUtil, Scoped7dModel: best.Scoped7dModel,
	}
	if outcome == pool.StickyHoldManual {
		held := pin.AccountID
		resp.PinHeldAccount = &held
	}
	if fallback && !r.ExhaustedUntil.IsZero() {
		// Tell the client when the fallback pick actually recovers — the latest
		// reset among its pegged windows, not necessarily the 5h one.
		resp.SoonestReset = &r.ExhaustedUntil
	}
	return resp
}

// selectKind renders the select log qualifier. A bind and a fallback are
// mutually exclusive (an unusable pin never binds); a held pin coexists with
// fallback only when the free ranking itself collapsed to the least-bad pick,
// and the fallback warning is the more urgent of the two.
func selectKind(outcome pool.StickyOutcome, fallback bool) string {
	switch {
	case outcome == pool.StickyBind:
		return " (sticky)"
	case fallback:
		return " (exhausted-fallback)"
	case outcome.Held():
		return " (pin-held)"
	default:
		return ""
	}
}

// runnerUp renders the next-best servable account after winnerID for the
// select log, empty when there is none. A fallback pick means nothing is
// Available, so candidates widen to PickFallback's own predicate — otherwise
// the one select kind that most needs forensic context would never log one.
func runnerUp(ranked []score.Result, winnerID int, fallback bool) string {
	for _, r := range ranked {
		if r.AccountID == winnerID {
			continue
		}
		if !r.Available && (!fallback || r.RateLimited) {
			continue
		}
		return fmt.Sprintf(" · runner-up acct-%02d %.1f", r.AccountID, r.Score)
	}
	return ""
}

// recordSticky upserts the cwd->account sticky record, logging (not failing)
// on error.
func (s *Server) recordSticky(cwd string, accountID int) {
	if err := s.m.RecordSticky(cwd, accountID, time.Now()); err != nil {
		s.log.Printf("record sticky for %s: %v", cwd, err)
	}
}

// handleCheckin closes sessions for a pid, adopts any rotated token, and
// releases this host's sync lease on each account whose last session closed.
func (s *Server) handleCheckin(ctx context.Context, req Request) Response {
	sessions, err := s.m.Store.ListActiveSessions()
	if err != nil {
		return Response{OK: false, Error: err.Error()}
	}
	closed := map[int]store.Account{}
	for _, se := range sessions {
		if se.PID != req.PID {
			continue
		}
		if err := s.m.Store.CloseSession(se.ID, time.Now()); err != nil {
			s.log.Printf("checkin close session %d: %v", se.ID, err)
		}
		if a, err := s.m.Store.GetAccount(se.AccountID); err == nil {
			actx, cancel := context.WithTimeout(ctx, preflightTimeout)
			if err := s.m.AdoptRotatedToken(actx, a); err != nil {
				s.log.Printf("acct-%02d adopt rotated token on checkin: %v", a.ID, err)
			}
			cancel()
			closed[a.ID] = a
		}
	}
	s.releaseIdleLeases(ctx, closed)
	return Response{OK: true}
}

// releaseIdleLeases releases this host's sync lease on each closed account
// with no remaining live local session.
func (s *Server) releaseIdleLeases(ctx context.Context, closed map[int]store.Account) {
	if !s.sync.active() || len(closed) == 0 {
		return
	}
	live, err := s.m.Store.ListActiveSessions()
	if err != nil {
		s.log.Printf("sync lease release: list sessions: %v", err)
		return
	}
	open := map[int]int{}
	for _, se := range live {
		open[se.AccountID]++
	}
	for id, a := range closed {
		if open[id] == 0 {
			s.sync.releaseLease(ctx, a)
		}
	}
}

// mountReady reports whether an account's overlay can serve a session now. A
// fuse row is ready iff the holder cache vouches for a live mirror at its dir —
// cached kernel truth with no filesystem touch, because an lstat through a dead
// fuse-t NFS mount can hang the select path; a dead holder's carcass (still a
// local mountpoint) is never trusted. On a cache-miss, one rate-limited refresh
// (bounded RPC, no fs touch) picks up truth the poll cadence misses (a select
// racing the startup prime, or a mirror `ccp add` just mounted). A non-fuse row
// needs the dir NOT mounted — a mountpoint under a symlink row is aborted-
// rollback wreckage serving a mirror whose backing no longer holds the
// account's identity; lstat on a plain dir is safe. A File Provider row also needs
// its dir un-mounted (the domain bridge symlink is never a mountpoint) AND its
// domain not wedged — a data-plane-wedged domain (control ops pass, reads hang) is
// kept out of a launching session until the heal loop recovers it.
func (s *Server) mountReady(a store.Account) bool {
	if fuseBackedRow(a.OverlayKind) {
		if !s.holder.ready(a.ConfigDir) {
			s.holder.refreshIfStale(s.holderClient())
		}
		return s.holder.ready(a.ConfigDir)
	}
	if fpBackedRow(a.OverlayKind) {
		return !overlayMounted(a.ConfigDir) && !s.fpWedged(a.ConfigDir)
	}
	return !overlayMounted(a.ConfigDir)
}

// fpWedged reports whether dir's File Provider domain has latched its wedge
// verdict on the fp.domain ledger; false when FP self-heal is not wired (bare
// test servers).
func (s *Server) fpWedged(dir string) bool {
	if !s.fpEnabled() {
		return false
	}
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	return s.led.faulted(fpDomainPolicy, dir)
}

// probeWinnerReady deep-probes a chosen fuse mirror at select time, reporting
// whether it is safe to assign. This is the ONLY probe of an IDLE mirror (the
// heal probe skips session-less mounts), so a partial wedge (shallow-alive,
// bulk reads hang) would otherwise go undetected until a session hung on it. A
// wedge is force-marked (excluding it from selection AND triggering a heal-loop
// remount) and reads not-ready; the caller refuses and the client retries. It
// is bounded by the 5s deep-probe timeout (under the handler's 10s deadline) so
// it never remounts inline — the heal loop owns the remount. Non-fuse, healthy,
// and pre-probe (ErrProbeMissing) mirrors read ready. One wedge is enough (no
// debounce): a NEW session has no live session a false positive could orphan.
func (s *Server) probeWinnerReady(ctx context.Context, a store.Account) bool {
	if fpBackedRow(a.OverlayKind) {
		return s.probeFPWinnerReady(ctx, a)
	}
	if !fuseBackedRow(a.OverlayKind) {
		return true
	}
	err := deepProbe(a.ConfigDir)
	if err != nil && !errors.Is(err, overlay.ErrProbeMissing) {
		s.holder.markDeepWedged(a.ConfigDir)
		s.log.Printf("acct-%02d mirror wedged at select (serves metadata but hangs reads); excluding it and letting the heal loop remount it — relaunch once it recovers: %v", a.ID, err)
		return false
	}
	if msg := s.holder.recordDeep(a.ConfigDir, err); msg != "" {
		s.log.Printf("%s", msg)
	}
	return true
}

// probeFPWinnerReady live-probes a chosen File Provider domain's data plane at
// select time (through the app control op, never a through-domain read), reporting
// whether it is safe to assign. A hard wedge verdict (ErrDomainNotServing → the
// domain answers control ops but not reads, or a 0-byte read for an account that
// has an identity) force-marks the domain wedged with NO debounce — a launching
// session has no live reads a false positive could orphan — so it is excluded now
// and the heal loop recovers it. A missing or empty-by-design .claude.json reads
// ready. A NoVerdict (app busy/unreachable/too old, or a restart) also reads ready
// WITHOUT force-wedging: a companion-app restart must never fleet-wedge selects. nil
// fp state (bare test servers) reads ready. Bounded to 3s so a slow probe never
// stalls the pick.
func (s *Server) probeFPWinnerReady(ctx context.Context, a store.Account) bool {
	if !s.fpEnabled() {
		return true
	}
	if s.fpWedged(a.ConfigDir) {
		return false // already known wedged (mountReady also excludes it)
	}
	probeCtx, cancel := context.WithTimeout(ctx, fpControlProbeTimeout)
	defer cancel()
	err := fpDomainProbe(probeCtx, a.ConfigDir)
	switch {
	case err == nil, errors.Is(err, overlay.ErrFPProbeMissing):
		return true
	case errors.Is(err, overlay.ErrFPProbeEmpty) && !s.fpSynth(a.ConfigDir):
		return true // 0 bytes served for an account with no identity yet
	case errors.Is(err, overlay.ErrFPProbeNoVerdict):
		return true // app restart / busy / unreachable — never fleet-wedge a select
	default:
		s.fpForceWedge(a.ConfigDir, err)
		s.log.Printf("acct-%02d file provider domain wedged at select (serves control ops but hangs reads); excluding it and letting the heal loop recover it — relaunch once it recovers: %v", a.ID, err)
		return false
	}
}

// holderClient returns an Owner-scoped client for the shared mount-holder socket:
// every List/Poll/Reclaim is filtered to cc-pool's own mounts, so the daemon never
// observes or disturbs another tenant's (e.g. cc-notes').
func (s *Server) holderClient() *mountd.Client {
	return &mountd.Client{Socket: s.holderSocket, Owner: pool.HolderOwner}
}

// startContentBridge binds the daemon's content.BridgeServer and waits for it to
// accept connections, so the first mount's manifest fetch over the bridge succeeds
// (holderfs.Build fails a mount loudly if the consumer is unreachable). The server
// runs until ctx is cancelled, tracked by wg.
func (s *Server) startContentBridge(ctx context.Context) {
	sock := pool.BridgeSocketPath()
	if err := os.MkdirAll(filepath.Dir(sock), 0o700); err != nil {
		s.log.Printf("content bridge: create socket dir: %v", err)
		return
	}
	bridge := &content.BridgeServer{Socket: sock, Source: s.contentSource, Version: version.String(), Log: s.log}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// Fail loud on a bind failure: every fuse mount's manifest fetch depends on
		// this bridge, so a dead bridge (and the every-mount-defers state it causes)
		// must be visible, not swallowed. A clean ctx-cancel shutdown returns nil.
		if err := bridge.Run(ctx); err != nil && ctx.Err() == nil {
			s.log.Printf("content bridge: serve %s failed; fuse mounts will defer until the daemon restarts: %v", sock, err)
		}
	}()
	cl := content.NewBridgeClient(sock)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil || cl.Available() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.log.Printf("content bridge: socket %s did not come up within 3s; mounts may defer until it does", sock)
}

// scan resolves session scanning through the test seam; nil means
// procscan.Scan.
func (s *Server) scan(ctx context.Context) ([]procscan.Session, error) {
	if s.scanSessions != nil {
		return s.scanSessions(ctx)
	}
	return procscan.Scan(ctx)
}

// overlayFor resolves a backend through the Manager's injectable seam (tests
// fake the fuse provider); nil means pool.OverlayProviderFor. A resolution
// failure is logged and yields nil — callers already fence on a wrong-backend
// (or here, nil) provider, refusing to mount through it.
func (s *Server) overlayFor(backend fkoverlay.Backend) fkoverlay.Provider {
	resolve := pool.OverlayProviderFor
	if s.m.OverlayFor != nil {
		resolve = s.m.OverlayFor
	}
	prov, err := resolve(backend)
	if err != nil {
		s.log.Printf("resolve overlay provider for backend %q: %v", backend, err)
		return nil
	}
	return prov
}

// overlayForRow resolves the overlay provider named by a's stored overlay_kind,
// keeping cc-pool blind: it carries the received Backend rather than re-deriving
// one. nil on an unparseable kind (logged) or an unresolvable backend; callers
// already fence on nil before mounting through the provider.
func (s *Server) overlayForRow(a store.Account) fkoverlay.Provider {
	backend, err := fkoverlay.Parse(a.OverlayKind)
	if err != nil {
		s.log.Printf("acct-%02d: unparseable overlay_kind %q: %v", a.ID, a.OverlayKind, err)
		return nil
	}
	return s.overlayFor(backend)
}

// fuseBackedRow reports whether a stored overlay_kind names a fuse backend.
// The daemon's hot paths check IsFuse on the stored string constantly; this is
// the one place that parse lives.
func fuseBackedRow(overlayKind string) bool {
	b, err := fkoverlay.Parse(overlayKind)
	if err != nil {
		// overlay_kind is cc-pool-written (always valid); an unparseable value is
		// corruption. Treat as non-fuse so the safe symlink path handles the dir
		// rather than mounting over it.
		return false
	}
	return b.IsFuse()
}

// rankWithReservations re-ranks snapshots with reservation and peer-lease
// penalties applied, returning the ranking plus a snapshot lookup by account
// id. A live peer lease counts as one extra active session — a penalty, never
// an exclusion, per the cross-host select rule.
func (s *Server) rankWithReservations(snaps []pool.Snapshot) ([]score.Result, map[int]pool.Snapshot) {
	peerLeases := s.sync.peerLeaseCounts(snaps)
	bySnap := map[int]pool.Snapshot{}
	inputs := make([]score.Input, 0, len(snaps))
	for _, sn := range snaps {
		bySnap[sn.Account.ID] = sn
		inputs = append(inputs, score.Input{
			AccountID:      sn.Account.ID,
			HasUsage:       sn.HasUsage,
			SampleTS:       time.Now().Add(-sn.SampleAge),
			Util5h:         sn.Util5h,
			Util7d:         sn.Util7d,
			Resets5h:       sn.Resets5h,
			Resets7d:       sn.Resets7d,
			Burn5hPerHour:  sn.Burn5hPerHour,
			ActiveSessions: sn.ActiveSessions + s.cl.reservedCount(sn.Account.ID) + peerLeases[sn.Account.ID],
			RateLimited:    sn.RateLimited,
			RefreshFailed:  sn.Stale && !sn.HasUsage,
			NeedsLogin:     sn.NeedsLogin,
			HasScoped7d:    sn.Scoped7dModel != "",
			Util7dScoped:   sn.Scoped7dUtil,
			Resets7dScoped: sn.Scoped7dResets,
		})
	}
	return score.Rank(inputs, time.Now()), bySnap
}

func soonestReset(snaps []pool.Snapshot) time.Time {
	var best time.Time
	for _, sn := range snaps {
		if sn.Resets5h.IsZero() {
			continue
		}
		if best.IsZero() || sn.Resets5h.Before(best) {
			best = sn.Resets5h
		}
	}
	return best
}

// ToStatuses converts snapshots into wire AccountStatus values.
func ToStatuses(snaps []pool.Snapshot) []AccountStatus {
	out := make([]AccountStatus, 0, len(snaps))
	for _, sn := range snaps {
		out = append(out, AccountStatus{
			ID:             sn.Account.ID,
			ConfigDir:      sn.Account.ConfigDir,
			Label:          sn.Account.Label,
			OverlayKind:    sn.Account.OverlayKind,
			Score:          sn.Score,
			Remaining5h:    sn.Remaining5h,
			Remaining7d:    sn.Remaining7d,
			ActiveSessions: sn.ActiveSessions,
			RateLimited:    sn.RateLimited,
			Exhausted:      sn.Exhausted,
			NeedsLogin:     sn.NeedsLogin,
			HasUsage:       sn.HasUsage,
			Stale:          sn.Stale,
			Resets5h:       sn.Resets5h,
			Resets7d:       sn.Resets7d,
			SampleAge:      sn.SampleAge.Round(time.Second).String(),
			// The wire ships the gated display forecast, never the raw
			// scoring burn (which stays live on stale samples).
			Burn5hPerHour:      sn.Forecast.BurnPerHour,
			Burn7dPerHour:      sn.Burn7dPerHour,
			Projected5hAtReset: sn.Forecast.AtReset,
			Depleted5hAt:       sn.Forecast.DepletedAt,
			ExtraEnabled:       sn.ExtraEnabled,
			ExtraUsed:          sn.ExtraUsed,
			ExtraLimit:         sn.ExtraLimit,
			Scoped7dUtil:       sn.Scoped7dUtil,
			Scoped7dResets:     sn.Scoped7dResets,
			Scoped7dModel:      sn.Scoped7dModel,
			WeeklyExhausted:    sn.WeeklyExhausted,
			Components:         sn.Components,
		})
	}
	return out
}

// reconcileOverlays brings each account's on-disk overlay in line with its row
// at startup, off the accept path; ctx is checked between accounts so a
// boot-time shutdown doesn't block wg.Wait on a slow account's mount timeout.
func (s *Server) reconcileOverlays(ctx context.Context) {
	// Reap dead-holder orphans first: a cold start over an already-dead holder
	// never fires the loss hook, and the reap is carcass-gated so it is always safe.
	s.reapPoolOrphans()
	// Prime the holder cache before any per-account decision: mountReady (and
	// so every select racing this reconcile) keys fuse readiness on it.
	s.holder.refresh(s.holderClient())
	accts, err := s.m.Store.ListAccounts()
	if err != nil {
		return
	}
	// Capability gate: one probe settles whether this machine can fuse at all
	// BEFORE any per-account cold mount — a hard "no" retreats the whole pool to
	// symlink in one pass (vs a doomed mount per account), and the loop below
	// reconciles the now-symlink rows.
	if reason := s.fuseHardUnavailable(); reason != "" {
		s.retreatPoolToSymlink(ctx, accts, reason)
		if accts, err = s.m.Store.ListAccounts(); err != nil {
			return
		}
	}
	for _, a := range accts {
		if ctx.Err() != nil {
			return
		}
		if !s.cl.hold(a.ID) {
			// An OpMigrate landed before startup reconcile reached this
			// account; the conversion leaves it consistent on its own.
			s.log.Printf("acct-%02d busy converting; skipping startup reconcile", a.ID)
			continue
		}
		s.reconcileAccount(ctx, a)
		s.cl.disownHold(a.ID)
	}
	// Clear any wedged carcass under accounts/ that no row owns (a pre-row `ccp
	// add` mount whose holder died); this startup sweep is its only cleaner.
	s.sweepOrphanMountpoints(ctx, accts)
}

// sweepOrphanMountpoints force-unmounts any mountpoint under the accounts dir
// that no account row owns (a hard-interrupted pre-row `ccp add` leaves a wedged
// carcass nothing row-driven names); every check here is non-blocking.
func (s *Server) sweepOrphanMountpoints(ctx context.Context, accts []store.Account) {
	rowDirs := make(map[string]bool, len(accts))
	for _, a := range accts {
		rowDirs[a.ConfigDir] = true
	}
	entries, err := os.ReadDir(pool.AccountsDir())
	if err != nil {
		if !os.IsNotExist(err) {
			s.log.Printf("orphan mount sweep: read accounts dir: %v", err)
		}
		return
	}
	for _, e := range entries {
		if ctx.Err() != nil {
			return
		}
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(pool.AccountsDir(), e.Name())
		if rowDirs[dir] || !overlayMounted(dir) {
			continue
		}
		// A carcass actively read by a live claude is not a carcass yet:
		// force-unmounting ANY busy NFS mirror panics the kernel, so defer to a
		// later sweep once the session relaunches. Leave it mounted and surface it.
		busy, n, err := s.unmountIdle(ctx, dir)
		if busy {
			s.log.Printf("orphaned mountpoint %s left under %d live session(s) — NOT force-unmounting (would panic the kernel); relaunch them", dir, n)
			continue
		}
		s.log.Printf("cleared orphaned mountpoint with no account row (pre-row add carcass?): %s", dir)
		if err != nil {
			s.log.Printf("orphan mount sweep: force-unmount %s: %v", dir, err)
		}
	}
	s.sweepOrphanMuxRoot(ctx, accts)
}

// sweepOrphanMuxRoot force-unmounts the shared native mux mount at MuxRootDir()
// iff it is a carcass no live holder owns; gated on zero live fuse sessions
// pool-wide, since dropping the shared mount drops EVERY subtree.
func (s *Server) sweepOrphanMuxRoot(ctx context.Context, accts []store.Account) {
	root := pool.MuxRootDir()
	if !overlayMounted(root) {
		return
	}
	// A live peer is not enough: a freshly respawned empty-registry holder does
	// not own a dead predecessor's root, and that carcass makes it refuse every
	// mux Setup as ClassForeignMount.
	if s.holderOwnsMuxRoot() {
		return
	}
	fuse := make([]store.Account, 0, len(accts))
	for _, a := range accts {
		if fuseBackedRow(a.OverlayKind) {
			fuse = append(fuse, a)
		}
	}
	if s.sweepMuxRootIdle(ctx, fuse) {
		s.log.Printf("cleared orphaned native mux mount (no live holder owns it): %s", root)
	}
}

// scheduleHolderLostSweep runs sweepHolderOrphans off the refresh caller when a
// healthy holder serving mounts becomes unreachable (markUnhealthy's transition
// gate fires it once per death). Async so a select's lazy refresh never blocks on
// the sweep; tracked by s.wg — every refresh caller already holds a wg token, so
// the Add never races the shutdown Wait.
func (s *Server) scheduleHolderLostSweep() {
	ctx := s.serveCtx
	if ctx == nil {
		return // no serve context yet (pre-serve wiring); nothing to sweep under
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.sweepHolderOrphans(ctx)
	}()
}

// sweepHolderOrphans is the holder-death recovery: reap the orphaned go-nfsv4
// servers (carcass-gated, safe unconditionally), then the idle-gated mux-root
// sweep. Short-circuits the per-row remount breaker — see ccn doc 1668381.
func (s *Server) sweepHolderOrphans(ctx context.Context) {
	s.reapPoolOrphans()
	accts, err := s.m.Store.ListAccounts()
	if err != nil {
		s.log.Printf("holder-loss orphan sweep: list accounts: %v", err)
		return
	}
	s.sweepOrphanMuxRoot(ctx, accts)
}

// reapPoolOrphans kills any-generation go-nfsv4 orphans bound under the pool's
// mount roots — the mux go-nfsv4 (bound to the mux root) and legacy per-dir
// servers (under accounts/). Carcass-gated AND kill-time-reconfirmed in
// fusekit, so a live holder's healthy servers are never candidates.
func (s *Server) reapPoolOrphans() {
	if pids := reapOrphanedServers([]string{pool.MuxRootDir(), pool.AccountsDir()}); len(pids) > 0 {
		s.log.Printf("reaped %d orphaned go-nfsv4 server(s) a crashed holder left bound to cc-pool mounts: %v", len(pids), pids)
	}
}

// fuseHardUnavailable reports a reason iff one capability probe proves this
// machine cannot host fuse mounts right now (ErrMountFailed on a throwaway
// probe mount); every "" return leaves the per-account heal in charge.
func (s *Server) fuseHardUnavailable() string {
	if !s.canSpawnHolder() {
		return "" // no cask holder to probe; per-account heal converts each fuse row as it fails
	}
	if healthy, _ := s.holder.view(); !healthy {
		return "" // holder not reachable yet; Setup respawns it, per-account heal mounts through it
	}
	if s.holder.wireStatus().Mounts > 0 {
		return "" // already serving a live mount: capability is proven
	}
	if _, err := s.holderClient().Probe(); errors.Is(err, mountd.ErrMountFailed) {
		return err.Error()
	}
	return ""
}

// retreatPoolToSymlink retreats every fuse row to symlink and records symlink
// as the new-account default; `ccp migrate --to fuse` re-promotes later.
func (s *Server) retreatPoolToSymlink(ctx context.Context, accts []store.Account, reason string) {
	fuse := make([]store.Account, 0, len(accts))
	for _, a := range accts {
		if fuseBackedRow(a.OverlayKind) {
			fuse = append(fuse, a)
		}
	}
	if len(fuse) == 0 {
		return
	}
	s.log.Printf("fuse is unavailable on this machine (%s); retreating %d fuse account(s) to symlink and defaulting new accounts to symlink — see %s",
		reason, len(fuse), pool.MountHolderLogPath())
	s.retreatAllFuseRows(ctx, fuse, "fuse-t cannot mount on this machine")
	if err := s.m.SetDefaultOverlayKind(fkoverlay.BackendSymlink); err != nil {
		s.log.Printf("capability retreat: record symlink as the new-account default: %v", err)
	}
}

// reconcileAccount brings one account's on-disk overlay in line with its row.
// Caller holds the poll claim.
func (s *Server) reconcileAccount(ctx context.Context, a store.Account) {
	if fpBackedRow(a.OverlayKind) {
		// File Provider rows reconcile through the domain host (Health, then an
		// idempotent Setup) — never the non-fuse arm below: its
		// HealStrandedPrivate would move the account's private files out of the
		// FP private store, through the domain bridge symlink.
		s.reconcileFileProvider(ctx, a)
		return
	}
	if fuseBackedRow(a.OverlayKind) {
		// One-time legacy→mux migration: a fuse row whose ConfigDir is still a real
		// dir — a pre-cutover per-account mount, or a bare dir a half-done migration
		// left — is converted to a shared-mux subtree + bridge symlink before it is
		// adopted or healed. Idempotent and crash-resumable; a re-run converges.
		if s.needsMuxMigration(a) {
			s.migrateLegacyFuseRow(ctx, a)
			return
		}
		prov := s.overlayForRow(a)
		if prov != nil && prov.Backend().IsFuse() && prov.Health(pool.ClaudeDir(), a.ConfigDir) == nil {
			// The detached holder kept the mirror live across the restart (the
			// common case): adopt it untouched and vouch for it in the cache
			// directly — a live mirror implies the holder serving it, and a
			// select must not depend on the startup refresh still being accurate.
			s.holder.noteMounted(a.ConfigDir)
			s.log.Printf("acct-%02d adopted live mount", a.ID)
			return
		}
		s.healFuse(ctx, a)
		return
	}
	// CRASH-WINDOW CONVERGENCE: a symlink row whose dir is itself a symlink is
	// convert wreckage — a symlink→fileprovider conversion laid the domain bridge
	// symlink but crashed before flipping the row (Setup done, row not flipped, the
	// identity-loss window). Retract the leaked domain (idempotent Teardown
	// deregisters it and removes the bridge symlink) and recreate the real dir so
	// HealStrandedPrivate below moves the private files back and re-lays the symlink
	// overlay. Lstat never follows the link, so this precedes the mount check (which
	// would traverse the live domain).
	if s.dirIsOverlaySymlink(a.ConfigDir) {
		// The listing that produced `a` can age: a symlink→fileprovider conversion
		// may complete between it and this poll claim (the socket serves OpMigrate
		// during the startup reconcile), leaving a legitimate File Provider row whose
		// dir is a domain bridge symlink. Re-read under the held poll claim (which
		// blocks any NEW conversion) so a stale symlink snapshot never drives the
		// retract below against a live domain; the retract itself runs only under the
		// convert claim and with no live session on the bridge, since beginPoll does
		// not hide the dir from a select landing on it.
		fresh, err := s.m.Store.GetAccount(a.ID)
		if err != nil {
			s.log.Printf("acct-%02d reconcile: re-read row before converge: %v", a.ID, err)
			return
		}
		if fpBackedRow(fresh.OverlayKind) || fuseBackedRow(fresh.OverlayKind) {
			return // converted off symlink under the aging listing; leave it to the fp/fuse arms
		}
		// A fresh per-account tick, not the startup table's shared one: this
		// crash-window retract is destructive, so its liveness check must be as
		// fresh as the old per-call liveSessionGate — one scan per reconciled
		// account, exactly as before.
		if !s.beginSymlinkHealHeld(s.newTick(ctx), fresh) {
			return
		}
		defer s.cl.disownConvert(fresh.ID)
		if !s.convergeSymlinkRowBridge(fresh) {
			return
		}
		a = fresh
	}
	// A live mountpoint under a FUSE row is normal at startup (the holder
	// survived the restart); under a NON-fuse row it is wreckage — an aborted
	// rollback's wedged unmount, or a conversion that died before its row flip —
	// serving a mirror whose backing no longer holds the account's identity. It
	// blocks every symlink repair (they refuse mountpoints); force it down first.
	if overlayMounted(a.ConfigDir) {
		// Force it down directly (backend-agnostic, no provider) so the symlink
		// repair below can proceed. But a live claude may still be reading this
		// carcass, and force-unmounting a busy NFS mirror panics the kernel — so
		// when a session is bound, leave it and re-check next tick.
		busy, n, err := s.unmountIdle(ctx, a.ConfigDir)
		if busy {
			s.log.Printf("acct-%02d: stale mountpoint left under %d live session(s) — NOT force-unmounting (would panic the kernel); relaunch them", a.ID, n)
			return
		}
		if err != nil {
			s.log.Printf("acct-%02d: unmount stale mountpoint: %v", a.ID, err)
			return
		}
		s.log.Printf("acct-%02d: cleared a stale mountpoint", a.ID)
	}
	// A symlink account can carry private files stranded in a fuse backing dir
	// by a conversion that died midway — restore them before anything launches.
	healed, err := s.m.HealStrandedPrivate(a)
	if err != nil {
		s.log.Printf("acct-%02d heal stranded private files: %v", a.ID, err)
		return
	}
	if healed {
		s.log.Printf("acct-%02d restored private files stranded by an interrupted migration", a.ID)
	}
}

// needsMuxMigration reports whether a fuse row still needs the one-time
// legacy→mux migration: its ConfigDir is not yet the bridge symlink into the
// shared mount (a pre-cutover per-dir mountpoint, a bare dir, or an absent dir).
// A migrated or freshly mux-created account dir is a bridge symlink, so it does
// not.
func (s *Server) needsMuxMigration(a store.Account) bool {
	return !pool.IsBridgeSymlink(a.ConfigDir)
}

// migrateLegacyFuseRow converts a pre-cutover per-account fuse mount into a
// shared-mux subtree bridged by a symlink. Caller holds the account's poll claim.
// A live legacy mountpoint comes down first (session-gated: force-unmounting a
// busy NFS mirror panics the kernel); its dir is then drained (shared orphans to
// base, stranded private files to the backing root) so the bridge symlink can
// replace it. The attach itself is delegated to healFuse, so a mount failure is
// classified (retry / TCC / gated symlink fallback) exactly as a steady-state
// heal, and a success lays the bridge symlink over the emptied dir. Every step is
// idempotent, so a crash mid-migration re-converges on the next reconcile.
func (s *Server) migrateLegacyFuseRow(ctx context.Context, a store.Account) {
	base, dir := pool.ClaudeDir(), a.ConfigDir
	// A holder predating MinHolderVersion silently ignores mux_root, so the
	// migration cannot re-attach a subtree on it — and tearing the working legacy
	// mount down before deferring the re-mount (healFuse's ErrHolderUnmitigated
	// arm) would strand the account until the cask upgrade lands. Defer the whole
	// migration loudly while the reachable holder is unmitigated; the legacy mount
	// keeps serving. view() reports "" when the holder is unreachable, and
	// HolderVersionMitigated("") is true, so an unreachable holder does not block.
	if _, ver := s.holder.view(); !pool.HolderVersionMitigated(ver) {
		s.log.Printf("acct-%02d deferring legacy→mux migration: holder %s predates %s; leaving the working legacy mount until `brew upgrade --cask fusekit-holder` lands", a.ID, ver, pool.MinHolderVersion)
		return
	}
	// The drain + teardown remake the dir a launching claude would land in, so
	// claim the account first (claim-before-scan, like fallbackToSymlink): once
	// converting is set tryReserve refuses for the whole span, and a live
	// reservation defers the migration. Released before healFuse re-attaches — its
	// own fallback path takes the claim afresh.
	if !s.cl.ownHeld(a.ID) {
		s.log.Printf("acct-%02d deferring legacy→mux migration: reserved by a pending select", a.ID)
		return
	}
	drained := s.drainLegacyFuseDir(ctx, a, base, dir)
	s.cl.disownConvert(a.ID)
	if !drained {
		return
	}
	s.healFuse(ctx, a)
}

// drainLegacyFuseDir tears down a legacy per-dir mount and empties the account dir
// so a bridge symlink can replace it, returning whether the dir is ready for
// healFuse to re-attach the mux subtree. Caller holds the convert claim. The
// live-session gate is unconditional (not only when dir is still a mountpoint): a
// re-run on a bare, half-migrated dir still swaps the account's CLAUDE_CONFIG_DIR
// out from under any live claude, so it must defer just like a mounted one.
func (s *Server) drainLegacyFuseDir(ctx context.Context, a store.Account, base, dir string) bool {
	// The idle gate is unconditional — it guards the drain (which swaps
	// CLAUDE_CONFIG_DIR even on a bare, half-migrated dir), not only the unmount.
	// A live mount comes down through the idle chokepoint; a bare dir takes the
	// same gate without unmounting.
	if overlayMounted(dir) {
		busy, n, err := s.unmountIdle(ctx, dir)
		if busy {
			s.log.Printf("acct-%02d deferring legacy→mux migration: %d live session(s) on %s; relaunch them", a.ID, n, dir)
			return false
		}
		if err != nil {
			s.log.Printf("acct-%02d mux migration: force-unmount legacy mount %s: %v", a.ID, dir, err)
			return false
		}
		// Drop the holder-cache vouch the instant the mount is gone: a select
		// racing the drain must never launch onto the torn-down dir.
		s.holder.noteUnmounted(dir)
		s.log.Printf("acct-%02d tore down legacy per-dir mount for mux migration", a.ID)
	} else if busy, n := s.liveSessionGate(ctx, dir); busy {
		s.log.Printf("acct-%02d deferring legacy→mux migration: %d live session(s) on %s; relaunch them", a.ID, n, dir)
		return false
	}
	if err := s.drainDirForBridge(a, base, dir); err != nil {
		s.log.Printf("acct-%02d mux migration: %v", a.ID, err)
		return false
	}
	return true
}

// drainDirForBridge empties a legacy account dir so a bridge symlink can replace
// it: private files stranded in the bare mountpoint move to the backing root,
// shared entries claude wrote there move to base, and skip litter is cleared so
// clearAccountDirForBridge sees an empty dir. Anything left is unclassified state
// — Setup refuses it loudly (never destroyed here). A symlink or absent dir is a
// no-op (Setup's AtomicSymlink handles it).
func (s *Server) drainDirForBridge(a store.Account, base, dir string) error {
	fi, err := os.Lstat(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lstat account dir: %w", err)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return nil
	}
	spec := s.m.OverlaySpec()
	switch has, herr := fkoverlay.HasPrivateEntries(dir, spec); {
	case herr != nil:
		return fmt.Errorf("check underlay for stranded private files: %w", herr)
	case has:
		if err := fkoverlay.MovePrivateEntries(dir, fkoverlay.FusePrivateRoot(dir), spec); err != nil {
			return fmt.Errorf("sweep stranded private files into the backing dir: %w", err)
		}
		s.log.Printf("acct-%02d swept stranded private files from the legacy mountpoint into the backing dir", a.ID)
	}
	if err := fkoverlay.MoveSharedOrphans(dir, base, spec); err != nil {
		return fmt.Errorf("relocate shared orphans into base: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read account dir: %w", err)
	}
	for _, e := range entries {
		if spec.Skipped(e.Name()) {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	return nil
}

// healOutcome classifies one healFuse attempt.
type healOutcome int

const (
	healMounted             healOutcome = iota // the mirror is up
	healRetry                                  // transient holder condition; retry next poll
	healTCCBlocked                             // mount blocked pending the TCC grant; recorded; retry next poll
	healFallback                               // genuine mount failure; gated symlink fallback attempted
	healDeferredBusy                           // dead/wedged mirror left mounted under a live session; force-unmount would panic the kernel, so defer
	healDeferredUnmitigated                    // holder predates the NFS kernel-panic mitigations; wait for the cask upgrade, no breaker strike
)

// errSweepStranded marks a failure in mountFuse's pre-Setup sweep of stranded
// private files — distinct from a mount failure: Setup was never attempted, so
// it is not a mount verdict. healFuse routes it to healRetry, never the
// irreversible symlink fallback: a fallback never auto-reverts (the scheduler
// only re-heals fuse rows), and the collision that refused the sweep would
// refuse the symlink retreat the same way — so converting fixes nothing.
var errSweepStranded = errors.New("sweep stranded private files")

// errRemountBusy marks mountFuse refusing to tear down a dead/wedged mirror
// while a live claude session is still bound: force-unmounting a busy NFS
// mirror panics the kernel (nfs_vinvalbuf2: ubc_msync failed), so the remount
// defers. healFuse routes it to healDeferredBusy, which backs off WITHOUT a
// hazard strike so the wedged breaker can never fire on a busy mount.
var errRemountBusy = errors.New("remount refused: live sessions on the mirror")

// healFuse establishes a fuse account's mirror, classifying failures instead of
// blindly converting (see the switch): transient conditions and a mount pending
// the macOS volume-access grant retry next poll; only a genuine mount failure
// falls back to symlink, itself gated on the account being idle (see
// fallbackToSymlink). Used by the startup reconcile, the per-poll self-heal, and
// the heal loop; callers hold the account's poll claim.
func (s *Server) healFuse(ctx context.Context, a store.Account) healOutcome {
	err := s.mountFuse(ctx, a)
	switch {
	case err == nil:
		return healMounted
	case errors.Is(err, errRemountBusy):
		// Live session still bound; mountFuse left the dead/wedged mirror mounted
		// (see errRemountBusy). Defer WITHOUT a hazard strike.
		s.log.Printf("acct-%02d dead/wedged mirror left mounted under live session(s) — NOT force-unmounting (would panic the kernel); relaunch them: %v", a.ID, err)
		return healDeferredBusy
	case errors.Is(err, pool.ErrHolderUnmitigated):
		// The provider's mitigation gate refused to host a mirror on a
		// pre-mitigation holder (the nfs_vinvalbuf2 panic vector). A benign wait
		// for the cask upgrade, not a kernel hazard: defer without a breaker
		// strike, and never demote the row — the remount resumes on its own once
		// the holder is upgraded.
		s.log.Printf("acct-%02d remount refused, retrying next poll: %v", a.ID, err)
		return healDeferredUnmitigated
	case errors.Is(err, mountd.ErrHolderUnavailable), errors.Is(err, mountd.ErrBusy):
		// Setup already attempts a lazy (re)spawn and launchd owns the respawn
		// policy, so there is nothing more to do this poll.
		s.log.Printf("acct-%02d mount deferred (holder unavailable or dir busy), retrying next poll: %v", a.ID, err)
		return healRetry
	case errors.Is(err, overlay.ErrUnmountWedged):
		// A wedged unmount says nothing about whether a fresh mount would work,
		// and the fallback's ConvertOverlay would hit the same wedge — converting
		// here would fail closed every poll for nothing.
		s.log.Printf("acct-%02d mount blocked by a wedged unmount, retrying next poll: %v", a.ID, err)
		return healRetry
	case errors.Is(err, fkoverlay.ErrAccountDirOccupied):
		// The account dir holds unclassified state where the bridge symlink must go
		// (a half-drained legacy dir). Never destroy it and never demote — the same
		// state would block the symlink retreat; retry loudly until a human clears it.
		s.log.Printf("acct-%02d mux migration blocked: account dir holds unclassified state where the bridge symlink belongs; refusing to clobber it, retrying next poll: %v", a.ID, err)
		return healRetry
	case errors.Is(err, mountd.ErrMuxMismatch):
		// Registry state: a mux subtree could not join its shared native mount
		// (unmount-then-retry). Never a mount verdict — retry, never demote to
		// symlink. Unreachable in cc-pool's steady state (one MuxRoot, uniform
		// options), so a loud retry is the safe classification if it ever fires.
		s.log.Printf("acct-%02d mux subtree could not join the shared mount (registry state), retrying next poll: %v", a.ID, err)
		return healRetry
	case errors.Is(err, mountd.ErrForeignMount):
		// A mux Setup hit a foreign mount at the SHARED ROOT — a carcass a dead
		// holder left that the fresh, empty-registry holder does not own and mountFuse's
		// per-dir Teardown cannot clear (MNT_FORCE is a root-only operation). Registry
		// state, never a mount verdict: sweep the orphaned root (pool-idle-gated) so the
		// next heal re-mounts and re-attaches — NEVER demote the whole pool to symlink.
		fuse, ferr := s.fuseAccounts()
		if ferr != nil {
			s.log.Printf("acct-%02d foreign-root sweep: list accounts: %v; retrying next poll", a.ID, ferr)
			return healRetry
		}
		if s.sweepMuxRootIdle(ctx, fuse) {
			s.log.Printf("acct-%02d cleared a foreign carcass at the shared mux root; the heal loop will re-mount and re-attach", a.ID)
		} else {
			s.log.Printf("acct-%02d mux setup blocked by a foreign carcass at the shared root, retrying next poll: %v", a.ID, err)
		}
		return healRetry
	case errors.Is(err, mountd.ErrUnknownClass):
		// Forward skew: a newer holder sent an error class this daemon predates.
		// Unclassifiable is not a mount verdict — retry, loudly, until the daemon
		// is upgraded.
		s.log.Printf("acct-%02d mount failed with an error class this daemon does not recognize (newer holder; upgrade the daemon), retrying next poll: %v", a.ID, err)
		return healRetry
	case errors.Is(err, overlay.ErrMountTimeout):
		// Timed out in a holder whose volume-access grant is already proven by an
		// earlier live mount — transient fuse-t slowness, not the TCC condition.
		s.log.Printf("acct-%02d fuse mount did not come up within the mount wait; retrying: %v", a.ID, err)
		return healRetry
	case errors.Is(err, overlay.ErrMountFailed):
		// A hard mount(2) rejection (fuse-t not installed/loadable, the kernel
		// refusing) — NEVER the TCC grant, so don't wait: retreat to symlink on
		// this first heal. The real cause is in the holder log.
		s.log.Printf("acct-%02d fuse mount rejected outright (not a TCC grant); falling back to symlink — see %s: %v", a.ID, pool.MountHolderLogPath(), err)
		s.fallbackToSymlink(ctx, a)
		return healFallback
	case errors.Is(err, overlay.ErrMountNotLive):
		// a is provably a valid fuse row (it reached healFuse via a fuse
		// overlay_kind), so Parse cannot fail; carry the backend so status renders
		// the right grant pane without cc-pool naming nfs/fskit.
		backend, _ := fkoverlay.Parse(a.OverlayKind)
		s.holder.recordTCC(err.Error(), backend)
		s.log.Printf("acct-%02d fuse mount blocked pending the macOS volume-access grant, retrying next poll: %v", a.ID, err)
		return healTCCBlocked
	case errors.Is(err, errSweepStranded):
		// Sweep failed before Setup (not a mount verdict; see errSweepStranded);
		// retry next poll, loudly.
		s.log.Printf("acct-%02d mount deferred (could not sweep stranded private files before mounting), retrying next poll: %v", a.ID, err)
		return healRetry
	case errors.Is(err, mountd.ErrContentUnavailable):
		// Holder refused only because the content bridge was unreachable (daemon
		// mid-restart, not a mount verdict). A symlink demotion would be
		// irreversible, so defer until the bridge is back.
		s.log.Printf("acct-%02d mount deferred (content bridge unavailable, daemon mid-restart?), retrying next poll: %v", a.ID, err)
		return healRetry
	default:
		s.log.Printf("acct-%02d mount failed; attempting gated symlink fallback: %v", a.ID, err)
		s.fallbackToSymlink(ctx, a)
		return healFallback
	}
}

// mountFuse establishes a fuse account's mirror through the resolved provider
// in a fixed order: a dead mount (fails Health) comes down first — never sweep
// or mount through one; then private files stranded in the underlay by a
// conversion killed before its row flip are swept into the backing dir (mounting
// over them would shadow the account's identity); then the provider mounts. A
// dead HOLDER's carcass registers as foreign on Setup, and a registry row
// pinning a DIFFERENT base (ErrBaseMismatch) — both registry state, not mount
// verdicts — get one unmount-then-retry. The Kind fence guards against
// wrong-kind injected fakes.
func (s *Server) mountFuse(ctx context.Context, a store.Account) error {
	prov := s.overlayForRow(a)
	if prov == nil || !prov.Backend().IsFuse() {
		return fmt.Errorf("no fuse provider resolved for acct-%02d; refusing to mount through it", a.ID)
	}
	base, dir := pool.ClaudeDir(), a.ConfigDir
	// Health is shallow (no deep read on the poll hot path), so a partial wedge
	// (shallow-alive, bulk reads hang) passes it — the deep-probe verdict
	// (deepWedged) catches that. A dead/wedged mirror must come down before
	// re-establishing, and that is session-breaking either way, so gate on a live
	// session first. A legacy per-dir mount (dir is a real kernel mountpoint) needs
	// an explicit kernel teardown, and force-unmounting a busy NFS mirror panics
	// the kernel. A mux subtree (dir is the bridge symlink) is re-established by the
	// holder's idempotent AddMount (drain → detach → re-attach, no kernel unmount),
	// so the Setup below suffices — but that still yields EIO/ENOENT on a live
	// session's open files, so it keeps the same gate, now session-breaking rather
	// than kernel-hazardous.
	legacy := overlayMounted(dir)
	if (legacy || pool.IsBridgeSymlink(dir)) && (prov.Health(base, dir) != nil || s.holder.deepWedged(dir)) {
		if busy, _ := s.liveSessionGate(ctx, dir); busy {
			return errRemountBusy
		}
		// Detach before re-establishing — for BOTH shapes. A legacy per-dir mount
		// needs a kernel force-unmount; a mux subtree needs a kernel-free logical
		// detach, and WITHOUT it the holder's idempotent AddMount sees the subtree
		// still registered+shallow-live and returns OK without re-attaching, so a
		// deep wedge would never clear (noteMounted would drop the verdict over an
		// unchanged mount) and the breaker/native recovery could never fire. mux-mode
		// Teardown is the kernel-free detach; a persisting wedge then surfaces as an
		// error and advances the breaker toward escalateWedgedRow.
		if err := prov.Teardown(base, dir); err != nil {
			return fmt.Errorf("clear dead mount before remounting: %w", err)
		}
		s.log.Printf("acct-%02d cleared a dead mount before remounting", a.ID)
	}
	err := s.sweepAndMount(prov, a, base, dir)
	if errors.Is(err, mountd.ErrForeignMount) || errors.Is(err, mountd.ErrBaseMismatch) {
		// The foreign/mismatched carcass clear also force-unmounts; gate it the
		// same way so a live session bound to the carcass never triggers the
		// kernel-panicking force-unmount.
		if busy, _ := s.liveSessionGate(ctx, dir); busy {
			return errRemountBusy
		}
		if terr := prov.Teardown(base, dir); terr != nil {
			return fmt.Errorf("clear foreign mount: %w", terr)
		}
		err = s.sweepAndMount(prov, a, base, dir)
	}
	if err != nil {
		return err
	}
	// Update the holder cache in place so a select landing before the next
	// poll's refresh trusts the fresh mount.
	s.holder.noteMounted(dir)
	return nil
}

// sweepAndMount is one sweep+Setup attempt for mountFuse: with no mount in
// the way, private files stranded in the underlay are swept into the backing
// dir, then the provider mounts.
func (s *Server) sweepAndMount(prov fkoverlay.Provider, a store.Account, base, dir string) error {
	// The sweep reads the underlay dir directly. Skip it when dir is a bridge
	// symlink into the shared mount — following it would traverse the live mirror
	// (and could hang on a wedge), and a mux account's private files live in the
	// backing root, never under the bridged path. A legacy real dir still gets swept.
	if !overlayMounted(dir) && !pool.IsBridgeSymlink(dir) {
		spec := s.m.OverlaySpec()
		switch has, err := fkoverlay.HasPrivateEntries(dir, spec); {
		case err != nil:
			return fmt.Errorf("%w: check underlay: %w", errSweepStranded, err)
		case has:
			if err := fkoverlay.MovePrivateEntries(dir, fkoverlay.FusePrivateRoot(dir), spec); err != nil {
				return fmt.Errorf("%w: move into backing dir: %w", errSweepStranded, err)
			}
			s.log.Printf("acct-%02d swept private files from the mount underlay into the backing dir", a.ID)
		}
	}
	return prov.Setup(base, dir)
}

// fallbackToSymlink converts an account to symlink after a genuine mount
// failure so its dir is usable again. ConvertOverlay force-unmounts the dir
// first, so the conversion is gated like a migrate — claim first, scan second:
// beginConvertUnderPoll refuses over a pending select reservation, and once the
// converting claim is set tryReserve refuses for the whole conversion, so no
// select can land between the idle check and the force-unmount. Never convert
// blind (a failed scan can't rule out a live claude on the dir) and never under
// a live session — defer instead. ConvertOverlay also moves private files back
// out of the fuse backing dir, restoring the account's .claude.json identity.
// Callers must hold the account's poll claim.
func (s *Server) fallbackToSymlink(ctx context.Context, a store.Account) {
	if !s.cl.ownHeld(a.ID) {
		s.log.Printf("acct-%02d deferring fuse→symlink fallback: reserved by a pending select or already converting", a.ID)
		return
	}
	defer s.cl.disownConvert(a.ID)
	sessions, err := s.scan(ctx)
	if err != nil {
		s.log.Printf("acct-%02d deferring fuse→symlink fallback: session scan: %v", a.ID, err)
		return
	}
	if n := procscan.CountByConfigDir(sessions, a.ConfigDir); n > 0 {
		s.log.Printf("acct-%02d deferring fuse→symlink fallback: %d live session(s)", a.ID, n)
		return
	}
	if _, err := s.m.ConvertOverlay(ctx, a, fkoverlay.BackendSymlink); err != nil {
		s.log.Printf("acct-%02d symlink fallback: %v", a.ID, err)
		return
	}
	// The mirror is down and the row is symlink; drop the cache entry so
	// HolderStatus.Mounts stops counting it.
	s.holder.noteUnmounted(a.ConfigDir)
	s.log.Printf("acct-%02d fell back to symlink after a genuine mount failure", a.ID)
}
