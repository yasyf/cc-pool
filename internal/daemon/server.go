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
	"syscall"
	"time"

	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/fusekit/mountd"
)

// reservationTTL is how long a select-reservation suppresses re-picking the
// same account before the real claude process is visible to procscan.
const reservationTTL = 30 * time.Second

// preflightTimeout bounds a best-effort preflight refresh so shutdown is never
// blocked on a slow network refresh.
const preflightTimeout = 8 * time.Second

// defaultEvictTimeout bounds how long a starting daemon waits for a
// version-skewed holder to release the socket after being told to step down.
const defaultEvictTimeout = 5 * time.Second

// overlayMounted is a test seam over the kernel mountpoint check; production
// never overrides it. overlay.Mounted reads the kernel mount table via
// Getfsstat (non-blocking, cannot wedge on a dead fuse-t mirror), so there is
// no fail-direction to fold — it answers membership directly. Every call site
// (mountReady's non-fuse arm, sweepAndMount, mountFuse's pre-clear, reconcile's
// stale-clear) consumes that plain bool.
var overlayMounted = overlay.Mounted

// Server is the running daemon.
type Server struct {
	m            *pool.Manager
	socket       string
	holderSocket string // mount-holder socket; tests point it at a fake holder
	snapshot     string // status mirror path; tests point it into a temp dir
	log          *log.Logger

	// holder caches mount-holder truth (reachability, version, per-dir mount
	// liveness) for the select path and status; primed at serve start,
	// refreshed at startup reconcile and once per scheduler poll, and lazily
	// refreshed (rate-limited) when a select hits a fuse dir it cannot vouch
	// for.
	holder holderState

	// evictTimeout bounds the wait for a skewed holder to release the socket.
	evictTimeout time.Duration

	// triggerShutdown cancels serve's context, ending the daemon. It is set once
	// in serve before the accept loop starts; the go-statement that spawns each
	// handler establishes the happens-before, so handlers read it without a lock.
	triggerShutdown context.CancelFunc

	// wg tracks every daemon goroutine (scheduler, connection handlers,
	// preflight refreshes); serve Waits on it before Run's deferred m.Close()
	// closes the database under them.
	wg sync.WaitGroup

	mu           sync.Mutex
	reservations map[int]time.Time // accountID -> reserved-at
	converting   map[int]bool      // accountID -> overlay conversion in flight
	polling      map[int]bool      // accountID -> scheduler/reconcile owns the dir this iteration
	replacing    bool              // a holder replacement is in flight; fences NEW conversions (see beginReplace)
	rlStreak     map[int]int       // accountID -> consecutive 429 count
	// authStreak counts consecutive unrecovered 401s per account; at
	// needsLoginAfter the account's poll backs off to needsLoginPollInterval
	// (it is NOT flagged — only a definitive ErrNeedsLogin flags). lastAuthAttempt
	// stamps the last sample of a backed-off or flagged account so it stops
	// 401-spamming every poll. Both are scheduler-goroutine-local, like rlStreak
	// — no lock.
	authStreak      map[int]int
	lastAuthAttempt map[int]time.Time

	// fuseGateFn overrides the migrate handler's fuse-capability gate; nil
	// means the real check (FuseBuilt + probe mount). Tests inject outcomes
	// alongside Manager.OverlayFor.
	fuseGateFn func() string

	// migrateBudget bounds one migrate request's conversion work; zero means
	// defaultMigrateBudget. Tests shrink it to pin the out-of-time path.
	migrateBudget time.Duration

	// scanSessions overrides procscan.Scan for the fuse→symlink fallback gate;
	// nil means the real scan. Tests inject session lists and scan failures.
	scanSessions func(context.Context) ([]procscan.Session, error)

	// startedAt is when this daemon began serving (stamped in Run). The
	// skew-replace gate requires uptime ≥ reservationTTL: a freshly-started
	// daemon's reservation map is empty while a ≤30s-old select may not have
	// exec'd its claude yet.
	startedAt time.Time

	// holderLog receives a spawned mount holder's stdout/stderr.
	holderLog string

	// superviseInterval is the holder supervision cadence; zero means
	// defaultSuperviseInterval. Tests shrink it.
	superviseInterval time.Duration

	// holderGoneWait bounds waiting for a retiring holder to release its
	// socket after acking Shutdown; zero means defaultHolderGoneWait. Tests
	// shrink it.
	holderGoneWait time.Duration

	// spawnHolder overrides holder spawning; nil means pool.SpawnHolder,
	// which only the fuse build can perform. Tests inject outcomes — which
	// also lets pure builds exercise the supervision flow.
	spawnHolder func(socket, logPath string, timeout time.Duration) error

	// killHolderPeer overrides the wedged-holder escape hatch; nil means
	// peerpid.KillPid, which resolves the socket's current peer and signals
	// it only when it matches wantPID — identity check and kill share one
	// resolution, so a successor that bound the socket in between can never
	// be shot. Tests inject it so no real process is ever signalled.
	killHolderPeer func(socket string, wantPID int) (int, error)

	// peerPID overrides peer-pid lookup on the holder socket — the identity
	// check gating the wedged-holder kill (the kill must land only on the
	// exact process that was gated, never a successor that bound the socket
	// in between); nil means peerpid.PeerPID. Tests inject it so no real
	// socket peer is ever resolved.
	peerPID func(socket string) (int, error)

	// sup is superviseHolder's tick-local state (respawn backoff, transition
	// logging); only the supervise goroutine touches it.
	sup supervisor
}

// Run is the entry point for `cc-pool daemon`. It blocks until the process
// is signalled.
func Run(ctx context.Context) error {
	m, err := pool.Open()
	if err != nil {
		return err
	}
	defer m.Close()

	s := &Server{
		m:               m,
		socket:          pool.SocketPath(),
		holderSocket:    pool.MountsSocketPath(),
		holderLog:       pool.MountHolderLogPath(),
		snapshot:        pool.StatusSnapshotPath(),
		log:             log.New(os.Stderr, "[cc-pool] ", log.LstdFlags),
		evictTimeout:    defaultEvictTimeout,
		startedAt:       time.Now(),
		reservations:    map[int]time.Time{},
		converting:      map[int]bool{},
		polling:         map[int]bool{},
		rlStreak:        map[int]int{},
		authStreak:      map[int]int{},
		lastAuthAttempt: map[int]time.Time{},
	}
	// Make overlay's crash-repair conflict resolution observable: every
	// private-file collision the migrate path reconciles (in the mount sweep,
	// either convert direction, or a stranded-private heal) is logged here.
	// Assigned once, before serve spawns any worker.
	overlay.ResolvedConflictLogf = s.log.Printf
	return s.serve(ctx)
}

// detectClaudeVersion runs `claude --version` (best-effort) to stamp the UA.
func detectClaudeVersion() string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "claude", "--version").Output()
	if err != nil {
		return ""
	}
	// Output looks like "2.1.166 (Claude Code)"; take the leading version token.
	fields := strings.Fields(string(out))
	if len(fields) > 0 {
		return fields[0]
	}
	return ""
}

func (s *Server) serve(ctx context.Context) error {
	ln, lock, err := s.listen()
	if err != nil {
		return err
	}
	// The flock on lock is the cross-process guarantee that only this daemon
	// may stale-check, remove, bind, or unlink the socket path. It must
	// outlive the listener (Close releases it), so this defer is registered
	// first and runs last.
	defer lock.Close()
	// closeListener unlinks the socket exactly once. *net.UnixListener.Close
	// unlinks the socket file and is NOT idempotent: a second Close (the late
	// deferred one, after a slow teardown) would delete a successor daemon's
	// freshly-bound socket. The sync.Once pins the unlink to the first close, at
	// ctx-cancel time. No explicit os.Remove for the same reason.
	var closeOnce sync.Once
	closeListener := func() { closeOnce.Do(func() { _ = ln.Close() }) }
	defer closeListener()

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// stop cancels ctx, so it doubles as the over-the-socket shutdown trigger
	// (OpShutdown). Set before the accept loop spawns any handler.
	s.triggerShutdown = stop

	s.log.Printf("daemon %s started; socket=%s", version.String(), s.socket)

	// One startup goroutine, off the accept path so Health is responsive from
	// the first instant, runs strictly in order:
	//  1. Prime the holder cache. The socket above is already accepting
	//     selects, and fuse readiness keys on the cache, so nothing heavy may
	//     stand between a cold start and the first refresh — a select-vs-prime
	//     race would otherwise refuse every fuse account while the detached
	//     holder serves the mounts fine. (mountReady's lazy refresh covers the
	//     residual bind→prime gap.)
	//  2. Detect the claude version — `claude --version` is a heavy Node CLI
	//     with up to a 3s timeout, kept off the pre-bind path so a slow probe
	//     can't make a freshly-started daemon look "not responding" to a
	//     waiting `ccp add`. It only stamps the OAuth User-Agent, whose sole
	//     consumer is the scheduler's first poll.
	//  3. Reconcile overlays, then start holder supervision, then run the
	//     scheduler. These stay sequential in one goroutine — not bare ones —
	//     because reconcileOverlays must finish before either the
	//     supervisor's first tick or the scheduler's first poll, both of
	//     which can touch fuse Setup (a check-then-act on the same
	//     mountpoint).
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.holder.refresh(s.holderClient())
		oauth.SetUserAgentVersion(detectClaudeVersion())
		s.reconcileOverlays(ctx)
		// Supervision starts only after the startup reconcile so it never
		// races the initial mounts; from here it owns crash→respawn→remount
		// and the idle-gated replacement of a version-skewed holder. The
		// Add(1) runs inside this already-tracked goroutine, so the counter
		// is ≥1 and cannot race a zero-counter Wait.
		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.superviseHolder(ctx) }()
		s.scheduler(ctx)
	}()

	// Break the accept loop on shutdown.
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

// listen binds the unix socket with 0600 perms, evicting a version-skewed
// holder first (see evictHolder) and refusing only a live same-version peer.
//
// An exclusive flock on socket+".lock" — returned to serve, which holds it
// for the daemon's lifetime — makes the stale-check/remove/bind sequence
// single-entrant across processes, the same shape mountd's listen pins.
// Without it, two concurrently starting daemons (a launchd KeepAlive respawn
// racing a manual start or a brew-services kickstart) both pass evictHolder's
// health probe before either binds; the loser's os.Remove unlinks the
// winner's freshly-bound socket, the invisible daemon keeps its scheduler and
// holder supervisor running with in-memory reservations nobody can see, and
// its *net.UnixListener.Close unlinks by PATH at exit — deleting the visible
// daemon's live socket too. The lock file itself is never removed: unlinking
// a held lock file would let a third daemon flock a fresh inode while the old
// inode's lock is still held, reopening the race.
func (s *Server) listen() (net.Listener, *os.File, error) {
	// Only the socket's parent dir is needed here (in production that is the
	// state dir); deriving it from s.socket keeps tests off the real ~/.cc-pool.
	if err := os.MkdirAll(filepath.Dir(s.socket), 0o700); err != nil {
		return nil, nil, fmt.Errorf("ensure socket dir: %w", err)
	}
	lock, err := s.flockSocket()
	if err != nil {
		return nil, nil, err
	}
	// The lock is ours, but a live peer may predate the lock discipline (an
	// old-version daemon holds no flock): evict or refuse it exactly as before.
	if err := s.evictHolder(); err != nil {
		lock.Close()
		return nil, nil, err
	}
	_ = os.Remove(s.socket) // stale socket: the lock is ours and any live peer was evicted
	ln, err := net.Listen("unix", s.socket)
	if err != nil {
		lock.Close()
		return nil, nil, err
	}
	if err := os.Chmod(s.socket, 0o600); err != nil {
		ln.Close()
		lock.Close()
		return nil, nil, err
	}
	return ln, lock, nil
}

// flockSocket takes the daemon's lifetime lock on socket+".lock". A held lock
// belongs to a flock-aware peer: a same-version peer is a genuine double
// start and is refused; a version-skewed peer is evicted (its death releases
// its flock) and the lock polled for the evict bound; a peer that answers no
// health probe may still be mid-start (post-flock, pre-bind) and is refused —
// launchd retries against whatever it becomes.
func (s *Server) flockSocket() (*os.File, error) {
	lock, err := os.OpenFile(s.socket+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open daemon lock: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
		return lock, nil
	}
	c := &Client{socket: s.socket}
	resp, herr := c.Health()
	if herr != nil {
		lock.Close()
		return nil, fmt.Errorf("another cc-pool daemon owns %s.lock but does not answer health yet (it may still be starting); refusing to start", s.socket)
	}
	if resp.Version == version.String() {
		lock.Close()
		return nil, errors.New("another cc-pool daemon at the same version is already running")
	}
	if err := s.evictPeer(c, resp.Version); err != nil {
		lock.Close()
		return nil, err
	}
	// Eviction is observed at socket death (WaitGone), but the peer's flock is
	// released only at its process exit — serve's lock Close is the last defer,
	// after the goroutine drain, and a freshly started evictee (the launchd
	// KeepAlive race this path exists for) can spend seconds there in
	// non-cancellable startup work. Poll the lock for the same bound WaitGone
	// used instead of failing the start on the first refusal.
	deadline := time.Now().Add(s.evictTimeout)
	for {
		err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return lock, nil
		}
		if time.Now().After(deadline) {
			lock.Close()
			return nil, fmt.Errorf("daemon lock still held %s after evicting the skewed peer: %w", s.evictTimeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// evictHolder makes way when the socket is already held. A holder at our own
// version is a genuine double-start and is refused. A version-skewed holder —
// an orphan launchd no longer tracks (a detached EnsureRunning spawn, or a
// pre-upgrade image KeepAlive held), which `brew services stop` cannot kill —
// is told to step down (OpShutdown) and waited out so we can rebind. Only the
// non-holder ever evicts, and only on a version mismatch, so two daemons can
// never mutually evict each other.
func (s *Server) evictHolder() error {
	c := &Client{socket: s.socket}
	resp, err := c.Health()
	if err != nil {
		return nil // no live holder: stale or missing socket
	}
	if resp.Version == version.String() {
		return errors.New("another cc-pool daemon at the same version is already running")
	}
	return s.evictPeer(c, resp.Version)
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
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	var req Request
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		writeResp(conn, Response{OK: false, Error: "bad request: " + err.Error()})
		return
	}
	if req.Op == OpMigrate {
		// A migrate legitimately outlives the 10s deadline: a probe mount plus
		// up to an 8s mount wait (and a bounded rollback) per account. Stay
		// under the client's 150s so the server, not a dead socket, reports
		// the outcome.
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
	case OpShutdown:
		return s.handleShutdown()
	default:
		return Response{OK: false, Error: "unknown op: " + string(req.Op)}
	}
}

// handleShutdown replies OK, then cancels serve's context so this instance steps
// down and releases the socket — the only eviction that works on an orphan
// launchd no longer tracks. Cancelling the ctx closes the listener, never this
// live connection, so the OK reply (written by handle after dispatch returns)
// still lands; wg.Wait then drains this handler normally. Idempotent on repeats.
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
	return Response{OK: true, Version: version.String(), Accounts: accts, Holder: s.holder.wireStatus()}
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

	// Forced account.
	if req.Account != nil {
		for _, sn := range snaps {
			if sn.Account.ID == *req.Account {
				if !s.mountReady(sn.Account) {
					if sn.Account.OverlayKind == string(overlay.KindFuse) {
						return Response{OK: false, Error: fmt.Sprintf("acct-%02d's fuse mount is not up yet; retry shortly", sn.Account.ID)}
					}
					return Response{OK: false, Error: fmt.Sprintf("acct-%02d's dir is unexpectedly a mountpoint (wedged unmount?); see `ccp doctor` and the daemon log", sn.Account.ID)}
				}
				if !s.probeWinnerReady(sn.Account) {
					return Response{OK: false, Error: fmt.Sprintf("acct-%02d's fuse mirror is wedged; the daemon is remounting it — retry shortly", sn.Account.ID)}
				}
				if !s.tryReserve(sn.Account.ID) {
					return Response{OK: false, Error: fmt.Sprintf("acct-%02d is migrating overlays; retry shortly", sn.Account.ID)}
				}
				if !req.NoMark && req.PID > 0 {
					if _, err := s.m.Store.OpenSession(sn.Account.ID, req.PID, sn.Account.ConfigDir, req.Cwd, time.Now()); err != nil {
						s.log.Printf("open session for acct-%02d pid %d: %v", sn.Account.ID, req.PID, err)
					}
				}
				s.recordSticky(req.Cwd, sn.Account.ID)
				id := sn.Account.ID
				return Response{OK: true, Dir: sn.Account.ConfigDir, SelectedID: &id,
					Remaining5h: sn.Remaining5h, Remaining7d: sn.Remaining7d, HasUsage: sn.HasUsage}
			}
		}
		return Response{OK: false, Error: fmt.Sprintf("account %d not found", *req.Account)}
	}

	// Reconcile session rows against reality before consulting the pin: a
	// claude that just exited must read as warm (bind), not live (hold), and
	// pollOnce's ~3.5-minute cadence is too coarse for a quick resume.
	if sessions, err := procscan.Scan(ctx); err == nil {
		if _, cerr := s.m.Store.CloseDeadSessions(procscan.AlivePIDs(sessions), time.Now()); cerr != nil {
			s.log.Printf("close dead sessions: %v", cerr)
		}
	}

	// An account mid-conversion or whose mirror is not mounted yet (daemon
	// still establishing mounts after startup, or a failed mount pending
	// fallback) cannot serve a session — its config dir is not in a usable
	// shape. Exclude rather than penalize.
	usable := make([]pool.Snapshot, 0, len(snaps))
	for _, sn := range snaps {
		if s.isConverting(sn.Account.ID) || !s.mountReady(sn.Account) {
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
	// Deep-probe the winner before handing it to a session — the only probe of
	// an idle mirror (the periodic supervisor probe skips session-less mounts).
	// A wedge marks it (excluding it from the retry's ranking) and refuses; the
	// client retries onto a healthy account, whose own probe runs then.
	if !s.probeWinnerReady(best.Account) {
		return Response{OK: false, Error: fmt.Sprintf("acct-%02d's fuse mirror is wedged; the daemon is remounting it — retry shortly", best.Account.ID)}
	}
	if !req.NoMark {
		if !s.tryReserve(best.Account.ID) {
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
	// Preflight refresh the winner if idle and expiring soon (best-effort).
	// The Add(1) runs inside an already-tracked handler goroutine, so the
	// counter is ≥1 here and can never race a zero-counter Wait.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		pctx, cancel := context.WithTimeout(ctx, preflightTimeout)
		defer cancel()
		if err := s.m.PreflightRefresh(pctx, best.Account); err != nil {
			s.log.Printf("acct-%02d preflight refresh: %v", best.Account.ID, err)
		}
	}()
	resp := Response{OK: true, Dir: best.Account.ConfigDir, SelectedID: &id,
		Sticky:      outcome == pool.StickyBind,
		Remaining5h: best.Remaining5h, Remaining7d: best.Remaining7d, HasUsage: best.HasUsage,
		ExhaustedFallback: fallback, ExtraEnabled: best.ExtraEnabled}
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
		if !r.Available && !(fallback && !r.RateLimited) {
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

// handleCheckin closes sessions for a pid and adopts any rotated token.
func (s *Server) handleCheckin(ctx context.Context, req Request) Response {
	sessions, err := s.m.Store.ListActiveSessions()
	if err != nil {
		return Response{OK: false, Error: err.Error()}
	}
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
		}
	}
	return Response{OK: true}
}

// tryReserve records a short-lived reservation for an account, refusing while
// an overlay conversion holds it (the conversion is about to remake the dir a
// launching claude would land in).
func (s *Server) tryReserve(id int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.converting[id] {
		return false
	}
	s.reservations[id] = time.Now()
	return true
}

// beginConvert claims an account for overlay conversion iff it has no live
// reservation, no conversion already in flight, the scheduler/reconcile is
// not mid-iteration on its dir, and no holder replacement is in flight (a
// conversion toward fuse would Mount through the very holder being retired —
// see beginReplace). The check-and-claim is one critical section, closing the
// race against tryReserve and beginPoll; the converting flag — not the mutex
// — then owns the account across the conversion's I/O, the same way
// reservations bridge select→spawn.
func (s *Server) beginConvert(id int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.replacing {
		return false
	}
	if t, ok := s.reservations[id]; ok && time.Since(t) <= reservationTTL {
		return false
	}
	if s.converting[id] || s.polling[id] {
		return false
	}
	if s.converting == nil {
		s.converting = map[int]bool{}
	}
	s.converting[id] = true
	return true
}

// beginConvertUnderPoll claims an account for an overlay conversion run from
// inside a poll iteration (the fuse→symlink fallback) iff it has no live
// reservation, no conversion already in flight, and no holder replacement in
// flight. Unlike beginConvert it tolerates the caller's own poll claim —
// healFuse runs under one — which is what makes the fallback claim-atomic
// against select: once converting is set, tryReserve refuses for the whole
// ConvertOverlay, closing the gate→convert window a snapshot check would
// leave open. Callers must hold the account's poll claim (or otherwise be the
// dir's sole owner) so two conversions can never interleave.
func (s *Server) beginConvertUnderPoll(id int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.replacing {
		return false
	}
	if t, ok := s.reservations[id]; ok && time.Since(t) <= reservationTTL {
		return false
	}
	if s.converting[id] {
		return false
	}
	if s.converting == nil {
		s.converting = map[int]bool{}
	}
	s.converting[id] = true
	return true
}

// endConvert releases a conversion claim.
func (s *Server) endConvert(id int) {
	s.mu.Lock()
	delete(s.converting, id)
	s.mu.Unlock()
}

// beginReplace claims every given fuse account for a holder replacement and
// fences NEW conversions pool-wide, in one critical section. It refuses while
// ANY conversion is in flight anywhere — a symlink→fuse migrate holds its
// converting claim on a row still reading symlink (the row flips at the end)
// and is about to Mount through the holder being retired, so per-fuse-row
// checks cannot see the dangerous direction — and while any given account
// holds a live reservation or is mid-poll. Once claimed, tryReserve refuses
// every fuse row and beginConvert/beginConvertUnderPoll refuse pool-wide
// until endReplace: no select can land on a dir while the retiring holder
// sweeps its mirror out from under it (the sweep runs BEFORE the Shutdown
// reply, so the unprotected window would otherwise span the whole sweep), and
// no conversion can start against a holder mid-replacement. Returns "" on
// success, else the blocking reason; on refusal nothing is claimed.
func (s *Server) beginReplace(ids []int) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.converting) > 0 {
		return "an overlay conversion is in flight"
	}
	for _, id := range ids {
		if t, ok := s.reservations[id]; ok && time.Since(t) <= reservationTTL {
			return fmt.Sprintf("acct-%02d is reserved by a pending select", id)
		}
		if s.polling[id] {
			return fmt.Sprintf("acct-%02d is mid-poll", id)
		}
	}
	if s.converting == nil {
		s.converting = map[int]bool{}
	}
	for _, id := range ids {
		s.converting[id] = true
	}
	s.replacing = true
	return ""
}

// endReplace releases a holder replacement's per-account claims and the
// pool-wide conversion fence.
func (s *Server) endReplace(ids []int) {
	s.mu.Lock()
	for _, id := range ids {
		delete(s.converting, id)
	}
	s.replacing = false
	s.mu.Unlock()
}

// isConverting reports whether an overlay conversion holds the account.
func (s *Server) isConverting(id int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.converting[id]
}

// beginPoll claims an account for one scheduler/reconcile iteration — the
// Sync/Setup/fallback and refresh work that must never interleave with a
// conversion's move/teardown/mount sequence. Unlike converting, a poll claim
// does not hide the account from select (sessions can land on a dir being
// health-checked); it only excludes conversions, two-sidedly with
// beginConvert. The claim — not the mutex — owns the account across the
// iteration's I/O.
func (s *Server) beginPoll(id int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.converting[id] || s.polling[id] {
		return false
	}
	if s.polling == nil {
		s.polling = map[int]bool{}
	}
	s.polling[id] = true
	return true
}

// endPoll releases a poll claim.
func (s *Server) endPoll(id int) {
	s.mu.Lock()
	delete(s.polling, id)
	s.mu.Unlock()
}

// mountReady reports whether an account's overlay can serve a session right
// now. A fuse row is ready iff the holder cache vouches for a live mirror at
// its dir (reachable holder + Live in its last List) — cached kernel truth
// with no filesystem touch, because an lstat through a dead fuse-t NFS mount
// can hang the select path; in particular a dead holder's carcass (still a
// mountpoint locally) is never trusted. When the cache cannot vouch, one
// rate-limited refresh (bounded socket RPC, still no filesystem touch) picks
// up truth the poll cadence misses: a select racing the startup prime, and a
// mirror `ccp add` just mounted from the CLI process. A non-fuse row needs
// the dir NOT mounted — a live mountpoint under a symlink row is the wreckage
// of an aborted rollback (wedged unmount), where the dir serves a mirror
// whose private backing no longer holds the account's identity; lstat on a
// plain dir is safe.
func (s *Server) mountReady(a store.Account) bool {
	if a.OverlayKind == string(overlay.KindFuse) {
		if !s.holder.ready(a.ConfigDir) {
			s.holder.refreshIfStale(s.holderClient())
		}
		return s.holder.ready(a.ConfigDir)
	}
	return !overlayMounted(a.ConfigDir)
}

// probeWinnerReady deep-probes a chosen fuse account's mirror at select time —
// right before it is handed to a session — and reports whether it is safe to
// assign. This is the ONLY probe of an IDLE mirror: the periodic supervisor
// probe skips mounts with no live session, so an idle mirror's partial wedge
// (shallow-alive, bulk reads hang) would otherwise go undetected until a
// session landed on it and hung. A live wedge is force-marked wedged — so
// selection excludes it AND the supervisor remounts it within a tick — and
// reads not-ready; the caller refuses the select and the client retries (onto
// a healthy account, whose own probe runs on that retry, or the same one once
// the supervisor has remounted it). It is bounded by the 5s deep-probe timeout
// — under the select handler's 10s connection deadline — so it never remounts
// inline (a teardown+remount can take far longer); the supervisor owns the
// heal. Non-fuse accounts and healthy or pre-probe (ErrProbeMissing) mirrors
// read ready. A single observed wedge is enough (no debounce): an idle mirror
// about to serve a NEW session has no live session a false positive could
// orphan.
func (s *Server) probeWinnerReady(a store.Account) bool {
	if a.OverlayKind != string(overlay.KindFuse) {
		return true
	}
	err := deepProbe(a.ConfigDir)
	if err != nil && !errors.Is(err, overlay.ErrProbeMissing) {
		s.holder.markDeepWedged(a.ConfigDir)
		s.log.Printf("acct-%02d mirror wedged at select (serves metadata but hangs reads); excluding it and letting the supervisor remount it — relaunch once it recovers: %v", a.ID, err)
		return false
	}
	if msg := s.holder.recordDeep(a.ConfigDir, err); msg != "" {
		s.log.Printf("%s", msg)
	}
	return true
}

// holderClient returns a client for the mount-holder socket.
func (s *Server) holderClient() *mountd.Client {
	return mountd.NewClient(s.holderSocket)
}

// scan resolves session scanning through the test seam; nil means
// procscan.Scan.
func (s *Server) scan(ctx context.Context) ([]procscan.Session, error) {
	if s.scanSessions != nil {
		return s.scanSessions(ctx)
	}
	return procscan.Scan(ctx)
}

// overlayFor resolves a kind through the Manager's injectable seam (tests fake
// the fuse provider); nil means pool.OverlayProviderFor.
func (s *Server) overlayFor(kind overlay.Kind) overlay.Provider {
	if s.m.OverlayFor != nil {
		return s.m.OverlayFor(kind)
	}
	return pool.OverlayProviderFor(kind)
}

// reservedCount returns the number of live reservations for an account.
func (s *Server) reservedCount(id int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.reservations[id]
	if !ok {
		return 0
	}
	if time.Since(t) > reservationTTL {
		delete(s.reservations, id)
		return 0
	}
	return 1
}

// rankWithReservations re-ranks snapshots with reservation penalties applied,
// returning the ranking plus a snapshot lookup by account id.
func (s *Server) rankWithReservations(snaps []pool.Snapshot) ([]score.Result, map[int]pool.Snapshot) {
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
			ActiveSessions: sn.ActiveSessions + s.reservedCount(sn.Account.ID),
			RateLimited:    sn.RateLimited,
			RefreshFailed:  sn.Stale && !sn.HasUsage,
			NeedsLogin:     sn.NeedsLogin,
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
			Components:         sn.Components,
		})
	}
	return out
}

// reconcileOverlays brings each account's on-disk overlay in line with its
// row at startup. It
// runs off the accept path; ctx is checked between accounts so a boot-time
// shutdown doesn't block wg.Wait for the full mount timeout of a slow account.
func (s *Server) reconcileOverlays(ctx context.Context) {
	// Prime the holder cache before any per-account decision: mountReady (and
	// so every select racing this reconcile) keys fuse readiness on it.
	s.holder.refresh(s.holderClient())
	accts, err := s.m.Store.ListAccounts()
	if err != nil {
		return
	}
	for _, a := range accts {
		if ctx.Err() != nil {
			return
		}
		if !s.beginPoll(a.ID) {
			// An OpMigrate landed before startup reconcile reached this
			// account; the conversion leaves it consistent on its own.
			s.log.Printf("acct-%02d busy converting; skipping startup reconcile", a.ID)
			continue
		}
		s.reconcileAccount(a)
		s.endPoll(a.ID)
	}
}

// reconcileAccount brings one account's on-disk overlay in line with its row.
// Caller holds the poll claim.
func (s *Server) reconcileAccount(a store.Account) {
	switch overlay.Kind(a.OverlayKind) {
	case overlay.KindFuse:
		prov := s.overlayFor(overlay.KindFuse)
		if prov.Kind() == overlay.KindFuse && prov.Health(pool.ClaudeDir(), a.ConfigDir) == nil {
			// The detached holder kept the mirror live across the daemon
			// restart — the common case. Adopt it untouched, and vouch for it
			// in the cache directly: a live mirror implies the holder serving
			// it, and a select must not depend on the startup refresh having
			// still been accurate by the time this account was reached.
			s.holder.noteMounted(a.ConfigDir)
			s.log.Printf("acct-%02d adopted live mount", a.ID)
			return
		}
		s.healFuse(a)
	default:
		// A live mountpoint under a FUSE row is normal at startup (the
		// detached holder survived the daemon restart) — but under a NON-fuse
		// row it is wreckage: an aborted rollback's wedged unmount, or a
		// conversion that died before its row flip, serving a mirror whose
		// private backing no longer holds the account's identity. It blocks
		// every symlink repair (they refuse mountpoints); force it down first.
		if overlayMounted(a.ConfigDir) {
			prov := s.overlayFor(overlay.KindFuse)
			if prov.Kind() != overlay.KindFuse {
				// Only a wrong-kind injected fake can land here; the real
				// resolver always yields a fuse provider.
				s.log.Printf("acct-%02d: dir is a stale mountpoint but the resolved provider reports kind %q; skipping", a.ID, prov.Kind())
				return
			}
			if err := prov.Teardown(pool.ClaudeDir(), a.ConfigDir); err != nil {
				s.log.Printf("acct-%02d: unmount stale mountpoint: %v", a.ID, err)
				return
			}
			s.log.Printf("acct-%02d: cleared a stale mountpoint", a.ID)
		}
		// A symlink account can carry private files stranded in a fuse
		// backing dir by a conversion (or pre-fix fallback) that died
		// midway — restore them before anything launches on the account.
		healed, err := s.m.HealStrandedPrivate(a)
		if err != nil {
			s.log.Printf("acct-%02d heal stranded private files: %v", a.ID, err)
			return
		}
		if healed {
			s.log.Printf("acct-%02d restored private files stranded by an interrupted migration", a.ID)
		}
	}
}

// healOutcome classifies one healFuse attempt.
type healOutcome int

const (
	healMounted    healOutcome = iota // the mirror is up
	healRetry                         // transient holder condition; retry next poll
	healTCCBlocked                    // mount blocked pending the TCC grant; recorded; retry next poll
	healFallback                      // genuine mount failure; gated symlink fallback attempted
)

// errSweepStranded marks a failure in mountFuse's pre-Setup sweep of stranded
// private files (HasPrivateEntries/MovePrivateEntries) — distinct from a mount
// failure: Setup was never attempted, so it is not a mount verdict. healFuse
// routes it to healRetry, never the irreversible symlink fallback. A transient
// local-I/O blip must not permanently demote the account (the scheduler only
// re-heals fuse rows, so a fallback never auto-reverts), and a same-identity
// collision that refuses the sweep would refuse the symlink retreat the same
// way — so converting fixes nothing and only fails closed every poll.
var errSweepStranded = errors.New("sweep stranded private files")

// healFuse establishes a fuse account's mirror, classifying failures instead
// of blindly converting: transient holder conditions (holder unreachable, the
// dir busy, a wedged unmount in the way, a mount-up timeout under a proven
// "Network Volumes" grant, an error class only a newer holder understands, or a
// failure sweeping stranded private files before Setup is even attempted — none
// is a mount verdict) and a
// mount blocked pending the macOS "Network Volumes" TCC grant all retry next
// poll, and only a genuine mount failure falls back to symlink — itself gated
// on the account being idle (see fallbackToSymlink). Used by the startup
// reconcile, the scheduler's per-poll self-heal, and the holder supervisor;
// callers hold the account's poll claim or the holder replace's converting
// claim (under the latter, the fallback's beginConvertUnderPoll refuses and
// the conversion defers to the next poll — by design).
func (s *Server) healFuse(a store.Account) healOutcome {
	err := s.mountFuse(a)
	switch {
	case err == nil:
		return healMounted
	case errors.Is(err, mountd.ErrHolderUnavailable), errors.Is(err, mountd.ErrBusy):
		// RemoteProvider.Setup already attempts a spawn, and superviseHolder
		// owns respawn policy (with backoff), so there is nothing more to do
		// this poll.
		s.log.Printf("acct-%02d mount deferred (holder unavailable or dir busy), retrying next poll: %v", a.ID, err)
		return healRetry
	case errors.Is(err, overlay.ErrUnmountWedged):
		// A wedged unmount (the pre-clear/foreign-clear Teardown, or the
		// holder's own dead-mirror remount) says nothing about whether a fresh
		// mount would work — and the fallback's ConvertOverlay would hit the
		// same wedge, so converting here would fail closed every poll, loudly
		// and for nothing.
		s.log.Printf("acct-%02d mount blocked by a wedged unmount, retrying next poll: %v", a.ID, err)
		return healRetry
	case errors.Is(err, mountd.ErrUnknownClass):
		// Forward skew: a newer holder sent an error class this daemon
		// predates. Unclassifiable is not a mount verdict — fail toward
		// retry, loudly, every poll until the daemon is upgraded (mirroring
		// the unknown-op-reads-as-not-supported policy, never as failure).
		s.log.Printf("acct-%02d mount failed with an error class this daemon does not recognize (newer holder; upgrade the daemon), retrying next poll: %v", a.ID, err)
		return healRetry
	case errors.Is(err, overlay.ErrMountTimeout):
		// The mount timed out in a holder whose "Network Volumes" grant is
		// already proven by an earlier live mount — transient fuse-t slowness,
		// never the TCC condition. No recordTCC, no scary guidance.
		s.log.Printf("acct-%02d fuse mount did not come up within the mount wait; retrying: %v", a.ID, err)
		return healRetry
	case errors.Is(err, overlay.ErrMountNotLive):
		s.holder.recordTCC(err.Error())
		s.log.Printf("acct-%02d fuse mount blocked pending the macOS \"Network Volumes\" grant, retrying next poll: %v", a.ID, err)
		return healTCCBlocked
	case errors.Is(err, errSweepStranded):
		// The sweep of stranded private files failed BEFORE Setup was attempted,
		// so this is not a mount verdict. Converting would hit the same collision
		// the other way (and never auto-reverts), so retry next poll, loudly.
		s.log.Printf("acct-%02d mount deferred (could not sweep stranded private files before mounting), retrying next poll: %v", a.ID, err)
		return healRetry
	default:
		s.log.Printf("acct-%02d mount failed; attempting gated symlink fallback: %v", a.ID, err)
		s.fallbackToSymlink(a)
		return healFallback
	}
}

// mountFuse establishes a fuse account's mirror through the resolved fuse
// provider, in a fixed order. A dead mount (a mountpoint that fails Health)
// comes down first — never sweep or mount through one. Then, with no mount in
// the way, private files stranded in the mount underlay (the real dir) are
// swept into the backing dir: a conversion killed between its file moves and
// its row flip leaves them there, and mounting over them would shadow the
// account's identity — a session would then mint a divergent one in the
// backing dir. Then the provider mounts. A dead HOLDER's carcass registers as
// foreign on Setup (the fresh holder has no registry row for the mountpoint
// and never stacks mounts): Teardown's registry-miss path clears it, and the
// sweep+mount is retried exactly once. A holder registry row pinning a
// DIFFERENT base (ErrBaseMismatch — registry state, never a mount verdict)
// gets the same unmount-then-retry treatment: the holder's handleUnmount
// tears down by its registered base, and the retry remounts the canonical
// one. The Kind fence guards against wrong-kind injected fakes — the real
// resolver always yields a fuse provider.
func (s *Server) mountFuse(a store.Account) error {
	prov := s.overlayFor(overlay.KindFuse)
	if prov.Kind() != overlay.KindFuse {
		return fmt.Errorf("provider resolved for fuse reports kind %q; refusing to mount through it", prov.Kind())
	}
	base, dir := pool.ClaudeDir(), a.ConfigDir
	// A dead mount comes down first — never mount through one. Health is
	// shallow (no deep read on the poll hot path), so a partial wedge
	// (shallow-alive, bulk reads hang) passes it; the daemon's own deep-probe
	// verdict catches that case. Without this, the remount RPC would hit the
	// holder's now-idempotent handleMount, which treats a shallow-live mirror
	// as already mounted and never replaces the wedged one.
	if overlayMounted(dir) && (prov.Health(base, dir) != nil || s.holder.deepWedged(dir)) {
		if err := prov.Teardown(base, dir); err != nil {
			return fmt.Errorf("clear dead mount before remounting: %w", err)
		}
		s.log.Printf("acct-%02d cleared a dead mount before remounting", a.ID)
	}
	err := s.sweepAndMount(prov, a, base, dir)
	if errors.Is(err, mountd.ErrForeignMount) || errors.Is(err, mountd.ErrBaseMismatch) {
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
func (s *Server) sweepAndMount(prov overlay.Provider, a store.Account, base, dir string) error {
	if !overlayMounted(dir) {
		switch has, err := overlay.HasPrivateEntries(dir); {
		case err != nil:
			return fmt.Errorf("%w: check underlay: %w", errSweepStranded, err)
		case has:
			if err := overlay.MovePrivateEntries(dir, overlay.FusePrivateRoot(dir)); err != nil {
				return fmt.Errorf("%w: move into backing dir: %w", errSweepStranded, err)
			}
			s.log.Printf("acct-%02d swept private files from the mount underlay into the backing dir", a.ID)
		}
	}
	return prov.Setup(base, dir)
}

// fallbackToSymlink converts an account to the symlink provider after a
// genuine mount failure so its dir is fully usable again. ConvertOverlay
// force-unmounts the dir before laying any symlink, so the conversion is
// gated exactly like a migrate, in the migrate path's order — claim first,
// scan second: beginConvertUnderPoll refuses over a pending select
// reservation, and once the converting claim is set tryReserve refuses for
// the whole conversion, so no select can land between the idle check and the
// force-unmount. Never convert blind either (a failed scan means we cannot
// know whether a live claude has this dir as its config dir), and never under
// a live session — defer to the next poll instead. ConvertOverlay also moves
// the private files back out of the fuse backing dir — the earlier
// hand-rolled fallback left them stranded there, severing the account from
// its .claude.json identity. Callers must hold the account's poll claim; the
// conversion must not race another overlay mutation on the dir.
func (s *Server) fallbackToSymlink(a store.Account) {
	if !s.beginConvertUnderPoll(a.ID) {
		s.log.Printf("acct-%02d deferring fuse→symlink fallback: reserved by a pending select or already converting", a.ID)
		return
	}
	defer s.endConvert(a.ID)
	sessions, err := s.scan(context.Background())
	if err != nil {
		s.log.Printf("acct-%02d deferring fuse→symlink fallback: session scan: %v", a.ID, err)
		return
	}
	if n := procscan.CountByConfigDir(sessions, a.ConfigDir); n > 0 {
		s.log.Printf("acct-%02d deferring fuse→symlink fallback: %d live session(s)", a.ID, n)
		return
	}
	if _, err := s.m.ConvertOverlay(a, overlay.KindSymlink); err != nil {
		s.log.Printf("acct-%02d symlink fallback: %v", a.ID, err)
		return
	}
	// The mirror is down and the row is symlink; drop the cache entry so
	// HolderStatus.Mounts stops counting it.
	s.holder.noteUnmounted(a.ConfigDir)
	s.log.Printf("acct-%02d fell back to symlink after a genuine mount failure", a.ID)
}
