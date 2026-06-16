package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/mountd"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/peerpid"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
)

// spawnRecorder is an injectable Server.spawnHolder seam: it records every
// call and returns a scripted error. On a nil-error spawn it binds a canned
// holder (our version, empty List) at the exact requested socket — like the
// real EnsureRunning, a "successful" spawn yields a holder that passes the
// daemon's post-spawn health verification. Internally locked so any -race
// report points at code under test.
type spawnRecorder struct {
	mu      sync.Mutex
	sockets []string
	err     error
	serve   bool
	lns     []net.Listener
}

// newSpawnRecorder returns a recorder whose successful spawns serve a canned
// holder; the test's cleanup closes every holder it bound.
func newSpawnRecorder(t *testing.T) *spawnRecorder {
	t.Helper()
	r := &spawnRecorder{serve: true}
	t.Cleanup(r.dropHolder)
	return r
}

func (r *spawnRecorder) fn(socket, _ string, _ time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sockets = append(r.sockets, socket)
	if r.err != nil {
		return r.err
	}
	if !r.serve {
		return nil
	}
	ln, err := net.Listen("unix", socket)
	if err != nil {
		return fmt.Errorf("bind canned holder at %s: %w", socket, err)
	}
	r.lns = append(r.lns, ln)
	go serveCannedHolder(ln, nil)
	return nil
}

func (r *spawnRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sockets)
}

func (r *spawnRecorder) setErr(err error) {
	r.mu.Lock()
	r.err = err
	r.mu.Unlock()
}

func (r *spawnRecorder) setServe(serve bool) {
	r.mu.Lock()
	r.serve = serve
	r.mu.Unlock()
}

// dropHolder closes every canned holder this recorder bound — the spawned
// holder "crashing".
func (r *spawnRecorder) dropHolder() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ln := range r.lns {
		_ = ln.Close()
	}
	r.lns = nil
}

// flipToFuse flips an account row to the fuse kind, returning the fresh row.
func flipToFuse(t *testing.T, s *Server, id int) store.Account {
	t.Helper()
	a, err := s.m.Store.GetAccount(id)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "fuse"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	return a
}

// flipToSymlink flips an account row back to the symlink kind.
func flipToSymlink(t *testing.T, s *Server, id int) {
	t.Helper()
	a, err := s.m.Store.GetAccount(id)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "symlink"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
}

// newSuperviseServer wires newMigrateServer for supervision tests: an idle
// session scan, a matured daemon (uptime past reservationTTL), a short
// gone-wait, a fixed peer pid, and a recording spawn seam whose successful
// spawns bind a real canned holder at the daemon's holder socket. That socket
// starts dead (nothing bound) — the crash scenario; skew tests repoint it at
// a canned skewed holder. macOS caps sun_path at 104 bytes, so the socket
// lives under a short /tmp dir.
func newSuperviseServer(t *testing.T) (*Server, map[int]string, *fakeFuseProv, *spawnRecorder) {
	t.Helper()
	s, dirs, fake := newMigrateServer(t)
	sockDir, err := os.MkdirTemp("/tmp", "ccp-sup")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	rec := newSpawnRecorder(t)
	s.spawnHolder = rec.fn
	s.holderSocket = filepath.Join(sockDir, "m.sock")
	s.scanSessions = func() ([]procscan.Session, error) { return nil, nil }
	s.startedAt = time.Now().Add(-reservationTTL - time.Second)
	s.holderGoneWait = 2 * time.Second
	s.peerPID = func(string) (int, error) { return 4242, nil }
	return s, dirs, fake, rec
}

// skewedHolder serves the mountd wire protocol with a non-matching version
// and scripted shutdown behavior, for skew-replace tests.
type skewedHolder struct {
	ln                net.Listener
	socket            string
	version           string
	mounts            []mountd.MountInfo
	releaseOnShutdown bool
	// noReplyShutdown closes the connection instead of writing the Shutdown
	// reply — the sweep-ran-but-the-reply-was-lost wire shape (mountd's real
	// Shutdown runs its unmount sweep BEFORE replying).
	noReplyShutdown bool

	// blockShutdown, when non-nil, parks the Shutdown reply until release();
	// shutdownEntered closes when the first Shutdown arrives. Together they
	// let a test interrogate the daemon mid-sweep — mountd's real Shutdown
	// runs its unmount sweep BEFORE replying, so this window is exactly the
	// one the replace claims must cover.
	blockShutdown   chan struct{}
	shutdownEntered chan struct{}
	releaseOnce     sync.Once

	mu        sync.Mutex
	shutdowns int
}

func startSkewedHolder(t *testing.T, mounts []mountd.MountInfo, releaseOnShutdown bool) *skewedHolder {
	t.Helper()
	return newSkewedHolder(t, mounts, releaseOnShutdown, false, false)
}

// startBlockingSkewedHolder is startSkewedHolder with a gated Shutdown reply.
func startBlockingSkewedHolder(t *testing.T, mounts []mountd.MountInfo, releaseOnShutdown bool) *skewedHolder {
	t.Helper()
	return newSkewedHolder(t, mounts, releaseOnShutdown, true, false)
}

// startNoReplySkewedHolder is startSkewedHolder whose Shutdown drops the
// connection without a reply — the errored-RPC, outcome-unknown shape.
func startNoReplySkewedHolder(t *testing.T, mounts []mountd.MountInfo, releaseOnShutdown bool) *skewedHolder {
	t.Helper()
	return newSkewedHolder(t, mounts, releaseOnShutdown, false, true)
}

func newSkewedHolder(t *testing.T, mounts []mountd.MountInfo, releaseOnShutdown, block, noReply bool) *skewedHolder {
	t.Helper()
	sockDir, err := os.MkdirTemp("/tmp", "ccp-skew")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	socket := filepath.Join(sockDir, "m.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	h := &skewedHolder{ln: ln, socket: socket, version: "0.0.0-skewed", mounts: mounts, releaseOnShutdown: releaseOnShutdown, noReplyShutdown: noReply}
	if block {
		h.blockShutdown = make(chan struct{})
		t.Cleanup(h.release) // never leave the serve goroutine parked
	}
	if block || noReply {
		h.shutdownEntered = make(chan struct{})
	}
	go h.serve()
	t.Cleanup(func() { _ = h.ln.Close() })
	return h
}

// release unparks a gated Shutdown reply; idempotent, no-op when ungated.
func (h *skewedHolder) release() {
	if h.blockShutdown == nil {
		return
	}
	h.releaseOnce.Do(func() { close(h.blockShutdown) })
}

func (h *skewedHolder) serve() {
	for {
		conn, err := h.ln.Accept()
		if err != nil {
			return // listener closed: defined exit
		}
		var req mountd.Request
		if err := json.NewDecoder(conn).Decode(&req); err != nil {
			conn.Close() // probe dial (WaitGone) with no request body
			continue
		}
		resp := mountd.Response{OK: true, Version: h.version}
		switch req.Op {
		case mountd.OpList:
			resp.Mounts = h.mounts
		case mountd.OpShutdown:
			h.mu.Lock()
			h.shutdowns++
			first := h.shutdowns == 1
			h.mu.Unlock()
			if first && h.shutdownEntered != nil {
				close(h.shutdownEntered)
			}
			if h.blockShutdown != nil {
				<-h.blockShutdown
			}
		}
		if req.Op == mountd.OpShutdown && h.noReplyShutdown {
			conn.Close() // reply lost on the wire; the "sweep" already ran
			if h.releaseOnShutdown {
				_ = h.ln.Close()
				return
			}
			continue
		}
		_ = json.NewEncoder(conn).Encode(resp)
		conn.Close()
		if req.Op == mountd.OpShutdown && h.releaseOnShutdown {
			_ = h.ln.Close()
			return
		}
	}
}

func (h *skewedHolder) shutdownCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.shutdowns
}

func TestSpawnBackoffDoublesAndCaps(t *testing.T) {
	cases := map[int]time.Duration{
		1:  spawnBackoffBase,  // first failure -> base
		2:  20 * time.Second,  // doubled
		3:  40 * time.Second,  // doubled again
		6:  320 * time.Second, // still under the cap
		7:  spawnBackoffCap,   // 640s capped to 10min
		12: spawnBackoffCap,   // stays capped
		0:  spawnBackoffBase,  // degenerate input never shrinks below base
	}
	for failures, want := range cases {
		if got := spawnBackoff(failures); got != want {
			t.Errorf("spawnBackoff(%d) = %v, want %v", failures, got, want)
		}
	}
}

// TestRemountBackoffDoublesAndCaps pins backoffAfter under the per-row
// remount constants: base-doubling per failure, capped at 2 minutes —
// deliberately under the 180s scheduler period, so supervision is never the
// slower recovery path.
func TestRemountBackoffDoublesAndCaps(t *testing.T) {
	cases := map[int]time.Duration{
		1:  remountBackoffBase, // first failure -> base
		2:  20 * time.Second,   // doubled
		3:  40 * time.Second,   // doubled again
		4:  80 * time.Second,   // still under the cap
		5:  remountBackoffCap,  // 160s capped to 2min
		12: remountBackoffCap,  // stays capped
		0:  remountBackoffBase, // degenerate input never shrinks below base
		-1: remountBackoffBase, // negative input never shrinks below base
	}
	for failures, want := range cases {
		if got := backoffAfter(failures, remountBackoffBase, remountBackoffCap); got != want {
			t.Errorf("backoffAfter(%d, base, cap) = %v, want %v", failures, got, want)
		}
	}
}

// TestSuperviseRespawnConditions pins when a dead holder is respawned at all:
// only when fuse rows (or mounts a holder previously served) need one, and
// never while a holder is healthy at our version.
func TestSuperviseRespawnConditions(t *testing.T) {
	cases := map[string]struct {
		fuseRow     bool
		priorMounts bool
		holderUp    bool
		wantSpawn   bool
	}{
		"dead holder with a fuse row respawns":                 {fuseRow: true, wantSpawn: true},
		"dead holder with no fuse rows and no history idles":   {},
		"dead holder with prior mounts respawns for carcasses": {priorMounts: true, wantSpawn: true},
		"healthy holder at our version is left alone":          {fuseRow: true, holderUp: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, dirs, _, rec := newSuperviseServer(t)
			if tc.fuseRow {
				flipToFuse(t, s, 1)
			}
			if tc.priorMounts {
				// noteMounted records history; the tick's failed refresh then
				// marks the cache unhealthy but history survives.
				s.holder.noteMounted(dirs[1])
			}
			if tc.holderUp {
				s.holderSocket = startCannedHolder(t, nil)
			}

			s.superviseTick(t.Context())

			if got := rec.count() > 0; got != tc.wantSpawn {
				t.Fatalf("spawn attempted = %v, want %v", got, tc.wantSpawn)
			}
		})
	}
}

// TestSuperviseRespawnBackoffAndSpawnError pins the failure ledger: each
// failed spawn doubles the wait, the failure text is surfaced on the status
// wire (SpawnError), a tick inside the window never attempts, and a success
// resets the backoff and clears the surface.
func TestSuperviseRespawnBackoffAndSpawnError(t *testing.T) {
	s, _, _, rec := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	rec.setErr(errors.New("spawn exploded"))

	s.superviseTick(t.Context())
	if rec.count() != 1 || s.sup.failures != 1 {
		t.Fatalf("after first failure: attempts=%d failures=%d, want 1/1", rec.count(), s.sup.failures)
	}
	if got := s.holder.wireStatus().SpawnError; !strings.Contains(got, "spawn exploded") {
		t.Fatalf("SpawnError = %q, want the spawn failure surfaced", got)
	}
	// The failure travels the status wire end to end.
	if resp := s.handleStatus(t.Context()); resp.Holder == nil || !strings.Contains(resp.Holder.SpawnError, "spawn exploded") {
		t.Fatalf("status holder = %+v, want SpawnError surfaced", resp.Holder)
	}

	// Inside the backoff window: no second attempt.
	s.superviseTick(t.Context())
	if rec.count() != 1 {
		t.Fatalf("attempts inside the backoff window = %d, want 1", rec.count())
	}

	// Window elapsed: the retry runs and the wait doubles.
	s.sup.retryAt = time.Now().Add(-time.Second)
	s.superviseTick(t.Context())
	if rec.count() != 2 || s.sup.failures != 2 {
		t.Fatalf("after second failure: attempts=%d failures=%d, want 2/2", rec.count(), s.sup.failures)
	}
	if wait := time.Until(s.sup.retryAt); wait <= spawnBackoffBase {
		t.Fatalf("second failure's wait = %v, want > %v (doubled)", wait, spawnBackoffBase)
	}

	// Success resets the backoff and clears the surfaced error.
	rec.setErr(nil)
	s.sup.retryAt = time.Now().Add(-time.Second)
	s.superviseTick(t.Context())
	if rec.count() != 3 {
		t.Fatalf("attempts after the success window = %d, want 3", rec.count())
	}
	if s.sup.failures != 0 || !s.sup.retryAt.IsZero() {
		t.Fatalf("backoff not reset on success: failures=%d retryAt=%v", s.sup.failures, s.sup.retryAt)
	}
	if got := s.holder.wireStatus().SpawnError; got != "" {
		t.Fatalf("SpawnError after success = %q, want cleared", got)
	}
}

// TestSuperviseZombieSocketEngagesBackoff pins the live-but-unresponsive
// holder shape: a socket that accepts connections but fails health checks
// defeats EnsureRunning's Available() short-circuit, so a "successful" spawn
// changes nothing. The post-spawn verification must book it as a failure —
// backoff engaged, SpawnError surfaced, no "respawned" celebration, and the
// unreachable transition logged once, not per tick.
func TestSuperviseZombieSocketEngagesBackoff(t *testing.T) {
	s, _, _, rec := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	// The zombie keeps the socket: spawn "succeeds" without binding anything,
	// exactly like EnsureRunning short-circuiting on Available().
	rec.setServe(false)
	ln, err := net.Listen("unix", s.holderSocket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed: defined exit
			}
			conn.Close() // accepts, never answers: the zombie shape
		}
	}()
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)

	s.superviseTick(t.Context())

	if rec.count() != 1 {
		t.Fatalf("spawn attempts = %d, want 1", rec.count())
	}
	if s.sup.failures != 1 {
		t.Fatalf("failures = %d, want the zombie spawn booked as a failure", s.sup.failures)
	}
	if got := s.holder.wireStatus().SpawnError; !strings.Contains(got, "failed its health check") {
		t.Fatalf("SpawnError = %q, want the failed verification surfaced", got)
	}
	if strings.Contains(buf.String(), "mount holder respawned") {
		t.Fatalf("zombie socket logged as a successful respawn:\n%s", buf.String())
	}

	// Next tick: still inside the backoff window — no second spawn, and the
	// unreachable transition is not re-logged (sawUnhealthy never reset).
	s.superviseTick(t.Context())
	if rec.count() != 1 {
		t.Fatalf("attempts inside the backoff window = %d, want 1", rec.count())
	}
	if got := strings.Count(buf.String(), "mount holder unreachable"); got != 1 {
		t.Fatalf("unreachable logged %d times across two ticks, want 1:\n%s", got, buf.String())
	}
	if got := strings.Count(buf.String(), "failed its health check"); got != 1 {
		t.Fatalf("identical verification failure logged %d times, want 1:\n%s", got, buf.String())
	}
}

// TestSuperviseRespawnRemountsUnservedRows pins the crash→remount loop: after
// a successful respawn, every fuse row the fresh holder does not serve is
// healed and vouched for — at the supervision cadence, not the 3.5min poll.
func TestSuperviseRespawnRemountsUnservedRows(t *testing.T) {
	s, dirs, fake, rec := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	flipToFuse(t, s, 2)

	s.superviseTick(t.Context())

	if rec.count() != 1 {
		t.Fatalf("spawn attempts = %d, want 1", rec.count())
	}
	if fake.setupCount() != 2 {
		t.Fatalf("setups = %d, want both fuse rows remounted", fake.setupCount())
	}
	if !s.holder.ready(dirs[1]) || !s.holder.ready(dirs[2]) {
		t.Fatal("remounted rows not vouched for in the holder cache")
	}
}

// TestSuperviseRespawnSkipsClaimedAccount pins the claim discipline: an
// account a poll or conversion owns is SKIPPED, never raced — and swept by a
// later revive once released.
func TestSuperviseRespawnSkipsClaimedAccount(t *testing.T) {
	s, dirs, fake, rec := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	flipToFuse(t, s, 2)
	if !s.beginPoll(1) {
		t.Fatal("beginPoll failed on a free account")
	}

	s.superviseTick(t.Context())

	if fake.setupCount() != 1 {
		t.Fatalf("setups = %d, want only the unclaimed account remounted", fake.setupCount())
	}
	if s.holder.ready(dirs[1]) {
		t.Fatal("claimed account was remounted (the supervisor raced its owner)")
	}
	if !s.holder.ready(dirs[2]) {
		t.Fatal("unclaimed account not remounted")
	}
	// The claim still belongs to its owner — the supervisor must not steal or
	// release it.
	if s.beginPoll(1) {
		t.Fatal("supervisor released a claim it did not own")
	}
	s.endPoll(1)

	// Released: crash the spawned holder so the next tick revives again and
	// sweeps the deferred account. The fresh holder serves nothing, so acct-2
	// heals again alongside — hence 3 setups, not 2.
	rec.dropHolder()
	s.superviseTick(t.Context())
	if fake.setupCount() != 3 || !s.holder.ready(dirs[1]) {
		t.Fatalf("deferred account not remounted after release: setups=%d, ready=%v",
			fake.setupCount(), s.holder.ready(dirs[1]))
	}
}

// TestRemountFuseRowsReReadsRow pins the stale-snapshot guard on the revive
// path: the supervisor re-reads each row under its poll claim, so an account
// converted to symlink between the listing and the remount is left alone.
func TestRemountFuseRowsReReadsRow(t *testing.T) {
	s, _, fake, _ := newSuperviseServer(t)
	a := flipToFuse(t, s, 1)
	stale := a // still says fuse
	a.OverlayKind = "symlink"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	s.remountFuseRows(t.Context(), []store.Account{stale})

	if fake.setupCount() != 0 {
		t.Fatal("a row converted mid-revive was remounted as fuse")
	}
	if !s.beginPoll(1) {
		t.Fatal("remount leaked its poll claim")
	}
	s.endPoll(1)
}

// mountTimeoutChain is the exact error chain RemoteProvider.Setup produces
// for a mount-up timeout under a proven "Network Volumes" grant: the
// provider's wrap around overlayClass's dual-wrap of the wire sentinel.
func mountTimeoutChain() error {
	return fmt.Errorf("mount: %w", fmt.Errorf("%w: %w", overlay.ErrMountTimeout, mountd.ErrMountTimeout))
}

// TestSuperviseTickRetriesUnvouchedRowWithBackoff pins the steady-state heal
// loop: a fuse row a healthy holder cannot vouch for is retried each tick
// under per-row backoff — attempts advance the failure count, the window
// gates the next tick, and a successful heal vouches and drops the ledger
// entry.
func TestSuperviseTickRetriesUnvouchedRowWithBackoff(t *testing.T) {
	s, dirs, fake, _ := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil) // healthy at our version, vouching for nothing
	fake.setupErr = mountTimeoutChain()

	// First steady tick: one attempt, booked as one failure with a window.
	s.superviseTick(t.Context())
	if fake.setupCount() != 1 {
		t.Fatalf("setups after the first tick = %d, want 1", fake.setupCount())
	}
	if st := s.sup.rowRetry[1]; st.failures != 1 || !st.retryAt.After(time.Now()) {
		t.Fatalf("rowRetry[1] = %+v, want one failure with a future retryAt", st)
	}

	// Immediately ticking again sits inside the window: no attempt.
	s.superviseTick(t.Context())
	if fake.setupCount() != 1 {
		t.Fatalf("setups inside the backoff window = %d, want still 1", fake.setupCount())
	}

	// Window rewound: the retry runs and the failure count advances.
	st := s.sup.rowRetry[1]
	st.retryAt = time.Now().Add(-time.Second)
	s.sup.rowRetry[1] = st
	s.superviseTick(t.Context())
	if fake.setupCount() != 2 || s.sup.rowRetry[1].failures != 2 {
		t.Fatalf("after the rewound window: setups=%d failures=%d, want 2/2",
			fake.setupCount(), s.sup.rowRetry[1].failures)
	}

	// Failure cleared: the next windowed attempt mounts, vouches, and drops
	// the ledger entry.
	fake.setupErr = nil
	st = s.sup.rowRetry[1]
	st.retryAt = time.Now().Add(-time.Second)
	s.sup.rowRetry[1] = st
	s.superviseTick(t.Context())
	if fake.setupCount() != 3 {
		t.Fatalf("setups after clearing the failure = %d, want 3", fake.setupCount())
	}
	if !s.holder.ready(dirs[1]) {
		t.Fatal("healed row not vouched for in the holder cache")
	}
	if _, ok := s.sup.rowRetry[1]; ok {
		t.Fatal("successful heal left a rowRetry entry")
	}
}

// TestSuperviseTickRetrySkipsClaimedAccount pins the skip-don't-race
// discipline on the steady-state loop: an eligible row someone else owns is
// neither attempted nor penalized — a skip is not a failure — and the next
// tick after release retries it.
func TestSuperviseTickRetrySkipsClaimedAccount(t *testing.T) {
	s, _, fake, _ := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = mountTimeoutChain()
	// An eligible ledger entry whose window has passed…
	s.sup.rowRetry = map[int]rowRetryState{1: {failures: 2, retryAt: time.Now().Add(-time.Second)}}
	// …on an account someone else owns.
	if !s.beginPoll(1) {
		t.Fatal("beginPoll failed on a free account")
	}

	s.superviseTick(t.Context())
	if fake.setupCount() != 0 {
		t.Fatal("supervisor raced the claim owner")
	}
	if got := s.sup.rowRetry[1].failures; got != 2 {
		t.Fatalf("failures after a skip = %d, want 2 unchanged", got)
	}

	// Released: the still-open window admits the next tick's attempt.
	s.endPoll(1)
	s.superviseTick(t.Context())
	if fake.setupCount() != 1 {
		t.Fatalf("setups after release = %d, want 1", fake.setupCount())
	}
	if got := s.sup.rowRetry[1].failures; got != 3 {
		t.Fatalf("failures after a real attempt = %d, want 3", got)
	}
}

// TestSuperviseTickRetryLeavesConvertedRowAndPrunes pins the ledger hygiene:
// a row that earned a retry entry while fuse and then converted to symlink is
// never healed as fuse, and its entry is pruned from the ledger.
func TestSuperviseTickRetryLeavesConvertedRowAndPrunes(t *testing.T) {
	s, _, fake, _ := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	// The row earned a ledger entry while fuse…
	s.sup.rowRetry = map[int]rowRetryState{1: {failures: 1, retryAt: time.Now().Add(-time.Second)}}
	// …then converted away.
	flipToSymlink(t, s, 1)

	s.superviseTick(t.Context())

	if fake.setupCount() != 0 {
		t.Fatal("a converted row was healed as fuse")
	}
	if len(s.sup.rowRetry) != 0 {
		t.Fatalf("rowRetry = %v, want the converted row's entry pruned", s.sup.rowRetry)
	}
}

// TestSuperviseTickRetriesTCCBlockedRowUnderBackoff pins the improved
// post-grant story: a TCC-blocked row rides the same backoff — attempted,
// bounded, never hot — with the guidance surfaced on the wire, and the first
// successful mount after the grant clears it via noteMounted.
func TestSuperviseTickRetriesTCCBlockedRowUnderBackoff(t *testing.T) {
	s, dirs, fake, _ := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = fmt.Errorf("mount: %w", overlay.ErrMountNotLive)

	s.superviseTick(t.Context())
	if fake.setupCount() != 1 || s.sup.rowRetry[1].failures != 1 {
		t.Fatalf("after the first tick: setups=%d failures=%d, want 1/1",
			fake.setupCount(), s.sup.rowRetry[1].failures)
	}
	if got := s.holder.wireStatus().TCCError; got == "" {
		t.Fatal("TCC guidance not surfaced for the blocked row")
	}
	// Inside the window: bounded, not hot.
	s.superviseTick(t.Context())
	if fake.setupCount() != 1 {
		t.Fatalf("setups inside the backoff window = %d, want still 1", fake.setupCount())
	}

	// Grant landed: the next windowed attempt mounts, vouches, and clears the
	// guidance through noteMounted.
	fake.setupErr = nil
	st := s.sup.rowRetry[1]
	st.retryAt = time.Now().Add(-time.Second)
	s.sup.rowRetry[1] = st
	s.superviseTick(t.Context())
	if !s.holder.ready(dirs[1]) {
		t.Fatal("granted row not mounted and vouched for")
	}
	if got := s.holder.wireStatus().TCCError; got != "" {
		t.Fatalf("TCCError after the successful mount = %q, want cleared via noteMounted", got)
	}
	if _, ok := s.sup.rowRetry[1]; ok {
		t.Fatal("successful heal left a rowRetry entry")
	}
}

// TestSuperviseTickRemountsHeldDeadRow pins the held-dead heal: a dir the
// holder NAMES in List (registered, shallow-mounted) but reports dead is
// logged loudly — the holder's deep-probe verdict picks the copy (a wedge
// hangs reads; a plain-dead mirror fails them outright), the live-session
// count and relaunch guidance appear in both shapes — and remounted through
// the ordinary healFuse path.
func TestSuperviseTickRemountsHeldDeadRow(t *testing.T) {
	const (
		wedgeCopy = "wedged mirror (serves metadata but hangs reads)"
		deadCopy  = "dead mirror (fails reads outright; unmounted out of band or its fuse worker died?)"
	)
	cases := map[string]struct {
		wedged   bool
		wantCopy string
		notCopy  string
	}{
		"deep-wedged mirror logs the wedge copy": {
			wedged:   true,
			wantCopy: wedgeCopy,
			notCopy:  deadCopy,
		},
		"plain-dead mirror logs the dead copy, never the wedge copy": {
			wantCopy: deadCopy,
			notCopy:  wedgeCopy,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, dirs, fake, _ := newSuperviseServer(t)
			flipToFuse(t, s, 1)
			s.holderSocket = startCannedHolder(t, []mountd.MountInfo{
				{Dir: dirs[1], Base: "/base", Live: false, Wedged: tc.wedged},
			})
			var buf bytes.Buffer
			s.log = log.New(&buf, "", 0)

			s.superviseTick(t.Context())

			if fake.setupCount() != 1 {
				t.Fatalf("setups = %d, want the held-dead mirror remounted", fake.setupCount())
			}
			if !s.holder.ready(dirs[1]) {
				t.Fatal("remounted mirror not vouched for")
			}
			out := buf.String()
			if !strings.Contains(out, tc.wantCopy) {
				t.Fatalf("held-dead log line missing %q:\n%s", tc.wantCopy, out)
			}
			if strings.Contains(out, tc.notCopy) {
				t.Fatalf("held-dead log line carries the wrong copy %q:\n%s", tc.notCopy, out)
			}
			if !strings.Contains(out, "live session") || !strings.Contains(out, "relaunch") {
				t.Fatalf("held-dead log line missing the session count or relaunch guidance:\n%s", out)
			}
			if _, ok := s.sup.rowRetry[1]; ok {
				t.Fatal("successful remount left a rowRetry entry")
			}
		})
	}
}

// TestSuperviseSkewGateLegs pins the idle gate one leg at a time: each leg
// alone must block the replace (no Shutdown, no spawn, no mount churn, no
// leaked claims — with Skewed still surfaced on the wire), and only the
// all-clear state replaces: Shutdown observed on the old holder, then spawn +
// remount at our version, claims released.
func TestSuperviseSkewGateLegs(t *testing.T) {
	cases := map[string]struct {
		extraDir    bool // the holder lists a dir with no account row
		mutate      func(t *testing.T, s *Server, dirs map[int]string, extra string)
		wantReplace bool
	}{
		"young daemon defers": {
			mutate: func(t *testing.T, s *Server, _ map[int]string, _ string) {
				s.startedAt = time.Now()
			},
		},
		"scan failure fails closed": {
			mutate: func(t *testing.T, s *Server, _ map[int]string, _ string) {
				s.scanSessions = func() ([]procscan.Session, error) { return nil, errors.New("ps exploded") }
			},
		},
		"live session on a fuse dir defers": {
			mutate: func(t *testing.T, s *Server, dirs map[int]string, _ string) {
				s.scanSessions = func() ([]procscan.Session, error) {
					return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
				}
			},
		},
		// A mount whose account row was deleted while a teardown was refused:
		// the dir exists only in the holder's List, and a session on it must
		// still block the replace — kernel truth over row truth. (Since the
		// pre-row leg landed, this defers on the missing row before the
		// session is even consulted; the session here is belt and braces.)
		"live session on a holder-only dir defers": {
			extraDir: true,
			mutate: func(t *testing.T, s *Server, _ map[int]string, extra string) {
				s.scanSessions = func() ([]procscan.Session, error) {
					return []procscan.Session{{PID: 4242, ConfigDir: extra}}, nil
				}
			},
		},
		// A holder-served dir with NO account row and no session is a
		// `ccp add` mid-login (the row lands at FinalizeAdd): the replace
		// claims cannot cover it, so the gate defers unconditionally.
		"holder-served dir without an account row defers": {
			extraDir: true,
		},
		"select reservation defers": {
			mutate: func(t *testing.T, s *Server, _ map[int]string, _ string) {
				if !s.tryReserve(1) {
					t.Fatal("tryReserve failed on a free account")
				}
			},
		},
		"mid-poll fuse account defers": {
			mutate: func(t *testing.T, s *Server, _ map[int]string, _ string) {
				if !s.beginPoll(1) {
					t.Fatal("beginPoll failed on a free account")
				}
			},
		},
		"in-flight conversion defers": {
			mutate: func(t *testing.T, s *Server, _ map[int]string, _ string) {
				if !s.beginConvert(2) {
					t.Fatal("beginConvert failed on a free account")
				}
			},
		},
		// A migrate-to-fuse holds its converting claim on a row still reading
		// symlink — the row flips only at the end — and its ConvertOverlay is
		// about to Mount through the very holder being retired. Per-fuse-row
		// checks cannot see it; the gate must defer on ANY in-flight
		// conversion.
		"in-flight conversion on a symlink row defers": {
			mutate: func(t *testing.T, s *Server, _ map[int]string, _ string) {
				flipToSymlink(t, s, 2)
				if !s.beginConvert(2) {
					t.Fatal("beginConvert failed on a free account")
				}
			},
		},
		"build that cannot spawn a successor defers": {
			mutate: func(t *testing.T, s *Server, _ map[int]string, _ string) {
				if overlay.FuseBuilt() {
					t.Skip("fuse build can spawn; this leg pins the pure build")
				}
				s.spawnHolder = nil
			},
		},
		"all clear replaces": {wantReplace: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, dirs, fake, rec := newSuperviseServer(t)
			flipToFuse(t, s, 1)
			flipToFuse(t, s, 2)
			extra := t.TempDir()
			mounts := []mountd.MountInfo{{Dir: dirs[1], Base: "/base", Live: true}}
			if tc.extraDir {
				mounts = append(mounts, mountd.MountInfo{Dir: extra, Base: "/base", Live: true})
			}
			h := startSkewedHolder(t, mounts, true)
			s.holderSocket = h.socket
			if tc.mutate != nil {
				tc.mutate(t, s, dirs, extra)
			}

			s.superviseTick(t.Context())

			if tc.wantReplace {
				if h.shutdownCount() != 1 {
					t.Fatalf("shutdowns = %d, want 1", h.shutdownCount())
				}
				if rec.count() != 1 {
					t.Fatalf("spawn attempts = %d, want 1", rec.count())
				}
				if fake.setupCount() != 2 {
					t.Fatalf("setups = %d, want both fuse rows remounted", fake.setupCount())
				}
				if !s.holder.ready(dirs[1]) || !s.holder.ready(dirs[2]) {
					t.Fatal("remounted rows not vouched for in the holder cache")
				}
				// The replace claims and the conversion fence lift with it.
				if !s.beginConvert(1) {
					t.Fatal("replace leaked its claims or the conversion fence")
				}
				s.endConvert(1)
				return
			}
			if h.shutdownCount() != 0 {
				t.Fatal("a blocked gate leg stopped the serving holder")
			}
			if rec.count() != 0 || fake.setupCount() != 0 {
				t.Fatalf("a blocked gate leg disturbed mounts: spawns=%d setups=%d", rec.count(), fake.setupCount())
			}
			// A blocked leg must not leave replace claims behind: acct-1 stays
			// selectable (tryReserve refuses only a converting claim on it).
			if !s.tryReserve(1) {
				t.Fatal("a blocked gate leg leaked replace claims on acct-01")
			}
			if ws := s.holder.wireStatus(); !ws.Skewed {
				t.Fatalf("deferred skew not surfaced on the wire: %+v", ws)
			}
		})
	}
}

// TestSuperviseReplaceFencesSelectsAndConversions pins the replace's claim
// fence through the most dangerous window: mountd's Shutdown sweeps every
// mount BEFORE replying (up to 60s), and for that whole stretch the holder
// still answers Health — so without claims taken at gate-clear, a select
// would reserve a fuse dir whose mirror is being swept out from under it, and
// a migrate could start a conversion about to Mount through the dying holder.
// Mid-sweep, both must refuse; after the replace, both must work again.
func TestSuperviseReplaceFencesSelectsAndConversions(t *testing.T) {
	s, dirs, fake, _ := newSuperviseServer(t)
	flipToFuse(t, s, 1) // acct-2 stays symlink: the fence, not its claim, must refuse it
	h := startBlockingSkewedHolder(t, []mountd.MountInfo{{Dir: dirs[1], Base: "/base", Live: true}}, true)
	s.holderSocket = h.socket

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.superviseTick(t.Context())
	}()
	select {
	case <-h.shutdownEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("replace never reached the holder's Shutdown")
	}

	// Mid-sweep: the holder is unmounting and still answering Health.
	if s.tryReserve(1) {
		t.Error("a select reserved a fuse account mid-replace")
	}
	if s.beginConvert(2) {
		t.Error("a conversion began mid-replace (the replacing fence is down)")
	}
	one := 1
	if resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &one, NoMark: true, Cwd: "/proj"}); resp.OK {
		t.Errorf("forced select succeeded mid-replace: %+v", resp)
	}

	h.release()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("replace did not finish after the sweep unblocked")
	}

	if fake.setupCount() != 1 || !s.holder.ready(dirs[1]) {
		t.Fatalf("replace did not remount the fuse row: setups=%d ready=%v", fake.setupCount(), s.holder.ready(dirs[1]))
	}
	// The fence and claims lift with the replace.
	if !s.beginConvert(2) {
		t.Fatal("the replacing fence outlived the replace")
	}
	s.endConvert(2)
	if !s.tryReserve(1) {
		t.Fatal("replace claims leaked: acct-01 not selectable after the replace")
	}
}

// TestSuperviseReplaceHonorsCtx pins M3: the replace chain (Shutdown 65s +
// WaitGone 70s + kill + WaitGone 70s + spawn) must not stall a daemon
// shutdown — ctx is checked between steps and inside the gone-waits.
func TestSuperviseReplaceHonorsCtx(t *testing.T) {
	t.Run("cancelled before shutdown aborts untouched", func(t *testing.T) {
		s, dirs, fake, rec := newSuperviseServer(t)
		flipToFuse(t, s, 1)
		h := startSkewedHolder(t, []mountd.MountInfo{{Dir: dirs[1], Base: "/base", Live: true}}, true)
		s.holderSocket = h.socket
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		s.superviseTick(ctx)

		if h.shutdownCount() != 0 {
			t.Fatal("a cancelled ctx still stopped the serving holder")
		}
		if rec.count() != 0 || fake.setupCount() != 0 {
			t.Fatalf("cancelled replace disturbed mounts: spawns=%d setups=%d", rec.count(), fake.setupCount())
		}
		if !s.beginConvert(1) {
			t.Fatal("cancelled replace leaked its claims")
		}
		s.endConvert(1)
	})

	t.Run("cancelled mid gone-wait skips the kill and exits", func(t *testing.T) {
		s, dirs, fake, rec := newSuperviseServer(t)
		flipToFuse(t, s, 1)
		// Wedged shape: the holder acks Shutdown but never releases.
		h := startBlockingSkewedHolder(t, []mountd.MountInfo{{Dir: dirs[1], Base: "/base", Live: true}}, false)
		s.holderSocket = h.socket
		s.holderGoneWait = 30 * time.Second // only ctx can end the wait in time
		var killed atomic.Bool
		s.killHolderPeer = func(string, int) (int, error) {
			killed.Store(true)
			return 0, errors.New("must not be reached")
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.superviseTick(ctx)
		}()
		select {
		case <-h.shutdownEntered:
		case <-time.After(10 * time.Second):
			t.Fatal("replace never reached the holder's Shutdown")
		}
		cancel()    // daemon shutdown lands while the sweep is in flight
		h.release() // sweep finishes; the gone-wait would now run for 30s

		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("cancelled replace did not exit promptly from the gone-wait")
		}
		if killed.Load() {
			t.Fatal("cancelled replace still killed the socket peer")
		}
		if rec.count() != 0 || fake.setupCount() != 0 {
			t.Fatalf("cancelled replace spawned/remounted: spawns=%d setups=%d", rec.count(), fake.setupCount())
		}
	})
}

// TestSuperviseSkewWedgedHolderKilled pins the escape hatch: a skewed holder
// that acks Shutdown but never releases its socket is killed by peer pid
// (loudly), then the successor is spawned and the fuse rows remounted. The
// twin negative pins that a failed kill aborts the replace — no spawn, no
// remount — leaving the gate to re-run next tick.
func TestSuperviseSkewWedgedHolderKilled(t *testing.T) {
	s, dirs, fake, rec := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	h := startSkewedHolder(t, []mountd.MountInfo{{Dir: dirs[1], Base: "/base", Live: true}}, false)
	s.holderSocket = h.socket
	s.holderGoneWait = 300 * time.Millisecond
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)
	var gotKillSocket string
	var gotWantPID int
	s.killHolderPeer = func(socket string, wantPID int) (int, error) {
		gotKillSocket, gotWantPID = socket, wantPID
		_ = h.ln.Close() // the "kill" releases the socket
		return 4242, nil
	}

	s.superviseTick(t.Context())

	if h.shutdownCount() != 1 {
		t.Fatalf("shutdowns = %d, want 1", h.shutdownCount())
	}
	if gotKillSocket != h.socket {
		t.Fatalf("killHolderPeer got socket %q, want the holder socket %q", gotKillSocket, h.socket)
	}
	if gotWantPID != 4242 {
		t.Fatalf("killHolderPeer got wantPID %d, want the pid captured at gate time (4242)", gotWantPID)
	}
	if rec.count() != 1 || fake.setupCount() != 1 || !s.holder.ready(dirs[1]) {
		t.Fatalf("replacement incomplete after the kill: spawns=%d setups=%d", rec.count(), fake.setupCount())
	}
	if !strings.Contains(buf.String(), "killed socket peer pid 4242") {
		t.Fatalf("kill not logged loudly: %q", buf.String())
	}

	s2, dirs2, fake2, rec2 := newSuperviseServer(t)
	flipToFuse(t, s2, 1)
	h2 := startSkewedHolder(t, []mountd.MountInfo{{Dir: dirs2[1], Base: "/base", Live: true}}, false)
	s2.holderSocket = h2.socket
	s2.holderGoneWait = 300 * time.Millisecond
	s2.killHolderPeer = func(string, int) (int, error) { return 0, errors.New("kill refused") }

	s2.superviseTick(t.Context())

	if h2.shutdownCount() != 1 {
		t.Fatalf("shutdowns = %d, want 1", h2.shutdownCount())
	}
	if rec2.count() != 0 || fake2.setupCount() != 0 {
		t.Fatalf("failed kill still spawned/remounted: spawns=%d setups=%d", rec2.count(), fake2.setupCount())
	}
}

// TestSuperviseReplaceShutdownErrorOutcomeUnknown pins the errored-Shutdown
// semantics: the holder sweeps its mounts BEFORE the Shutdown reply, so an
// errored RPC is outcome-unknown, never nothing-happened. The cache must stop
// vouching and the replace claims must span the gone-wait; a holder observed
// gone continues the replace, and one still serving defers WITHOUT a kill (it
// may never have received the Shutdown — a healthy holder is never killed).
func TestSuperviseReplaceShutdownErrorOutcomeUnknown(t *testing.T) {
	t.Run("holder gone after errored shutdown continues the replace", func(t *testing.T) {
		s, dirs, fake, rec := newSuperviseServer(t)
		flipToFuse(t, s, 1)
		h := startNoReplySkewedHolder(t, []mountd.MountInfo{{Dir: dirs[1], Base: "/base", Live: true}}, true)
		s.holderSocket = h.socket

		s.superviseTick(t.Context())

		if h.shutdownCount() != 1 {
			t.Fatalf("shutdowns = %d, want 1", h.shutdownCount())
		}
		if rec.count() != 1 || fake.setupCount() != 1 || !s.holder.ready(dirs[1]) {
			t.Fatalf("replace did not continue after the errored shutdown: spawns=%d setups=%d ready=%v",
				rec.count(), fake.setupCount(), s.holder.ready(dirs[1]))
		}
		if !s.tryReserve(1) {
			t.Fatal("replace leaked its claims")
		}
	})

	t.Run("holder still serving defers with claims held and no kill", func(t *testing.T) {
		s, dirs, fake, rec := newSuperviseServer(t)
		flipToFuse(t, s, 1)
		h := startNoReplySkewedHolder(t, []mountd.MountInfo{{Dir: dirs[1], Base: "/base", Live: true}}, false)
		s.holderSocket = h.socket
		s.holderGoneWait = time.Second
		var killed atomic.Bool
		s.killHolderPeer = func(string, int) (int, error) {
			killed.Store(true)
			return 0, errors.New("must not be reached")
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			s.superviseTick(t.Context())
		}()
		select {
		case <-h.shutdownEntered:
		case <-time.After(10 * time.Second):
			t.Fatal("replace never reached the holder's Shutdown")
		}
		// Outcome-unknown window: the gone-wait is running and the sweep may be
		// in flight. The claims taken at gate time must still fence selects.
		if s.tryReserve(1) {
			t.Error("a select reserved a fuse account during the outcome-unknown gone-wait")
		}
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("replace did not defer after the gone-wait")
		}
		if killed.Load() {
			t.Fatal("an unacked shutdown still killed the serving holder")
		}
		if rec.count() != 0 || fake.setupCount() != 0 {
			t.Fatalf("deferred replace spawned/remounted: spawns=%d setups=%d", rec.count(), fake.setupCount())
		}
		// The sweep may have run even though the reply was lost: the cache must
		// have stopped vouching for the holder's mirrors.
		if s.holder.ready(dirs[1]) {
			t.Fatal("cache still vouches for a possibly-swept mirror")
		}
		if !s.tryReserve(1) {
			t.Fatal("deferred replace leaked its claims")
		}
	})
}

// TestSuperviseSkewWedgedKillIdentityGate pins the kill's identity discipline:
// the seam (peerpid.KillPid in production) resolves the socket's CURRENT peer
// and signals it only when it matches the pid captured at gate time — inside
// ONE dial, so a successor that bound the socket inside a WaitGone probe gap
// is refused with no signal sent. An uncaptured identity aborts before the
// seam is even consulted; a peer that vanished between probes needs no kill
// at all (ErrUnreachable continues the replace over the freed socket).
func TestSuperviseSkewWedgedKillIdentityGate(t *testing.T) {
	newWedged := func(t *testing.T) (*Server, map[int]string, *fakeFuseProv, *spawnRecorder, *skewedHolder, *bytes.Buffer, *bool) {
		t.Helper()
		s, dirs, fake, rec := newSuperviseServer(t)
		flipToFuse(t, s, 1)
		h := startSkewedHolder(t, []mountd.MountInfo{{Dir: dirs[1], Base: "/base", Live: true}}, false)
		s.holderSocket = h.socket
		s.holderGoneWait = 300 * time.Millisecond
		buf := &bytes.Buffer{}
		s.log = log.New(buf, "", 0)
		signalled := false
		s.killHolderPeer = func(string, int) (int, error) {
			signalled = true
			_ = h.ln.Close()
			return 4242, nil
		}
		return s, dirs, fake, rec, h, buf, &signalled
	}

	t.Run("changed peer is refused inside the kill seam", func(t *testing.T) {
		s, _, fake, rec, h, buf, signalled := newWedged(t)
		// KillPid's contract: a peer other than wantPID is refused in the same
		// dial that resolved it — no signal lands on the successor.
		s.killHolderPeer = func(socket string, wantPID int) (int, error) {
			return 0, fmt.Errorf("socket %s is held by pid 5555, not pid %d; refusing to kill", socket, wantPID)
		}

		s.superviseTick(t.Context())

		if h.shutdownCount() != 1 {
			t.Fatalf("shutdowns = %d, want 1", h.shutdownCount())
		}
		if *signalled {
			t.Fatal("kill fired on a peer that is not the gated holder")
		}
		if rec.count() != 0 || fake.setupCount() != 0 {
			t.Fatalf("aborted kill still spawned/remounted: spawns=%d setups=%d", rec.count(), fake.setupCount())
		}
		if !strings.Contains(buf.String(), "refusing to kill") {
			t.Fatalf("refused kill not logged: %q", buf.String())
		}
	})

	t.Run("uncaptured identity aborts before the kill seam", func(t *testing.T) {
		s, _, fake, rec, h, buf, signalled := newWedged(t)
		s.peerPID = func(string) (int, error) { return 0, errors.New("getsockopt refused") }

		s.superviseTick(t.Context())

		if h.shutdownCount() != 1 {
			t.Fatalf("shutdowns = %d, want 1", h.shutdownCount())
		}
		if *signalled {
			t.Fatal("kill seam consulted with no captured identity to match against")
		}
		if rec.count() != 0 || fake.setupCount() != 0 {
			t.Fatalf("aborted kill still spawned/remounted: spawns=%d setups=%d", rec.count(), fake.setupCount())
		}
		if !strings.Contains(buf.String(), "not captured at gate time") {
			t.Fatalf("uncaptured identity not logged: %q", buf.String())
		}
	})

	t.Run("vanished peer proceeds without a kill", func(t *testing.T) {
		s, dirs, fake, rec, h, _, signalled := newWedged(t)
		// The holder died between WaitGone's last probe and the kill dial:
		// the socket has no peer, nothing to signal, the replace proceeds.
		s.killHolderPeer = func(string, int) (int, error) {
			_ = h.ln.Close()
			return 0, peerpid.ErrUnreachable
		}

		s.superviseTick(t.Context())

		if *signalled {
			t.Fatal("kill fired on a vanished peer")
		}
		if rec.count() != 1 || fake.setupCount() != 1 || !s.holder.ready(dirs[1]) {
			t.Fatalf("replacement incomplete after the peer vanished: spawns=%d setups=%d", rec.count(), fake.setupCount())
		}
	})
}

// TestSuperviseLogsOncePerTransition pins the log discipline: state is logged
// on transitions (healthy→unhealthy, new spawn-error text, new deferral
// reason), never per tick.
func TestSuperviseLogsOncePerTransition(t *testing.T) {
	t.Run("unreachable and spawn failure", func(t *testing.T) {
		s, _, _, rec := newSuperviseServer(t)
		flipToFuse(t, s, 1)
		var buf bytes.Buffer
		s.log = log.New(&buf, "", 0)
		rec.setErr(errors.New("spawn exploded A"))

		s.superviseTick(t.Context())
		s.sup.retryAt = time.Now().Add(-time.Second) // force a real second attempt
		s.superviseTick(t.Context())

		if got := strings.Count(buf.String(), "mount holder unreachable"); got != 1 {
			t.Fatalf("unreachable logged %d times across two ticks, want 1:\n%s", got, buf.String())
		}
		if got := strings.Count(buf.String(), "spawn exploded A"); got != 1 {
			t.Fatalf("identical spawn failure logged %d times, want 1:\n%s", got, buf.String())
		}

		// New error text is a new transition: logged again, exactly once.
		rec.setErr(errors.New("spawn exploded B"))
		s.sup.retryAt = time.Now().Add(-time.Second)
		s.superviseTick(t.Context())
		s.sup.retryAt = time.Now().Add(-time.Second)
		s.superviseTick(t.Context())
		if got := strings.Count(buf.String(), "spawn exploded B"); got != 1 {
			t.Fatalf("new spawn failure logged %d times, want 1:\n%s", got, buf.String())
		}
	})

	t.Run("skew deferral", func(t *testing.T) {
		s, dirs, _, _ := newSuperviseServer(t)
		flipToFuse(t, s, 1)
		h := startSkewedHolder(t, []mountd.MountInfo{{Dir: dirs[1], Base: "/base", Live: true}}, true)
		s.holderSocket = h.socket
		// A stable blocking reason, so consecutive ticks defer identically.
		s.scanSessions = func() ([]procscan.Session, error) { return nil, errors.New("ps exploded") }
		var buf bytes.Buffer
		s.log = log.New(&buf, "", 0)

		s.superviseTick(t.Context())
		s.superviseTick(t.Context())

		if got := strings.Count(buf.String(), "deferring replacement"); got != 1 {
			t.Fatalf("deferral logged %d times across two ticks, want 1:\n%s", got, buf.String())
		}
		if h.shutdownCount() != 0 {
			t.Fatal("deferred replace still stopped the holder")
		}
	})
}

// TestSuperviseHolderLoopTicksAndExits pins the loop plumbing: ticks fire on
// the (shrunken) interval, the dead-holder respawn+remount actually runs from
// the loop, and the goroutine exits on ctx cancellation.
func TestSuperviseHolderLoopTicksAndExits(t *testing.T) {
	s, _, fake, _ := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	s.superviseInterval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.superviseHolder(ctx); close(done) }()

	deadline := time.Now().Add(5 * time.Second)
	for fake.setupCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("supervision never remounted the fuse row")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("superviseHolder did not exit on ctx cancellation")
	}
}

// TestHolderStateSupervisionAccessors pins the cache surface supervision keys
// on: view, hadMounts (which survives markUnhealthy — a dead holder's
// carcasses still warrant a respawn), mountDirs (live AND dead entries), and
// the SpawnError record.
func TestHolderStateSupervisionAccessors(t *testing.T) {
	var h holderState
	if healthy, ver := h.view(); healthy || ver != "" {
		t.Fatalf("zero cache view = (%v, %q), want unhealthy and empty", healthy, ver)
	}
	if h.hadMounts() || len(h.mountDirs()) != 0 {
		t.Fatal("zero cache claims mount history")
	}

	socket := startCannedHolder(t, []mountd.MountInfo{
		{Dir: "/pool/a", Base: "/b", Live: true},
		{Dir: "/pool/dead", Base: "/b", Live: false},
	})
	h.refresh(mountd.NewClient(socket))
	if healthy, ver := h.view(); !healthy || ver != version.String() {
		t.Fatalf("view after refresh = (%v, %q), want healthy at %q", healthy, ver, version.String())
	}
	dirs := h.mountDirs()
	sort.Strings(dirs)
	if !reflect.DeepEqual(dirs, []string{"/pool/a", "/pool/dead"}) {
		t.Fatalf("mountDirs = %v, want both the live and the dead entry", dirs)
	}
	if !h.hadMounts() {
		t.Fatal("refresh with mounts did not record history")
	}

	h.markUnhealthy()
	if !h.hadMounts() {
		t.Fatal("mount history lost on markUnhealthy")
	}
	if len(h.mountDirs()) != 0 {
		t.Fatal("unhealthy cache still lists dirs")
	}

	h.recordSpawnError("spawn exploded")
	if got := h.wireStatus().SpawnError; got != "spawn exploded" {
		t.Fatalf("SpawnError = %q, want the recorded failure", got)
	}
	h.recordSpawnError("")
	if got := h.wireStatus().SpawnError; got != "" {
		t.Fatalf("SpawnError not cleared: %q", got)
	}
}

// TestEvictionNeverDialsMountsSocket pins holder isolation on the daemon
// startup path: evicting a version-skewed DAEMON from the daemon socket —
// clean step-down or wedged-orphan kill — must never touch the mount-holder
// socket. The canned mounts listener tattles on any connection.
func TestEvictionNeverDialsMountsSocket(t *testing.T) {
	tattle := func(t *testing.T) (string, *atomic.Int32) {
		t.Helper()
		sockDir, err := os.MkdirTemp("/tmp", "ccp-tattle")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(sockDir) })
		socket := filepath.Join(sockDir, "m.sock")
		ln, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		var dials atomic.Int32
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return // listener closed: defined exit
				}
				dials.Add(1)
				conn.Close()
			}
		}()
		return socket, &dials
	}

	t.Run("clean step-down", func(t *testing.T) {
		guardKillSocketPeer(t)
		f := newFakeDaemon(t, "0.0.0-old", true)
		mounts, dials := tattle(t)
		s := testServer(f.socket, 3*time.Second)
		s.holderSocket = mounts
		ln, lock, err := s.listen()
		if err != nil {
			t.Fatalf("listen should evict the skewed daemon and bind: %v", err)
		}
		defer ln.Close()
		defer lock.Close()
		if got := dials.Load(); got != 0 {
			t.Fatalf("daemon eviction dialed the mounts socket %d time(s)", got)
		}
	})

	t.Run("wedged orphan killed", func(t *testing.T) {
		f := newFakeDaemon(t, "0.0.0-old", false)
		mounts, dials := tattle(t)
		setKillSocketPeer(t, func(socket string) (int, error) {
			if socket != f.socket {
				t.Errorf("kill aimed at %q, want the daemon socket %q", socket, f.socket)
			}
			f.ln.Close() // the "kill" releases the daemon socket
			return 999001, nil
		})
		s := testServer(f.socket, 2*time.Second)
		s.holderSocket = mounts
		ln, lock, err := s.listen()
		if err != nil {
			t.Fatalf("listen should reap the wedged orphan and bind: %v", err)
		}
		defer ln.Close()
		defer lock.Close()
		if got := dials.Load(); got != 0 {
			t.Fatalf("daemon eviction dialed the mounts socket %d time(s)", got)
		}
	})
}

// bindVersionedHolder binds a canned holder at an exact socket path replying
// ver on every op — including ver "" (a healthy holder whose version is
// unknown on the wire). A non-nil list answers each List with its result
// (called per RPC, so a fixture can vouch for mounts as they land); nil
// lists nothing. Shutdowns are counted and release the socket, like a real
// holder exiting. Returns the shutdown counter.
func bindVersionedHolder(t *testing.T, socket, ver string, list func() []mountd.MountInfo) *atomic.Int32 {
	t.Helper()
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var shutdowns atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed: defined exit
			}
			var req mountd.Request
			resp := mountd.Response{OK: true, Version: ver}
			isShutdown := false
			if json.NewDecoder(conn).Decode(&req) == nil {
				switch req.Op {
				case mountd.OpShutdown:
					isShutdown = true
				case mountd.OpList:
					if list != nil {
						resp.Mounts = list()
					}
				}
			}
			_ = json.NewEncoder(conn).Encode(resp)
			conn.Close()
			if isShutdown {
				shutdowns.Add(1)
				_ = ln.Close()
				return
			}
		}
	}()
	return &shutdowns
}

// TestSuperviseReverseSkewSteadyState pins the reverse-skew loop breaker: a
// daemon-initiated spawn execs the binary at the holder's install path, which
// a brew upgrade may have swapped under this still-running daemon — so the
// spawned holder can report a NEWER version than ours. That version is this
// daemon's steady state: re-replacing would exec the same binary, observe the
// same skew, and sweep every mirror each idle tick forever. Exactly one
// replace (or revive) may occur; subsequent ticks must leave the successor
// serving, with the converge guidance logged once and Skewed still surfaced.
func TestSuperviseReverseSkewSteadyState(t *testing.T) {
	t.Run("revive spawns a newer holder and settles", func(t *testing.T) {
		s, dirs, fake, _ := newSuperviseServer(t)
		flipToFuse(t, s, 1)
		var buf bytes.Buffer
		s.log = log.New(&buf, "", 0)
		var spawns atomic.Int32
		var successor *atomic.Int32
		s.spawnHolder = func(socket, _ string, _ time.Duration) error {
			spawns.Add(1)
			// Like a real holder, the successor vouches for the row once the
			// revive's remount lands — so the steady ticks must stay quiet.
			successor = bindVersionedHolder(t, socket, "9.9.9-next", func() []mountd.MountInfo {
				if fake.setupCount() == 0 {
					return nil
				}
				return []mountd.MountInfo{{Dir: dirs[1], Base: "/base", Live: true}}
			})
			return nil
		}

		for range 4 {
			s.superviseTick(t.Context())
		}

		if got := spawns.Load(); got != 1 {
			t.Fatalf("spawns = %d, want exactly 1 (reverse skew must settle, not loop)", got)
		}
		if got := successor.Load(); got != 0 {
			t.Fatalf("the newer spawned holder was shut down %d time(s); reverse skew must never re-replace", got)
		}
		// Exactly one remount, from the revive: the successor's List vouches
		// for it, so every steady tick's retryUnvouchedFuseRows pass finds the
		// row ready and touches nothing — per-tick mount quiescence is part of
		// the settling this test pins.
		if fake.setupCount() != 1 {
			t.Fatalf("setups=%d, want exactly 1 (revive remount only; steady ticks must stay quiet)", fake.setupCount())
		}
		if got := strings.Count(buf.String(), "restart the daemon to converge"); got != 1 {
			t.Fatalf("converge guidance logged %d time(s), want once: %q", got, buf.String())
		}
		if ws := s.holder.wireStatus(); !ws.Skewed {
			t.Fatalf("reverse skew not surfaced on the wire: %+v", ws)
		}
	})

	t.Run("replace mints a still-skewed successor exactly once", func(t *testing.T) {
		s, dirs, fake, _ := newSuperviseServer(t)
		flipToFuse(t, s, 1)
		old := startSkewedHolder(t, []mountd.MountInfo{{Dir: dirs[1], Base: "/base", Live: true}}, true)
		s.holderSocket = old.socket
		var spawns atomic.Int32
		var successor *atomic.Int32
		s.spawnHolder = func(socket, _ string, _ time.Duration) error {
			spawns.Add(1)
			// Like a real holder, the successor vouches for the row once the
			// replace's remount lands — so the steady ticks must stay quiet.
			successor = bindVersionedHolder(t, socket, "9.9.9-next", func() []mountd.MountInfo {
				if fake.setupCount() == 0 {
					return nil
				}
				return []mountd.MountInfo{{Dir: dirs[1], Base: "/base", Live: true}}
			})
			return nil
		}

		for range 3 {
			s.superviseTick(t.Context())
		}

		if old.shutdownCount() != 1 {
			t.Fatalf("old holder shutdowns = %d, want exactly 1 replace attempt", old.shutdownCount())
		}
		if got := spawns.Load(); got != 1 {
			t.Fatalf("spawns = %d, want 1", got)
		}
		if got := successor.Load(); got != 0 {
			t.Fatalf("still-skewed successor was shut down %d time(s); want steady state", got)
		}
		// Exactly one remount, from the replace: the successor's List vouches
		// for it, so the steady ticks neither re-replace (the spawn/shutdown
		// pins above) nor churn mounts — per-tick quiescence.
		if fake.setupCount() != 1 {
			t.Fatalf("setups=%d, want exactly 1 (replace remount only; steady ticks must stay quiet)", fake.setupCount())
		}
		if !s.tryReserve(1) {
			t.Fatal("replace leaked its claims")
		}
	})
}

// TestSuperviseTickUnknownVersionNoReplace pins the empty-version guard: a
// healthy holder whose reported version is "" (unknown) is not skew evidence
// — wireStatus's Skewed guard agrees — so the tick must neither replace nor
// respawn it.
func TestSuperviseTickUnknownVersionNoReplace(t *testing.T) {
	s, _, fake, rec := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	shutdowns := bindVersionedHolder(t, s.holderSocket, "", nil)

	for range 2 {
		s.superviseTick(t.Context())
	}

	if got := shutdowns.Load(); got != 0 {
		t.Fatalf("holder with unknown version was shut down %d time(s)", got)
	}
	if rec.count() != 0 || fake.setupCount() != 0 {
		t.Fatalf("unknown version disturbed mounts: spawns=%d setups=%d", rec.count(), fake.setupCount())
	}
}

// TestReviveRemountsPreRowMounts pins the pre-row carry: a dead holder's
// registry can serve a dir no account row names — a `ccp add` mid-login,
// whose row only lands at FinalizeAdd. The revive must remount it by the base
// recorded in the holder's last List (carriedBases survives markUnhealthy)
// instead of dropping it and stranding the add.
func TestReviveRemountsPreRowMounts(t *testing.T) {
	s, dirs, fake, rec := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	preRow, err := os.MkdirTemp("/tmp", "ccp-prerow")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(preRow) })
	var mu sync.Mutex
	var pairs [][2]string
	fake.setupFn = func(base, dir string) error {
		mu.Lock()
		pairs = append(pairs, [2]string{base, dir})
		mu.Unlock()
		return nil
	}
	old := startSkewedHolder(t, []mountd.MountInfo{
		{Dir: dirs[1], Base: "/base", Live: true},
		{Dir: preRow, Base: "/base2", Live: true},
	}, false)
	s.holderSocket = old.socket
	s.holder.refresh(s.holderClient()) // prime mounts AND bases from List
	_ = old.ln.Close()                 // the holder crashes

	s.superviseTick(t.Context())

	if rec.count() != 1 {
		t.Fatalf("spawns = %d, want 1", rec.count())
	}
	mu.Lock()
	got := append([][2]string(nil), pairs...)
	mu.Unlock()
	var sawPreRow bool
	for _, p := range got {
		if p[1] == preRow {
			sawPreRow = true
			if p[0] != "/base2" {
				t.Fatalf("pre-row mount remounted with base %q, want the carried base %q", p[0], "/base2")
			}
		}
	}
	if !sawPreRow {
		t.Fatalf("pre-row mount %s was dropped by the revive; setups: %v", preRow, got)
	}
	if !s.holder.ready(preRow) || !s.holder.ready(dirs[1]) {
		t.Fatalf("revive cache state: ready(preRow)=%v ready(row)=%v, want both vouched",
			s.holder.ready(preRow), s.holder.ready(dirs[1]))
	}
}

// swapForceUnmount replaces the daemon's direct force-unmount seam for one
// test, restoring it after. Tests using it must not run in parallel.
func swapForceUnmount(t *testing.T, fn func(string) error) {
	t.Helper()
	prev := forceUnmount
	forceUnmount = fn
	t.Cleanup(func() { forceUnmount = prev })
}

// stringSet builds a set from dirs for order-independent comparison.
func stringSet(dirs []string) map[string]bool {
	m := make(map[string]bool, len(dirs))
	for _, d := range dirs {
		m[d] = true
	}
	return m
}

// TestReviveForceUnmountsOrphansBeforeSpawn pins Fix 1a: the moment the holder
// is detected dead, every wedged fuse carcass is force-unmounted DIRECTLY (no
// holder) BEFORE the respawn, and a dir that is not a mountpoint is skipped.
func TestReviveForceUnmountsOrphansBeforeSpawn(t *testing.T) {
	cases := map[string]struct {
		mounted  map[int]bool // which fuse dirs read as (wedged) mountpoints
		wantDirs []int        // account ids whose dirs must be force-unmounted
	}{
		"both wedged carcasses cleared before respawn": {
			mounted:  map[int]bool{1: true, 2: true},
			wantDirs: []int{1, 2},
		},
		"non-mountpoint skipped": {
			mounted:  map[int]bool{1: true},
			wantDirs: []int{1},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, dirs, _, rec := newSuperviseServer(t)
			flipToFuse(t, s, 1)
			flipToFuse(t, s, 2)

			mountedByDir := map[string]bool{}
			for id, m := range tc.mounted {
				mountedByDir[dirs[id]] = m
			}
			fakeOverlayMounted(t, func(dir string) bool { return mountedByDir[dir] })

			var (
				mu              sync.Mutex
				unmounted       []string
				unmountsAtSpawn = -1
			)
			swapForceUnmount(t, func(dir string) error {
				mu.Lock()
				unmounted = append(unmounted, dir)
				mu.Unlock()
				return nil
			})
			base := rec.fn
			s.spawnHolder = func(socket, logPath string, to time.Duration) error {
				mu.Lock()
				if unmountsAtSpawn < 0 {
					unmountsAtSpawn = len(unmounted)
				}
				mu.Unlock()
				return base(socket, logPath, to)
			}

			s.superviseTick(t.Context())

			want := make([]string, 0, len(tc.wantDirs))
			for _, id := range tc.wantDirs {
				want = append(want, dirs[id])
			}
			mu.Lock()
			defer mu.Unlock()
			if !reflect.DeepEqual(stringSet(unmounted), stringSet(want)) {
				t.Fatalf("force-unmounted %v, want %v", unmounted, want)
			}
			if rec.count() != 1 {
				t.Fatalf("spawns = %d, want 1 (the respawn must still run after clearing carcasses)", rec.count())
			}
			if unmountsAtSpawn != len(want) {
				t.Fatalf("unmounts before spawn = %d, want %d (every carcass cleared before respawn)", unmountsAtSpawn, len(want))
			}
		})
	}
}

// TestReviveForceUnmountsCarriedOrphanBeforeSpawn pins Fix 1a's carry branch:
// orphanDirs unions the fuse account rows with the dead holder's carried pre-row
// mounts (carriedBases) — a `ccp add` mid-login the holder served before its row
// landed at FinalizeAdd. That carried dir is a wedged carcass too, so it must be
// force-unmounted DIRECTLY (no holder), BEFORE the respawn, even though no
// account row names it. (TestReviveForceUnmountsOrphansBeforeSpawn covers only
// the account-row dirs; carriedBases stays empty there.)
func TestReviveForceUnmountsCarriedOrphanBeforeSpawn(t *testing.T) {
	s, dirs, _, rec := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	preRow, err := os.MkdirTemp("/tmp", "ccp-prerow")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(preRow) })

	// Prime carriedBases from a live holder serving a pre-row dir (no account
	// row), then crash it: the snapshot survives markUnhealthy into the revive.
	old := startSkewedHolder(t, []mountd.MountInfo{
		{Dir: dirs[1], Base: "/base", Live: true},
		{Dir: preRow, Base: "/base2", Live: true},
	}, false)
	s.holderSocket = old.socket
	s.holder.refresh(s.holderClient())
	_ = old.ln.Close() // the holder crashes; carriedBases keeps the pre-row dir

	// Only the carried pre-row dir reads as a wedged mountpoint.
	fakeOverlayMounted(t, func(dir string) bool { return dir == preRow })

	var (
		mu              sync.Mutex
		unmounted       []string
		unmountsAtSpawn = -1
	)
	swapForceUnmount(t, func(dir string) error {
		mu.Lock()
		unmounted = append(unmounted, dir)
		mu.Unlock()
		return nil
	})
	base := rec.fn
	s.spawnHolder = func(socket, logPath string, to time.Duration) error {
		mu.Lock()
		if unmountsAtSpawn < 0 {
			unmountsAtSpawn = len(unmounted)
		}
		mu.Unlock()
		return base(socket, logPath, to)
	}

	s.superviseTick(t.Context())

	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(stringSet(unmounted), stringSet([]string{preRow})) {
		t.Fatalf("force-unmounted %v, want the carried pre-row dir [%s]", unmounted, preRow)
	}
	if rec.count() != 1 {
		t.Fatalf("spawns = %d, want 1 (the respawn must still run after clearing the carried carcass)", rec.count())
	}
	if unmountsAtSpawn != 1 {
		t.Fatalf("unmounts before spawn = %d, want 1 (the carried carcass cleared before respawn)", unmountsAtSpawn)
	}
}

// driveRetryTicks runs n steady-state supervision ticks against a settled
// holder, rewinding the per-row backoff window before each so every tick makes
// a real heal attempt (mirroring the supervise cadence without sleeping).
func driveRetryTicks(t *testing.T, s *Server, id, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if st, ok := s.sup.rowRetry[id]; ok {
			st.retryAt = time.Now().Add(-time.Second)
			s.sup.rowRetry[id] = st
		}
		s.superviseTick(t.Context())
	}
}

// TestRemountBreakerThreshold pins the breaker const guard: a single transient
// mount failure must never escalate, so the threshold must be at least 2.
func TestRemountBreakerThreshold(t *testing.T) {
	if remountBreakerThreshold < 2 {
		t.Fatalf("remountBreakerThreshold = %d, want >= 2 so a single transient mount failure never escalates", remountBreakerThreshold)
	}
}

// TestSuperviseRemountBreakerEscalates pins Fix 1b: after exactly
// remountBreakerThreshold consecutive wedged remounts the breaker fires —
// force-unmounting the carcass, converting the row to symlink, dropping the
// ledger entry, dropping the holder-cache vouch, and surfacing it loudly.
func TestSuperviseRemountBreakerEscalates(t *testing.T) {
	s, dirs, fake, _ := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil) // healthy, vouches for nothing
	fake.setupErr = mountTimeoutChain()        // healRetry forever — the wedged shape
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })

	var (
		mu        sync.Mutex
		unmounted []string
	)
	swapForceUnmount(t, func(dir string) error {
		mu.Lock()
		unmounted = append(unmounted, dir)
		mu.Unlock()
		return nil
	})
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)

	driveRetryTicks(t, s, 1, remountBreakerThreshold)

	mu.Lock()
	gotUnmounted := append([]string(nil), unmounted...)
	mu.Unlock()
	if len(gotUnmounted) != 1 || gotUnmounted[0] != dirs[1] {
		t.Fatalf("breaker force-unmounted %v, want exactly [%s]", gotUnmounted, dirs[1])
	}
	if got := kindOf(t, s, 1); got != "symlink" {
		t.Fatalf("row kind after the breaker = %q, want symlink", got)
	}
	if _, ok := s.sup.rowRetry[1]; ok {
		t.Fatal("breaker left a rowRetry entry; the churn would continue")
	}
	if s.holder.ready(dirs[1]) {
		t.Fatal("breaker did not drop the holder-cache vouch for the converted dir")
	}
	if !strings.Contains(buf.String(), "never recovered") {
		t.Fatalf("breaker escalation not surfaced in the log:\n%s", buf.String())
	}
}

// TestSuperviseRemountBreakerEscalatesUnderLiveSession pins that the breaker is
// UNGATED by live sessions: a never-recovering mount is a whole-machine hazard,
// not a healthy mount, so escalateWedgedRow force-unmounts the carcass and
// converts the row to symlink even with a live session on the dir (relaunch is
// the documented fix, exactly as the held-dead remount policy accepts). The
// idle/session gate belongs only to fallbackToSymlink on the genuine-mount path;
// a regression adding one here would reintroduce the freeze hazard the kill-9
// incident exposed — with every other breaker test running an idle scan, this is
// the only case that fails if it does.
func TestSuperviseRemountBreakerEscalatesUnderLiveSession(t *testing.T) {
	s, dirs, fake, _ := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil) // healthy, vouches for nothing
	fake.setupErr = mountTimeoutChain()        // healRetry forever — the wedged shape
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })
	// A live session on the very dir the breaker is about to force down: a
	// session gate would consult it and bail; the breaker must not.
	s.scanSessions = func() ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}

	var (
		mu        sync.Mutex
		unmounted []string
	)
	swapForceUnmount(t, func(dir string) error {
		mu.Lock()
		unmounted = append(unmounted, dir)
		mu.Unlock()
		return nil
	})

	driveRetryTicks(t, s, 1, remountBreakerThreshold)

	mu.Lock()
	gotUnmounted := append([]string(nil), unmounted...)
	mu.Unlock()
	if len(gotUnmounted) != 1 || gotUnmounted[0] != dirs[1] {
		t.Fatalf("breaker force-unmounted %v under a live session, want exactly [%s] (it must stay ungated)", gotUnmounted, dirs[1])
	}
	if got := kindOf(t, s, 1); got != "symlink" {
		t.Fatalf("row kind after the breaker = %q, want symlink (a live session must not block escalation)", got)
	}
	if _, ok := s.sup.rowRetry[1]; ok {
		t.Fatal("breaker left a rowRetry entry; the churn would continue")
	}
	if s.holder.ready(dirs[1]) {
		t.Fatal("breaker did not drop the holder-cache vouch for the converted dir")
	}
}

// TestSuperviseRemountBreakerHoldsUnderThreshold pins that fewer than the
// threshold consecutive failures keep retrying — no escalation, the row stays
// fuse, and the ledger keeps counting.
func TestSuperviseRemountBreakerHoldsUnderThreshold(t *testing.T) {
	s, dirs, fake, _ := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = mountTimeoutChain()
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })

	var unmounts int
	swapForceUnmount(t, func(string) error { unmounts++; return nil })

	driveRetryTicks(t, s, 1, remountBreakerThreshold-1)

	if unmounts != 0 {
		t.Fatalf("force-unmounts under the threshold = %d, want 0", unmounts)
	}
	if got := kindOf(t, s, 1); got != "fuse" {
		t.Fatalf("row kind under the threshold = %q, want fuse (still retrying)", got)
	}
	if got := s.sup.rowRetry[1].hazard; got != remountBreakerThreshold-1 {
		t.Fatalf("hazard count = %d, want %d (one short of the breaker)", got, remountBreakerThreshold-1)
	}
}

// TestSuperviseRemountBreakerResetsOnMount pins that a successful mount before
// the threshold clears the breaker's hazard count: a row that recovers never
// fires the breaker, and a later failure restarts the count from one.
func TestSuperviseRemountBreakerResetsOnMount(t *testing.T) {
	s, dirs, fake, _ := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = mountTimeoutChain()
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })

	var unmounts int
	swapForceUnmount(t, func(string) error { unmounts++; return nil })

	// A few wedged attempts short of the breaker…
	driveRetryTicks(t, s, 1, remountBreakerThreshold-2)
	if got := s.sup.rowRetry[1].hazard; got != remountBreakerThreshold-2 {
		t.Fatalf("hazard before recovery = %d, want %d", got, remountBreakerThreshold-2)
	}

	// …then the mount comes up: the ledger entry is dropped entirely.
	fake.setupErr = nil
	driveRetryTicks(t, s, 1, 1)
	if _, ok := s.sup.rowRetry[1]; ok {
		t.Fatal("a successful mount left a rowRetry entry")
	}
	if !s.holder.ready(dirs[1]) {
		t.Fatal("recovered row not vouched for")
	}

	// A fresh wedge starts the hazard count over from one — never near the
	// threshold — so the recovered row never carries stale breaker progress.
	fake.setupErr = mountTimeoutChain()
	driveRetryTicks(t, s, 1, 1)
	if got := s.sup.rowRetry[1].hazard; got != 1 {
		t.Fatalf("hazard after a fresh failure = %d, want 1 (reset by the recovery)", got)
	}
	if unmounts != 0 {
		t.Fatalf("force-unmounts across a recovering row = %d, want 0", unmounts)
	}
}

// TestSuperviseRemountBreakerNeverEscalatesTCC pins the load-bearing exclusion:
// a TCC-blocked row is a CLEAN not-mounted state, so it must retry FOREVER for
// the human to grant Network Volumes — the breaker must never escalate it, even
// far past the threshold.
func TestSuperviseRemountBreakerNeverEscalatesTCC(t *testing.T) {
	s, dirs, fake, _ := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = fmt.Errorf("mount: %w", overlay.ErrMountNotLive) // healTCCBlocked
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })

	var unmounts int
	swapForceUnmount(t, func(string) error { unmounts++; return nil })

	ticks := remountBreakerThreshold + 3
	driveRetryTicks(t, s, 1, ticks)

	if unmounts != 0 {
		t.Fatalf("TCC-blocked row force-unmounted %d time(s); it must retry forever", unmounts)
	}
	if got := kindOf(t, s, 1); got != "fuse" {
		t.Fatalf("TCC-blocked row kind = %q, want fuse (still waiting on the grant)", got)
	}
	st, ok := s.sup.rowRetry[1]
	if !ok {
		t.Fatal("TCC-blocked row dropped its retry ledger; it would stop retrying")
	}
	if st.failures != ticks {
		t.Fatalf("TCC failures = %d, want %d (kept backing off)", st.failures, ticks)
	}
	if st.hazard != 0 {
		t.Fatalf("TCC hazard = %d, want 0 (never counts toward the breaker)", st.hazard)
	}
}
