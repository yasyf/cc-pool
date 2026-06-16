package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
)

// newTestServer builds a Server over a temp-dir store with two accounts:
// acct-1 emptier (util 10) than acct-2 (util 50), both freshly sampled. The
// empty fake keychain makes any best-effort preflight refresh a harmless miss.
//
// scanSessions is stubbed to report no live sessions so the daemon tests never
// shell out to the real `ps`: a live claude could never be attributed to these
// temp config dirs anyway (so the result is identical), and the real ps is an
// external the suite must not depend on — doubly so here, where a wedged mount
// on the dev box would hang it (the very failure this package's scan bound
// guards against). Tests that need live pids set their own s.scanSessions.
func newTestServer(t *testing.T) (*Server, map[int]string) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	dirs := map[int]string{}
	now := time.Now()
	for id, util := range map[int]float64{1: 10, 2: 50} {
		dir := filepath.Join(t.TempDir(), "acct")
		dirs[id] = dir
		if err := st.UpsertAccount(store.Account{
			ID: id, ConfigDir: dir, OverlayKind: "symlink",
			KeychainService: "ccp-test-missing", KeychainAccount: "ccp-test",
		}); err != nil {
			t.Fatal(err)
		}
		if err := st.InsertUsageSample(store.UsageSample{AccountID: id, TS: now, Util5h: util, Util7d: util}); err != nil {
			t.Fatal(err)
		}
	}
	return &Server{
		m: &pool.Manager{
			Store: st, OAuth: &fakeOAuth{}, Keychain: newFakeKeychain(), LockDir: t.TempDir(),
		},
		snapshot:        filepath.Join(t.TempDir(), "status.json"),
		log:             log.New(io.Discard, "", 0),
		scanSessions:    func(context.Context) ([]procscan.Session, error) { return nil, nil },
		reservations:    map[int]time.Time{},
		converting:      map[int]bool{},
		rlStreak:        map[int]int{},
		authStreak:      map[int]int{},
		lastAuthAttempt: map[int]time.Time{},
	}, dirs
}

func TestReservedCountExpiresAfterTTL(t *testing.T) {
	s := &Server{reservations: map[int]time.Time{}}

	if got := s.reservedCount(1); got != 0 {
		t.Fatalf("reservedCount before reserve = %d, want 0", got)
	}

	s.tryReserve(1)
	if got := s.reservedCount(1); got != 1 {
		t.Fatalf("reservedCount after reserve = %d, want 1", got)
	}

	// Backdate past the TTL: the reservation must read as expired AND be pruned.
	s.mu.Lock()
	s.reservations[1] = time.Now().Add(-reservationTTL - time.Second)
	s.mu.Unlock()
	if got := s.reservedCount(1); got != 0 {
		t.Fatalf("reservedCount after TTL = %d, want 0", got)
	}
	s.mu.Lock()
	_, ok := s.reservations[1]
	s.mu.Unlock()
	if ok {
		t.Fatal("expired reservation was not deleted")
	}
}

func TestHandleSelectRecordsSticky(t *testing.T) {
	s, dirs := newTestServer(t)
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[1] {
		t.Fatalf("expected emptier acct-1 (%s), got %+v", dirs[1], resp)
	}
	if resp.Sticky {
		t.Fatal("first select must not report sticky")
	}
	// A sampled pick carries its raw 5h/7d remaining back for the diagnostic line.
	if !resp.HasUsage || resp.Remaining5h <= 0 || resp.Remaining7d <= 0 {
		t.Fatalf("expected remaining headroom on a sampled pick, got HasUsage=%v Remaining5h=%.1f Remaining7d=%.1f", resp.HasUsage, resp.Remaining5h, resp.Remaining7d)
	}
	st, ok, err := s.m.Store.GetSticky("/proj")
	if err != nil || !ok || st.AccountID != 1 {
		t.Fatalf("winner not recorded: %+v ok=%v err=%v", st, ok, err)
	}
}

func TestHandleSelectHonorsSticky(t *testing.T) {
	s, dirs := newTestServer(t)
	// Sticky points at the WORSE account; it must still win.
	if err := s.m.Store.UpsertSticky("/proj", 2, time.Now()); err != nil {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[2] || !resp.Sticky {
		t.Fatalf("expected sticky acct-2 (%s), got %+v", dirs[2], resp)
	}
}

// TestHandleSelectSkipsExhaustedStickyPin replays the 2026-06-10 incident: the
// cwd is pinned to an account whose 5h window is pegged with the reset ~21
// minutes out (reset credit keeps its eff5 ≈ 93). The pin must be abandoned,
// the pick must be a healthy account, and the sticky row rewritten.
func TestHandleSelectSkipsExhaustedStickyPin(t *testing.T) {
	s, dirs := newTestServer(t)
	now := time.Now().Add(time.Minute) // newer than the harness samples
	if err := s.m.Store.InsertUsageSample(store.UsageSample{
		AccountID: 2, TS: now, Util5h: 100, Util7d: 21,
		Resets5h: now.Add(21 * time.Minute), Resets7d: now.Add(24 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.m.Store.UpsertSticky("/proj", 2, now); err != nil {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[1] {
		t.Fatalf("expected healthy acct-1 (%s) over the exhausted pin, got %+v", dirs[1], resp)
	}
	if resp.Sticky || resp.ExhaustedFallback {
		t.Fatalf("a fresh healthy pick must report neither sticky nor fallback: %+v", resp)
	}
	st, ok, err := s.m.Store.GetSticky("/proj")
	if err != nil || !ok || st.AccountID != 1 {
		t.Fatalf("sticky row not rewritten to the winner: %+v ok=%v err=%v", st, ok, err)
	}
}

// TestHandleSelectMarksSessionWithCwd: a select carrying a pid opens a session
// row attributed to the caller's cwd, feeding the sticky activity rules.
func TestHandleSelectMarksSessionWithCwd(t *testing.T) {
	s, _ := newTestServer(t)
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, PID: 4242, Cwd: "/proj"})
	if !resp.OK || resp.SelectedID == nil {
		t.Fatalf("select failed: %+v", resp)
	}
	live, err := s.m.Store.ListActiveSessions()
	if err != nil || len(live) != 1 {
		t.Fatalf("sessions = %+v err=%v", live, err)
	}
	if live[0].PID != 4242 || live[0].Cwd != "/proj" || live[0].AccountID != *resp.SelectedID {
		t.Fatalf("session row = %+v, want pid 4242 cwd /proj acct %d", live[0], *resp.SelectedID)
	}

	// Negative: NoMark must not open a row.
	s2, _ := newTestServer(t)
	if resp := s2.handleSelect(t.Context(), Request{Op: OpSelect, PID: 4242, NoMark: true, Cwd: "/proj"}); !resp.OK {
		t.Fatalf("select failed: %+v", resp)
	}
	if live, _ := s2.m.Store.ListActiveSessions(); len(live) != 0 {
		t.Fatalf("NoMark must not mark: %+v", live)
	}
}

// TestHandleSelectBindsWarmEndedSession is the headline activity-rule fix: a
// session that outlived the old selected_at TTL ended minutes ago, so the pin
// must still bind — the warm cache is exactly what stickiness protects.
func TestHandleSelectBindsWarmEndedSession(t *testing.T) {
	s, dirs := newTestServer(t)
	now := time.Now()
	if err := s.m.Store.UpsertSticky("/proj", 2, now.Add(-3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	id, err := s.m.Store.OpenSession(2, 0, dirs[2], "/proj", now.Add(-3*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.m.Store.CloseSession(id, now.Add(-10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[2] || !resp.Sticky {
		t.Fatalf("expected sticky acct-2 (%s) via warm ended session, got %+v", dirs[2], resp)
	}
}

// TestHandleSelectHoldsLiveOnlyPin: when the pinned dir's only session is
// still live, a new session cannot resume it — rank freely, but never repoint
// the pin (it binds for a TTL once the session ends).
func TestHandleSelectHoldsLiveOnlyPin(t *testing.T) {
	s, dirs := newTestServer(t)
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)
	now := time.Now()
	if err := s.m.Store.UpsertSticky("/proj", 2, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.m.Store.OpenSession(2, 0, dirs[2], "/proj", now.Add(-10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[1] || resp.Sticky {
		t.Fatalf("expected free non-sticky acct-1 (%s), got %+v", dirs[1], resp)
	}
	if resp.PinHeldAccount != nil {
		t.Fatalf("an auto hold must not flag a held manual pin: %+v", resp.PinHeldAccount)
	}
	st, ok, _ := s.m.Store.GetSticky("/proj")
	if !ok || st.AccountID != 2 {
		t.Fatalf("held pin was repointed: %+v ok=%v", st, ok)
	}
	// Drain the preflight goroutine before reading the shared log buffer.
	s.wg.Wait()
	if !strings.Contains(buf.String(), "select (pin-held): /proj -> acct-01") {
		t.Fatalf("held pin not logged: %q", buf.String())
	}
}

// TestHandleSelectQuickResumeBindsAfterReap: a session whose claude just died
// must not hold the pin until the next ~3.5-minute poll — handleSelect
// reconciles before deciding, so the dead row reads as a warm end and the pin
// binds.
func TestHandleSelectQuickResumeBindsAfterReap(t *testing.T) {
	s, dirs := newTestServer(t)
	now := time.Now()
	if err := s.m.Store.UpsertSticky("/proj", 2, now.Add(-3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// pid 4000000 can never belong to a live claude (macOS pids are 5-digit),
	// so handleSelect's procscan-backed sweep reaps the row. A reconcile saw
	// it alive 10 minutes ago, so the reap stamps a warm end.
	if _, err := s.m.Store.OpenSession(2, 4000000, dirs[2], "/proj", now.Add(-3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.m.Store.CloseDeadSessions(map[int]bool{4000000: true}, now.Add(-10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[2] || !resp.Sticky {
		t.Fatalf("quick resume must bind the pin via the reaped warm end, got %+v", resp)
	}
}

// TestHandleSelectForcedMarksSession: a forced select carrying a pid marks the
// checkout like a ranked one, so forced sessions feed the activity rules.
func TestHandleSelectForcedMarksSession(t *testing.T) {
	s, _ := newTestServer(t)
	forced := 2
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &forced, PID: 4242, Cwd: "/proj"})
	if !resp.OK {
		t.Fatalf("forced select failed: %+v", resp)
	}
	live, err := s.m.Store.ListActiveSessions()
	if err != nil || len(live) != 1 {
		t.Fatalf("sessions = %+v err=%v", live, err)
	}
	if live[0].PID != 4242 || live[0].Cwd != "/proj" || live[0].AccountID != 2 {
		t.Fatalf("session row = %+v, want pid 4242 cwd /proj acct 2", live[0])
	}

	// Negative: NoMark suppresses the row on the forced path too.
	s2, _ := newTestServer(t)
	if resp := s2.handleSelect(t.Context(), Request{Op: OpSelect, Account: &forced, PID: 4242, NoMark: true, Cwd: "/proj"}); !resp.OK {
		t.Fatalf("forced select failed: %+v", resp)
	}
	if live, _ := s2.m.Store.ListActiveSessions(); len(live) != 0 {
		t.Fatalf("forced NoMark must not mark: %+v", live)
	}
}

// TestHandleSelectHoldsUnusableManualPin: a manual pin to an account that
// cannot serve is bypassed loudly (PinHeldAccount) but never repointed.
func TestHandleSelectHoldsUnusableManualPin(t *testing.T) {
	s, dirs := newTestServer(t)
	now := time.Now().Add(time.Minute) // newer than the harness samples
	if err := s.m.Store.PinManual("/proj", 2, now); err != nil {
		t.Fatal(err)
	}
	if err := s.m.Store.InsertUsageSample(store.UsageSample{
		AccountID: 2, TS: now, Util5h: 100, Util7d: 21,
		Resets5h: now.Add(21 * time.Minute), Resets7d: now.Add(24 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[1] || resp.Sticky {
		t.Fatalf("expected free acct-1 (%s) over the exhausted manual pin, got %+v", dirs[1], resp)
	}
	if resp.PinHeldAccount == nil || *resp.PinHeldAccount != 2 {
		t.Fatalf("held manual pin must be surfaced, got %+v", resp.PinHeldAccount)
	}
	st, ok, _ := s.m.Store.GetSticky("/proj")
	if !ok || st.AccountID != 2 || !st.Manual {
		t.Fatalf("manual pin lost on hold: %+v ok=%v", st, ok)
	}
}

// TestHandleSelectForcedKeepsManualPin: a one-shot forced select must not
// silently destroy an explicit manual pin to a different account.
func TestHandleSelectForcedKeepsManualPin(t *testing.T) {
	s, dirs := newTestServer(t)
	now := time.Now()
	if err := s.m.Store.PinManual("/proj", 1, now); err != nil {
		t.Fatal(err)
	}
	forced := 2
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &forced, NoMark: true, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[2] {
		t.Fatalf("forced select failed: %+v", resp)
	}
	st, ok, _ := s.m.Store.GetSticky("/proj")
	if !ok || st.AccountID != 1 || !st.Manual {
		t.Fatalf("forced select repointed the manual pin: %+v ok=%v", st, ok)
	}
}

// TestHandleSelectExhaustedFallback: with every account exhausted, select must
// return the least-bad one flagged ExhaustedFallback (never an error), carrying
// the pick's extra-usage flag and recovery time for the client warning, and the
// log must name the exhausted runner-up (no Available candidates exist).
func TestHandleSelectExhaustedFallback(t *testing.T) {
	s, dirs := newTestServer(t)
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)
	now := time.Now().Add(time.Minute)
	reset := now.Add(20 * time.Minute)
	for id, util7 := range map[int]float64{1: 90, 2: 10} {
		if err := s.m.Store.InsertUsageSample(store.UsageSample{
			AccountID: id, TS: now, Util5h: 100, Util7d: util7,
			Resets5h: reset, ExtraEnabled: id == 2,
		}); err != nil {
			t.Fatal(err)
		}
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true, Cwd: "/proj"})
	if !resp.OK || !resp.ExhaustedFallback {
		t.Fatalf("expected a flagged fallback pick, got %+v", resp)
	}
	if resp.Dir != dirs[2] || !resp.ExtraEnabled {
		t.Fatalf("expected least-bad acct-2 (%s) with extra usage surfaced, got %+v", dirs[2], resp)
	}
	if resp.SoonestReset == nil || !resp.SoonestReset.Equal(reset.Truncate(time.Second)) {
		t.Fatalf("fallback must carry the pick's recovery time %v for the warning, got %v", reset, resp.SoonestReset)
	}
	// Drain the preflight goroutine before reading the shared log buffer.
	s.wg.Wait()
	logged := buf.String()
	if !strings.Contains(logged, "select (exhausted-fallback): /proj -> acct-02") {
		t.Fatalf("fallback pick not logged as such: %q", logged)
	}
	if !strings.Contains(logged, "runner-up acct-01") {
		t.Fatalf("fallback log must name the exhausted runner-up: %q", logged)
	}
}

// TestHandleSelectNoFallback: a --wait client (NoFallback) refuses the
// least-bad exhausted pick, and the daemon must not commit the discarded
// pick's side effects — no sticky rewrite, no reservation.
func TestHandleSelectNoFallback(t *testing.T) {
	s, _ := newTestServer(t)
	now := time.Now().Add(time.Minute)
	for id := 1; id <= 2; id++ {
		if err := s.m.Store.InsertUsageSample(store.UsageSample{
			AccountID: id, TS: now, Util5h: 100, Resets5h: now.Add(20 * time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj", NoFallback: true})
	if resp.OK || !resp.NoneAvailable {
		t.Fatalf("NoFallback over an exhausted pool must report none available, got %+v", resp)
	}
	if _, ok, _ := s.m.Store.GetSticky("/proj"); ok {
		t.Fatal("a refused fallback pick must not rewrite the sticky record")
	}
	for id := 1; id <= 2; id++ {
		if s.reservedCount(id) != 0 {
			t.Fatalf("a refused fallback pick must not reserve acct-%d", id)
		}
	}
}

// TestHandleStatusPropagatesExhaustionAndOverage pins the status pipeline both
// the pool layer (sample → Snapshot) and the wire layer (Snapshot → toStatuses)
// — the operator-facing signal for exactly the state this fix gates on.
func TestHandleStatusPropagatesExhaustionAndOverage(t *testing.T) {
	s, _ := newTestServer(t)
	now := time.Now().Add(time.Minute)
	if err := s.m.Store.InsertUsageSample(store.UsageSample{
		AccountID: 1, TS: now, Util5h: 100, Util7d: 21,
		Resets5h:     now.Add(20 * time.Minute),
		ExtraEnabled: true, ExtraUsed: 177, ExtraLimit: 5000,
	}); err != nil {
		t.Fatal(err)
	}
	resp := s.handleStatus(t.Context())
	if !resp.OK || len(resp.Accounts) != 2 {
		t.Fatalf("status failed: %+v", resp)
	}
	var acct1 *AccountStatus
	for i := range resp.Accounts {
		if resp.Accounts[i].ID == 1 {
			acct1 = &resp.Accounts[i]
		} else if resp.Accounts[i].Exhausted || resp.Accounts[i].ExtraEnabled {
			t.Fatalf("healthy acct must carry no exhaustion/overage: %+v", resp.Accounts[i])
		}
	}
	if acct1 == nil {
		t.Fatal("acct-1 missing from status")
	}
	if !acct1.Exhausted {
		t.Fatalf("pegged account must report exhausted: %+v", acct1)
	}
	if !acct1.ExtraEnabled || acct1.ExtraUsed != 177 || acct1.ExtraLimit != 5000 {
		t.Fatalf("overage state must survive the wire: %+v", acct1)
	}
}

// TestHandleSelectNoneAvailable: all rate-limited → structured NoneAvailable
// (not just an error string) plus the soonest reset for --wait.
func TestHandleSelectNoneAvailable(t *testing.T) {
	s, _ := newTestServer(t)
	now := time.Now().Add(time.Minute)
	reset := now.Add(30 * time.Minute)
	for id := 1; id <= 2; id++ {
		if err := s.m.Store.InsertUsageSample(store.UsageSample{
			AccountID: id, TS: now, Util5h: 50, Resets5h: reset, RateLimited: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true, Cwd: "/proj"})
	if resp.OK || !resp.NoneAvailable {
		t.Fatalf("expected structured none-available, got %+v", resp)
	}
	if resp.SoonestReset == nil || !resp.SoonestReset.Equal(reset.Truncate(time.Second)) {
		t.Fatalf("expected soonest reset %v, got %v", reset, resp.SoonestReset)
	}
}

// TestHandleSelectLogsPick: every select logs its outcome — the 2026-06-10
// incident needed DB forensics because fresh picks logged nothing.
func TestHandleSelectLogsPick(t *testing.T) {
	s, _ := newTestServer(t)
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)
	if resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true, Cwd: "/proj"}); !resp.OK {
		t.Fatalf("select failed: %+v", resp)
	}
	// Drain the preflight goroutine before reading the shared log buffer.
	s.wg.Wait()
	logged := buf.String()
	if !strings.Contains(logged, "select: /proj -> acct-01") {
		t.Fatalf("fresh pick not logged: %q", logged)
	}
	if !strings.Contains(logged, "5h 10% used") || !strings.Contains(logged, "runner-up acct-02") {
		t.Fatalf("log line missing usage/runner-up: %q", logged)
	}
}

func TestHandleSelectForcedRecordsSticky(t *testing.T) {
	s, dirs := newTestServer(t)
	acct := 2
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &acct, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[2] {
		t.Fatalf("expected forced acct-2 (%s), got %+v", dirs[2], resp)
	}
	if resp.Sticky {
		t.Fatal("forced select must not report sticky (ranking was not overridden)")
	}
	st, ok, err := s.m.Store.GetSticky("/proj")
	if err != nil || !ok || st.AccountID != 2 {
		t.Fatalf("forced account not recorded: %+v ok=%v err=%v", st, ok, err)
	}
}

// TestServeDrainsInFlightHandlerOnShutdown pins the shutdown ordering: serve
// must wait for in-flight request handlers before returning (after which Run's
// deferred m.Close() closes the database under them).
//
// Synchronization is structural, not sleep-based: a first connection is parked
// mid-request (its handler is wg-tracked the moment the accept loop dequeues
// it), then a second connection completes a full health round-trip. The accept
// loop is sequential and unix sockets accept FIFO, so the health response
// proves the parked connection was already accepted and tracked — only then is
// the ctx cancelled.
func TestServeDrainsInFlightHandlerOnShutdown(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	// macOS caps sun_path at 104 bytes; t.TempDir's /var/folders/... path plus
	// the long test name exceeds it, so the socket gets its own short dir.
	sockDir, err := os.MkdirTemp("/tmp", "ccp-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })

	var logBuf bytes.Buffer
	s := &Server{
		m:               &pool.Manager{Store: st},
		socket:          filepath.Join(sockDir, "d.sock"),
		snapshot:        filepath.Join(t.TempDir(), "status.json"),
		log:             log.New(&logBuf, "", 0),
		reservations:    map[int]time.Time{},
		rlStreak:        map[int]int{},
		authStreak:      map[int]int{},
		lastAuthAttempt: map[int]time.Time{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var serveErr, closeErr error
	done := make(chan struct{})
	go func() {
		// Mirror Run's defer ordering: the DB closes as soon as serve returns.
		serveErr = s.serve(ctx)
		closeErr = st.Close()
		close(done)
	}()

	dial := func() net.Conn {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			conn, err := net.Dial("unix", s.socket)
			if err == nil {
				return conn
			}
			if time.Now().After(deadline) {
				t.Fatalf("dial daemon socket: %v", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Park a handler mid-request: it blocks in Decode awaiting the closing brace.
	parked := dial()
	defer parked.Close()
	if _, err := parked.Write([]byte(`{"op":"status"`)); err != nil {
		t.Fatal(err)
	}

	// A full round-trip on a second connection orders the parked connection's
	// accept (and wg tracking) before the cancellation below.
	probe := dial()
	defer probe.Close()
	if _, err := probe.Write([]byte(`{"op":"health"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	var health Response
	if err := json.NewDecoder(probe).Decode(&health); err != nil || !health.OK {
		t.Fatalf("health probe failed: %+v err=%v", health, err)
	}

	cancel()

	// The structural drain assertion: with a handler still parked, serve must
	// not return. Without handler tracking, wg.Wait sees only the (instantly
	// exiting) scheduler and serve returns within this window deterministically.
	select {
	case <-done:
		t.Fatal("serve returned while a handler was still in flight")
	case <-time.After(300 * time.Millisecond):
	}

	// Finish the parked request after shutdown began; the drain must let it
	// complete against a still-open DB.
	if _, err := parked.Write([]byte("}\n")); err != nil {
		t.Fatal(err)
	}
	var resp Response
	if err := json.NewDecoder(parked).Decode(&resp); err != nil {
		t.Fatalf("decode in-flight response: %v", err)
	}
	if !resp.OK || resp.Error != "" {
		t.Fatalf("in-flight request failed during shutdown: %+v", resp)
	}

	<-done
	if serveErr != nil {
		t.Fatalf("serve: %v", serveErr)
	}
	if closeErr != nil {
		t.Fatalf("store close: %v", closeErr)
	}
	// logBuf is safe to read here: every writer goroutine exited before done.
	if strings.Contains(logBuf.String(), "database is closed") {
		t.Fatalf("teardown raced an in-flight handler:\n%s", logBuf.String())
	}
}

// TestServeShutdownLeavesMountsUntouched pins step 6's core fix: daemon
// shutdown leaves the fuse mirrors to the detached holder — no Teardown, local
// or over the holder socket, on any shutdown path. Every provider resolution
// in the daemon flows through the injected fake, so an empty call recording
// proves no unmount (and no remount) was attempted anywhere between startup
// and exit, while a fuse account sat mounted the whole time — exactly the
// state the deleted teardownMounts used to sweep.
func TestServeShutdownLeavesMountsUntouched(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	s, dirs := newTestServer(t)
	fake := &fakeFuseProv{} // Health nil: the startup reconcile adopts the mount
	s.m.OverlayFor = func(kind overlay.Kind) overlay.Provider {
		if kind == overlay.KindFuse {
			return fake
		}
		return &overlay.SymlinkProvider{}
	}
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "fuse"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })

	// macOS caps sun_path at 104 bytes; the socket gets its own short dir.
	sockDir, err := os.MkdirTemp("/tmp", "ccp-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	s.socket = filepath.Join(sockDir, "d.sock")
	s.evictTimeout = defaultEvictTimeout
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx) }()

	// The socket binds before the startup reconcile goroutine runs, so wait
	// for the reconcile to reach acct-1's adopt decision (its Health probe)
	// before shutting down — otherwise the cancelled ctx skips the reconcile
	// and the adopt assertion below would race startup.
	deadline := time.Now().Add(10 * time.Second)
	for fake.healthCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("startup reconcile never probed the fuse mount")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cl := &Client{socket: s.socket}
	if resp, err := cl.Shutdown(); err != nil || !resp.OK {
		t.Fatalf("shutdown: resp = %+v, err = %v", resp, err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not return after OpShutdown")
	}

	if got := fake.callOrder(); len(got) != 0 {
		t.Fatalf("daemon lifecycle touched the mount: provider calls = %v, want none", got)
	}
	if !strings.Contains(buf.String(), "adopted live mount") {
		t.Fatalf("startup reconcile did not adopt the live mount:\n%s", buf.String())
	}
}
