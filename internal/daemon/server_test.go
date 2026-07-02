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

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// newTestServer builds a Server with acct-1 emptier than acct-2. scanSessions
// is stubbed: real `ps` can hang on a wedged mount.
func newTestServer(t *testing.T) (*Server, map[int]string) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

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
	s := &Server{
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
	}
	// Production serve() Waits on s.wg before Run's deferred Close; tests must
	// too. handleSelect's preflight goroutine creates the winner's LockDir lock
	// file — unwaited, it races t.TempDir's RemoveAll ("directory not empty")
	// and the store Close. Registered after the t.TempDir calls above, so this
	// cleanup runs before theirs (LIFO), after t.Context() is cancelled.
	t.Cleanup(func() { s.wg.Wait() })
	return s, dirs
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
	// Sticky points at the WORSE account.
	if err := s.m.Store.UpsertSticky("/proj", 2, time.Now()); err != nil {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[2] || !resp.Sticky {
		t.Fatalf("expected sticky acct-2 (%s), got %+v", dirs[2], resp)
	}
}

// TestHandleSelectSkipsExhaustedStickyPin replays the 2026-06-10 incident:
// reset credit (eff5 ≈ 93, reset ~21m out) must not keep a pegged pin alive.
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

// TestHandleSelectMarksSessionWithCwd: marked sessions feed the sticky
// activity rules.
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

	s2, _ := newTestServer(t)
	if resp := s2.handleSelect(t.Context(), Request{Op: OpSelect, PID: 4242, NoMark: true, Cwd: "/proj"}); !resp.OK {
		t.Fatalf("select failed: %+v", resp)
	}
	if live, _ := s2.m.Store.ListActiveSessions(); len(live) != 0 {
		t.Fatalf("NoMark must not mark: %+v", live)
	}
}

// TestHandleSelectBindsWarmEndedSession: a pin whose session ended minutes
// ago must still bind — the warm cache is what stickiness protects.
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

// TestHandleSelectHoldsLiveOnlyPin: a still-live session cannot be resumed,
// so ranking runs free — but the pin is never repointed.
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

// TestHandleSelectQuickResumeBindsAfterReap: handleSelect reconciles before
// deciding, so a just-died session reads as a warm end and the pin binds.
func TestHandleSelectQuickResumeBindsAfterReap(t *testing.T) {
	s, dirs := newTestServer(t)
	now := time.Now()
	if err := s.m.Store.UpsertSticky("/proj", 2, now.Add(-3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// pid 4000000 is impossible (macOS pids are 5-digit), so handleSelect's
	// sweep reaps the row; the -10m reconcile below makes the reap a warm end.
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

	s2, _ := newTestServer(t)
	if resp := s2.handleSelect(t.Context(), Request{Op: OpSelect, Account: &forced, PID: 4242, NoMark: true, Cwd: "/proj"}); !resp.OK {
		t.Fatalf("forced select failed: %+v", resp)
	}
	if live, _ := s2.m.Store.ListActiveSessions(); len(live) != 0 {
		t.Fatalf("forced NoMark must not mark: %+v", live)
	}
}

func TestHandleSelectHoldsUnusableManualPin(t *testing.T) {
	s, dirs := newTestServer(t)
	now := time.Now().Add(time.Minute)
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

// TestHandleSelectExhaustedFallback: an exhausted pool yields the least-bad
// pick flagged ExhaustedFallback — never an error.
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
	s.wg.Wait()
	logged := buf.String()
	if !strings.Contains(logged, "select (exhausted-fallback): /proj -> acct-02") {
		t.Fatalf("fallback pick not logged as such: %q", logged)
	}
	if !strings.Contains(logged, "runner-up acct-01") {
		t.Fatalf("fallback log must name the exhausted runner-up: %q", logged)
	}
}

// TestHandleSelectNoFallback: --wait (NoFallback) must not commit the
// discarded pick's sticky or reservation.
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
// plus the soonest reset for --wait, read through to each account's last
// known-good sample.
func TestHandleSelectNoneAvailable(t *testing.T) {
	s, _ := newTestServer(t)
	now := time.Now().Add(time.Minute)
	reset := now.Add(30 * time.Minute)
	for id := 1; id <= 2; id++ {
		// A real prior reading carrying the window reset, then a rate-limit
		// marker on top (zeroed, as the production 429 path records it).
		if err := s.m.Store.InsertUsageSample(store.UsageSample{
			AccountID: id, TS: now, Util5h: 50, Resets5h: reset,
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.m.Store.InsertUsageSample(store.UsageSample{
			AccountID: id, TS: now.Add(time.Second), RateLimited: true,
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

// TestServeDrainsInFlightHandlerOnShutdown: serve must drain in-flight
// handlers before returning — Run's deferred m.Close() follows immediately.
func TestServeDrainsInFlightHandlerOnShutdown(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	// macOS caps sun_path at 104 bytes; the socket gets its own short dir.
	sockDir, err := os.MkdirTemp("/tmp", "ccp-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })

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
		// Mirror Run's defer ordering.
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
	defer func() { _ = parked.Close() }()
	if _, err := parked.Write([]byte(`{"op":"status"`)); err != nil {
		t.Fatal(err)
	}

	// This round-trip proves the parked connection is already accepted and
	// wg-tracked: the accept loop is sequential and unix sockets accept FIFO.
	probe := dial()
	defer func() { _ = probe.Close() }()
	if _, err := probe.Write([]byte(`{"op":"health"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	var health Response
	if err := json.NewDecoder(probe).Decode(&health); err != nil || !health.OK {
		t.Fatalf("health probe failed: %+v err=%v", health, err)
	}

	cancel()

	// Without handler tracking, wg.Wait sees only the scheduler and serve
	// returns within this window.
	select {
	case <-done:
		t.Fatal("serve returned while a handler was still in flight")
	case <-time.After(300 * time.Millisecond):
	}

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

// TestServeShutdownLeavesMountsUntouched: daemon shutdown leaves fuse mirrors
// to the detached holder — no Teardown on any path. All provider resolution
// flows through the injected fake, so zero recorded calls proves it.
func TestServeShutdownLeavesMountsUntouched(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	s, dirs := newTestServer(t)
	fake := &fakeFuseProv{} // Health nil: the startup reconcile adopts the mount
	s.m.OverlayFor = func(backend fkoverlay.Backend) (fkoverlay.Provider, error) {
		if backend.IsFuse() {
			return fake, nil
		}
		return &fkoverlay.SymlinkProvider{Spec: s.m.OverlaySpec()}, nil
	}
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })

	sockDir, err := os.MkdirTemp("/tmp", "ccp-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	s.socket = filepath.Join(sockDir, "d.sock")
	s.evictTimeout = defaultEvictTimeout
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx) }()

	// Wait for the startup reconcile to reach acct-1's Health probe before
	// shutting down; otherwise the cancelled ctx skips the reconcile and the
	// adopt assertion races startup.
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
