package daemon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/lease"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

var daemonTestToken atomic.Uint64

func nextDaemonTestToken() string {
	return fmt.Sprintf("%032x", daemonTestToken.Add(1))
}

// holdSessionLease simulates a live select/env handout by taking account a's
// session lease (its current-shape key) under the server's temp lease root; the
// returned handle releases it, and a t.Cleanup closes it as a backstop.
func holdSessionLease(t *testing.T, s *Server, a store.Account) *lease.Handle {
	t.Helper()
	root, err := s.m.LeaseRoot()
	if err != nil {
		t.Fatal(err)
	}
	h, err := lease.Acquire(root, pool.SessionLeaseDir(a), pool.HolderOwner)
	if err != nil {
		t.Fatalf("hold session lease on %s: %v", pool.SessionLeaseDir(a), err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// newTestServer builds a Server with acct-1 emptier than acct-2. scanSessions
// is stubbed: real `ps` can hang on a wedged mount.
func newTestServer(t *testing.T) (*Server, map[int]string) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	// Point the session-lease root at a temp dir so any Seize/Probe under test
	// never touches real ~/.fusekit.
	leaseRoot := filepath.Join(t.TempDir(), "leases")
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
	contentRoot := t.TempDir()
	claudeDir := filepath.Join(contentRoot, ".claude")
	if err := os.MkdirAll(claudeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	s := &Server{
		m: &pool.Manager{
			Store: st, OAuth: &fakeOAuth{}, Creds: credstest.NewFake(), LockDir: t.TempDir(),
			LeaseRoot: func() (string, error) { return leaseRoot, nil },
		},
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		cl:           newClaims(),
		led:          newLedgers(),
		contentSource: overlay.NewPoolContentSource(
			claudeDir, filepath.Join(contentRoot, ".claude.json"), filepath.Join(contentRoot, "content-stamps"),
		),
	}
	// Production serve() Waits on s.wg before Run's deferred Close; tests must
	// too. handleSelect's preflight goroutine creates the winner's LockDir lock
	// file — unwaited, it races t.TempDir's RemoveAll ("directory not empty")
	// and the store Close. Registered after the t.TempDir calls above, so this
	// cleanup runs before theirs (LIFO), after t.Context() is cancelled.
	t.Cleanup(func() { s.wg.Wait() })
	return s, dirs
}

func activateDaemonTestSession(t *testing.T, s *Server, accountID, pid int, cwd string, started time.Time) int64 {
	t.Helper()
	started = started.Truncate(time.Microsecond)
	a, err := s.m.Store.GetAccount(accountID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.m.Store.ActivateSelection(store.SelectionActivation{
		Token:     nextDaemonTestToken(),
		AccountID: accountID, ExpectedInstanceID: a.InstanceID, ExpectedGeneration: a.Generation,
		Process: store.ProcessIdentity{PID: pid, StartedAt: started},
		Cwd:     cwd, At: started,
	}); err != nil {
		t.Fatal(err)
	}
	sessions, err := s.m.Store.ListActiveSessions()
	if err != nil {
		t.Fatal(err)
	}
	for _, session := range sessions {
		if session.PID == pid && session.ProcessStartedAt.Equal(started) && session.Cwd == cwd {
			return session.ID
		}
	}
	t.Fatal("activated session was not stored")
	return 0
}

func expireCommittedReservations(c *claims, accountID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, selection := range c.selections {
		if selection.accountID == accountID {
			selection.expiresAt = time.Now().Add(-time.Second)
		}
	}
}

func TestReservedCountExpiresAfterTTL(t *testing.T) {
	s := &Server{cl: newClaims()}

	if got := s.cl.reservedCount(1); got != 0 {
		t.Fatalf("reservedCount before reserve = %d, want 0", got)
	}

	s.cl.reserve(1)
	if got := s.cl.reservedCount(1); got != 1 {
		t.Fatalf("reservedCount after reserve = %d, want 1", got)
	}

	expireCommittedReservations(s.cl, 1)
	if got := s.cl.reservedCount(1); got != 0 {
		t.Fatalf("reservedCount after TTL = %d, want 0", got)
	}
	s.cl.mu.Lock()
	left := len(s.cl.selections)
	s.cl.mu.Unlock()
	if left != 0 {
		t.Fatalf("expired reservations were not deleted: %d remain", left)
	}
}

func TestHandleSelectCanceledClaimWaitCreatesNoReservation(t *testing.T) {
	s, _ := newTestServer(t)
	newCoordinatorTestSource(t, s)
	if !s.cl.hold(1) {
		t.Fatal("could not hold poll claim")
	}
	accountID := 1
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	resp := s.handleSelect(ctx, Request{Account: &accountID, Cwd: "/project"})
	if resp.OK {
		t.Fatalf("selection succeeded after request deadline: %+v", resp)
	}
	s.cl.disownHold(1)
	s.cl.mu.Lock()
	selections := len(s.cl.selections)
	s.cl.mu.Unlock()
	if selections != 0 {
		t.Fatalf("late reservation after canceled catch-up: selections=%d", selections)
	}
}

func TestProbeWinnerHonorsSelectionDeadline(t *testing.T) {
	t.Run("fuse refuses when deep-probe budget does not fit", func(t *testing.T) {
		s, _ := newTestServer(t)
		setRowKind(t, s, 1, fkoverlay.BackendNFS)
		account, err := s.m.Store.GetAccount(1)
		if err != nil {
			t.Fatal(err)
		}
		old := deepProbe
		calls := 0
		deepProbe = func(string) error { calls++; return nil }
		t.Cleanup(func() { deepProbe = old })
		ctx, cancel := context.WithTimeout(t.Context(), overlay.DeepProbeBound-time.Second)
		defer cancel()
		if s.probeWinnerReady(ctx, account) {
			t.Fatal("fuse winner reported ready without enough probe budget")
		}
		if calls != 0 {
			t.Fatalf("deep probe called %d times without enough deadline", calls)
		}
	})

	t.Run("file provider cancellation is not NoVerdict-ready", func(t *testing.T) {
		s, _ := newTestServer(t)
		setRowKind(t, s, 1, fkoverlay.BackendFileProvider)
		account, err := s.m.Store.GetAccount(1)
		if err != nil {
			t.Fatal(err)
		}
		s.fpSynth = alwaysNonEmpty
		swapFPDomainProbe(t, func(ctx context.Context, _ string) error {
			<-ctx.Done()
			return overlay.ErrFPProbeNoVerdict
		})
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancel()
		if s.probeWinnerReady(ctx, account) {
			t.Fatal("canceled FP probe reported winner ready")
		}
	})
}

func TestRawSelectHasNoActivationEffects(t *testing.T) {
	s, dirs := newTestServer(t)
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[1] {
		t.Fatalf("expected emptier acct-1 (%s), got %+v", dirs[1], resp)
	}
	if resp.Sticky {
		t.Fatal("first select must not report sticky")
	}
	if !resp.HasUsage || resp.Remaining5h <= 0 || resp.Remaining7d <= 0 {
		t.Fatalf("expected remaining headroom on a sampled pick, got HasUsage=%v Remaining5h=%.1f Remaining7d=%.1f", resp.HasUsage, resp.Remaining5h, resp.Remaining7d)
	}
	if resp.ReservationToken != "" {
		t.Fatalf("raw select returned reservation token %q", resp.ReservationToken)
	}
	if got := s.cl.reservedCount(1); got != 0 {
		t.Fatalf("raw select retained %d reservations, want zero", got)
	}
	if st, ok, err := s.m.Store.GetSticky("/proj"); err != nil || ok {
		t.Fatalf("raw select recorded sticky: %+v ok=%v err=%v", st, ok, err)
	}
	if sessions, err := s.m.Store.ListActiveSessions(); err != nil || len(sessions) != 0 {
		t.Fatalf("raw select sessions = %+v, err=%v; raw select has no process owner", sessions, err)
	}
}

func TestHandleSelectTransactionAbortAndExclude(t *testing.T) {
	s, dirs := newTestServer(t)
	first := s.handleSelect(t.Context(), Request{Op: OpSelect, PID: 4242, ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj"})
	if !first.OK || first.Dir != dirs[1] || first.ReservationToken == "" {
		t.Fatalf("provisional select = %+v, want acct-1 with token", first)
	}
	if live, _ := s.m.Store.ListActiveSessions(); len(live) != 0 {
		t.Fatalf("provisional select opened phantom sessions: %+v", live)
	}
	if _, ok, _ := s.m.Store.GetSticky("/proj"); ok {
		t.Fatal("provisional select recorded phantom sticky state")
	}
	if got := s.cl.reservedCount(1); got != 1 {
		t.Fatalf("pending reservation count = %d, want 1", got)
	}
	if resp := s.handleSelectAbort(context.Background(), Request{Op: OpSelectAbort, ReservationToken: first.ReservationToken}); !resp.OK {
		t.Fatalf("abort = %+v", resp)
	}
	if got := s.cl.reservedCount(1); got != 0 {
		t.Fatalf("reservation survived abort: %d", got)
	}

	second := s.handleSelect(t.Context(), Request{Op: OpSelect, PID: 4242, ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj", ExcludeIDs: []int{1}})
	if !second.OK || second.Dir != dirs[2] {
		t.Fatalf("excluded retry = %+v, want acct-2", second)
	}
	commitSelectResponse(t, s, second)
	if live, _ := s.m.Store.ListActiveSessions(); len(live) != 1 || live[0].AccountID != 2 {
		t.Fatalf("committed sessions = %+v, want only acct-2", live)
	}
	if sticky, ok, _ := s.m.Store.GetSticky("/proj"); !ok || sticky.AccountID != 2 {
		t.Fatalf("committed sticky = %+v ok=%v, want acct-2", sticky, ok)
	}
}

func TestCommitSelectionReplaySurvivesDaemonRestart(t *testing.T) {
	s, _ := newTestServer(t)
	selected := s.handleSelect(t.Context(), Request{
		Op: OpSelect, PID: 4242, ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj",
	})
	if !selected.OK || selected.ReservationToken == "" {
		t.Fatalf("select = %+v", selected)
	}
	if committed := s.handleSelectCommit(t.Context(), Request{
		Op: OpSelectCommit, ReservationToken: selected.ReservationToken,
	}); !committed.OK {
		t.Fatalf("commit = %+v", committed)
	}

	restarted := &Server{m: s.m, cl: newClaims()}
	if restarted.cl.knowsSelection(selected.ReservationToken) {
		t.Fatal("fresh daemon unexpectedly retained the old in-memory selection")
	}
	if replayed := restarted.handleSelectCommit(t.Context(), Request{
		Op: OpSelectCommit, ReservationToken: selected.ReservationToken,
	}); !replayed.OK {
		t.Fatalf("durable commit replay after daemon restart = %+v", replayed)
	}
	if sessions, err := s.m.Store.ListActiveSessions(); err != nil || len(sessions) != 1 {
		t.Fatalf("durable replay sessions = %+v, err=%v; want exactly one", sessions, err)
	}
	if sticky, ok, err := s.m.Store.GetSticky("/proj"); err != nil || !ok || sticky.AccountID != 1 {
		t.Fatalf("durable replay sticky = %+v ok=%v err=%v", sticky, ok, err)
	}
}

func TestForcedSelectionRefusesAccountUnderConversion(t *testing.T) {
	s, _ := newTestServer(t)
	accountID := 1
	if !s.cl.own(accountID) {
		t.Fatal("could not acquire conversion claim")
	}
	defer s.cl.disownConvert(accountID)
	for name, process := range map[string]store.ProcessIdentity{
		"inspect": {},
		"run":     {PID: 4242, StartedAt: time.Now().Add(-time.Minute)},
	} {
		t.Run(name, func(t *testing.T) {
			var processStartedAt int64
			if !process.StartedAt.IsZero() {
				processStartedAt = process.StartedAt.UnixMicro()
			}
			resp := s.handleSelect(t.Context(), Request{
				Op: OpSelect, Account: &accountID, PID: process.PID,
				ProcessStartedAt: processStartedAt, Cwd: "/proj",
			})
			if resp.OK || !strings.Contains(resp.Error, "migrating overlays") {
				t.Fatalf("selection under conversion = %+v", resp)
			}
			if resp.ReservationToken != "" || s.cl.reservedCount(accountID) != 0 {
				t.Fatalf("selection under conversion retained reservation: %+v", resp)
			}
		})
	}
}

func TestCommitSelectionFailureReleasesPromotedReservation(t *testing.T) {
	s, _ := newTestServer(t)
	forced := 1
	started := time.Now().Add(-time.Minute).UnixMicro()
	first := s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &forced, PID: 4241, ProcessStartedAt: started, Cwd: "/first"})
	second := s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &forced, PID: 4242, ProcessStartedAt: started, Cwd: "/second"})
	if !first.OK || !second.OK {
		t.Fatalf("provisional selects = %+v / %+v", first, second)
	}
	if committed := s.handleSelectCommit(context.Background(), Request{Op: OpSelectCommit, ReservationToken: first.ReservationToken}); !committed.OK {
		t.Fatalf("first commit = %+v", committed)
	}
	s.wg.Wait()
	if err := s.m.Store.Close(); err != nil {
		t.Fatal(err)
	}
	failed := s.handleSelectCommit(context.Background(), Request{Op: OpSelectCommit, ReservationToken: second.ReservationToken})
	if failed.OK || !strings.Contains(failed.Error, "activate selection") {
		t.Fatalf("second commit against closed store = %+v", failed)
	}
	if got := s.cl.reservedCount(forced); got != 0 {
		t.Fatalf("terminal commits retained %d reservations, want zero", got)
	}
	replayed := s.handleSelectCommit(context.Background(), Request{Op: OpSelectCommit, ReservationToken: second.ReservationToken})
	if replayed.OK || replayed.Error != failed.Error {
		t.Fatalf("terminal failure replay = %+v, want %+v", replayed, failed)
	}
}

func TestProvisionalSelectionExpiresWithoutEffects(t *testing.T) {
	s, _ := newTestServer(t)
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, PID: 4242, ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj"})
	if !resp.OK {
		t.Fatalf("select = %+v", resp)
	}
	s.cl.mu.Lock()
	s.cl.selections[resp.ReservationToken].expiresAt = time.Now().Add(-time.Second)
	s.cl.mu.Unlock()

	s.cl.pruneSelections(time.Now())
	if committed := s.handleSelectCommit(context.Background(), Request{Op: OpSelectCommit, ReservationToken: resp.ReservationToken}); committed.OK {
		t.Fatalf("expired commit unexpectedly succeeded: %+v", committed)
	}
	if live, _ := s.m.Store.ListActiveSessions(); len(live) != 0 {
		t.Fatalf("expired selection opened sessions: %+v", live)
	}
	if _, ok, _ := s.m.Store.GetSticky("/proj"); ok {
		t.Fatal("expired selection recorded sticky state")
	}
}

func TestRunCommitRejectsAccountGenerationChange(t *testing.T) {
	s, _ := newTestServer(t)
	forced := 1
	resp := s.handleSelect(t.Context(), Request{
		Op: OpSelect, Account: &forced, PID: 4242,
		ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj",
	})
	if !resp.OK {
		t.Fatalf("select = %+v", resp)
	}
	if err := s.m.Store.SetAccountOverlayKind(forced, "fileprovider"); err != nil {
		t.Fatal(err)
	}

	committed := s.handleSelectCommit(context.Background(), Request{
		Op: OpSelectCommit, ReservationToken: resp.ReservationToken,
	})
	if committed.OK || !strings.Contains(committed.Error, "account generation changed") {
		t.Fatalf("commit after generation change = %+v", committed)
	}
	if live, err := s.m.Store.ListActiveSessions(); err != nil {
		t.Fatal(err)
	} else if len(live) != 0 {
		t.Fatalf("generation-raced run opened sessions: %+v", live)
	}
	if sticky, ok, err := s.m.Store.GetSticky("/proj"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("generation-raced run recorded sticky state: %+v", sticky)
	}
	if got := s.cl.reservedCount(forced); got != 0 {
		t.Fatalf("generation-raced run retained %d reservations, want zero", got)
	}
}

func TestHandleSelectHonorsSticky(t *testing.T) {
	s, dirs := newTestServer(t)
	// Sticky points at the WORSE account.
	if err := s.m.Store.UpsertSticky("/proj", 2, time.Now()); err != nil {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
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
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[1] {
		t.Fatalf("expected healthy acct-1 (%s) over the exhausted pin, got %+v", dirs[1], resp)
	}
	if resp.Sticky || resp.ExhaustedFallback {
		t.Fatalf("a fresh healthy pick must report neither sticky nor fallback: %+v", resp)
	}
	st, ok, err := s.m.Store.GetSticky("/proj")
	if err != nil || !ok || st.AccountID != 2 {
		t.Fatalf("raw inspection rewrote sticky row: %+v ok=%v err=%v", st, ok, err)
	}
}

// TestHandleSelectMarksSessionWithCwd: marked sessions feed the sticky
// activity rules.
func TestHandleSelectMarksSessionWithCwd(t *testing.T) {
	s, _ := newTestServer(t)
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, PID: 4242, ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj"})
	if !resp.OK || resp.SelectedID == nil {
		t.Fatalf("select failed: %+v", resp)
	}
	commitSelectResponse(t, s, resp)
	live, err := s.m.Store.ListActiveSessions()
	if err != nil || len(live) != 1 {
		t.Fatalf("sessions = %+v err=%v", live, err)
	}
	if live[0].PID != 4242 || live[0].Cwd != "/proj" || live[0].AccountID != *resp.SelectedID {
		t.Fatalf("session row = %+v, want pid 4242 cwd /proj acct %d", live[0], *resp.SelectedID)
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
	id := activateDaemonTestSession(t, s, 2, 800002, "/proj", now.Add(-3*time.Hour))
	if err := s.m.Store.CloseSession(id, now.Add(-10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
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
	started := now.Add(-10 * time.Minute).Truncate(time.Microsecond)
	activateDaemonTestSession(t, s, 2, 800002, "/proj", started)
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 800002, ConfigDir: dirs[2], StartedAt: started}}, nil
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
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
	activateDaemonTestSession(t, s, 2, 4000000, "/proj", now.Add(-3*time.Hour))
	if _, err := s.m.Store.CloseDeadSessions(map[int]time.Time{4000000: now.Add(-3 * time.Hour).Truncate(time.Microsecond)}, now.Add(-10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[2] || !resp.Sticky {
		t.Fatalf("quick resume must bind the pin via the reaped warm end, got %+v", resp)
	}
}

func TestHandleSelectForcedMarksSession(t *testing.T) {
	s, _ := newTestServer(t)
	forced := 2
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &forced, PID: 4242, ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj"})
	if !resp.OK {
		t.Fatalf("forced select failed: %+v", resp)
	}
	commitSelectResponse(t, s, resp)
	live, err := s.m.Store.ListActiveSessions()
	if err != nil || len(live) != 1 {
		t.Fatalf("sessions = %+v err=%v", live, err)
	}
	if live[0].PID != 4242 || live[0].Cwd != "/proj" || live[0].AccountID != 2 {
		t.Fatalf("session row = %+v, want pid 4242 cwd /proj acct 2", live[0])
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
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
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
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &forced, Cwd: "/proj"})
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
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
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
		if s.cl.reservedCount(id) != 0 {
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

// TestHandleStatusSurfacesContentHealth pins the status wire's content-health
// field: a nil or healthy content source reads empty, recorded degraded-read
// failures surface with errors.Join's newlines folded to "; " (doctor renders
// one line), and the next successful reads clear it.
func TestHandleStatusSurfacesContentHealth(t *testing.T) {
	s, _ := newTestServer(t)
	if got := s.handleStatus(t.Context()).ContentHealth; got != "" {
		t.Fatalf("nil content source must read healthy, got %q", got)
	}

	home := t.TempDir()
	claudeDir := filepath.Join(home, ".claude")
	baseJSON := filepath.Join(home, ".claude.json")
	if err := os.MkdirAll(claudeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	domain := filepath.Join(home, "acct-01")
	priv := fkoverlay.FusePrivateRoot(domain)
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(priv, ".claude.json"), []byte(`{"userID":"u"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s.contentSource = overlay.NewPoolContentSource(claudeDir, baseJSON, filepath.Join(t.TempDir(), "content-stamps"))
	if got := s.handleStatus(t.Context()).ContentHealth; got != "" {
		t.Fatalf("healthy content source must read empty, got %q", got)
	}

	// Corrupt both shared bases: each ReadSynth degrades to raw bytes (no
	// error to the session) while recording the failure for HealthErrors.
	if err := os.WriteFile(baseJSON, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(claudeDir, "settings.json"), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.contentSource.ReadSynth(domain, ".claude.json"); err != nil {
		t.Fatalf("degraded claude.json read must not error: %v", err)
	}
	if _, err := s.contentSource.ReadSynth(domain, "settings.json"); err != nil {
		t.Fatalf("degraded settings.json read must not error: %v", err)
	}
	got := s.handleStatus(t.Context()).ContentHealth
	for _, frag := range []string{"merge claude.json for " + domain, "inject plansDirectory"} {
		if !strings.Contains(got, frag) {
			t.Errorf("content health %q missing %q", got, frag)
		}
	}
	if strings.Contains(got, "\n") {
		t.Errorf("content health must be newline-free on the wire, got %q", got)
	}
	if !strings.Contains(got, "; ") {
		t.Errorf("joined failures must be %q-separated, got %q", "; ", got)
	}

	// The next successful read of each entry clears its recorded failure.
	if err := os.WriteFile(baseJSON, []byte(`{"firstStartTime":"2026-01-01"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(claudeDir, "settings.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.contentSource.ReadSynth(domain, ".claude.json"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.contentSource.ReadSynth(domain, "settings.json"); err != nil {
		t.Fatal(err)
	}
	if got := s.handleStatus(t.Context()).ContentHealth; got != "" {
		t.Fatalf("cleared failures still on the wire: %q", got)
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
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
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
	if resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"}); !resp.OK {
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
	resp := s.handleSelect(t.Context(), Request{
		Op: OpSelect, Account: &acct, PID: 4242,
		ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj",
	})
	if !resp.OK || resp.Dir != dirs[2] {
		t.Fatalf("expected forced acct-2 (%s), got %+v", dirs[2], resp)
	}
	if resp.Sticky {
		t.Fatal("forced select must not report sticky (ranking was not overridden)")
	}
	commitSelectResponse(t, s, resp)
	st, ok, err := s.m.Store.GetSticky("/proj")
	if err != nil || !ok || st.AccountID != 2 {
		t.Fatalf("forced account not recorded: %+v ok=%v err=%v", st, ok, err)
	}
}

func commitSelectResponse(t *testing.T, s *Server, resp Response) {
	t.Helper()
	committed := s.handleSelectCommit(context.Background(), Request{Op: OpSelectCommit, ReservationToken: resp.ReservationToken})
	if !committed.OK {
		t.Fatalf("commit selection: %+v", committed)
	}
}

// TestServeDrainsInFlightHandlerOnShutdown: a work op admitted at frame receipt
// settles to its terminal response before serve returns, so Run's deferred
// m.Close() cannot race it. drain.Settle blocks the teardown until the admitted
// handler finishes — the settle-before-cancel guarantee.
func TestServeDrainsInFlightHandlerOnShutdown(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	// macOS caps sun_path at 104 bytes; the socket gets its own short dir.
	sockDir, err := os.MkdirTemp("/tmp", "ccp-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })

	entered := make(chan struct{})
	release := make(chan struct{})
	settledCtx := make(chan error, 1)
	var logBuf bytes.Buffer
	s := &Server{
		m:        &pool.Manager{Store: st},
		socket:   filepath.Join(sockDir, "d.sock"),
		snapshot: filepath.Join(t.TempDir(), "status.json"),
		log:      log.New(&logBuf, "", 0),
		cl:       newClaims(),
		led:      newLedgers(),
	}
	// A work op that blocks in dispatch after admission: it pins the drain open,
	// proving serve settles admitted work before returning (and before m.Close).
	var enteredOnce sync.Once
	s.fpBridgeCheckFn = func(ctx context.Context) FPBridgeStatus {
		enteredOnce.Do(func() { close(entered) })
		<-release
		settledCtx <- ctx.Err()
		return FPBridgeStatus{Verdict: FPBridgeServing}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var serveErr error
	done := make(chan struct{})
	go func() {
		serveErr = s.serve(ctx)
		close(done)
	}()

	// Admit a work op and block it in dispatch.
	client := &Client{socket: s.socket, sessions: make(map[*clientSession]struct{})}
	defer func() { _ = client.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for !client.Available() {
		if time.Now().After(deadline) {
			t.Fatal("daemon never became available")
		}
		time.Sleep(10 * time.Millisecond)
	}
	type callResult struct {
		resp *Response
		err  error
	}
	called := make(chan callResult, 1)
	go func() {
		resp, err := client.FPBridgeCheck()
		called <- callResult{resp: resp, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("work handler never entered dispatch")
	}

	cancel()

	// drain.Settle blocks on the admitted handler: serve must not return yet.
	select {
	case <-done:
		t.Fatal("serve returned while an admitted handler was still in flight")
	case <-time.After(300 * time.Millisecond):
	}

	close(release)
	if err := <-settledCtx; err != nil {
		t.Fatalf("in-flight handler context canceled before settle: %v", err)
	}
	result := <-called
	if result.err != nil {
		t.Fatalf("in-flight request failed during shutdown: %v", result.err)
	}
	if !result.resp.OK || result.resp.Error != "" {
		t.Fatalf("in-flight request failed during shutdown: %+v", result.resp)
	}

	<-done
	if serveErr != nil {
		t.Fatalf("serve: %v", serveErr)
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
	fake := &fakeFuseProv{} // Provider Check nil: the startup reconcile adopts the mount
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
	// A migrated fuse account's dir is a mux bridge symlink; the startup reconcile
	// adopts the live mirror (fake provider Check nil) rather than migrating it.
	makeBridge(t, dirs[1])
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

	// Wait for the startup reconcile to reach acct-1's provider Check before
	// shutting down; otherwise the cancelled ctx skips the reconcile and the
	// adopt assertion races startup.
	deadline := time.Now().Add(10 * time.Second)
	for fake.checkCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("startup reconcile never probed the fuse mount")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cl := &Client{socket: s.socket}
	defer func() { _ = cl.Close() }()
	if resp, err := cl.Shutdown(); err != nil || !resp.OK {
		t.Fatalf("shutdown: resp = %+v, err = %v", resp, err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not return after lifecycle shutdown")
	}

	if got := fake.callOrder(); len(got) != 0 {
		t.Fatalf("daemon lifecycle touched the mount: provider calls = %v, want none", got)
	}
	if !strings.Contains(buf.String(), "adopted live mount") {
		t.Fatalf("startup reconcile did not adopt the live mount:\n%s", buf.String())
	}
}
