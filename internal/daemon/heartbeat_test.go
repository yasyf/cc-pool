package daemon

import (
	"context"
	"errors"
	"io"
	"log"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
)

func TestHeartbeatFailureRetainsLastKnownActive(t *testing.T) {
	dir := "/pool/acct-01"
	calls := 0
	s := &Server{
		log: log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) {
			calls++
			if calls == 1 {
				return []procscan.Session{{PID: 4242, ConfigDir: dir}}, nil
			}
			return nil, errors.New("proc table raced")
		},
	}
	h := s.heartbeatFor()
	first := h.refresh(t.Context(), 0)
	if !first.success || first.snapshot.sessionCount(dir) != 1 {
		t.Fatalf("first heartbeat = %+v, want one active session", first)
	}
	failed := h.refresh(t.Context(), 0)
	if failed.success || failed.snapshot.lastScanOK {
		t.Fatalf("failed heartbeat = %+v, want unsuccessful scan", failed)
	}
	if failed.snapshot.sessionCount(dir) != 1 || failed.snapshot.idle(dir) {
		t.Fatalf("failed heartbeat lost last-known activity: %+v", failed.snapshot)
	}
}

func TestHeartbeatFailureMakesPreviouslyIdleDirBusy(t *testing.T) {
	dir := "/pool/acct-01"
	calls := 0
	s := &Server{
		log: log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) {
			calls++
			if calls == 1 {
				return nil, nil
			}
			return nil, errors.New("proc table raced")
		},
	}
	h := s.heartbeatFor()
	first := h.refresh(t.Context(), 0)
	if !first.success || !first.snapshot.idle(dir) {
		t.Fatalf("first heartbeat = %+v, want clean idle", first)
	}
	failed := h.refresh(t.Context(), 0)
	if failed.success || failed.snapshot.idle(dir) {
		t.Fatalf("failed heartbeat left previously-idle dir actionable: %+v", failed)
	}
	if (&tick{snapshot: failed.snapshot}).idle(dir) {
		t.Fatal("maintenance tick treated unknown liveness as idle")
	}
}

func TestHeartbeatStartupReapQueuesIdleAdoption(t *testing.T) {
	s, dirs := newTestServer(t)
	dir := dirs[1]
	adoptions := 0
	s.adoptRotated = func(context.Context, store.Account) error {
		adoptions++
		return nil
	}
	activateDaemonTestSession(t, s, 1, 4242, "", time.Now().Add(-2*time.Minute))
	s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil }
	delta := s.heartbeatFor().refresh(t.Context(), 0)
	if !containsString(delta.idle, dir) {
		t.Fatalf("startup idle candidates = %v, want %s seeded from durable session", delta.idle, dir)
	}
	s.handleHeartbeatDelta(t.Context(), delta)
	if count, err := s.m.Store.ActiveSessionCount(1); err != nil || count != 0 {
		t.Fatalf("active sessions after heartbeat = (%d, %v), want 0", count, err)
	}
	if adoptions != 1 {
		t.Fatal("startup missed-exit did not attempt adoption after reaping the durable row")
	}
}

func TestIdleAdoptionDoesNotHoldAccountClaimAcrossCredentialIO(t *testing.T) {
	s, dirs := newTestServer(t)
	dir := dirs[1]
	s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil }
	if delta := s.heartbeatFor().refresh(t.Context(), 0); !delta.success {
		t.Fatal("heartbeat failed")
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	s.adoptRotated = func(context.Context, store.Account) error {
		close(entered)
		<-release
		return nil
	}
	done := make(chan struct{})
	go func() {
		s.handleIdleTransition(t.Context(), dir)
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("adoption did not start")
	}
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	token, err := s.cl.beginReservation(a)
	if err != nil {
		t.Fatalf("beginReservation during adoption = %v, want no account claim", err)
	}
	s.cl.abortReservation(token)
	close(release)
	<-done
}

func TestIdleAdoptionDefersForPendingSelection(t *testing.T) {
	s, dirs := newTestServer(t)
	dir := dirs[1]
	s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil }
	if delta := s.heartbeatFor().refresh(t.Context(), 0); !delta.success {
		t.Fatal("heartbeat failed")
	}
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	token, err := s.cl.beginReservation(account)
	if err != nil {
		t.Fatal(err)
	}
	defer s.cl.abortReservation(token)
	called := false
	s.adoptRotated = func(context.Context, store.Account) error {
		called = true
		return nil
	}
	s.handleIdleTransition(t.Context(), dir)
	if called {
		t.Fatal("idle adoption ran while a selection reservation was pending")
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
