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
	if _, err := s.m.Store.OpenSession(1, 4242, dir, "", time.Now().Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}
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

func TestCheckinScanFailureDoesNotAdoptFromRetainedSnapshot(t *testing.T) {
	s, dirs := newTestServer(t)
	dir := dirs[1]
	if _, err := s.m.Store.OpenSession(1, 4242, dir, "", time.Now().Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	h := s.heartbeatFor()
	adoptions := 0
	s.adoptRotated = func(context.Context, store.Account) error { adoptions++; return nil }
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dir}}, nil
	}
	if delta := h.refresh(t.Context(), 0); !delta.success {
		t.Fatal("initial heartbeat failed")
	}
	s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, errors.New("scan failed") }
	accountID := 1
	resp := s.handleCheckin(t.Context(), Request{Account: &accountID, PID: 4242})
	if !resp.OK {
		t.Fatalf("handleCheckin = %+v", resp)
	}
	s.handleHeartbeatDelta(t.Context(), h.refresh(t.Context(), 0))
	if adoptions != 0 {
		t.Fatal("checkin adopted from retained heartbeat state after a failed fresh scan")
	}
}

func TestCheckinAdoptsAfterFreshIdleScanWithoutObservedActiveEdge(t *testing.T) {
	s, dirs := newTestServer(t)
	dir := dirs[1]
	if _, err := s.m.Store.OpenSession(1, 4242, dir, "", time.Now().Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	adoptions := 0
	s.adoptRotated = func(context.Context, store.Account) error {
		adoptions++
		return nil
	}
	s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil }

	accountID := 1
	resp := s.handleCheckin(t.Context(), Request{Account: &accountID, PID: 4242})
	if !resp.OK {
		t.Fatalf("handleCheckin = %+v", resp)
	}
	delta := s.heartbeatFor().refresh(t.Context(), 0)
	s.handleHeartbeatDelta(t.Context(), delta)
	if adoptions != 1 {
		t.Fatalf("adoptions = %d, want 1 after the fresh idle heartbeat", adoptions)
	}
}

func TestIdleAdoptionFencesNewReservations(t *testing.T) {
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
	if _, err := s.cl.beginReservation(1); !errors.Is(err, errAccountConverting) {
		t.Fatalf("beginReservation during adoption = %v, want errAccountConverting", err)
	}
	close(release)
	<-done
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
