package daemon

import (
	"errors"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// TestLedgersWireSortsDaemonRows pins deterministic policy-then-resource order.
func TestLedgersWireSortsDaemonRows(t *testing.T) {
	s, dirs := newTestServer(t)
	now := time.Unix(1750000000, 0)

	s.ledMu.Lock()
	s.led.strike(authStreakPolicy, dirs[1], now, errors.New("401"))
	s.led.attempt(acctRateLimitPolicy, "/z/acct-b", now)
	s.led.attempt(acctRateLimitPolicy, "/a/acct-a", now)
	s.ledMu.Unlock()

	got := s.ledgersWire()
	if len(got) != 3 {
		t.Fatalf("got %d rows %+v, want 3", len(got), got)
	}
	wantOrder := []struct{ policy, resource string }{
		{"auth.streak", dirs[1]},
		{"ratelimit.acct", "/a/acct-a"},
		{"ratelimit.acct", "/z/acct-b"},
	}
	for i, want := range wantOrder {
		if got[i].Policy != want.policy || got[i].Resource != want.resource {
			t.Errorf("row[%d] = (%s, %s), want (%s, %s)", i, got[i].Policy, got[i].Resource, want.policy, want.resource)
		}
	}
	if got[0].Faulted || got[0].Strikes != 1 {
		t.Errorf("auth.streak row = %+v, want 1 strike, no fault", got[0])
	}
	wantDue := now.Add(proc.Backoff{Base: rateLimitBackoffBase, Cap: rateLimitBackoffCap}.After(1))
	if got[1].Attempts != 1 || !got[1].NextDue.Equal(wantDue) {
		t.Errorf("ratelimit row = %+v, want 1 attempt due at %v", got[1], wantDue)
	}
}

// TestLedgersWireEmptyIsNil pins that a healthy pool omits the block entirely.
func TestLedgersWireEmptyIsNil(t *testing.T) {
	s, _ := newTestServer(t)
	if got := s.ledgersWire(); got != nil {
		t.Fatalf("ledgersWire on a healthy pool = %+v, want nil", got)
	}
}
