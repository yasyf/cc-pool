package daemon

import (
	"errors"
	"testing"
	"time"

	"github.com/yasyf/fusekit/proc"
)

func ledgerHasFaultedFP(rows []LedgerState, dir string) bool {
	for _, r := range rows {
		if r.Policy == "fp.domain" && r.Resource == dir && r.Faulted {
			return true
		}
	}
	return false
}

func countPolicy(rows []LedgerState, policy string) int {
	n := 0
	for _, r := range rows {
		if r.Policy == policy {
			n++
		}
	}
	return n
}

// TestLedgersWireCarriesFPDebounceState pins both phases of the fp.domain row:
// a latched wedge is faulted while a single pre-debounce strike remains healthy.
func TestLedgersWireCarriesFPDebounceState(t *testing.T) {
	s, dirs := newTestServer(t)
	now := time.Unix(1750000000, 0)

	// dirs[1]: a genuinely wedged fp.domain (faulted). dirs[2]: struck once, below the
	// 2-strike debounce, so faulted stays false while the row stays observable.
	s.ledMu.Lock()
	s.led.forceFault(fpDomainPolicy, dirs[1], now, errors.New("fp wedged"))
	s.led.strike(fpDomainPolicy, dirs[2], now, errors.New("one blip"))
	s.ledMu.Unlock()

	ledgers := s.ledgersWire()
	if !ledgerHasFaultedFP(ledgers, dirs[1]) {
		t.Fatalf("faulted fp.domain %s missing from Ledgers %+v", dirs[1], ledgers)
	}
	if ledgerHasFaultedFP(ledgers, dirs[2]) {
		t.Fatalf("struck-not-faulted fp.domain row read as faulted in Ledgers: %+v", ledgers)
	}
	if n := countPolicy(ledgers, "fp.domain"); n != 2 {
		t.Fatalf("Ledgers carries %d fp.domain rows, want 2 (faulted + struck)", n)
	}
}

// TestLedgersWireComposesBothStores pins the composed Ledgers block: rows from
// the Server-owned store AND the holder cache's store appear together, in
// deterministic policy-then-resource order, without merging the stores.
func TestLedgersWireComposesBothStores(t *testing.T) {
	s, dirs := newTestServer(t)
	now := time.Unix(1750000000, 0)

	s.ledMu.Lock()
	s.led.forceFault(fpDomainPolicy, dirs[2], now, errors.New("fp wedged"))
	s.led.strike(authStreakPolicy, dirs[1], now, errors.New("401"))
	s.led.attempt(acctRateLimitPolicy, "/z/acct-b", attemptPrimary, now)
	s.led.attempt(acctRateLimitPolicy, "/a/acct-a", attemptPrimary, now)
	s.ledMu.Unlock()
	s.holder.markDeepWedged(dirs[1])

	got := s.ledgersWire()
	if len(got) != 5 {
		t.Fatalf("got %d rows %+v, want 5", len(got), got)
	}
	wantOrder := []struct{ policy, resource string }{
		{"auth.streak", dirs[1]},
		{"fp.domain", dirs[2]},
		{"fuse.deepwedge", dirs[1]}, // the holder-store row, composed in
		{"ratelimit.acct", "/a/acct-a"},
		{"ratelimit.acct", "/z/acct-b"},
	}
	for i, want := range wantOrder {
		if got[i].Policy != want.policy || got[i].Resource != want.resource {
			t.Errorf("row[%d] = (%s, %s), want (%s, %s)", i, got[i].Policy, got[i].Resource, want.policy, want.resource)
		}
	}
	if !got[2].Faulted {
		t.Error("holder-store fuse.deepwedge row lost its fault in composition")
	}
	if !got[1].Faulted || got[1].Parked || got[1].LastErr != "fp wedged" {
		t.Errorf("fp.domain row = %+v, want faulted, not parked, lastErr carried", got[1])
	}
	if got[0].Faulted || got[0].Strikes != 1 {
		t.Errorf("auth.streak row = %+v, want 1 strike, no fault", got[0])
	}
	wantDue := now.Add(proc.Backoff{Base: rateLimitBackoffBase, Cap: rateLimitBackoffCap}.After(1))
	if got[3].Attempts != 1 || !got[3].NextDue.Equal(wantDue) {
		t.Errorf("ratelimit row = %+v, want 1 attempt due at %v", got[3], wantDue)
	}
}

// TestLedgersWireEmptyIsNil pins that a healthy pool omits the block entirely
// (nil, so omitempty drops the JSON key) — including with a never-touched
// holder store.
func TestLedgersWireEmptyIsNil(t *testing.T) {
	s, _ := newTestServer(t)
	if got := s.ledgersWire(); got != nil {
		t.Fatalf("ledgersWire on a healthy pool = %+v, want nil", got)
	}
}

// TestLedgersWireParkedPerPolicy pins the wire Parked bit against each policy
// shape: two-lane (fuse.remount — either lane trips, alternating lanes never
// do), single-lane (fp.domain — the attempts clock), and gate (ratelimit — no
// breaker, never parked).
func TestLedgersWireParkedPerPolicy(t *testing.T) {
	now := time.Unix(1750000000, 0)
	cases := map[string]struct {
		seed   func(ls *ledgers)
		policy string
		parked bool
	}{
		"two-lane primary lane at the hazard breaker parks": {
			seed: func(ls *ledgers) {
				for i := 0; i < remountBreakerThreshold; i++ {
					ls.attempt(fuseRemountPolicy, "/r", attemptPrimary, now)
				}
			},
			policy: "fuse.remount", parked: true,
		},
		"two-lane alt lane at the TCC threshold parks": {
			seed: func(ls *ledgers) {
				for i := 0; i < tccBreakerThreshold; i++ {
					ls.attempt(fuseRemountPolicy, "/r", attemptAlt, now)
				}
			},
			policy: "fuse.remount", parked: true,
		},
		"two-lane alternating lanes never park": {
			seed: func(ls *ledgers) {
				for i := 0; i < 4*tccBreakerThreshold; i++ {
					kind := attemptPrimary
					if i%2 == 0 {
						kind = attemptAlt
					}
					ls.attempt(fuseRemountPolicy, "/r", kind, now)
				}
			},
			policy: "fuse.remount", parked: false,
		},
		"single-lane parks on the attempts clock": {
			seed: func(ls *ledgers) {
				ls.forceFault(fpDomainPolicy, "/r", now, errors.New("wedged"))
				for i := 0; i < fpRecoveryBreaker; i++ {
					ls.attempt(fpDomainPolicy, "/r", attemptPrimary, now)
				}
			},
			policy: "fp.domain", parked: true,
		},
		"single-lane under the breaker stays unparked": {
			seed: func(ls *ledgers) {
				ls.forceFault(fpDomainPolicy, "/r", now, errors.New("wedged"))
				for i := 0; i < fpRecoveryBreaker-1; i++ {
					ls.attempt(fpDomainPolicy, "/r", attemptPrimary, now)
				}
			},
			policy: "fp.domain", parked: false,
		},
		"gate policy never parks": {
			seed: func(ls *ledgers) {
				for i := 0; i < 50; i++ {
					ls.attempt(poolRateLimitPolicy, "/r", attemptPrimary, now)
				}
			},
			policy: "ratelimit.pool", parked: false,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := &Server{led: newLedgers()}
			tc.seed(s.led)
			got := s.ledgersWire()
			if len(got) != 1 {
				t.Fatalf("got %d rows %+v, want 1", len(got), got)
			}
			if got[0].Policy != tc.policy {
				t.Fatalf("policy = %q, want %q", got[0].Policy, tc.policy)
			}
			if got[0].Parked != tc.parked {
				t.Errorf("Parked = %v, want %v (row %+v)", got[0].Parked, tc.parked, got[0])
			}
		})
	}
}
