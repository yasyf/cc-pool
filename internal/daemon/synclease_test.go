package daemon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
)

// TestSelectClaimsHolderAndLease pins that a select claims holdership and a
// 45-minute lease — claim first, lease second — before handing the account to
// the session, on both the ranked and forced-account paths.
func TestSelectClaimsHolderAndLease(t *testing.T) {
	fixed := time.Now()

	t.Run("ranked select claims the winner", func(t *testing.T) {
		s, _ := newTestServer(t)
		for id := 1; id <= 2; id++ {
			if err := s.m.Store.SetAccountUUID(id, fmt.Sprintf("u%d", id)); err != nil {
				t.Fatal(err)
			}
		}
		svc, _ := attachGate(s, "host-a", regWith())
		s.sync.now = func() time.Time { return fixed }

		resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true, Cwd: "/proj"})
		if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 1 {
			t.Fatalf("expected acct-1 to win, got %+v", resp)
		}
		want := []string{
			"claim u1 host-a",
			fmt.Sprintf("renew u1 host-a %d", fixed.Add(holderLeaseDuration).UnixMilli()),
		}
		got := svc.snapshot()
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("sync calls = %v, want %v", got, want)
		}
	})

	t.Run("forced select claims the named account", func(t *testing.T) {
		s, _ := newTestServer(t)
		for id := 1; id <= 2; id++ {
			if err := s.m.Store.SetAccountUUID(id, fmt.Sprintf("u%d", id)); err != nil {
				t.Fatal(err)
			}
		}
		svc, _ := attachGate(s, "host-a", regWith())
		s.sync.now = func() time.Time { return fixed }

		two := 2
		resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &two, NoMark: true, Cwd: "/proj"})
		if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 2 {
			t.Fatalf("expected forced acct-2, got %+v", resp)
		}
		got := svc.snapshot()
		if len(got) != 2 || got[0] != "claim u2 host-a" {
			t.Fatalf("sync calls = %v, want a claim on u2 then its lease", got)
		}
	})

	t.Run("uuid-less winner is skipped without failing the select", func(t *testing.T) {
		s, _ := newTestServer(t)
		svc, _ := attachGate(s, "host-a", regWith())

		resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true, Cwd: "/proj"})
		if !resp.OK {
			t.Fatalf("select must not fail on a uuid-less account: %+v", resp)
		}
		if got := svc.snapshot(); len(got) != 0 {
			t.Fatalf("no uuid → no registry traffic; calls = %v", got)
		}
	})
}

// TestLeaseRenewWhileBusyAndReleaseOnLastCheckin pins the lease lifecycle: the
// poll renews this host's expiring lease while a session runs (never clobbering
// a peer's), and checkin releases only on the last local close.
func TestLeaseRenewWhileBusyAndReleaseOnLastCheckin(t *testing.T) {
	fixed := time.Now()
	lease := func(host string, until time.Time) *hostsync.Lease {
		return &hostsync.Lease{Host: host, Until: until.UnixMilli()}
	}
	entry := func(l *hostsync.Lease) hostsync.AccountValue {
		return hostsync.AccountValue{
			UUID:  "u1",
			Chain: hostsync.ChainStamp{Holder: "host-a", ExpiresAt: fixed.Add(time.Hour).UnixMilli(), RotatedAt: fixed.UnixMilli()},
			Lease: l,
		}
	}
	busyServer := func(t *testing.T, l *hostsync.Lease) (*Server, *fakeRegistrySvc, store.Account) {
		t.Helper()
		cred := &creds.Credential{}
		cred.ClaudeAiOauth.AccessToken = "at-0"
		cred.ClaudeAiOauth.RefreshToken = "rt-0"
		cred.ClaudeAiOauth.ExpiresAt = fixed.Add(2 * time.Hour).UnixMilli() // fresh: no refresh in play
		s, _, a := newGateServer(t, cred, nil)
		s.scanSessions = func(context.Context) ([]procscan.Session, error) {
			return []procscan.Session{{PID: 4242, ConfigDir: a.ConfigDir, StartedAt: fixed}}, nil
		}
		svc, _ := attachGate(s, "host-a", regWith(entry(l)))
		s.sync.now = func() time.Time { return fixed }
		return s, svc, a
	}

	renewCall := fmt.Sprintf("renew u1 host-a %d", fixed.Add(holderLeaseDuration).UnixMilli())

	t.Run("own lease under 20m renews", func(t *testing.T) {
		s, svc, _ := busyServer(t, lease("host-a", fixed.Add(10*time.Minute)))
		s.pollOnce(t.Context())
		if got := svc.snapshot(); len(got) != 1 || got[0] != renewCall {
			t.Fatalf("calls = %v, want [%s]", got, renewCall)
		}
	})

	t.Run("own lease with plenty left does not renew", func(t *testing.T) {
		s, svc, _ := busyServer(t, lease("host-a", fixed.Add(30*time.Minute)))
		s.pollOnce(t.Context())
		if got := svc.snapshot(); len(got) != 0 {
			t.Fatalf("calls = %v, want none (%.0f min remain)", got, (30 * time.Minute).Minutes())
		}
	})

	t.Run("live peer lease is never clobbered", func(t *testing.T) {
		s, svc, _ := busyServer(t, lease("host-b", fixed.Add(5*time.Minute)))
		s.pollOnce(t.Context())
		if got := svc.snapshot(); len(got) != 0 {
			t.Fatalf("calls = %v, want none — a live peer lease is not ours to renew", got)
		}
	})

	t.Run("absent lease is re-established while busy", func(t *testing.T) {
		s, svc, _ := busyServer(t, nil)
		s.pollOnce(t.Context())
		if got := svc.snapshot(); len(got) != 1 || got[0] != renewCall {
			t.Fatalf("calls = %v, want [%s]", got, renewCall)
		}
	})

	t.Run("expired peer lease is re-established while busy", func(t *testing.T) {
		s, svc, _ := busyServer(t, lease("host-b", fixed.Add(-time.Minute)))
		s.pollOnce(t.Context())
		if got := svc.snapshot(); len(got) != 1 || got[0] != renewCall {
			t.Fatalf("calls = %v, want [%s]", got, renewCall)
		}
	})

	t.Run("checkin releases only on the last session", func(t *testing.T) {
		s, svc, a := busyServer(t, lease("host-a", fixed.Add(30*time.Minute)))
		if _, err := s.m.Store.OpenSession(a.ID, 111, a.ConfigDir, "/p", fixed); err != nil {
			t.Fatal(err)
		}
		if _, err := s.m.Store.OpenSession(a.ID, 222, a.ConfigDir, "/p", fixed); err != nil {
			t.Fatal(err)
		}

		if resp := s.handleCheckin(t.Context(), Request{Op: OpCheckin, PID: 111}); !resp.OK {
			t.Fatalf("checkin failed: %+v", resp)
		}
		if got := svc.snapshot(); len(got) != 0 {
			t.Fatalf("first checkin must not release (one session remains); calls = %v", got)
		}

		if resp := s.handleCheckin(t.Context(), Request{Op: OpCheckin, PID: 222}); !resp.OK {
			t.Fatalf("checkin failed: %+v", resp)
		}
		if got := svc.snapshot(); len(got) != 1 || got[0] != "release u1 host-a" {
			t.Fatalf("last checkin must release the lease; calls = %v", got)
		}
	})
}

// TestPeerLeasePenalizesRanking pins the select penalty: a live peer lease
// counts as one extra active session — never an exclusion — while an own-host
// or expired lease costs nothing.
func TestPeerLeasePenalizesRanking(t *testing.T) {
	fixed := time.Now()
	snaps := []pool.Snapshot{
		{Account: store.Account{ID: 1, AccountUUID: "u1"}, HasUsage: true, Util5h: 40, Util7d: 40},
		{Account: store.Account{ID: 2, AccountUUID: "u2"}, HasUsage: true, Util5h: 40, Util7d: 40},
	}
	entry := func(uuid string, l *hostsync.Lease) hostsync.AccountValue {
		return hostsync.AccountValue{UUID: uuid, Chain: hostsync.ChainStamp{Holder: "host-b"}, Lease: l}
	}
	scoreOf := func(t *testing.T, s *Server, id int) float64 {
		t.Helper()
		ranked, _ := s.rankWithReservations(snaps)
		for _, r := range ranked {
			if r.AccountID == id {
				if !r.Available {
					t.Fatalf("acct-%d must stay available (penalize, never exclude): %+v", id, r)
				}
				return r.Score
			}
		}
		t.Fatalf("acct-%d missing from ranking", id)
		return 0
	}

	cases := map[string]struct {
		lease     *hostsync.Lease
		penalized bool
	}{
		"live peer lease penalizes":        {&hostsync.Lease{Host: "host-b", Until: fixed.Add(20 * time.Minute).UnixMilli()}, true},
		"own-host lease costs nothing":     {&hostsync.Lease{Host: "host-a", Until: fixed.Add(20 * time.Minute).UnixMilli()}, false},
		"expired peer lease costs nothing": {&hostsync.Lease{Host: "host-b", Until: fixed.Add(-time.Minute).UnixMilli()}, false},
		"no lease costs nothing":           {nil, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := &Server{reservations: map[int]time.Time{}}
			_, _ = attachGate(s, "host-a", regWith(entry("u1", nil), entry("u2", tc.lease)))
			s.sync.now = func() time.Time { return fixed }

			s1, s2 := scoreOf(t, s, 1), scoreOf(t, s, 2)
			if tc.penalized && s2 >= s1 {
				t.Fatalf("leased acct-2 (%.2f) must rank strictly below identical acct-1 (%.2f)", s2, s1)
			}
			if !tc.penalized && s2 != s1 {
				t.Fatalf("acct-2 (%.2f) must tie acct-1 (%.2f) — no penalty applies", s2, s1)
			}
		})
	}
}

// TestSynclessNilGate pins the nil-gate contract: with s.sync nil, select,
// ranking, and checkin behave exactly as a syncless build; accounts carry
// UUIDs so a reordered uuid check still trips it.
func TestSynclessNilGate(t *testing.T) {
	withUUIDs := func(t *testing.T) *Server {
		t.Helper()
		s, _ := newTestServer(t)
		for id := 1; id <= 2; id++ {
			if err := s.m.Store.SetAccountUUID(id, fmt.Sprintf("u%d", id)); err != nil {
				t.Fatal(err)
			}
		}
		return s
	}

	t.Run("ranked select", func(t *testing.T) {
		s := withUUIDs(t)
		resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true, Cwd: "/proj"})
		if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 1 {
			t.Fatalf("nil-gate ranked select = %+v, want acct-1", resp)
		}
	})

	t.Run("forced select", func(t *testing.T) {
		s := withUUIDs(t)
		two := 2
		resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &two, NoMark: true, Cwd: "/proj"})
		if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 2 {
			t.Fatalf("nil-gate forced select = %+v, want acct-2", resp)
		}
	})

	t.Run("ranking applies no peer penalty", func(t *testing.T) {
		s := &Server{reservations: map[int]time.Time{}}
		snaps := []pool.Snapshot{
			{Account: store.Account{ID: 1, AccountUUID: "u1"}, HasUsage: true, Util5h: 40, Util7d: 40},
			{Account: store.Account{ID: 2, AccountUUID: "u2"}, HasUsage: true, Util5h: 40, Util7d: 40},
		}
		ranked, bySnap := s.rankWithReservations(snaps)
		if len(ranked) != 2 || len(bySnap) != 2 {
			t.Fatalf("ranking = %d results / %d snaps, want 2/2", len(ranked), len(bySnap))
		}
		if ranked[0].Score != ranked[1].Score {
			t.Fatalf("identical accounts must tie with no gate: %.2f vs %.2f", ranked[0].Score, ranked[1].Score)
		}
	})

	t.Run("checkin closes the last session without a release", func(t *testing.T) {
		s := withUUIDs(t)
		a, ok, err := s.m.Store.GetAccountByUUID("u1")
		if err != nil || !ok {
			t.Fatalf("GetAccountByUUID: %v ok=%v", err, ok)
		}
		if _, err := s.m.Store.OpenSession(a.ID, 111, a.ConfigDir, "/p", time.Now()); err != nil {
			t.Fatal(err)
		}
		if resp := s.handleCheckin(t.Context(), Request{Op: OpCheckin, PID: 111}); !resp.OK {
			t.Fatalf("nil-gate checkin = %+v, want OK", resp)
		}
	})
}
