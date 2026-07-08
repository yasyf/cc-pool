package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/cregistry"
)

// fakeRegistrySvc is an in-memory registryService recording every call in
// order, so tests assert both effects and sequencing (claim before renew,
// claim before refresh).
type fakeRegistrySvc struct {
	mu       sync.Mutex
	claimErr error
	calls    []string
}

func (f *fakeRegistrySvc) ClaimHolder(_ context.Context, uuid, host string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fmt.Sprintf("claim %s %s", uuid, host))
	return f.claimErr
}

func (f *fakeRegistrySvc) RenewLease(_ context.Context, uuid, host string, until int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fmt.Sprintf("renew %s %s %d", uuid, host, until))
	return nil
}

func (f *fakeRegistrySvc) ReleaseLease(_ context.Context, uuid, host string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fmt.Sprintf("release %s %s", uuid, host))
	return nil
}

func (f *fakeRegistrySvc) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// regWith builds a registry holding the given present values.
func regWith(vals ...hostsync.AccountValue) hostsync.Registry {
	reg := cregistry.New[hostsync.AccountValue]()
	for _, v := range vals {
		reg.Add(v.UUID, v, cregistry.UnixMicros(time.Now()))
	}
	return reg
}

// attachGate wires a syncGate backed by the fake service and a swappable
// registry onto s, returning the fake and the swap func.
func attachGate(s *Server, self string, reg hostsync.Registry) (*fakeRegistrySvc, func(hostsync.Registry)) {
	svc := &fakeRegistrySvc{}
	var mu sync.Mutex
	cur := reg
	s.sync = &syncGate{
		svc: svc,
		load: func() (hostsync.Registry, error) {
			mu.Lock()
			defer mu.Unlock()
			return cur, nil
		},
		self: self,
		now:  time.Now,
		log:  s.log,
	}
	return svc, func(r hostsync.Registry) {
		mu.Lock()
		defer mu.Unlock()
		cur = r
	}
}

// newGateServer builds a one-account daemon Server whose account carries
// AccountUUID "u1" and the given credential; sessions drives busy/idle.
func newGateServer(t *testing.T, cred *creds.Credential, sessions []procscan.Session) (*Server, *fakeOAuth, store.Account) {
	t.Helper()
	return newGateServerUUID(t, cred, sessions, "u1")
}

// newGateServerUUID is newGateServer with an explicit account uuid ("" builds
// a not-yet-backfilled account).
func newGateServerUUID(t *testing.T, cred *creds.Credential, sessions []procscan.Session, uuid string) (*Server, *fakeOAuth, store.Account) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
		KeychainService: "svc", KeychainAccount: "user", AccountUUID: uuid,
	}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	fk := credstest.NewFake()
	fk.Put(a.KeychainService, a.KeychainAccount, cred)
	fo := &fakeOAuth{currentRT: cred.ClaudeAiOauth.RefreshToken}
	s := &Server{
		m:            &pool.Manager{Store: st, OAuth: fo, Creds: fk, LockDir: t.TempDir()},
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return sessions, nil },
		cl:           newClaims(),
		led:          newLedgers(),
	}
	return s, fo, a
}

// nearExpiryCred returns a credential inside RefreshLeadTime so an idle poll
// wants to refresh it.
func nearExpiryCred() *creds.Credential {
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-0"
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Minute).UnixMilli()
	return cred
}

// expiredCred returns a credential already past expiry.
func expiredCred() *creds.Credential {
	cred := nearExpiryCred()
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
	return cred
}

// TestRefreshGateSuppressesNonHolder pins the idle leg of the holder gate: a
// peer-held, unexpired chain suppresses this host's preemptive refresh, and
// flipping the holder to self re-enables it.
func TestRefreshGateSuppressesNonHolder(t *testing.T) {
	s, fo, _ := newGateServer(t, nearExpiryCred(), nil)
	svc, swap := attachGate(s, "host-a", regWith(hostsync.AccountValue{
		UUID:  "u1",
		Chain: hostsync.ChainStamp{Holder: "host-b", ExpiresAt: time.Now().Add(time.Hour).UnixMilli(), RotatedAt: time.Now().UnixMilli()},
	}))

	s.pollOnce(t.Context())
	if got := fo.refreshCount(); got != 0 {
		t.Fatalf("non-holder refreshed %d time(s), want 0", got)
	}
	if calls := svc.snapshot(); len(calls) != 0 {
		t.Fatalf("unexpired peer chain must not be taken over; calls = %v", calls)
	}

	swap(regWith(hostsync.AccountValue{
		UUID:  "u1",
		Chain: hostsync.ChainStamp{Holder: "host-a", ExpiresAt: time.Now().Add(time.Hour).UnixMilli(), RotatedAt: time.Now().UnixMilli()},
	}))
	s.pollOnce(t.Context())
	if got := fo.refreshCount(); got != 1 {
		t.Fatalf("holder refreshed %d time(s), want 1", got)
	}
}

// TestRefreshGateSuppressesBusyRefreshToo pins the AllowBusyRefresh leg: a
// busy account that would busy-refresh must not while a peer holds the chain,
// and must once this host holds it.
func TestRefreshGateSuppressesBusyRefreshToo(t *testing.T) {
	busyOn := func(dir string) []procscan.Session {
		return []procscan.Session{{PID: 4242, ConfigDir: dir, StartedAt: time.Now()}}
	}

	t.Run("peer holder suppresses busy refresh", func(t *testing.T) {
		cred := expiredCred()
		s, fo, a := newGateServer(t, cred, nil)
		s.scanSessions = func(context.Context) ([]procscan.Session, error) { return busyOn(a.ConfigDir), nil }
		fo.setUsage401(true)
		s.led.row(authStreakPolicy, a.ConfigDir).strikes = 1 // busy-refresh heuristic armed
		attachGate(s, "host-a", regWith(hostsync.AccountValue{
			UUID: "u1",
			// The peer already rotated: its chain is fresher and unexpired.
			Chain: hostsync.ChainStamp{Holder: "host-b", ExpiresAt: time.Now().Add(time.Hour).UnixMilli(), RotatedAt: time.Now().UnixMilli()},
		}))

		s.pollOnce(t.Context())
		if got := fo.refreshCount(); got != 0 {
			t.Fatalf("busy non-holder refreshed %d time(s), want 0 — this is the chain-fork path", got)
		}
	})

	t.Run("self holder busy-refreshes as before", func(t *testing.T) {
		cred := expiredCred()
		s, fo, a := newGateServer(t, cred, nil)
		s.scanSessions = func(context.Context) ([]procscan.Session, error) { return busyOn(a.ConfigDir), nil }
		fo.setUsage401(true)
		s.led.row(authStreakPolicy, a.ConfigDir).strikes = 1
		attachGate(s, "host-a", regWith(hostsync.AccountValue{
			UUID:  "u1",
			Chain: hostsync.ChainStamp{Holder: "host-a", ExpiresAt: time.Now().Add(time.Hour).UnixMilli(), RotatedAt: time.Now().UnixMilli()},
		}))

		s.pollOnce(t.Context())
		if got := fo.refreshCount(); got != 1 {
			t.Fatalf("busy holder refreshed %d time(s), want 1", got)
		}
	})
}

// TestRefreshGateNilAllowsAll pins the sync-disabled baseline: a nil gate, a
// uuid-less account, an account absent from the registry, and a tombstoned
// entry all leave refresh behavior byte-identical to a syncless daemon.
func TestRefreshGateNilAllowsAll(t *testing.T) {
	tombstoned := func() hostsync.Registry {
		reg := regWith(hostsync.AccountValue{
			UUID:  "u1",
			Chain: hostsync.ChainStamp{Holder: "host-b", ExpiresAt: time.Now().Add(time.Hour).UnixMilli()},
		})
		reg.Remove("u1", cregistry.UnixMicros(time.Now())+1)
		return reg
	}

	peerHeld := func() hostsync.Registry {
		return regWith(hostsync.AccountValue{
			UUID:  "u1",
			Chain: hostsync.ChainStamp{Holder: "host-b", ExpiresAt: time.Now().Add(time.Hour).UnixMilli()},
		})
	}
	cases := map[string]struct {
		uuid    string
		arrange func(t *testing.T, s *Server)
	}{
		"nil gate": {uuid: "u1", arrange: func(t *testing.T, s *Server) {}},
		"gate disabled by enabled func": {uuid: "u1", arrange: func(t *testing.T, s *Server) {
			attachGate(s, "host-a", peerHeld())
			s.sync.enabled = func() bool { return false }
		}},
		"account without uuid": {uuid: "", arrange: func(t *testing.T, s *Server) {
			attachGate(s, "host-a", peerHeld())
		}},
		"no registry entry": {uuid: "u1", arrange: func(t *testing.T, s *Server) {
			attachGate(s, "host-a", regWith())
		}},
		"tombstoned entry": {uuid: "u1", arrange: func(t *testing.T, s *Server) {
			attachGate(s, "host-a", tombstoned())
		}},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, fo, _ := newGateServerUUID(t, nearExpiryCred(), nil, tc.uuid)
			tc.arrange(t, s)
			s.pollOnce(t.Context())
			if got := fo.refreshCount(); got != 1 {
				t.Fatalf("idle near-expiry account refreshed %d time(s), want 1", got)
			}
		})
	}
}

// TestTakeoverAfterHolderStale pins the dead-holder takeover: expired chain,
// no live peer lease, and stale rotation let this host claim FIRST then
// refresh; any missing precondition blocks both.
func TestTakeoverAfterHolderStale(t *testing.T) {
	nowMS := func() int64 { return time.Now().UnixMilli() }
	staleRotated := func() int64 {
		return time.Now().Add(-(takeoverStaleAfter + takeoverJitterSpan + time.Minute)).UnixMilli()
	}

	t.Run("stale dead holder is taken over, claim before refresh", func(t *testing.T) {
		s, fo, _ := newGateServer(t, expiredCred(), nil)
		svc, _ := attachGate(s, "host-a", regWith(hostsync.AccountValue{
			UUID:  "u1",
			Chain: hostsync.ChainStamp{Holder: "host-b", ExpiresAt: nowMS() - 3_600_000, RotatedAt: staleRotated()},
		}))

		s.pollOnce(t.Context())
		calls := svc.snapshot()
		if len(calls) != 1 || calls[0] != "claim u1 host-a" {
			t.Fatalf("calls = %v, want exactly [claim u1 host-a]", calls)
		}
		if got := fo.refreshCount(); got != 1 {
			t.Fatalf("takeover refreshed %d time(s), want 1", got)
		}
	})

	t.Run("live peer lease blocks takeover", func(t *testing.T) {
		s, fo, _ := newGateServer(t, expiredCred(), nil)
		svc, _ := attachGate(s, "host-a", regWith(hostsync.AccountValue{
			UUID:  "u1",
			Chain: hostsync.ChainStamp{Holder: "host-b", ExpiresAt: nowMS() - 3_600_000, RotatedAt: staleRotated()},
			Lease: &hostsync.Lease{Host: "host-b", Until: time.Now().Add(10 * time.Minute).UnixMilli()},
		}))

		s.pollOnce(t.Context())
		if calls := svc.snapshot(); len(calls) != 0 {
			t.Fatalf("live peer lease must block the claim; calls = %v", calls)
		}
		if got := fo.refreshCount(); got != 0 {
			t.Fatalf("refreshed %d time(s) under a live peer lease, want 0", got)
		}
	})

	t.Run("expired peer lease does not block takeover", func(t *testing.T) {
		s, fo, _ := newGateServer(t, expiredCred(), nil)
		svc, _ := attachGate(s, "host-a", regWith(hostsync.AccountValue{
			UUID:  "u1",
			Chain: hostsync.ChainStamp{Holder: "host-b", ExpiresAt: nowMS() - 3_600_000, RotatedAt: staleRotated()},
			Lease: &hostsync.Lease{Host: "host-b", Until: time.Now().Add(-time.Minute).UnixMilli()},
		}))

		s.pollOnce(t.Context())
		if calls := svc.snapshot(); len(calls) != 1 || calls[0] != "claim u1 host-a" {
			t.Fatalf("calls = %v, want [claim u1 host-a]", calls)
		}
		if got := fo.refreshCount(); got != 1 {
			t.Fatalf("refreshed %d time(s), want 1", got)
		}
	})

	t.Run("fresh rotation blocks takeover", func(t *testing.T) {
		s, fo, _ := newGateServer(t, expiredCred(), nil)
		svc, _ := attachGate(s, "host-a", regWith(hostsync.AccountValue{
			UUID: "u1",
			// Expired chain but the holder published a rotation just now — the
			// jittered staleness threshold (>= takeoverStaleAfter) is not met.
			Chain: hostsync.ChainStamp{Holder: "host-b", ExpiresAt: nowMS() - 3_600_000, RotatedAt: nowMS()},
		}))

		s.pollOnce(t.Context())
		if calls := svc.snapshot(); len(calls) != 0 {
			t.Fatalf("fresh rotation must block the claim; calls = %v", calls)
		}
		if got := fo.refreshCount(); got != 0 {
			t.Fatalf("refreshed %d time(s), want 0", got)
		}
	})

	t.Run("failed claim blocks the refresh", func(t *testing.T) {
		s, fo, _ := newGateServer(t, expiredCred(), nil)
		svc, _ := attachGate(s, "host-a", regWith(hostsync.AccountValue{
			UUID:  "u1",
			Chain: hostsync.ChainStamp{Holder: "host-b", ExpiresAt: nowMS() - 3_600_000, RotatedAt: staleRotated()},
		}))
		svc.claimErr = errors.New("registry unavailable")

		s.pollOnce(t.Context())
		if got := fo.refreshCount(); got != 0 {
			t.Fatalf("refresh must wait for the claim to land; refreshed %d time(s)", got)
		}
	})
}

// TestHostJitterDeterministicAndBounded pins the takeover jitter contract:
// per-host deterministic, always inside [0, takeoverJitterSpan), and actually
// spreading distinct hosts apart.
func TestHostJitterDeterministicAndBounded(t *testing.T) {
	hosts := []string{"mba", "studio", "mini", "yasyf-home", "build-1", "build-2", "a", "b"}
	seen := map[time.Duration]bool{}
	for _, h := range hosts {
		j := hostJitter(h)
		if j != hostJitter(h) {
			t.Fatalf("hostJitter(%q) is not deterministic: %v vs %v", h, j, hostJitter(h))
		}
		if j < 0 || j >= takeoverJitterSpan {
			t.Fatalf("hostJitter(%q) = %v, want in [0, %v)", h, j, takeoverJitterSpan)
		}
		seen[j] = true
	}
	if len(seen) < 2 {
		t.Fatalf("jitter collapsed to a single offset across %d hosts — it cannot separate takeovers", len(hosts))
	}
}
