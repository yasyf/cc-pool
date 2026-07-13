package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/store"
)

// revokedServer builds a server whose single account holds a stale refresh
// token — the definitive ErrNeedsLogin path into flagNeedsLogin.
func revokedServer(t *testing.T) (*Server, store.Account, *creds.Credential) {
	t.Helper()
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-stale" // ≠ fakeOAuth currentRT → invalid_grant
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
	s, fo, a := newGateServer(t, cred, nil)
	// The server's chain moved on: our rt-stale refresh gets invalid_grant.
	fo.currentRT = "rt-0"
	fo.setUsage401(true)
	return s, a, cred
}

// pullHealing returns a syncPull that installs a synced (refresh-token-free)
// credential at expiresAt into the account's store — the effect of a converge
// pull of a peer's stripped chain. When healsUsage, it also flips the fake OAuth
// usage endpoint to succeed, modeling an origin token that actually works.
func pullHealing(s *Server, a store.Account, expiresAt int64, healsUsage bool) func(context.Context) error {
	return func(context.Context) error {
		fresh := &creds.Credential{}
		fresh.ClaudeAiOauth.AccessToken = "at-peer"
		fresh.ClaudeAiOauth.ExpiresAt = expiresAt
		if err := s.m.Creds.Store(a, creds.SourceKeychain).Write(fresh); err != nil {
			return err
		}
		if healsUsage {
			s.m.OAuth.(*fakeOAuth).setUsage401(false)
		}
		return nil
	}
}

// wireSyncRegistry attaches a minimal sync service whose registry names origin
// as uuid's chain holder, so authKind classifies at persist time.
func wireSyncRegistry(t *testing.T, s *Server, uuid, origin string) {
	t.Helper()
	svc := &hostsync.Service{
		Registry: hostsync.NewRegistryFile(t.TempDir()),
		StampDir: filepath.Join(t.TempDir(), "stamps"),
	}
	err := svc.PublishAccount(context.Background(), hostsync.AccountValue{
		UUID:  uuid,
		Chain: hostsync.ChainStamp{Origin: origin, ExpiresAt: 1, Hash: "h-" + uuid},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.m.Store.SetMeta(metaSyncEnabled, "1"); err != nil {
		t.Fatal(err)
	}
	s.syncSvc = svc
	s.syncSelf = "host-self"
}

// TestSyncHealDecidesEachTick pins the reworked flagNeedsLogin: it heals, then
// ALWAYS decides this tick. A fresher chain that samples clean clears the flag;
// a fresher chain that still cannot sample persists it same tick (the anti-loop
// litmus); a no-op, equal-expiry, or failing pull flags exactly as before.
func TestSyncHealDecidesEachTick(t *testing.T) {
	t.Run("fresher chain that samples clean clears the flag", func(t *testing.T) {
		s, a, _ := revokedServer(t)
		s.syncPull = pullHealing(s, a, time.Now().Add(time.Hour).UnixMilli(), true)

		s.pollOnce(t.Context())
		if h, _ := s.m.Store.GetAuthHealth(a.ID); h.NeedsLogin {
			t.Fatal("a fresher chain that samples clean must clear needs-login")
		}
		if l := s.led.peek(authStreakPolicy, a.ConfigDir); l != nil && l.faulted {
			t.Fatal("a clean resample must clear the auth streak, not leave it faulted")
		}
	})

	t.Run("fresher chain that still fails persists the flag same tick (anti-loop)", func(t *testing.T) {
		s, a, _ := revokedServer(t)
		// The pull lands a strictly-fresher chain, but usage still 401s: the one
		// inline resample fails, so the flag MUST persist this tick rather than
		// looping forever unflagged while SampleUsage never succeeds (defect 6).
		s.syncPull = pullHealing(s, a, time.Now().Add(time.Hour).UnixMilli(), false)

		s.pollOnce(t.Context())
		if h, _ := s.m.Store.GetAuthHealth(a.ID); !h.NeedsLogin {
			t.Fatal("a heal that improved expiry but still cannot sample must persist needs-login THIS tick")
		}
		if l := s.led.peek(authStreakPolicy, a.ConfigDir); l == nil || l.lastAt.IsZero() {
			t.Fatal("the attempt clock must be stamped so the backoff engages")
		}
	})

	t.Run("expired grace-served synced pull keeps the flag as awaiting-origin", func(t *testing.T) {
		s, a, _ := revokedServer(t)
		wireSyncRegistry(t, s, "u1", "peer-b")
		// The pull lands a strictly-fresher but ALREADY-EXPIRED synced copy and
		// /usage grace-serves it with a 200. This host still cannot refresh it,
		// so the flag must persist (needs-login gates selection) as
		// awaiting-origin, not clear until the next poll re-flags.
		s.syncPull = pullHealing(s, a, time.Now().Add(-time.Minute).UnixMilli(), true)

		s.pollOnce(t.Context())
		h, _ := s.m.Store.GetAuthHealth(a.ID)
		if !h.NeedsLogin {
			t.Fatal("an expired synced pull must NOT clear needs-login even when /usage grace-serves it")
		}
		if h.Kind != store.AuthKindAwaitingOrigin {
			t.Fatalf("kind = %q, want %q", h.Kind, store.AuthKindAwaitingOrigin)
		}
	})

	t.Run("resample 429 arms the rate-limit gates with Retry-After", func(t *testing.T) {
		s, a, _ := revokedServer(t)
		fo := s.m.OAuth.(*fakeOAuth)
		fo.rlByAT = map[string]bool{"at-peer": true}
		fo.retryAfter = 30 * time.Minute
		s.syncPull = pullHealing(s, a, time.Now().Add(time.Hour).UnixMilli(), false)

		s.pollOnce(t.Context())
		l := s.led.peek(poolRateLimitPolicy, poolResource)
		if l == nil {
			t.Fatal("the healed token's 429 must arm the pool gate, not be swallowed by the resample")
		}
		if got := l.nextDue.Sub(l.lastAt); got != 30*time.Minute {
			t.Fatalf("pool gate window = %v, want the 429's 30m Retry-After", got)
		}
		if !s.poolRateLimited() {
			t.Fatal("the pool gate must hold immediately after the resample's 429")
		}
		if got := s.acctRateLimitStreak(a.ConfigDir); got != 1 {
			t.Fatalf("acct 429 streak = %d, want 1", got)
		}
		if !s.pollGated(a) {
			t.Fatal("the account must back off after the resample's 429")
		}
	})

	t.Run("pull with nothing fresher flags", func(t *testing.T) {
		s, a, _ := revokedServer(t)
		pulled := false
		s.syncPull = func(context.Context) error { pulled = true; return nil }

		s.pollOnce(t.Context())
		if !pulled {
			t.Fatal("the self-heal must run one converge pull before flagging")
		}
		if h, _ := s.m.Store.GetAuthHealth(a.ID); !h.NeedsLogin {
			t.Fatal("a pull that changes nothing must still flag needs-login")
		}
	})

	t.Run("equal-expiry pull flags (strictly-fresher required)", func(t *testing.T) {
		s, a, cred := revokedServer(t)
		// Even a would-succeed usage cannot rescue a non-fresher pull: syncHeal
		// reports no improvement, so no resample runs and the flag persists.
		s.syncPull = pullHealing(s, a, cred.ClaudeAiOauth.ExpiresAt, true)

		s.pollOnce(t.Context())
		if h, _ := s.m.Store.GetAuthHealth(a.ID); !h.NeedsLogin {
			t.Fatal("an equal-expiry chain is not fresher and must flag needs-login")
		}
	})

	t.Run("failing pull flags", func(t *testing.T) {
		s, a, _ := revokedServer(t)
		s.syncPull = func(context.Context) error { return errors.New("mesh unreachable") }

		s.pollOnce(t.Context())
		if h, _ := s.m.Store.GetAuthHealth(a.ID); !h.NeedsLogin {
			t.Fatal("a failing pull must fall through to flagging needs-login")
		}
	})
}

// TestSyncHealAbsentFlagsAsBefore pins the sync-disabled baseline: with a nil
// syncPull seam a confirmed revocation flags needs-login exactly as a syncless
// daemon does, via ErrNeedsLogin rather than the 401 streak.
func TestSyncHealAbsentFlagsAsBefore(t *testing.T) {
	s, a, _ := revokedServer(t)

	s.pollOnce(t.Context())
	if h, _ := s.m.Store.GetAuthHealth(a.ID); !h.NeedsLogin {
		t.Fatal("nil syncPull must leave the needs-login flagging byte-identical to today")
	}
	if l := s.led.peek(authStreakPolicy, a.ConfigDir); l != nil && (l.strikes != 0 || l.faulted) {
		t.Fatalf("confirmed revocation flags via ErrNeedsLogin, not the 401 streak; strikes=%d faulted=%v", l.strikes, l.faulted)
	}
}
