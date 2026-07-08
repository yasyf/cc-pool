package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
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

// pullInstalling returns a syncPull func that installs a rotated credential
// with the given expiry into the account's store — the effect of a converge
// pull that fetched a fresher chain from a peer.
func pullInstalling(s *Server, a store.Account, expiresAt int64) func(context.Context) error {
	return func(context.Context) error {
		fresh := &creds.Credential{}
		fresh.ClaudeAiOauth.AccessToken = "at-peer"
		fresh.ClaudeAiOauth.RefreshToken = "rt-peer"
		fresh.ClaudeAiOauth.ExpiresAt = expiresAt
		return s.m.Creds.Store(a, creds.SourceKeychain).Write(fresh)
	}
}

// TestInvalidGrantSyncHealSkipsNeedsLogin pins the invalid_grant self-heal: a
// pull landing a fresher chain skips SetNeedsLogin; a no-op or failing pull
// flags exactly as today.
func TestInvalidGrantSyncHealSkipsNeedsLogin(t *testing.T) {
	t.Run("fresher chain arrives, flag skipped", func(t *testing.T) {
		s, a, _ := revokedServer(t)
		s.syncPull = pullInstalling(s, a, time.Now().Add(time.Hour).UnixMilli())

		s.pollOnce(t.Context())
		if h, _ := s.m.Store.GetAuthHealth(a.ID); h.NeedsLogin {
			t.Fatal("a strictly-fresher pulled chain must skip needs-login")
		}
		if _, ok := s.lastAuthAttempt[a.ID]; !ok {
			t.Fatal("the attempt clock must still be stamped so the backoff engages")
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
		s.syncPull = pullInstalling(s, a, cred.ClaudeAiOauth.ExpiresAt)

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

	t.Run("heal skips setting but never clears an existing flag", func(t *testing.T) {
		s, a, _ := revokedServer(t)
		if _, err := s.m.Store.SetNeedsLogin(a.ID, time.Now(), "prior failure"); err != nil {
			t.Fatal(err)
		}
		s.syncPull = pullInstalling(s, a, time.Now().Add(time.Hour).UnixMilli())

		s.pollOnce(t.Context())
		if h, _ := s.m.Store.GetAuthHealth(a.ID); !h.NeedsLogin {
			t.Fatal("syncHeal must only SKIP setting the flag — clearing is owned elsewhere")
		}
	})
}

// TestSyncHealAbsentFlagsAsBefore pins the sync-disabled baseline: with a nil
// syncPull seam a confirmed revocation flags needs-login exactly as a
// syncless daemon does.
func TestSyncHealAbsentFlagsAsBefore(t *testing.T) {
	s, a, _ := revokedServer(t)

	s.pollOnce(t.Context())
	if h, _ := s.m.Store.GetAuthHealth(a.ID); !h.NeedsLogin {
		t.Fatal("nil syncPull must leave the needs-login flagging byte-identical to today")
	}
	if got := s.authStreak[a.ID]; got != 0 {
		t.Fatalf("confirmed revocation flags via ErrNeedsLogin, not the 401 streak; authStreak = %d", got)
	}
}
