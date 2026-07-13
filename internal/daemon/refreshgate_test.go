package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/procscan"
)

// TestSyncedAccountNeverRefreshes pins the structural refresh gate that replaced
// the cross-host mayRefresh check: a synced (refresh-token-free) credential can
// never trigger an OAuth refresh, under every opts combination the scheduler
// derives — idle AllowRefresh, busy+streak AllowBusyRefresh, busy+recovery
// AllowBusyRefresh, and busy-with-neither. Only the origin holds a refresh token.
func TestSyncedAccountNeverRefreshes(t *testing.T) {
	syncedCred := func(expiresAt int64) *creds.Credential {
		c := &creds.Credential{}
		c.ClaudeAiOauth.AccessToken = "at-synced" // no refresh token: a synced copy
		c.ClaudeAiOauth.ExpiresAt = expiresAt
		return c
	}
	busyOn := func(dir string) []procscan.Session {
		return []procscan.Session{{PID: 4242, ConfigDir: dir, StartedAt: time.Now()}}
	}
	unexpired := time.Now().Add(time.Hour).UnixMilli()
	expired := time.Now().Add(-time.Hour).UnixMilli()

	cases := map[string]struct {
		expiresAt int64
		busy      bool
		streak    bool
		recovery  bool
	}{
		"idle unexpired (AllowRefresh)":              {unexpired, false, false, false},
		"idle expired (AllowRefresh)":                {expired, false, false, false},
		"busy + streak expired (AllowBusyRefresh)":   {expired, true, true, false},
		"busy + recovery expired (AllowBusyRefresh)": {expired, true, false, true},
		"busy with neither (no refresh)":             {expired, true, false, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, fo, a := newGateServer(t, syncedCred(tc.expiresAt), nil)
			fo.setUsage401(true) // usage 401s; only a (forbidden) refresh could clear it
			if tc.busy {
				s.scanSessions = func(context.Context) ([]procscan.Session, error) { return busyOn(a.ConfigDir), nil }
			}
			if tc.streak {
				s.authStrike(a.ConfigDir, errors.New("prior 401")) // arm the busy-refresh heuristic
			}
			s.pollAccount(t.Context(), s.newTick(t.Context()), a, tc.recovery)
			if got := fo.refreshCount(); got != 0 {
				t.Fatalf("synced account refreshed %d time(s), want 0 — a synced credential must never spend a refresh token", got)
			}
		})
	}
}
