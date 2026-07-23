package daemon

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

func TestDaemonAuthRoutesCannotBypassAwaitingOriginAdmission(t *testing.T) {
	s, _ := newOutageServer(t, 1)
	accounts, err := s.m.Store.ListAccounts()
	if err != nil || len(accounts) != 1 {
		t.Fatalf("accounts = %+v err=%v", accounts, err)
	}
	account := accounts[0]
	if _, err := s.m.Store.SetNeedsLogin(
		account.ID, time.Now(), store.AuthReasonAwaitingOrigin,
		store.DigestReason("awaiting exact admission"), store.AuthKindAwaitingOrigin,
	); err != nil {
		t.Fatal(err)
	}
	s.handleAuthOutcome(t.Context(), account, nil)
	assertDaemonAwaitingOrigin(t, s, account.ID)
	s.syncAuthKind = func(context.Context, int, string) (store.AuthKind, error) {
		return store.AuthKindOwned, nil
	}
	s.flagNeedsLogin(t.Context(), account, pool.ErrNeedsLogin)
	assertDaemonAwaitingOrigin(t, s, account.ID)
}

func assertDaemonAwaitingOrigin(t *testing.T, s *Server, accountID int) {
	t.Helper()
	health, err := s.m.Store.GetAuthHealth(accountID)
	if err != nil || !health.NeedsLogin || health.Kind != store.AuthKindAwaitingOrigin ||
		health.Reason != store.AuthReasonAwaitingOrigin {
		t.Fatalf("awaiting-origin daemon fence = %+v err=%v", health, err)
	}
}

func TestRetainedNeedsLoginLiveProbeOrdering(t *testing.T) {
	for _, tc := range []struct {
		name        string
		probeErr    error
		wantOutcome sampleOutcome
	}{
		{
			name:        "usage-401",
			probeErr:    &oauth.UsageError{Status: http.StatusUnauthorized},
			wantOutcome: outcomeNonNetwork,
		},
		{
			name:        "network",
			probeErr:    errors.Join(oauth.ErrNetwork, errors.New("transport unavailable")),
			wantOutcome: outcomeNetwork,
		},
		{
			name: "usage-429",
			probeErr: &oauth.UsageError{
				Status: http.StatusTooManyRequests, RetryAfter: 37 * time.Second,
			},
			wantOutcome: outcomeNonNetwork,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newOutageServer(t, 1)
			accounts, err := s.m.Store.ListAccounts()
			if err != nil || len(accounts) != 1 {
				t.Fatalf("accounts = %+v err=%v", accounts, err)
			}
			account := accounts[0]
			err = errors.Join(
				pool.ErrNeedsLogin,
				pool.ErrCredentialOperationReplayed,
				pool.ErrCredentialOperationLiveProbe,
				tc.probeErr,
			)

			if got := classifyOutcome(err); got != tc.wantOutcome {
				t.Fatalf("classify retained %s live probe = %v, want %v", tc.name, got, tc.wantOutcome)
			}
			s.handleAuthOutcome(t.Context(), account, err)
			if ledger := s.led.peek(authStreakPolicy, account.ConfigDir); ledger != nil && (ledger.strikes != 0 || ledger.faulted) {
				t.Fatalf("retained %s live probe added a transient auth strike: %+v", tc.name, ledger)
			}
			health, healthErr := s.m.Store.GetAuthHealth(account.ID)
			if healthErr != nil || !health.NeedsLogin {
				t.Fatalf("retained %s definitive auth verdict = %+v err=%v", tc.name, health, healthErr)
			}
		})
	}
}
