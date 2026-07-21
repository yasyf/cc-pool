package daemon

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/pool"
)

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
