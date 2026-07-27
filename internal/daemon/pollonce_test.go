package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/testhome"
)

// The daemon's Manager is fed the shared credstest seam fake; fakeOAuth is
// internally locked so -race points at code under test.
var _ pool.Credentials = (*credstest.Fake)(nil)

type fakeOAuth struct {
	mu              sync.Mutex
	currentRT       string
	refreshes       int
	refreshAttempts int
	usageCalls      int
	usage401        bool
	refresh5xx      bool

	// usageNet makes every Usage return a network-class (oauth.ErrNetwork) error.
	usageNet   bool
	usage429   bool            // every Usage returns a 429 UsageError
	rlByAT     map[string]bool // scope the 429 to specific access tokens
	retryAfter time.Duration   // Retry-After stamped onto the 429
	// netByAT overrides the response for a specific access token so a
	// multi-account sweep can diverge per account; usageByAT counts calls per
	// access token so a test can assert exactly which accounts were probed.
	netByAT   map[string]bool
	usageByAT map[string]int
}

// netError returns an error that classifies as an oauth network outage.
func netError() error { return fmt.Errorf("simulated transport failure: %w", oauth.ErrNetwork) }

func (f *fakeOAuth) Refresh(_ context.Context, _, refreshToken string) (*oauth.TokenResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refreshAttempts++
	if f.refresh5xx {
		// 503 is not confirmed invalid_grant: it is recoverable, not needs-login.
		return nil, &oauth.RefreshError{Status: 503}
	}
	if refreshToken != f.currentRT {
		return nil, &oauth.RefreshError{Status: 400, ConfirmedInvalidGrant: true}
	}
	f.refreshes++
	f.currentRT = fmt.Sprintf("rt-%d", f.refreshes)
	return &oauth.TokenResponse{
		AccessToken:  fmt.Sprintf("at-%d", f.refreshes),
		RefreshToken: f.currentRT,
		ExpiresIn:    3600,
	}, nil
}

func (f *fakeOAuth) refreshAttemptCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refreshAttempts
}

func (f *fakeOAuth) refreshCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refreshes
}

func (f *fakeOAuth) Usage(_ context.Context, at string) (*oauth.Usage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.usageCalls++
	if f.usageByAT == nil {
		f.usageByAT = map[string]int{}
	}
	f.usageByAT[at]++
	if f.usageNet || f.netByAT[at] {
		return nil, netError()
	}
	if f.usage429 || f.rlByAT[at] {
		return nil, &oauth.UsageError{Status: 429, RetryAfter: f.retryAfter}
	}
	if f.usage401 {
		return nil, &oauth.UsageError{Status: 401}
	}
	return &oauth.Usage{}, nil
}

func (f *fakeOAuth) usageCountFor(at string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.usageByAT[at]
}

func (f *fakeOAuth) usageCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.usageCalls
}

func (f *fakeOAuth) setUsage401(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.usage401 = v
}

// TestPollOnceSkipsReservedAccountRefresh pins that a reserved account (just
// selected, claude not yet scannable) is never refreshed or adopted out from
// under the launching session; an expired reservation refreshes as usual.
func TestPollOnceSkipsReservedAccountRefresh(t *testing.T) {
	// Redirect ClaudeDir/StateDir off the real ~/.claude and ~/.cc-pool.
	testhome.Sandbox(t, t.TempDir())

	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
		KeychainService: "svc", KeychainAccount: "user",
	}
	a = admitDaemonTestAccount(t, st, a)

	fk := credstest.NewFake()
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-0"
	// Near-expiry (< RefreshLeadTime) so an idle poll must refresh.
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Minute).UnixMilli()
	fk.Put(a.KeychainService, a.KeychainAccount, cred)
	seedWrites := fk.WriteCount()
	fo := &fakeOAuth{currentRT: "rt-0"}

	s := &Server{
		m:            newDaemonTestManager(t, st, fo, fk),
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		cl:           newClaims(),
		led:          newLedgers(),
	}

	s.cl.reserve(a.ID)
	s.pollOnce(t.Context())
	if got := fo.refreshCount(); got != 0 {
		t.Fatalf("reserved account was POST-refreshed %d time(s)", got)
	}
	if got := fk.WriteCount(); got != seedWrites {
		t.Fatalf("reserved account's credential was written %d time(s)", got-seedWrites)
	}

	expireCommittedReservations(s.cl, a.ID)
	s.pollOnce(t.Context())
	if got := fo.refreshCount(); got != 1 {
		t.Fatalf("idle near-expiry account refreshed %d time(s), want 1", got)
	}
}

// TestPollOnceFailsClosedOnScanError pins that a failed scan makes pollOnce treat
// every account as busy (no idle refresh, no adopt); a clean scan still refreshes.
func TestPollOnceFailsClosedOnScanError(t *testing.T) {
	setup := func(t *testing.T, scan func(context.Context) ([]procscan.Session, error)) (*Server, *fakeOAuth, *credstest.Fake) {
		t.Helper()
		testhome.Sandbox(t, t.TempDir())
		st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = st.Close() })
		a := store.Account{
			ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
			KeychainService: "svc", KeychainAccount: "user",
		}
		a = admitDaemonTestAccount(t, st, a)
		fk := credstest.NewFake()
		cred := &creds.Credential{}
		cred.ClaudeAiOauth.AccessToken = "at-0"
		cred.ClaudeAiOauth.RefreshToken = "rt-0"
		cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Minute).UnixMilli() // near-expiry
		fk.Put(a.KeychainService, a.KeychainAccount, cred)
		fo := &fakeOAuth{currentRT: "rt-0"}
		s := &Server{
			m:            newDaemonTestManager(t, st, fo, fk),
			snapshot:     filepath.Join(t.TempDir(), "status.json"),
			log:          log.New(io.Discard, "", 0),
			scanSessions: scan,
			cl:           newClaims(),
			led:          newLedgers(),
		}
		return s, fo, fk
	}

	t.Run("scan error refreshes and adopts nothing", func(t *testing.T) {
		s, fo, fk := setup(t, func(context.Context) ([]procscan.Session, error) {
			return nil, fmt.Errorf("procscan: simulated EIO")
		})
		before := fk.WriteCount()
		s.pollOnce(t.Context())
		if got := fo.refreshCount(); got != 0 {
			t.Fatalf("scan-failed poll refreshed %d time(s), want 0", got)
		}
		if got := fk.WriteCount(); got != before {
			t.Fatalf("scan-failed poll wrote the credential %d time(s) (adopt must be skipped), want 0", got-before)
		}
	})

	t.Run("clean scan refreshes the idle near-expiry account", func(t *testing.T) {
		s, fo, _ := setup(t, func(context.Context) ([]procscan.Session, error) { return nil, nil })
		s.pollOnce(t.Context())
		if got := fo.refreshCount(); got != 1 {
			t.Fatalf("clean idle poll refreshed %d time(s), want 1", got)
		}
	})
}

// TestPollOnceFlagsAndRecoversNeedsLogin pins that a confirmed revocation
// (invalid_grant on the pre-flight refresh) flags needs-login and a recovered
// credential clears it on the next due poll.
func TestPollOnceFlagsAndRecoversNeedsLogin(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
		KeychainService: "svc", KeychainAccount: "user",
	}
	a = admitDaemonTestAccount(t, st, a)
	fk := credstest.NewFake()
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-dead" // ≠ currentRT → invalid_grant, definitive
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
	fk.Put(a.KeychainService, a.KeychainAccount, cred)
	fo := &fakeOAuth{currentRT: "rt-0", usage401: true}

	s := &Server{
		m:            newDaemonTestManager(t, st, fo, fk),
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		cl:           newClaims(),
		led:          newLedgers(),
	}

	s.pollOnce(t.Context())
	if h, _ := st.GetAuthHealth(1); !h.NeedsLogin {
		t.Fatal("confirmed revocation should flag needs-login")
	}
	if got := fo.refreshCount(); got != 0 {
		t.Fatalf("dead refresh token, but %d refresh(es) succeeded", got)
	}
	// The confirmed invalid_grant also stripped the spent refresh token.
	if stored, ok := fk.Get(a.KeychainService, a.KeychainAccount); !ok || stored.HasRefreshToken() {
		t.Fatalf("stored blob = %+v, want the refresh token stripped", stored)
	}

	cred.ClaudeAiOauth.RefreshToken = "rt-0"
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
	fk.Put(a.KeychainService, a.KeychainAccount, cred)
	fo.setUsage401(false)
	// Move the throttle clock past the 15m window so the recovery poll is due.
	s.led.row(authStreakPolicy, a.ConfigDir).lastAt = time.Now().Add(-needsLoginPollInterval - time.Second)

	s.pollOnce(t.Context())
	if h, _ := st.GetAuthHealth(1); h.NeedsLogin {
		t.Fatal("a recovered account should clear needs-login")
	}
}

// TestPollOnceAbsentCredentialAuthHealth pins the fully-absent classifier at
// the scheduler boundary, including the locked-Keychain negative and recovery.
func TestPollOnceAbsentCredentialAuthHealth(t *testing.T) {
	cases := map[string]struct {
		keychainFault  error
		wantNeedsLogin bool
		wantAvailable  bool
		recover        bool
	}{
		"absent credential flags unavailable then recovers": {
			wantNeedsLogin: true,
			wantAvailable:  false,
			recover:        true,
		},
		"unavailable keychain stays selectable": {
			keychainFault: creds.ErrUnavailable,
			wantAvailable: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			testhome.Sandbox(t, t.TempDir())
			st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })

			a := store.Account{
				ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
				KeychainService: "svc", KeychainAccount: "user",
			}
			a = admitDaemonTestAccount(t, st, a)
			fk := credstest.NewFake()
			fk.KeychainFaults = credstest.Faults{Read: tc.keychainFault}
			fo := &fakeOAuth{}
			s := &Server{
				m:            newDaemonTestManager(t, st, fo, fk),
				snapshot:     filepath.Join(t.TempDir(), "status.json"),
				log:          log.New(io.Discard, "", 0),
				scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
				cl:           newClaims(),
				led:          newLedgers(),
			}

			s.pollOnce(t.Context())
			h, err := st.GetAuthHealth(a.ID)
			if err != nil {
				t.Fatal(err)
			}
			if h.NeedsLogin != tc.wantNeedsLogin {
				t.Fatalf("NeedsLogin = %v, want %v", h.NeedsLogin, tc.wantNeedsLogin)
			}
			gotAvailable := score.Score(score.Input{AccountID: a.ID, NeedsLogin: h.NeedsLogin}, time.Now()).Available
			if gotAvailable != tc.wantAvailable {
				t.Fatalf("Available = %v, want %v", gotAvailable, tc.wantAvailable)
			}

			if !tc.recover {
				return
			}
			cred := &creds.Credential{}
			cred.ClaudeAiOauth.AccessToken = "at-clean"
			cred.ClaudeAiOauth.RefreshToken = "rt-clean"
			cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
			fk.Put(a.KeychainService, a.KeychainAccount, cred)
			s.led.row(authStreakPolicy, a.ConfigDir).lastAt = time.Now().Add(-needsLoginPollInterval - time.Second)

			s.pollOnce(t.Context())
			h, err = st.GetAuthHealth(a.ID)
			if err != nil {
				t.Fatal(err)
			}
			if h.NeedsLogin {
				t.Fatal("clean credential poll did not clear needs-login")
			}
			if !score.Score(score.Input{AccountID: a.ID, NeedsLogin: h.NeedsLogin}, time.Now()).Available {
				t.Fatal("recovered account remains unavailable")
			}
		})
	}
}

// TestPollOnceTransient401StaysSelectable pins that one durable transient
// refresh failure cannot be replayed into multiple auth strikes or needs-login.
func TestPollOnceTransient401StaysSelectable(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
		KeychainService: "svc", KeychainAccount: "user",
	}
	a = admitDaemonTestAccount(t, st, a)
	fk := credstest.NewFake()
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-0" // present → a 401 is recoverable, not definitive
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
	fk.Put(a.KeychainService, a.KeychainAccount, cred)
	fo := &fakeOAuth{currentRT: "rt-0", usage401: true, refresh5xx: true}

	s := &Server{
		m:            newDaemonTestManager(t, st, fo, fk),
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		cl:           newClaims(),
		led:          newLedgers(),
	}

	s.pollOnce(t.Context())
	before := fo.usageCallCount()
	refreshesBefore := fo.refreshAttemptCount()
	for range needsLoginAfter + 1 {
		s.pollOnce(t.Context())
	}
	if l := s.led.peek(authStreakPolicy, a.ConfigDir); l != nil {
		t.Fatalf("durably replayed transient failure booked auth evidence: %+v", l)
	}
	if got := fo.usageCallCount() - before; got != needsLoginAfter+1 {
		t.Fatalf("retained failure read-only Usage calls = %d, want %d", got, needsLoginAfter+1)
	}
	if got := fo.refreshAttemptCount(); got != refreshesBefore {
		t.Fatalf("durably replayed failure repeated refresh %d time(s)", got-refreshesBefore)
	}
	if h, _ := st.GetAuthHealth(1); h.NeedsLogin {
		t.Fatal("transient failure replay must not flag needs-login")
	} else if !score.Score(score.Input{AccountID: 1, NeedsLogin: h.NeedsLogin}, time.Now()).Available {
		t.Fatal("transient failure replay made the account unselectable")
	}
}

func TestClassifyOutcomeRetainedEvidenceIsNotAProbe(t *testing.T) {
	if got := classifyOutcome(errors.Join(pool.ErrCredentialOperationReplayed, oauth.ErrNetwork)); got != outcomeNoProbe {
		t.Fatalf("replayed network outcome = %v, want no-probe", got)
	}
	if got := classifyOutcome(errors.Join(
		pool.ErrCredentialOperationReplayed,
		&oauth.RefreshError{Status: http.StatusServiceUnavailable},
	)); got != outcomeNoProbe {
		t.Fatalf("replayed server outcome = %v, want no-probe", got)
	}
	if got := classifyOutcome(oauth.ErrNetwork); got != outcomeNetwork {
		t.Fatalf("live network outcome = %v, want network", got)
	}
	if got := classifyOutcome(errors.Join(
		pool.ErrCredentialOperationReplayed,
		pool.ErrCredentialOperationLiveProbe,
		oauth.ErrNetwork,
	)); got != outcomeNetwork {
		t.Fatalf("retained evidence with live network probe = %v, want network", got)
	}
	if got := classifyOutcome(errors.Join(
		pool.ErrCredentialOperationReplayed,
		pool.ErrCredentialOperationLiveProbe,
		&oauth.UsageError{Status: http.StatusUnauthorized},
	)); got != outcomeNonNetwork {
		t.Fatalf("retained evidence with live response = %v, want nonnetwork", got)
	}
}

// TestPollOnceOutageProbesAfterRetainedFailure pins that durable failure
// evidence cannot keep outage mode alive without network I/O. The scheduler
// follows a replay-only credential result with one read-only usage probe.
func TestPollOnceOutageProbesAfterRetainedFailure(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	fk := credstest.NewFake()
	first := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct-1"),
		KeychainService: "svc-1", KeychainAccount: "user",
	}
	first = admitDaemonTestAccount(t, st, first)
	expired := &creds.Credential{}
	expired.ClaudeAiOauth.AccessToken = "at-1"
	expired.ClaudeAiOauth.RefreshToken = "rt-1"
	expired.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
	fk.Put(first.KeychainService, first.KeychainAccount, expired)

	fo := &fakeOAuth{currentRT: "rt-1", usage401: true, refresh5xx: true}
	s := &Server{
		m:            newDaemonTestManager(t, st, fo, fk),
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		pollSpacing:  time.Millisecond,
		cl:           newClaims(),
		led:          newLedgers(),
	}

	// Settle account 1's refresh-server failure into durable evidence.
	s.pollOnce(t.Context())
	before := fo.usageCountFor("at-1")

	second := store.Account{
		ID: 2, ConfigDir: filepath.Join(t.TempDir(), "acct-2"),
		KeychainService: "svc-2", KeychainAccount: "user",
	}
	second = admitDaemonTestAccount(t, st, second)
	live := &creds.Credential{}
	live.ClaudeAiOauth.AccessToken = "at-2"
	live.ClaudeAiOauth.RefreshToken = "rt-2"
	live.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
	fk.Put(second.KeychainService, second.KeychainAccount, live)
	fo.usage401 = false
	fo.refresh5xx = false
	s.netOutage = true

	s.pollOnce(t.Context())

	if s.netOutage {
		t.Fatal("a live fallback probe did not clear outage mode")
	}
	if got := fo.usageCountFor("at-1"); got != before+1 {
		t.Fatalf("retained failure fallback performed %d new Usage call(s), want 1", got-before)
	}
	if got := fo.usageCountFor("at-2"); got != 1 {
		t.Fatalf("live second canary sampled %d time(s), want 1", got)
	}
}

func TestPollOnceRestartProbesWhenAllCredentialResultsRetained(t *testing.T) {
	for _, test := range []struct {
		name        string
		networkDown bool
	}{
		{name: "reachable"},
		{name: "network outage", networkDown: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			testhome.Sandbox(t, t.TempDir())
			databasePath := filepath.Join(t.TempDir(), "pool-v1.db")
			st, err := store.Open(databasePath)
			if err != nil {
				t.Fatal(err)
			}

			fk := credstest.NewFake()
			fo := &fakeOAuth{}
			manager := newDaemonTestManager(t, st, fo, fk)
			for id := 1; id <= 2; id++ {
				account := store.Account{
					ID: id, ConfigDir: filepath.Join(t.TempDir(), fmt.Sprintf("acct-%d", id)),
					KeychainService: fmt.Sprintf("svc-%d", id), KeychainAccount: "user",
				}
				admitDaemonTestAccount(t, st, account)
				account, err = st.GetAccount(id)
				if err != nil {
					t.Fatal(err)
				}
				credential := &creds.Credential{}
				credential.ClaudeAiOauth.AccessToken = fmt.Sprintf("at-%d", id)
				credential.ClaudeAiOauth.RefreshToken = fmt.Sprintf("rt-%d", id)
				credential.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
				fk.Put(account.KeychainService, account.KeychainAccount, credential)
				observation, err := manager.CredentialExternalState(t.Context(), account)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := st.QuarantineCredential(store.QuarantineCredentialRequest{
					AccountID: account.ID, AccountInstanceID: account.InstanceID,
					AccountGeneration: account.Generation,
					LocatorDigest: store.CredentialKeychainLocatorDigest(
						account.KeychainService, account.KeychainAccount,
					),
					Observation:  observation,
					Reason:       store.CredentialResultAmbiguous,
					FailureClass: store.CredentialFailureInternal,
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}

			st, err = store.Open(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			fo.usageNet = test.networkDown
			snapshotPath := filepath.Join(t.TempDir(), "status.json")
			s := &Server{
				m:            newDaemonTestManager(t, st, fo, fk),
				snapshot:     snapshotPath,
				log:          log.New(io.Discard, "", 0),
				scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
				pollSpacing:  time.Millisecond,
				cl:           newClaims(),
				led:          newLedgers(),
			}

			s.pollOnce(t.Context())

			if got := fo.usageCallCount(); got != 1 {
				t.Fatalf("restart fallback Usage calls = %d, want exactly 1", got)
			}
			if got := fo.usageCountFor("at-1"); got != 1 {
				t.Fatalf("first retained account probes = %d, want 1", got)
			}
			if got := fo.usageCountFor("at-2"); got != 0 {
				t.Fatalf("second retained account probes = %d, want 0", got)
			}
			for id := 1; id <= 2; id++ {
				if _, err := st.CredentialQuarantine(id); err != nil {
					t.Fatalf("account %d quarantine after read-only probe: %v", id, err)
				}
			}
			if s.netOutage != test.networkDown {
				t.Fatalf("netOutage = %t, want %t", s.netOutage, test.networkDown)
			}
			_, statErr := os.Stat(snapshotPath)
			if test.networkDown {
				if !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("outage restart snapshot stat = %v, want absent", statErr)
				}
			} else if statErr != nil {
				t.Fatalf("reachable restart snapshot stat: %v", statErr)
			}
		})
	}
}

// TestPollOnceFlagsConfirmedRevocation pins that a confirmed revocation (400
// invalid_grant, credential unchanged on re-read) flags needs-login on the
// first poll, independent of the transient-401 streak.
func TestPollOnceFlagsConfirmedRevocation(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
		KeychainService: "svc", KeychainAccount: "user",
	}
	a = admitDaemonTestAccount(t, st, a)
	fk := credstest.NewFake()
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-stale" // ≠ server's currentRT → invalid_grant
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
	fk.Put(a.KeychainService, a.KeychainAccount, cred)
	fo := &fakeOAuth{currentRT: "rt-0", usage401: true}

	s := &Server{
		m:            newDaemonTestManager(t, st, fo, fk),
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		cl:           newClaims(),
		led:          newLedgers(),
	}

	s.pollOnce(t.Context())
	if h, _ := st.GetAuthHealth(1); !h.NeedsLogin {
		t.Fatal("a confirmed 400 invalid_grant revocation must flag needs-login")
	}
	if l := s.led.peek(authStreakPolicy, a.ConfigDir); l != nil && (l.strikes != 0 || l.faulted) {
		t.Fatalf("a confirmed revocation flags via ErrNeedsLogin and must not touch the 401 streak; strikes=%d faulted=%v", l.strikes, l.faulted)
	}
}

// newOutageServer builds a daemon Server with n idle accounts, each holding a
// distinct, unexpired credential (access token at-<i>), and a nil session scan
// so every account reads idle. The shared fakeOAuth drives per-account Usage
// behavior via its usageNet/netByAT fields.
func newOutageServer(t *testing.T, n int) (*Server, *fakeOAuth) {
	t.Helper()
	testhome.Sandbox(t, t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	fk := credstest.NewFake()
	for i := 1; i <= n; i++ {
		a := store.Account{
			ID: i, ConfigDir: filepath.Join(t.TempDir(), fmt.Sprintf("acct-%d", i)),
			KeychainService: fmt.Sprintf("svc-%d", i), KeychainAccount: "user",
		}
		a = admitDaemonTestAccount(t, st, a)
		cred := &creds.Credential{}
		cred.ClaudeAiOauth.AccessToken = fmt.Sprintf("at-%d", i)
		cred.ClaudeAiOauth.RefreshToken = fmt.Sprintf("rt-%d", i)
		cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
		fk.Put(a.KeychainService, a.KeychainAccount, cred)
	}
	fo := &fakeOAuth{currentRT: "rt-0"}
	s := &Server{
		m:            newDaemonTestManager(t, st, fo, fk),
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		pollSpacing:  time.Millisecond, // keep multi-account sweeps out of real-time sleeps
		cl:           newClaims(),
		led:          newLedgers(),
	}
	return s, fo
}

// TestPollOnceEntersNetOutage pins that a full sweep whose every attempted
// account fails network-class enters outage mode, and that the network arm of
// handleAuthOutcome leaves every auth counter untouched.
func TestPollOnceEntersNetOutage(t *testing.T) {
	s, fo := newOutageServer(t, 1)
	fo.usageNet = true

	s.pollOnce(t.Context())

	if !s.netOutage {
		t.Fatal("an all-network-fail sweep must enter outage mode")
	}
	accts, err := s.m.Store.ListAccounts()
	if err != nil {
		t.Fatal(err)
	}
	dir := accts[0].ConfigDir
	if l := s.led.peek(authStreakPolicy, dir); l != nil {
		t.Fatalf("a network error must not touch the auth streak: %+v", l)
	}
	if l := s.led.peek(acctRateLimitPolicy, dir); l != nil {
		t.Fatalf("a network error must not arm the per-account 429 streak: %+v", l)
	}
	if h, _ := s.m.Store.GetAuthHealth(1); h.NeedsLogin {
		t.Fatal("a network error must not flag needs-login")
	}
}

// TestPollOnceMixedSweepStaysHealthy pins that a single reachable account keeps
// the fleet out of outage mode even when others fail network-class.
func TestPollOnceMixedSweepStaysHealthy(t *testing.T) {
	s, fo := newOutageServer(t, 2)
	fo.netByAT = map[string]bool{"at-1": true} // acct1 fails, acct2 succeeds

	s.pollOnce(t.Context())

	if s.netOutage {
		t.Fatal("a mixed sweep (one account reachable) must NOT enter outage mode")
	}
}

// TestPollOnceOutageProbesOnlyCanary pins that, while in outage mode, pollOnce
// samples only the first pollable account (the canary); a still-failing canary
// keeps the outage and never fans out to the rest.
func TestPollOnceOutageProbesOnlyCanary(t *testing.T) {
	s, fo := newOutageServer(t, 2)
	fo.usageNet = true // the network is still down
	s.netOutage = true

	s.pollOnce(t.Context())

	if !s.netOutage {
		t.Fatal("a still-failing canary must keep outage mode")
	}
	if got := fo.usageCountFor("at-1"); got != 1 {
		t.Fatalf("canary (acct1) sampled %d time(s), want 1", got)
	}
	if got := fo.usageCountFor("at-2"); got != 0 {
		t.Fatalf("non-canary (acct2) sampled %d time(s), want 0 — outage polls only the canary", got)
	}
}

// TestPollOnceCanaryRecoveryRunsFullSweep pins that a canary answering anything
// non-network exits outage mode and re-samples the whole fleet in the same
// invocation.
func TestPollOnceCanaryRecoveryRunsFullSweep(t *testing.T) {
	s, fo := newOutageServer(t, 2) // both accounts answer now
	s.netOutage = true

	s.pollOnce(t.Context())

	if s.netOutage {
		t.Fatal("a recovered canary must exit outage mode")
	}
	if got := fo.usageCountFor("at-1"); got != 1 {
		t.Fatalf("canary sampled %d time(s), want 1", got)
	}
	if got := fo.usageCountFor("at-2"); got != 1 {
		t.Fatalf("acct2 sampled %d time(s), want 1 — the recovery sweep must re-reach previously-skipped accounts", got)
	}
}

// TestPollOnceRecoverySweepReentersOutage pins that a recovery sweep whose
// post-canary accounts all fail network-class (connectivity dropped again after
// the canary reached the API) re-enters outage, and abandons the sweep early once
// recoveryAbandonThreshold consecutive failures accumulate so the trailing
// accounts aren't each charged a sample timeout.
func TestPollOnceRecoverySweepReentersOutage(t *testing.T) {
	s, fo := newOutageServer(t, 5)
	// The canary (acct1) reaches the API and flips recovery; every other account
	// then fails network-class.
	fo.netByAT = map[string]bool{"at-2": true, "at-3": true, "at-4": true, "at-5": true}
	s.netOutage = true

	s.pollOnce(t.Context())

	if !s.netOutage {
		t.Fatal("a recovery sweep that all-network-fails must re-enter outage mode")
	}
	if got := fo.usageCountFor("at-1"); got != 1 {
		t.Fatalf("canary sampled %d time(s), want 1", got)
	}
	for _, at := range []string{"at-2", "at-3", "at-4"} {
		if got := fo.usageCountFor(at); got != 1 {
			t.Fatalf("%s sampled %d time(s), want 1 (recovery samples before abandon)", at, got)
		}
	}
	if got := fo.usageCountFor("at-5"); got != 0 {
		t.Fatalf("acct5 sampled %d time(s), want 0 — the recovery sweep must abandon after %d consecutive network failures", got, recoveryAbandonThreshold)
	}
}

// TestPollOnceOutageLogThrottle pins that, while an outage persists, the
// per-canary "network unreachable" line logs only once per netProbeLogEvery
// probes so a multi-hour outage doesn't spam the log.
func TestPollOnceOutageLogThrottle(t *testing.T) {
	s, fo := newOutageServer(t, 1)
	fo.usageNet = true
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)
	s.netOutage = true

	// Each outage poll runs exactly one canary probe; logs fire on probes
	// 1, netProbeLogEvery+1, 2*netProbeLogEvery+1, ...
	const polls = netProbeLogEvery*2 + 1
	for i := 0; i < polls; i++ {
		s.pollOnce(t.Context())
	}
	if !s.netOutage {
		t.Fatal("a still-down canary must keep outage mode")
	}
	if got, want := strings.Count(buf.String(), "network unreachable"), 3; got != want {
		t.Fatalf("network-unreachable lines = %d over %d probes, want %d (throttled 1-per-%d)", got, polls, want, netProbeLogEvery)
	}
}

// TestPollOnceRecoverySweepHealsBusy401 pins that the post-outage recovery sweep
// bypasses the busy-refresh streak gate (a busy account whose token expired
// during the outage refreshes same-pass), while a normal first-401 busy account
// still does not refresh (the existing heuristic gate is preserved).
func TestPollOnceRecoverySweepHealsBusy401(t *testing.T) {
	newBusy := func(t *testing.T) (*Server, *fakeOAuth) {
		t.Helper()
		testhome.Sandbox(t, t.TempDir())
		st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = st.Close() })
		a := store.Account{
			ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
			KeychainService: "svc", KeychainAccount: "user",
		}
		a = admitDaemonTestAccount(t, st, a)
		fk := credstest.NewFake()
		cred := &creds.Credential{}
		cred.ClaudeAiOauth.AccessToken = "at-0"
		cred.ClaudeAiOauth.RefreshToken = "rt-0"
		cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli() // expired
		fk.Put(a.KeychainService, a.KeychainAccount, cred)
		fo := &fakeOAuth{currentRT: "rt-0", usage401: true}
		s := &Server{
			m:        newDaemonTestManager(t, st, fo, fk),
			snapshot: filepath.Join(t.TempDir(), "status.json"),
			log:      log.New(io.Discard, "", 0),
			scanSessions: func(context.Context) ([]procscan.Session, error) { // a live session → busy
				return []procscan.Session{{PID: 4242, ConfigDir: a.ConfigDir, StartedAt: time.Now()}}, nil
			},
			cl:  newClaims(),
			led: newLedgers(),
		}
		return s, fo
	}

	t.Run("recovery sweep refreshes a busy expired token", func(t *testing.T) {
		s, fo := newBusy(t)
		s.netOutage = true // the canary probe runs under recovery semantics

		s.pollOnce(t.Context())

		if got := fo.refreshCount(); got != 1 {
			t.Fatalf("recovery sweep refreshed %d time(s), want 1 (the busy-refresh streak gate must be bypassed)", got)
		}
		if s.netOutage {
			t.Fatal("the canary's 401 answer proves connectivity and must exit outage")
		}
	})

	t.Run("normal poll leaves a busy first-401 unrefreshed", func(t *testing.T) {
		s, fo := newBusy(t)

		s.pollOnce(t.Context())

		if got := fo.refreshCount(); got != 0 {
			t.Fatalf("a normal first-401 busy account refreshed %d time(s), want 0 (streak gate preserved)", got)
		}
		accts, err := s.m.Store.ListAccounts()
		if err != nil {
			t.Fatal(err)
		}
		if l := s.led.peek(authStreakPolicy, accts[0].ConfigDir); l == nil || l.strikes != 1 || l.faulted {
			t.Fatalf("auth streak = %+v, want strikes 1 (a busy 401 arms the streak)", l)
		}
	})
}
