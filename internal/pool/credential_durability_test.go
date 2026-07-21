package pool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
)

func TestCredentialQuarantineRequiresTokenChainReplacement(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-quarantine-chain",
		KeychainAccount: "account-quarantine-chain",
	})
	credentials := credstest.NewFake()
	original := cred401("at-original", "rt-original", time.Now().Add(time.Hour))
	original.ClaudeAiOauth.SubscriptionType = "max"
	credentials.Put(account.KeychainService, account.KeychainAccount, original)
	manager := credentialRecoveryManager(t, st, credentials, "quarantine-chain-owner")

	observation, err := manager.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	filePath := creds.FileCredentialPath(account.ConfigDir)
	quarantine, err := st.QuarantineCredential(store.QuarantineCredentialRequest{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation,
		LocatorDigest: store.CredentialLocatorDigest(
			account.KeychainService, account.KeychainAccount, filePath,
		),
		FileLocatorDigest: store.CredentialFileLocatorDigest(filePath),
		Observation:       observation,
		Reason:            store.CredentialResultAmbiguous,
		FailureClass:      store.CredentialFailureInternal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if quarantine.TokenChainDigest != nil {
		t.Fatal("new quarantine unexpectedly had a token-chain binding")
	}

	if _, err := manager.credentialMutationObservation(t.Context(), account); !errors.Is(err, ErrCredentialOperationQuarantined) {
		t.Fatalf("initial quarantine observation = %v, want quarantine", err)
	}
	bound, err := st.CredentialQuarantine(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest := credentialTokenChainDigest(original)
	if bound.TokenChainDigest == nil || *bound.TokenChainDigest != wantDigest {
		t.Fatalf("token-chain binding = %v, want %x", bound.TokenChainDigest, wantDigest)
	}

	metadataOnly := *original
	metadataOnly.ClaudeAiOauth.ExpiresAt = time.Now().Add(4 * time.Hour).UnixMilli()
	metadataOnly.ClaudeAiOauth.SubscriptionType = "team"
	credentials.Put(account.KeychainService, account.KeychainAccount, &metadataOnly)
	if _, err := manager.credentialMutationObservation(t.Context(), account); !errors.Is(err, ErrCredentialOperationQuarantined) {
		t.Fatalf("metadata-only change = %v, want quarantine retained", err)
	}
	assertCredentialQuarantineDigest(t, st, account.ID, wantDigest)

	if err := credstest.FileStore(account.ConfigDir).Write(t.Context(), &metadataOnly); err != nil {
		t.Fatal(err)
	}
	credentials.Remove(account.KeychainService, account.KeychainAccount)
	if _, err := manager.credentialMutationObservation(t.Context(), account); !errors.Is(err, ErrCredentialOperationQuarantined) {
		t.Fatalf("source-only change = %v, want quarantine retained", err)
	}
	assertCredentialQuarantineDigest(t, st, account.ID, wantDigest)

	replacement := metadataOnly
	replacement.ClaudeAiOauth.AccessToken = "at-replacement"
	replacement.ClaudeAiOauth.RefreshToken = "rt-replacement"
	if err := credstest.FileStore(account.ConfigDir).Write(t.Context(), &replacement); err != nil {
		t.Fatal(err)
	}

	const resolvers = 8
	errCh := make(chan error, resolvers)
	var wait sync.WaitGroup
	for range resolvers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			resolved, err := manager.credentialMutationObservation(t.Context(), account)
			if err == nil && !credentialStateReadable(resolved) {
				err = errors.New("resolved observation is not readable")
			}
			errCh <- err
		}()
	}
	wait.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent quarantine resolution: %v", err)
		}
	}
	if _, err := st.CredentialQuarantine(account.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("quarantine after token-chain replacement = %v", err)
	}
}

func TestCredentialQuarantineTracksNormalizedTokenChainSet(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *credstest.Fake, store.Account, *creds.Credential)
	}{
		{
			name: "remove one chain",
			mutate: func(_ *testing.T, credentials *credstest.Fake, account store.Account, _ *creds.Credential) {
				credentials.Remove(account.KeychainService, account.KeychainAccount)
			},
		},
		{
			name: "replace one chain",
			mutate: func(t *testing.T, _ *credstest.Fake, account store.Account, fileCredential *creds.Credential) {
				replacement := *fileCredential
				replacement.ClaudeAiOauth.AccessToken = "at-file-replacement"
				replacement.ClaudeAiOauth.RefreshToken = "rt-file-replacement"
				if err := credstest.FileStore(account.ConfigDir).Write(t.Context(), &replacement); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := openTestStore(t)
			account := persistTestAccount(t, st, store.Account{
				ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-quarantine-set",
				KeychainAccount: "account-quarantine-set",
			})
			credentials := credstest.NewFake()
			keychainCredential := cred401("at-keychain", "rt-keychain", time.Now().Add(4*time.Hour))
			keychainCredential.ClaudeAiOauth.SubscriptionType = "max"
			fileCredential := cred401("at-file", "rt-file", time.Now().Add(2*time.Hour))
			fileCredential.ClaudeAiOauth.SubscriptionType = "team"
			credentials.Put(account.KeychainService, account.KeychainAccount, keychainCredential)
			if err := credstest.FileStore(account.ConfigDir).Write(t.Context(), fileCredential); err != nil {
				t.Fatal(err)
			}
			manager := credentialRecoveryManager(t, st, credentials, "quarantine-set-owner")

			winner, source, err := manager.ReadCredential(t.Context(), account)
			if err != nil || source != creds.SourceKeychain ||
				winner.ClaudeAiOauth.RefreshToken != "rt-keychain" {
				t.Fatalf("initial winner = credential=%+v source=%v err=%v", winner, source, err)
			}
			observation, err := manager.credentialObservation(t.Context(), account)
			if err != nil {
				t.Fatal(err)
			}
			filePath := creds.FileCredentialPath(account.ConfigDir)
			if _, err := st.QuarantineCredential(store.QuarantineCredentialRequest{
				AccountID: account.ID, AccountInstanceID: account.InstanceID,
				AccountGeneration: account.Generation,
				LocatorDigest: store.CredentialLocatorDigest(
					account.KeychainService, account.KeychainAccount, filePath,
				),
				FileLocatorDigest: store.CredentialFileLocatorDigest(filePath),
				Observation:       observation,
				Reason:            store.CredentialResultAmbiguous,
				FailureClass:      store.CredentialFailureInternal,
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := manager.credentialMutationObservation(t.Context(), account); !errors.Is(err, ErrCredentialOperationQuarantined) {
				t.Fatalf("bind normalized token set = %v, want quarantine", err)
			}
			bound, err := st.CredentialQuarantine(account.ID)
			if err != nil || bound.TokenChainDigest == nil {
				t.Fatalf("bound normalized token set = %+v err=%v", bound, err)
			}

			keychainMetadata := *keychainCredential
			keychainMetadata.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
			keychainMetadata.ClaudeAiOauth.SubscriptionType = "enterprise"
			credentials.Put(account.KeychainService, account.KeychainAccount, &keychainMetadata)
			fileMetadata := *fileCredential
			fileMetadata.ClaudeAiOauth.ExpiresAt = time.Now().Add(5 * time.Hour).UnixMilli()
			fileMetadata.ClaudeAiOauth.SubscriptionType = "max"
			if err := credstest.FileStore(account.ConfigDir).Write(t.Context(), &fileMetadata); err != nil {
				t.Fatal(err)
			}
			winner, source, err = manager.ReadCredential(t.Context(), account)
			if err != nil || source != creds.SourceFile || winner.ClaudeAiOauth.RefreshToken != "rt-file" {
				t.Fatalf("metadata-flipped winner = credential=%+v source=%v err=%v", winner, source, err)
			}
			if _, err := manager.credentialMutationObservation(t.Context(), account); !errors.Is(err, ErrCredentialOperationQuarantined) {
				t.Fatalf("winner-only metadata change = %v, want quarantine retained", err)
			}
			retained, err := st.CredentialQuarantine(account.ID)
			if err != nil || retained.TokenChainDigest == nil ||
				*retained.TokenChainDigest != *bound.TokenChainDigest {
				t.Fatalf("normalized token set drifted: before=%+v after=%+v err=%v", bound, retained, err)
			}

			tc.mutate(t, credentials, account, &fileMetadata)
			resolved, err := manager.credentialMutationObservation(t.Context(), account)
			if err != nil || !credentialStateReadable(resolved) {
				t.Fatalf("token-set mutation resolution = %+v err=%v", resolved, err)
			}
			if _, err := st.CredentialQuarantine(account.ID); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("quarantine after token-set mutation = %v", err)
			}
		})
	}
}

func assertCredentialQuarantineDigest(
	t *testing.T,
	st *store.Store,
	accountID int,
	want store.CredentialDigest,
) {
	t.Helper()
	quarantine, err := st.CredentialQuarantine(accountID)
	if err != nil {
		t.Fatal(err)
	}
	if quarantine.TokenChainDigest == nil || *quarantine.TokenChainDigest != want {
		t.Fatalf("quarantine token-chain digest = %v, want %x", quarantine.TokenChainDigest, want)
	}
}

type retainedRefreshFailureOAuth struct {
	mu            sync.Mutex
	refreshErr    error
	refreshTokens []string
	usageTokens   []string
}

func (f *retainedRefreshFailureOAuth) Refresh(
	_ context.Context,
	_, refreshToken string,
) (*oauth.TokenResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refreshTokens = append(f.refreshTokens, refreshToken)
	return nil, f.refreshErr
}

func (f *retainedRefreshFailureOAuth) Usage(
	_ context.Context,
	accessToken string,
) (*oauth.Usage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.usageTokens = append(f.usageTokens, accessToken)
	return nil, &oauth.UsageError{Status: http.StatusUnauthorized}
}

func (f *retainedRefreshFailureOAuth) counts() (refresh []string, usage []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.refreshTokens...), append([]string(nil), f.usageTokens...)
}

func TestSampleUsageNeverRepeatsRetainedFailedRefresh(t *testing.T) {
	for _, tc := range []struct {
		name           string
		err            error
		wantQuarantine bool
	}{
		{
			name:           "network",
			err:            errors.Join(oauth.ErrNetwork, context.DeadlineExceeded),
			wantQuarantine: true,
		},
		{name: "server", err: &oauth.RefreshError{Status: http.StatusServiceUnavailable}, wantQuarantine: true},
		{name: "plain-401", err: &oauth.RefreshError{Status: http.StatusUnauthorized}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			credentials := credstest.NewFake()
			oauthClient := &retainedRefreshFailureOAuth{refreshErr: tc.err}
			manager, account := newHealManager(t, credentials, oauthClient)
			credentials.Put(
				account.KeychainService,
				account.KeychainAccount,
				cred401("at-stale", "rt-single-use", time.Now().Add(-time.Hour)),
			)
			expected, err := manager.credentialMutationObservation(t.Context(), account)
			if err != nil {
				t.Fatal(err)
			}
			operationID, err := store.NewCredentialOperationID(
				account.InstanceID, account.Generation,
				store.CredentialOperationEnsureFresh, store.CredentialTargetAll,
				store.CredentialLocatorDigest(
					account.KeychainService, account.KeychainAccount,
					creds.FileCredentialPath(account.ConfigDir),
				),
				expected,
				credentialIntentDigest(
					store.CredentialOperationEnsureFresh, RefreshLeadTime.String(), "true",
				),
			)
			if err != nil {
				t.Fatal(err)
			}

			_, _, _, firstErr := manager.SampleUsage(
				t.Context(), account, SampleOpts{AllowRefresh: true},
			)
			if firstErr == nil {
				t.Fatal("first SampleUsage unexpectedly succeeded")
			}
			refreshes, usage := oauthClient.counts()
			if len(refreshes) != 1 || refreshes[0] != "rt-single-use" {
				t.Fatalf("first SampleUsage refresh tokens = %q, want one single-use token", refreshes)
			}
			if len(usage) != 1 || usage[0] != "at-stale" {
				t.Fatalf("first SampleUsage usage tokens = %q, want one read-only probe", usage)
			}
			receipt, err := manager.Store.CredentialOperationReceiptByID(operationID)
			if err != nil {
				t.Fatalf("refresh receipt after %v: %v", firstErr, err)
			}
			wantTerminal := store.CredentialTerminalFailed
			if tc.wantQuarantine {
				wantTerminal = store.CredentialTerminalQuarantined
			}
			if receipt.TerminalStatus != wantTerminal {
				t.Fatalf("refresh receipt terminal = %q, want %q: %+v", receipt.TerminalStatus, wantTerminal, receipt)
			}
			_, quarantineErr := manager.Store.CredentialQuarantine(account.ID)
			if tc.wantQuarantine != (quarantineErr == nil) {
				t.Fatalf("credential quarantine err = %v, want present=%t", quarantineErr, tc.wantQuarantine)
			}

			replayCtx, replayCancel := context.WithTimeout(t.Context(), 2*time.Second)
			_, _, _, replayErr := manager.SampleUsage(
				replayCtx, account, SampleOpts{AllowRefresh: true},
			)
			replayCancel()
			if replayErr == nil {
				t.Fatal("retained-receipt SampleUsage unexpectedly succeeded")
			}
			if errors.Is(replayErr, context.DeadlineExceeded) {
				t.Fatalf("retained-receipt SampleUsage waited instead of replaying: %v", replayErr)
			}
			wantReplayErr := ErrCredentialOperationFailed
			if tc.wantQuarantine {
				wantReplayErr = ErrCredentialOperationQuarantined
			}
			if !errors.Is(replayErr, wantReplayErr) {
				t.Fatalf("retained-receipt SampleUsage = %v, want %v", replayErr, wantReplayErr)
			}
			refreshes, usage = oauthClient.counts()
			if len(refreshes) != 1 || refreshes[0] != "rt-single-use" {
				t.Fatalf("retained receipt re-POSTed refresh tokens: %q", refreshes)
			}
			wantUsageCalls := 2
			if tc.wantQuarantine {
				wantUsageCalls = 1
			}
			if len(usage) != wantUsageCalls {
				t.Fatalf("retained receipt usage tokens = %q, want %d call(s)", usage, wantUsageCalls)
			}
			for _, token := range usage {
				if token != "at-stale" {
					t.Fatalf("retained receipt usage token = %q, want at-stale", token)
				}
			}
		})
	}
}

type needsLoginReplayOAuth struct {
	mu         sync.Mutex
	usage      string
	retryAfter time.Duration
	refreshes  int
	usageCalls int
}

func (f *needsLoginReplayOAuth) Refresh(
	context.Context,
	string,
	string,
) (*oauth.TokenResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refreshes++
	return nil, &oauth.RefreshError{Status: http.StatusBadRequest, ConfirmedInvalidGrant: true}
}

func (f *needsLoginReplayOAuth) Usage(context.Context, string) (*oauth.Usage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.usageCalls++
	switch f.usage {
	case "401":
		return nil, &oauth.UsageError{Status: http.StatusUnauthorized}
	case "network":
		return nil, errors.Join(oauth.ErrNetwork, context.DeadlineExceeded)
	case "429":
		return nil, &oauth.UsageError{
			Status: http.StatusTooManyRequests, RetryAfter: f.retryAfter,
		}
	default:
		return nil, fmt.Errorf("unknown usage outcome %q", f.usage)
	}
}

func (f *needsLoginReplayOAuth) counts() (refreshes, usage int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refreshes, f.usageCalls
}

func TestRetainedNeedsLoginReplayPreservesLiveProbeOutcome(t *testing.T) {
	for _, tc := range []struct {
		name            string
		usage           string
		retryAfter      time.Duration
		wantRateLimited bool
		wantNetwork     bool
		wantStatus      int
	}{
		{name: "usage-401", usage: "401", wantStatus: http.StatusUnauthorized},
		{name: "network", usage: "network", wantNetwork: true},
		{
			name: "usage-429", usage: "429", retryAfter: 37 * time.Second,
			wantRateLimited: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := openTestStore(t)
			account := persistTestAccount(t, st, store.Account{
				ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-needs-login-replay",
				KeychainAccount: "account-needs-login-replay",
			})
			credentials := credstest.NewFake()
			oauthClient := &needsLoginReplayOAuth{
				usage: tc.usage, retryAfter: tc.retryAfter,
			}
			manager := credentialRecoveryManager(t, st, credentials, "needs-login-replay-owner")
			manager.OAuth = oauthClient
			retainedCredential := cred401(
				"at-retained", "", time.Now().Add(time.Hour),
			)
			credentials.Put(
				account.KeychainService,
				account.KeychainAccount,
				retainedCredential,
			)
			before, err := manager.credentialObservation(t.Context(), account)
			if err != nil {
				t.Fatal(err)
			}
			operation := beginCredentialOperation(
				t,
				manager,
				account,
				store.CredentialOperationEnsureFresh,
				store.CredentialTargetAll,
				credentialIntentDigest(
					store.CredentialOperationEnsureFresh,
					RefreshLeadTime.String(),
					fmt.Sprintf("%t", true),
				),
				before,
			)
			if _, err := manager.Store.CommitPreparedCredentialOperation(
				operation.Fence(),
				before,
				store.CredentialResultNeedsLogin,
				time.Now().Add(time.Minute),
			); err != nil {
				t.Fatal(err)
			}

			_, rateLimited, retryAfter, err := manager.SampleUsage(
				t.Context(), account, SampleOpts{AllowRefresh: true},
			)
			for name, marker := range map[string]error{
				"needs-login": ErrNeedsLogin,
				"replayed":    ErrCredentialOperationReplayed,
				"live-probe":  ErrCredentialOperationLiveProbe,
			} {
				if !errors.Is(err, marker) {
					t.Fatalf("retained %s error = %v, want %s marker", tc.name, err, name)
				}
			}
			if errors.Is(err, oauth.ErrNetwork) != tc.wantNetwork {
				t.Fatalf("retained %s network marker = %t, want %t: %v",
					tc.name, errors.Is(err, oauth.ErrNetwork), tc.wantNetwork, err)
			}
			var usageErr *oauth.UsageError
			if tc.wantStatus != 0 &&
				(!errors.As(err, &usageErr) || usageErr.Status != tc.wantStatus) {
				t.Fatalf("retained %s usage response = %+v, want status %d: %v",
					tc.name, usageErr, tc.wantStatus, err)
			}
			if rateLimited != tc.wantRateLimited || retryAfter != tc.retryAfter {
				t.Fatalf("retained %s rate limit = %t/%s, want %t/%s",
					tc.name, rateLimited, retryAfter, tc.wantRateLimited, tc.retryAfter)
			}
			refreshes, usageCalls := oauthClient.counts()
			if refreshes != 0 || usageCalls != 1 {
				t.Fatalf("retained %s calls = refresh %d usage %d, want 0/1",
					tc.name, refreshes, usageCalls)
			}
		})
	}
}

type admissionFlipCredentials struct {
	mu      sync.Mutex
	reads   int
	fresh   *creds.Credential
	expired *creds.Credential
}

func (c *admissionFlipCredentials) Store(account store.Account, source creds.Source) creds.Store {
	if source == creds.SourceFile {
		return credstest.FileStore(account.ConfigDir)
	}
	return admissionFlipStore{credentials: c}
}

func (c *admissionFlipCredentials) Stores(account store.Account) []creds.Store {
	return []creds.Store{
		c.Store(account, creds.SourceKeychain),
		c.Store(account, creds.SourceFile),
	}
}

func (*admissionFlipCredentials) Discover(context.Context, string) (string, error) {
	return "account-admission-flip", nil
}

type admissionFlipStore struct {
	credentials *admissionFlipCredentials
}

func (admissionFlipStore) Source() creds.Source { return creds.SourceKeychain }

func (s admissionFlipStore) Read(context.Context) (*creds.Credential, error) {
	s.credentials.mu.Lock()
	defer s.credentials.mu.Unlock()
	s.credentials.reads++
	credential := s.credentials.expired
	if s.credentials.reads == 1 {
		credential = s.credentials.fresh
	}
	clone := *credential
	return &clone, nil
}

func (s admissionFlipStore) Write(_ context.Context, credential *creds.Credential) error {
	s.credentials.mu.Lock()
	defer s.credentials.mu.Unlock()
	clone := *credential
	s.credentials.expired = &clone
	return nil
}

func (s admissionFlipStore) Delete(context.Context) error {
	return errors.New("admission-flip credential delete is unexpected")
}

func (admissionFlipStore) String() string { return "admission-flip keychain" }

func TestEnsureFreshAdmissionRaceUsesCrossedBoundaryAndAllStoreIdentity(t *testing.T) {
	for _, mode := range []string{"ensure-fresh", "sample-usage"} {
		t.Run(mode, func(t *testing.T) {
			st := openTestStore(t)
			account := persistTestAccount(t, st, store.Account{
				ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-admission-flip",
				KeychainAccount: "account-admission-flip",
			})
			credentials := &admissionFlipCredentials{
				fresh:   cred401("at-race", "rt-race", time.Now().Add(4*time.Hour)),
				expired: cred401("at-race", "rt-race", time.Now().Add(-time.Hour)),
			}
			oauthClient := &retainedRefreshFailureOAuth{
				refreshErr: &oauth.RefreshError{Status: http.StatusServiceUnavailable},
			}
			manager := credentialRecoveryManager(t, st, credentials, "admission-flip-owner")
			manager.OAuth = oauthClient

			switch mode {
			case "ensure-fresh":
				result, err := manager.ensureFreshTokenOperation(
					t.Context(), account, RefreshLeadTime, true,
				)
				if err == nil {
					t.Fatal("EnsureFresh unexpectedly succeeded after the crossed refresh failed")
				}
				if !result.RefreshAttempted {
					t.Fatalf("EnsureFresh result = %+v, want crossed-boundary RefreshAttempted", result)
				}
			case "sample-usage":
				if _, _, _, err := manager.SampleUsage(
					t.Context(), account, SampleOpts{AllowRefresh: true},
				); err == nil {
					t.Fatal("SampleUsage unexpectedly succeeded")
				}
			default:
				t.Fatalf("unknown mode %q", mode)
			}

			refreshes, usage := oauthClient.counts()
			if len(refreshes) != 1 || refreshes[0] != "rt-race" {
				t.Fatalf("refresh POSTs = %q, want the changed preliminary state consumed once", refreshes)
			}
			wantUsage := 0
			if mode == "sample-usage" {
				wantUsage = 1
			}
			if len(usage) != wantUsage {
				t.Fatalf("read-only Usage calls = %q, want %d", usage, wantUsage)
			}

			filePath := creds.FileCredentialPath(account.ConfigDir)
			active, receipt, err := st.CredentialOperationEvidence(store.CredentialOperationEvidenceQuery{
				AccountID: account.ID, AccountInstanceID: account.InstanceID,
				AccountGeneration: account.Generation,
				LocatorDigest: store.CredentialLocatorDigest(
					account.KeychainService, account.KeychainAccount, filePath,
				),
				FileLocatorDigest: store.CredentialFileLocatorDigest(filePath),
				Kind:              store.CredentialOperationEnsureFresh,
				Target:            store.CredentialTargetAll,
				IntentDigest: credentialIntentDigest(
					store.CredentialOperationEnsureFresh,
					RefreshLeadTime.String(),
					fmt.Sprintf("%t", true),
				),
			})
			if err != nil || active != nil || receipt == nil {
				t.Fatalf("all-store EnsureFresh evidence = active=%+v receipt=%+v err=%v", active, receipt, err)
			}
			if receipt.Target != store.CredentialTargetAll ||
				receipt.FailureClass != store.CredentialFailureRefreshServer {
				t.Fatalf("EnsureFresh receipt = %+v, want all-store server failure", receipt)
			}
		})
	}
}

func TestStripFailureQuarantineSurvivesReceiptGCUntilTokenReplacement(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "pool-v1.db")
	st, err := store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-strip-gc",
		KeychainAccount: "account-strip-gc",
	})
	writeErr := errors.New("strip write failed")
	credentials := credstest.NewFake()
	credentials.KeychainFaults = credstest.Faults{Write: writeErr}
	spent := cred401("at-spent", "rt-spent", time.Now().Add(-time.Hour))
	credentials.Put(account.KeychainService, account.KeychainAccount, spent)
	oauthClient := &fakeOAuth{currentRT: "rt-current"}
	manager := credentialRecoveryManager(t, st, credentials, "strip-gc-owner")
	manager.OAuth = oauthClient

	if _, _, err := manager.EnsureFreshToken(
		t.Context(), account, RefreshLeadTime, true,
	); !errors.Is(err, ErrNeedsLogin) || !errors.Is(err, writeErr) ||
		!errors.Is(err, ErrCredentialOperationQuarantined) {
		t.Fatalf("failed strip = %v, want needs-login plus durable quarantine", err)
	}
	quarantine, err := st.CredentialQuarantine(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if quarantine.Reason != store.CredentialResultCleanupFailed ||
		quarantine.TokenChainDigest == nil ||
		*quarantine.TokenChainDigest != credentialTokenChainDigest(spent) {
		t.Fatalf("cleanup quarantine = %+v", quarantine)
	}
	oauthClient.mu.Lock()
	gotRefreshes := oauthClient.invalidGrants
	oauthClient.mu.Unlock()
	if got := gotRefreshes; got != 1 {
		t.Fatalf("initial refresh POSTs = %d, want 1", got)
	}

	rawDB, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rawDB.Close() })
	committedAt := time.Now().Add(-2 * time.Hour)
	acknowledgedAt := committedAt.Add(time.Minute)
	expiresAt := committedAt.Add(5 * time.Minute)
	if _, err := rawDB.Exec(
		`UPDATE credential_operation_receipts
		 SET committed_at=?, acknowledged_at=?, expires_at=? WHERE account_id=?`,
		committedAt.UnixNano(), acknowledgedAt.UnixNano(), expiresAt.UnixNano(), account.ID,
	); err != nil {
		t.Fatal(err)
	}
	if deleted, err := st.DeleteExpiredCredentialOperationReceipts(16); err != nil || deleted != 0 {
		t.Fatalf("receipt GC while quarantined: deleted=%d err=%v", deleted, err)
	}
	if _, _, err := manager.EnsureFreshToken(
		t.Context(), account, RefreshLeadTime, true,
	); !errors.Is(err, ErrCredentialOperationQuarantined) {
		t.Fatalf("post-GC retry = %v, want quarantine", err)
	}
	oauthClient.mu.Lock()
	gotRefreshes = oauthClient.invalidGrants
	oauthClient.mu.Unlock()
	if got := gotRefreshes; got != 1 {
		t.Fatalf("post-GC refresh POSTs = %d, want retained 1", got)
	}

	replacement := cred401("at-replacement", "rt-replacement", time.Now().Add(time.Hour))
	credentials.Put(account.KeychainService, account.KeychainAccount, replacement)
	credential, refreshed, err := manager.EnsureFreshToken(
		t.Context(), account, RefreshLeadTime, true,
	)
	if err != nil || refreshed || credential == nil ||
		credential.ClaudeAiOauth.RefreshToken != "rt-replacement" {
		t.Fatalf("replacement result = credential=%+v refreshed=%t err=%v", credential, refreshed, err)
	}
	if _, err := st.CredentialQuarantine(account.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("quarantine after replacement = %v", err)
	}
	oauthClient.mu.Lock()
	gotRefreshes = oauthClient.invalidGrants
	oauthClient.mu.Unlock()
	if got := gotRefreshes; got != 1 {
		t.Fatalf("replacement triggered refresh: POSTs=%d, want 1", got)
	}
	if deleted, err := st.DeleteExpiredCredentialOperationReceipts(16); err != nil || deleted != 1 {
		t.Fatalf("receipt GC after replacement: deleted=%d err=%v", deleted, err)
	}
}
