package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
)

// newHealManager builds a Manager over the in-memory fake seam with the given
// OAuth client and an upserted account, matching the other pool tests' shape.
func newHealManager(t *testing.T, fk *credstest.Fake, oa Refresher) (*Manager, store.Account) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "svc-heal", KeychainAccount: "user"}
	a = persistTestAccount(t, st, a)
	manager := &Manager{Store: st, OAuth: oa, Creds: fk}
	bindTestWorkerAuthority(t, manager, "usage-heal")
	return manager, a
}

// refreshOnly builds the account-1 corruption shape: a blob whose access token
// the `claude` binary blanked, leaving only the refresh token to heal from.
func refreshOnly(rt string, exp time.Time) *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.RefreshToken = rt
	c.ClaudeAiOauth.ExpiresAt = exp.UnixMilli()
	return c
}

// TestEnsureFreshTokenHealsRefreshOnlyBlob pins the recovery: an idle account
// whose keychain holds a refresh-only blob (empty access token) refreshes on the
// next EnsureFreshToken and lands a complete blob — the future expiry proves the
// empty access token alone forced the refresh.
func TestEnsureFreshTokenHealsRefreshOnlyBlob(t *testing.T) {
	fk := credstest.NewFake()
	fo := &fakeOAuth{currentRT: "rt-0"}
	m, a := newHealManager(t, fk, fo)
	fk.Put(a.KeychainService, a.KeychainAccount, refreshOnly("rt-0", time.Now().Add(time.Hour)))

	cred, refreshed, err := m.EnsureFreshToken(context.Background(), a, RefreshLeadTime, true)
	if err != nil {
		t.Fatalf("EnsureFreshToken: %v", err)
	}
	if !refreshed {
		t.Fatal("refreshed = false, want true (an empty access token forces the refresh despite a future expiry)")
	}
	if cred.ClaudeAiOauth.AccessToken != "at-1" {
		t.Errorf("returned access token = %q, want the freshly minted \"at-1\"", cred.ClaudeAiOauth.AccessToken)
	}
	fo.mu.Lock()
	refreshes := fo.refreshes
	fo.mu.Unlock()
	if refreshes != 1 {
		t.Errorf("refreshes = %d, want exactly 1", refreshes)
	}
	healed, ok := fk.Get(a.KeychainService, a.KeychainAccount)
	if !ok {
		t.Fatal("keychain item missing after the heal")
	}
	if healed.ClaudeAiOauth.AccessToken != "at-1" {
		t.Errorf("keychain access token = %q, want the healed \"at-1\"", healed.ClaudeAiOauth.AccessToken)
	}
	if !healed.Expiry().After(time.Now()) {
		t.Error("healed credential has no future expiry")
	}
}

// TestEnsureFreshTokenRefreshOnlyRevoked pins the demotion of a refresh-only
// blob whose refresh token the server confirmed dead: needs-login surfaces and
// the strip leaves a both-empty tombstone (ErrNoTokens → needs-login) — a dead
// blob left "owned" would block peer-heal forever — which a subsequent
// InstallSyncedCredential heals.
func TestEnsureFreshTokenRefreshOnlyRevoked(t *testing.T) {
	fk := credstest.NewFake()
	m, a := newHealManager(t, fk, fakeOAuthRevoked{})
	blob := fmt.Sprintf(`{"claudeAiOauth":{"accessToken":"","refreshToken":"rt-dead","expiresAt":%d,"subscriptionType":"max"}}`,
		time.Now().Add(time.Hour).UnixMilli())
	if err := os.WriteFile(creds.FileCredentialPath(a.ConfigDir), []byte(blob), 0o600); err != nil {
		t.Fatal(err)
	}

	_, _, err := m.EnsureFreshToken(context.Background(), a, RefreshLeadTime, true)
	if !errors.Is(err, ErrNeedsLogin) {
		t.Fatalf("err = %v, want ErrNeedsLogin", err)
	}
	raw, err := os.ReadFile(creds.FileCredentialPath(a.ConfigDir))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "refreshToken") || strings.Contains(string(raw), "rt-dead") {
		t.Fatalf("stored blob still carries the dead refresh token: %s", raw)
	}
	if _, rerr := credstest.FileStore(a.ConfigDir).Read(t.Context()); !errors.Is(rerr, creds.ErrNoTokens) {
		t.Fatalf("stripped blob reads back as %v, want the ErrNoTokens tombstone", rerr)
	}
	_, _, err = m.EnsureFreshToken(context.Background(), a, RefreshLeadTime, true)
	if !errors.Is(err, ErrNeedsLogin) || !errors.Is(err, creds.ErrNoTokens) {
		t.Fatalf("tombstone classifies as %v, want ErrNeedsLogin naming creds.ErrNoTokens", err)
	}

	installed, err := m.InstallSyncedCredential(context.Background(), a, synced("at-peer", time.Now().Add(time.Hour)))
	if err != nil || !installed {
		t.Fatalf("InstallSyncedCredential over the tombstone = (%v, %v), want a heal", installed, err)
	}
	healed, _, err := m.ReadCredential(t.Context(), a)
	if err != nil || healed.ClaudeAiOauth.AccessToken != "at-peer" {
		t.Fatalf("post-heal credential = (%+v, %v), want the peer's synced copy", healed, err)
	}
}

// synced builds the peer shape: an access token with no refresh token.
func synced(at string, exp time.Time) *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.AccessToken = at
	c.ClaudeAiOauth.ExpiresAt = exp.UnixMilli()
	return c
}

// TestEnsureFreshTokenSynced pins the synced-blob classification: a synced
// token serves until actually expired — the refresh lead window is meaningless
// for a token with nothing to refresh — and only an expired one is
// ErrUnrefreshable, never ErrNeedsLogin; no refresh is ever POSTed.
func TestEnsureFreshTokenSynced(t *testing.T) {
	cases := map[string]struct {
		expiry  time.Time
		wantErr error
	}{
		"unexpired synced is a non-event":            {expiry: time.Now().Add(time.Hour)},
		"synced inside the lead window still serves": {expiry: time.Now().Add(time.Minute)},
		"expired synced cannot refresh":              {expiry: time.Now().Add(-time.Hour), wantErr: ErrUnrefreshable},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fk := credstest.NewFake()
			fo := &fakeOAuth{currentRT: "rt-elsewhere"}
			m, a := newHealManager(t, fk, fo)
			fk.Put(a.KeychainService, a.KeychainAccount, synced("at-synced", tc.expiry))

			cred, refreshed, err := m.EnsureFreshToken(context.Background(), a, RefreshLeadTime, true)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if errors.Is(err, ErrNeedsLogin) {
				t.Fatalf("err = %v; a synced blob must never classify as needs-login", err)
			}
			if refreshed {
				t.Fatal("refreshed = true, want false")
			}
			if tc.wantErr == nil && (cred == nil || cred.ClaudeAiOauth.AccessToken != "at-synced") {
				t.Fatalf("cred = %+v, want the synced credential returned as-is", cred)
			}
			fo.mu.Lock()
			refreshes := fo.refreshes
			invalidGrants := fo.invalidGrants
			fo.mu.Unlock()
			if refreshes != 0 || invalidGrants != 0 {
				t.Fatalf("refresh POSTs = %d/%d, want 0/0 (synced blobs never refresh)", refreshes, invalidGrants)
			}
			if fk.WriteCount() != 0 {
				t.Fatalf("writes = %d, want 0", fk.WriteCount())
			}
		})
	}
}

// TestSampleUsageNearExpirySyncedStillSamples pins that a synced token inside
// the refresh lead but still valid keeps sampling: it has nothing to refresh,
// so the lead window must not stop the usage request for its final minutes.
func TestSampleUsageNearExpirySyncedStillSamples(t *testing.T) {
	kc := &rotatingCreds{current: cred401("at-0", "", time.Now().Add(30*time.Second))}
	fo := newFakeOAuth401("", "at-0") // at-0 valid: the request must be sent and served
	m, a := newManager401(t, kc, fo)

	usage, rateLimited, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: true})
	if err != nil {
		t.Fatalf("SampleUsage = %v, want the still-valid synced token to sample", err)
	}
	if rateLimited {
		t.Fatal("rateLimited = true, want false")
	}
	if usage == nil || usage.FiveHour.Utilization != 31 {
		t.Fatalf("usage = %+v, want the served windows (the usage request must actually fire)", usage)
	}
	if fo.refreshes != 0 {
		t.Fatalf("refreshes = %d, want 0 (synced tokens never refresh)", fo.refreshes)
	}
}

// TestPreflightInvalidGrantStripsRefreshToken pins the strip-on-invalid_grant
// demotion at the pre-flight refresh: a server-confirmed dead chain keeps its
// access token, expiry, and metadata but loses the refresh token, and the
// persisted bytes omit the refreshToken key entirely (a present-but-empty one
// reads as a tombstone to claude).
func TestPreflightInvalidGrantStripsRefreshToken(t *testing.T) {
	fk := credstest.NewFake()
	fo := &fakeOAuth{currentRT: "rt-current"} // stored rt-spent → invalid_grant
	m, a := newHealManager(t, fk, fo)
	spent := &creds.Credential{}
	spent.ClaudeAiOauth.AccessToken = "at-live"
	spent.ClaudeAiOauth.RefreshToken = "rt-spent"
	spent.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Minute).UnixMilli()
	spent.ClaudeAiOauth.SubscriptionType = "max"
	if err := credstest.FileStore(a.ConfigDir).Write(t.Context(), spent); err != nil {
		t.Fatal(err)
	}

	_, _, err := m.EnsureFreshToken(context.Background(), a, RefreshLeadTime, true)
	if !errors.Is(err, ErrNeedsLogin) {
		t.Fatalf("err = %v, want ErrNeedsLogin", err)
	}
	raw, err := os.ReadFile(creds.FileCredentialPath(a.ConfigDir))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "refreshToken") {
		t.Fatalf("stripped blob bytes still carry a refreshToken key: %s", raw)
	}
	got, rerr := credstest.FileStore(a.ConfigDir).Read(t.Context())
	if rerr != nil {
		t.Fatalf("read stripped blob back: %v", rerr)
	}
	if !got.Synced() {
		t.Fatalf("stored blob = %+v, want the synced shape", got.ClaudeAiOauth)
	}
	if got.ClaudeAiOauth.AccessToken != "at-live" ||
		got.ClaudeAiOauth.ExpiresAt != spent.ClaudeAiOauth.ExpiresAt ||
		got.ClaudeAiOauth.SubscriptionType != "max" {
		t.Fatalf("strip dropped fields: %+v", got.ClaudeAiOauth)
	}
	assertNeverCanonical(t, fk.TouchedServices())
}

// TestStripAbortsOnRefreshTokenRotatedUnderfoot pins the full-credential CAS
// ahead of the strip: a login rotating only the refresh token (same access
// token) between the failed refresh and the write must abort the strip — an
// access-token-only compare would destroy the live rotated chain.
func TestStripAbortsOnRefreshTokenRotatedUnderfoot(t *testing.T) {
	kc := &rotatingCreds{
		// Reads #1-#4 are target classification, durable admission, apply
		// verification, and pre-flight. The strip's source re-read sees the
		// rotation: same access token, new refresh token.
		rotateAfter: 4,
		current:     cred401("at-0", "rt-stale", time.Now().Add(-time.Hour)),
		rotated:     cred401("at-0", "rt-live", time.Now().Add(time.Hour)),
	}
	fo := newFakeOAuth401("rt-current") // rt-stale → invalid_grant
	m, a := newManager401(t, kc, fo)

	got, refreshed, err := m.EnsureFreshToken(context.Background(), a, RefreshLeadTime, true)
	if err != nil || refreshed || got == nil || got.ClaudeAiOauth.RefreshToken != "rt-live" {
		t.Fatalf("rotated credential = %+v refreshed=%t err=%v, want live chain", got, refreshed, err)
	}
	kc.mu.Lock()
	after, rotated := *kc.current, kc.rotated
	kc.mu.Unlock()
	if rotated == nil {
		t.Fatal("a write landed (rotation cancelled); the strip must abort, not write")
	}
	if after.ClaudeAiOauth.RefreshToken != "rt-stale" || rotated.ClaudeAiOauth.RefreshToken != "rt-live" {
		t.Fatalf("stored chains mutated (current=%q rotated=%q), want both untouched",
			after.ClaudeAiOauth.RefreshToken, rotated.ClaudeAiOauth.RefreshToken)
	}
}

// TestStripFailureReplaysNeedsLoginWithoutRefresh pins the ambiguous write
// edge: once the provider has confirmed invalid_grant, a failed local strip
// still settles the auth verdict durably. Retrying returns the same
// needs-login result from retained evidence without re-spending the token.
func TestStripFailureReplaysNeedsLoginWithoutRefresh(t *testing.T) {
	writeErr := errors.New("strip write failed")
	fk := credstest.NewFake()
	fk.KeychainFaults = credstest.Faults{Write: writeErr}
	fo := &fakeOAuth{currentRT: "rt-current"}
	m, a := newHealManager(t, fk, fo)
	spent := cred401("at-stale", "rt-spent", time.Now().Add(-time.Hour))
	fk.Put(a.KeychainService, a.KeychainAccount, spent)

	_, _, firstErr := m.EnsureFreshToken(t.Context(), a, RefreshLeadTime, true)
	if !errors.Is(firstErr, ErrNeedsLogin) || !errors.Is(firstErr, writeErr) {
		t.Fatalf("first err = %v, want needs-login joined with strip failure", firstErr)
	}
	fo.mu.Lock()
	firstRefreshes := fo.invalidGrants
	fo.mu.Unlock()
	if firstRefreshes != 1 {
		t.Fatalf("invalid-grant refreshes = %d, want 1", firstRefreshes)
	}

	_, _, replayErr := m.EnsureFreshToken(t.Context(), a, RefreshLeadTime, true)
	if !errors.Is(replayErr, ErrNeedsLogin) ||
		!errors.Is(replayErr, ErrCredentialOperationReplayed) {
		t.Fatalf("replay err = %v, want retained needs-login evidence", replayErr)
	}
	fo.mu.Lock()
	refreshesAfterReplay := fo.invalidGrants
	fo.mu.Unlock()
	if refreshesAfterReplay != firstRefreshes {
		t.Fatalf("replay issued %d new refresh(es), want 0", refreshesAfterReplay-firstRefreshes)
	}
	stored, ok := fk.Get(a.KeychainService, a.KeychainAccount)
	if !ok || stored.ClaudeAiOauth.RefreshToken != "rt-spent" {
		t.Fatalf("stored credential = %+v, want the failed strip left untouched", stored)
	}
}

// fakeOAuthPlain401 401s every refresh without an OAuth error code — the
// transient-401 shape that must never destroy a refresh token.
type fakeOAuthPlain401 struct{}

func (fakeOAuthPlain401) Refresh(_ context.Context, _, _ string) (*oauth.TokenResponse, error) {
	return nil, &oauth.RefreshError{Status: 401}
}

func (fakeOAuthPlain401) Usage(_ context.Context, _ string) (*oauth.Usage, error) {
	return nil, &oauth.UsageError{Status: 401}
}

// TestPlain401IsTransientAndDoesNotStrip pins the strip gate's negative: a 401
// without a confirmed invalid_grant remains transient and must not clear the
// refresh token.
func TestPlain401IsTransientAndDoesNotStrip(t *testing.T) {
	fk := credstest.NewFake()
	m, a := newHealManager(t, fk, fakeOAuthPlain401{})
	owned := &creds.Credential{}
	owned.ClaudeAiOauth.AccessToken = "at-live"
	owned.ClaudeAiOauth.RefreshToken = "rt-live"
	owned.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Minute).UnixMilli()
	fk.Put(a.KeychainService, a.KeychainAccount, owned)

	_, _, err := m.EnsureFreshToken(context.Background(), a, RefreshLeadTime, true)
	if errors.Is(err, ErrNeedsLogin) {
		t.Fatalf("err = %v, plain 401 must not require login", err)
	}
	var refreshErr *oauth.RefreshError
	if !errors.As(err, &refreshErr) || refreshErr.Status != 401 {
		t.Fatalf("err = %v, want transient refresh 401", err)
	}
	if fk.WriteCount() != 0 {
		t.Fatalf("writes = %d, want 0 (an unconfirmed 401 must not strip)", fk.WriteCount())
	}
	stored, ok := fk.Get(a.KeychainService, a.KeychainAccount)
	if !ok || stored.ClaudeAiOauth.RefreshToken != "rt-live" {
		t.Fatalf("stored blob = %+v, want the refresh token intact", stored)
	}
	entry, ok, entryErr := m.Store.LastRefresh(a.ID)
	if entryErr != nil || !ok || entry.Category != store.RefreshRejected {
		t.Fatalf("refresh audit = %+v ok=%t err=%v, want rejected", entry, ok, entryErr)
	}
}

// TestFetchUsageInvalidGrantStrips pins the strip in fetchUsage's confirmed-
// revocation branch: busy-guard refresh hits invalid_grant with an unchanged
// on-disk chain, so the blob is demoted to synced and needs-login surfaces.
func TestFetchUsageInvalidGrantStrips(t *testing.T) {
	kc := &rotatingCreds{current: cred401("at-0", "rt-stale", time.Now().Add(-time.Hour))}
	fo := newFakeOAuth401("rt-current") // rt-stale → invalid_grant
	m, a := newManager401(t, kc, fo)

	_, _, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowBusyRefresh: true})
	if !errors.Is(err, ErrNeedsLogin) {
		t.Fatalf("err = %v, want ErrNeedsLogin", err)
	}
	kc.mu.Lock()
	after := *kc.current
	kc.mu.Unlock()
	if after.ClaudeAiOauth.AccessToken != "at-0" || after.ClaudeAiOauth.RefreshToken != "" {
		t.Fatalf("stored blob = %+v, want at-0 with the refresh token stripped", after.ClaudeAiOauth)
	}
	assertNeverCanonical(t, kc.touchedServices())
}

// TestFetchUsageRotationUnderfootBlocksStrip pins the sameTokens guard ahead
// of the strip: a session rotating the chain between the failed refresh and
// the revocation re-read makes the invalid_grant transient — no strip, no
// needs-login.
func TestFetchUsageRotationUnderfootBlocksStrip(t *testing.T) {
	kc := &rotatingCreds{
		// read#1 pre-flight, read#2 rung-1 re-read, read#3 busy guard — all
		// current; read#4 (the revocation re-read) sees the rotated chain.
		rotateAfter: 3,
		current:     cred401("at-0", "rt-stale", time.Now().Add(-time.Hour)),
		rotated:     cred401("at-9", "rt-9", time.Now().Add(time.Hour)),
	}
	fo := newFakeOAuth401("rt-current") // rt-stale → invalid_grant
	m, a := newManager401(t, kc, fo)

	_, _, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowBusyRefresh: true})
	if err == nil {
		t.Fatal("err = nil, want the original 401 to propagate")
	}
	if errors.Is(err, ErrNeedsLogin) || errors.Is(err, ErrUnrefreshable) {
		t.Fatalf("err = %v; a rotation underfoot is transient, not a classification", err)
	}
	kc.mu.Lock()
	after := *kc.current
	kc.mu.Unlock()
	if after.ClaudeAiOauth.RefreshToken != "rt-stale" {
		t.Fatalf("stored refresh token = %q, want rt-stale untouched (no strip)", after.ClaudeAiOauth.RefreshToken)
	}
}

// TestSampleUsageTokenlessFlagsNeedsLogin pins the tombstone path end to end:
// a blob claude blanked entirely (no access or refresh token) must surface
// ErrNeedsLogin from SampleUsage rather than panic in fetchUsage on the nil
// credential ensureFreshToken returns for it (the v0.50.1 regression).
func TestSampleUsageTokenlessFlagsNeedsLogin(t *testing.T) {
	fk := credstest.NewFake()
	m, a := newHealManager(t, fk, &fakeOAuth{})
	tombstone := `{"claudeAiOauth":{"accessToken":"","refreshToken":"","expiresAt":0,"scopes":["user:inference"],"subscriptionType":"max","rateLimitTier":"default_claude_max_20x"}}`
	if err := os.WriteFile(creds.FileCredentialPath(a.ConfigDir), []byte(tombstone), 0o600); err != nil {
		t.Fatal(err)
	}

	_, rateLimited, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: true})
	if !errors.Is(err, ErrNeedsLogin) {
		t.Fatalf("err = %v, want ErrNeedsLogin", err)
	}
	if !errors.Is(err, creds.ErrNoTokens) {
		t.Fatalf("err = %v, want it to also name creds.ErrNoTokens", err)
	}
	if rateLimited {
		t.Error("rateLimited = true, want false")
	}
}

// TestSampleUsageAbsentFlagsNeedsLogin pins the fully-absent credential path:
// SampleUsage must return the two classifications rather than reach fetchUsage
// with a nil credential.
func TestSampleUsageAbsentFlagsNeedsLogin(t *testing.T) {
	fk := credstest.NewFake()
	m, a := newHealManager(t, fk, &fakeOAuth{})

	_, rateLimited, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: true})
	if !errors.Is(err, ErrNeedsLogin) {
		t.Fatalf("err = %v, want ErrNeedsLogin", err)
	}
	if !errors.Is(err, creds.ErrNotFound) {
		t.Fatalf("err = %v, want it to also name creds.ErrNotFound", err)
	}
	if rateLimited {
		t.Error("rateLimited = true, want false")
	}
}

// TestEnsureFreshTokenClassification pins the narrowness of the needs-login
// mapping: tokenless and fully absent credentials flag the account, while a
// locked or opaque keychain must never be classified as needs-login, so a
// reboot-time Keychain wedge never signs healthy accounts out.
func TestEnsureFreshTokenClassification(t *testing.T) {
	errBoom := errors.New("keychain read exploded")
	cases := []struct {
		name         string
		kcReadFault  error
		fileDeadBlob bool
		wantErrIs    []error
		wantNotErrIs []error
	}{
		{
			name:         "tokenless blob flags needs-login and names no-tokens",
			fileDeadBlob: true,
			wantErrIs:    []error{ErrNeedsLogin, creds.ErrNoTokens},
		},
		{
			name:         "unsearchable keychain is not needs-login",
			kcReadFault:  creds.ErrUnavailable,
			wantErrIs:    []error{creds.ErrUnavailable},
			wantNotErrIs: []error{ErrNeedsLogin},
		},
		{
			name:         "opaque keychain read error is not needs-login",
			kcReadFault:  errBoom,
			wantErrIs:    []error{errBoom},
			wantNotErrIs: []error{ErrNeedsLogin},
		},
		{
			name:      "absent everywhere flags needs-login and names not-found",
			wantErrIs: []error{ErrNeedsLogin, creds.ErrNotFound},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fk := credstest.NewFake()
			fk.KeychainFaults = credstest.Faults{Read: tc.kcReadFault}
			m, a := newHealManager(t, fk, &fakeOAuth{})
			if tc.fileDeadBlob {
				if err := os.WriteFile(creds.FileCredentialPath(a.ConfigDir), []byte(`{"claudeAiOauth":{}}`), 0o600); err != nil {
					t.Fatal(err)
				}
			}

			_, _, err := m.EnsureFreshToken(context.Background(), a, RefreshLeadTime, true)
			if err == nil {
				t.Fatal("EnsureFreshToken err = nil, want an error")
			}
			for _, want := range tc.wantErrIs {
				if !errors.Is(err, want) {
					t.Errorf("err %v does not satisfy errors.Is(%v)", err, want)
				}
			}
			for _, notWant := range tc.wantNotErrIs {
				if errors.Is(err, notWant) {
					t.Errorf("err %v must NOT satisfy errors.Is(%v)", err, notWant)
				}
			}
		})
	}
}

// TestSampleUsageBusyHealsEmptyAccessToken pins the busy path's heal: a live
// account whose blob lost its access token (allowRefresh=false) recovers through
// the 401 ladder's busy-refresh guard — the empty access token makes Expired()
// true despite a future expiry, satisfying the guard and healing the blob.
func TestSampleUsageBusyHealsEmptyAccessToken(t *testing.T) {
	fk := credstest.NewFake()
	fo := newFakeOAuth401("rt-0")
	m, a := newManager401(t, fk, fo)
	fk.Put(a.KeychainService, a.KeychainAccount, refreshOnly("rt-0", time.Now().Add(time.Hour)))

	_, rateLimited, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowBusyRefresh: true})
	if err != nil {
		t.Fatalf("SampleUsage: %v", err)
	}
	if rateLimited {
		t.Fatal("rateLimited = true, want false")
	}
	if fo.refreshes != 1 {
		t.Fatalf("refreshes = %d, want exactly 1 (the busy guard heals the empty access token)", fo.refreshes)
	}
	healed, ok := fk.Get(a.KeychainService, a.KeychainAccount)
	if !ok {
		t.Fatal("keychain item missing after the busy heal")
	}
	if healed.ClaudeAiOauth.AccessToken == "" {
		t.Error("keychain still holds an empty access token after the busy heal")
	}
	assertNeverCanonical(t, fk.TouchedServices())
}
