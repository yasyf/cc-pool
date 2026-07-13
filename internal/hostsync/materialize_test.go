package hostsync

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/pool"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

const materializeManifest = "/cfg/synckit/manifests/cc-pool.json"

// stubRefresher satisfies pool.Refresher: Usage always succeeds so FinalizeAdd's
// best-effort validation passes, and Refresh is never reached by these tests
// (their pulled credentials carry a future expiry, so no preemptive refresh runs).
type stubRefresher struct{}

func (stubRefresher) Refresh(context.Context, string, string) (*oauth.TokenResponse, error) {
	return &oauth.TokenResponse{AccessToken: "at-refreshed", RefreshToken: "rt-refreshed", ExpiresIn: 3600}, nil
}

func (stubRefresher) Usage(context.Context, string) (*oauth.Usage, error) {
	return &oauth.Usage{}, nil
}

// runRecorder captures every external-command exec the Service issues (the
// synckitd nudge), so tests assert the nudge fired without spawning a real binary.
type runRecorder struct{ calls [][]string }

func (r *runRecorder) run(_ context.Context, name string, args ...string) error {
	r.calls = append(r.calls, append([]string{name}, args...))
	return nil
}

// newMaterializeService wires a Service over a real (temp) pool Manager: symlink
// overlay, an in-memory Keychain + on-disk file store, and a captured command
// runner. HOME/USER point at temp dirs so nothing touches real state.
func newMaterializeService(t *testing.T) (*Service, *pool.Manager, *credstest.Fake, *runRecorder) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("USER", "tester")
	if err := os.MkdirAll(pool.ClaudeDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	m, err := pool.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	fk := credstest.NewFake()
	m.Creds = fk
	m.OAuth = stubRefresher{}
	m.DetectOverlay = func() (fkoverlay.Backend, string) { return fkoverlay.BackendSymlink, "" }
	m.LockDir = filepath.Join(t.TempDir(), "locks")
	if _, err := m.Init(); err != nil {
		t.Fatal(err)
	}
	rf := tempRegistry(t)
	rec := &runRecorder{}
	s := &Service{
		M:        m,
		Registry: &rf,
		StampDir: filepath.Join(t.TempDir(), "stamps"),
		Run:      rec.run,
	}
	return s, m, fk, rec
}

// freshEnvelope is a pulled STRIPPED envelope whose access token has not
// expired, so FinalizeAdd's usage validation never triggers a refresh.
func freshEnvelope(access string) *creds.Credential {
	c := cred(access, "")
	c.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli()
	return c
}

// materializeVal builds a peer-added registry value with a full, verbatim
// oauthAccount object and an origin-owned chain stamp.
func materializeVal(uuid, email string, oauthAccount json.RawMessage) AccountValue {
	return AccountValue{
		UUID:         uuid,
		Email:        email,
		Label:        "peer-" + uuid,
		OAuthAccount: oauthAccount,
		Chain:        ChainStamp{Origin: "hostA", ExpiresAt: 1_800_000_000_000, Hash: "h-" + uuid},
	}
}

func pullConst(c *creds.Credential) PullCredential {
	return func(context.Context, string, ChainStamp, []string) (*creds.Credential, error) { return c, nil }
}

// TestMaterializeHappyPath pins the full peer-add: a dir, the verbatim identity,
// the pulled credential in the Keychain, a registered row with the backfilled
// uuid, and a synckitd nudge — with no interactive login.
func TestMaterializeHappyPath(t *testing.T) {
	s, m, fk, rec := newMaterializeService(t)

	// A ~/.claude.json exists, so PrepareAdd seeds SeedCopied (not the bootstrap path).
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{"numStartups":3,"oauthAccount":{"accountUuid":"PLAIN"}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	oauthAccount := json.RawMessage(`{"accountUuid":"u-happy","emailAddress":"happy@example.com","organizationUuid":"org-1","nested":{"k":1},"flag":true}`)
	env := freshEnvelope("at-happy")

	res, err := s.Materialize(context.Background(), materializeVal("u-happy", "happy@example.com", oauthAccount), []string{"hostB"}, pullConst(env), materializeManifest)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if res.Deferred || res.FileFallback || res.Bootstrapped {
		t.Fatalf("result flags = %+v, want all false on the keychain happy path", res)
	}
	if res.UUID != "u-happy" || res.AccountID != 1 {
		t.Fatalf("result = %+v, want uuid u-happy / acct 1", res)
	}

	configDir := pool.AccountDir(1)
	if _, err := os.Stat(configDir); err != nil {
		t.Fatalf("account dir not created: %v", err)
	}

	// Identity injected verbatim and resolvable through the established reader.
	id, err := pool.AccountIdentity(fkoverlay.BackendSymlink, configDir)
	if err != nil {
		t.Fatalf("AccountIdentity: %v", err)
	}
	if id.AccountUUID != "u-happy" || id.EmailAddress != "happy@example.com" {
		t.Fatalf("identity = %+v, want u-happy / happy@example.com", id)
	}
	raw, err := os.ReadFile(filepath.Join(configDir, ".claude.json")) //nolint:gosec // G304: configDir is a cc-pool-managed/test-owned dir, not external input
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatal(err)
	}
	var gotOAuth, wantOAuth any
	if err := json.Unmarshal(top["oauthAccount"], &gotOAuth); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(oauthAccount, &wantOAuth); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotOAuth, wantOAuth) {
		t.Fatalf("oauthAccount = %s, want %s (verbatim)", top["oauthAccount"], oauthAccount)
	}

	// Row registered, credential in the Keychain, uuid backfilled.
	row, err := m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if row.AccountUUID != "u-happy" {
		t.Fatalf("row AccountUUID = %q, want u-happy (backfill)", row.AccountUUID)
	}
	byUUID, ok, err := m.Store.GetAccountByUUID("u-happy")
	if err != nil || !ok {
		t.Fatalf("GetAccountByUUID: ok=%v err=%v", ok, err)
	}
	if byUUID.ID != 1 {
		t.Fatalf("GetAccountByUUID id = %d, want 1", byUUID.ID)
	}
	gotCred, src, err := m.ReadCredential(row)
	if err != nil {
		t.Fatalf("ReadCredential: %v", err)
	}
	if src != creds.SourceKeychain {
		t.Fatalf("credential source = %v, want keychain", src)
	}
	if gotCred.ClaudeAiOauth.AccessToken != "at-happy" || gotCred.HasRefreshToken() {
		t.Fatalf("installed credential = %+v, want the stripped at-happy with no refresh token", gotCred.ClaudeAiOauth)
	}
	if _, ok := fk.Get(row.KeychainService, creds.AccountLabel()); !ok {
		t.Fatalf("keychain item %q/%q absent, want the installed envelope", row.KeychainService, creds.AccountLabel())
	}

	// synckitd nudged with the manifest path.
	if !recorded(rec, []string{"synckitd", "register", materializeManifest}) {
		t.Fatalf("nudge calls = %v, want a synckitd register of %q", rec.calls, materializeManifest)
	}
}

// TestMaterializeSeedNoSourceBootstraps pins carry-forward #2: with no
// ~/.claude.json to seed from, the materializer bootstraps a minimal private
// document so WriteIdentity lands, and the account completes end to end.
func TestMaterializeSeedNoSourceBootstraps(t *testing.T) {
	s, m, _, _ := newMaterializeService(t)
	// No ~/.claude.json written: PrepareAdd reports SeedNoSource.

	oauthAccount := json.RawMessage(`{"accountUuid":"u-nosrc","emailAddress":"nosrc@example.com"}`)
	res, err := s.Materialize(context.Background(), materializeVal("u-nosrc", "nosrc@example.com", oauthAccount), []string{"hostB"}, pullConst(freshEnvelope("at-n")), materializeManifest)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if !res.Bootstrapped {
		t.Fatalf("result = %+v, want Bootstrapped true on the SeedNoSource path", res)
	}
	if res.AccountID != 1 || res.Deferred {
		t.Fatalf("result = %+v, want a completed acct 1", res)
	}

	configDir := pool.AccountDir(1)
	id, err := pool.AccountIdentity(fkoverlay.BackendSymlink, configDir)
	if err != nil {
		t.Fatalf("AccountIdentity: %v", err)
	}
	if id.AccountUUID != "u-nosrc" {
		t.Fatalf("identity uuid = %q, want u-nosrc", id.AccountUUID)
	}
	// The bootstrapped document held only the injected identity (started as "{}").
	raw, err := os.ReadFile(filepath.Join(configDir, ".claude.json")) //nolint:gosec // G304: configDir is a cc-pool-managed/test-owned dir, not external input
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatal(err)
	}
	if len(top) != 1 {
		t.Fatalf("bootstrapped doc keys = %v, want only oauthAccount", keysOf(top))
	}
	row, err := m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if row.AccountUUID != "u-nosrc" {
		t.Fatalf("row AccountUUID = %q, want u-nosrc", row.AccountUUID)
	}
}

// TestMaterializeKeychainUnavailableFallsBackToFile pins carry-forward #1: an
// unsearchable login Keychain routes the credential to the plaintext file store,
// and the fallback is surfaced on the result (not merely logged).
func TestMaterializeKeychainUnavailableFallsBackToFile(t *testing.T) {
	s, m, fk, _ := newMaterializeService(t)
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	// The login keychain is unsearchable this session: every keychain probe/read
	// reports ErrUnavailable.
	fk.KeychainFaults = credstest.Faults{Read: creds.ErrUnavailable}

	oauthAccount := json.RawMessage(`{"accountUuid":"u-file","emailAddress":"file@example.com"}`)
	res, err := s.Materialize(context.Background(), materializeVal("u-file", "file@example.com", oauthAccount), []string{"hostB"}, pullConst(freshEnvelope("at-f")), materializeManifest)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if !res.FileFallback {
		t.Fatalf("result = %+v, want FileFallback true when the keychain is unavailable", res)
	}
	if res.AccountID != 1 {
		t.Fatalf("result = %+v, want a completed acct 1", res)
	}

	configDir := pool.AccountDir(1)
	if _, err := os.Stat(creds.FileCredentialPath(configDir)); err != nil {
		t.Fatalf("file credential not written: %v", err)
	}
	// No keychain item was written (the fallback avoided it).
	if _, ok := fk.Get(creds.ServiceName(configDir), creds.AccountLabel()); ok {
		t.Fatal("keychain item present, want none on the file-fallback path")
	}
	row, err := m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	gotCred, src, err := m.ReadCredential(row)
	if err != nil {
		t.Fatalf("ReadCredential: %v", err)
	}
	if src != creds.SourceFile {
		t.Fatalf("credential source = %v, want file", src)
	}
	if gotCred.ClaudeAiOauth.AccessToken != "at-f" {
		t.Fatalf("installed credential = %+v, want the pulled at-f", gotCred.ClaudeAiOauth)
	}
	if row.AccountUUID != "u-file" {
		t.Fatalf("row AccountUUID = %q, want u-file", row.AccountUUID)
	}
}

// TestMaterializeNoEnvelopeAborts pins the required-envelope rule: a pull with
// no credential rolls the half-built account back — dir gone, reservation
// released, no row — and returns a retryable error.
func TestMaterializeNoEnvelopeAborts(t *testing.T) {
	pullBoom := errors.New("all peers unreachable")
	cases := map[string]PullCredential{
		"pull errors": func(context.Context, string, ChainStamp, []string) (*creds.Credential, error) {
			return nil, pullBoom
		},
		"pull returns nil credential": func(context.Context, string, ChainStamp, []string) (*creds.Credential, error) {
			return nil, nil
		},
	}
	for name, pull := range cases {
		t.Run(name, func(t *testing.T) {
			s, m, _, rec := newMaterializeService(t)
			if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
				t.Fatal(err)
			}
			oauthAccount := json.RawMessage(`{"accountUuid":"u-x","emailAddress":"x@example.com"}`)

			res, err := s.Materialize(context.Background(), materializeVal("u-x", "x@example.com", oauthAccount), []string{"hostB"}, pull, materializeManifest)
			if !errors.Is(err, ErrMaterializeNoEnvelope) {
				t.Fatalf("err = %v, want errors.Is ErrMaterializeNoEnvelope", err)
			}
			if res != (MaterializeResult{}) {
				t.Fatalf("result = %+v, want zero on abort", res)
			}

			// Dir torn down.
			if _, statErr := os.Stat(pool.AccountDir(1)); !os.IsNotExist(statErr) {
				t.Fatalf("account dir stat err = %v, want not-exist (AbandonAdd must remove it)", statErr)
			}
			// Reservation released: the freed index is handed straight back.
			n, rerr := m.Store.ReserveAccountIndex()
			if rerr != nil {
				t.Fatal(rerr)
			}
			if n != 1 {
				t.Fatalf("next reserved index = %d, want the released 1", n)
			}
			// No row registered.
			accounts, lerr := m.Store.ListAccounts()
			if lerr != nil {
				t.Fatal(lerr)
			}
			if len(accounts) != 0 {
				t.Fatalf("accounts = %+v, want none after abort", accounts)
			}
			// No nudge on a failed materialization.
			if len(rec.calls) != 0 {
				t.Fatalf("nudge calls = %v, want none on abort", rec.calls)
			}
		})
	}
}

// TestMaterializeRejectedEnvelopeReleasesNotAbandons pins the rejection path:
// a tokenless or RT-bearing envelope aborts WITHOUT AbandonAdd — the dir (and
// any retained login state in it) survives, only the reservation is released,
// and no row or nudge lands.
func TestMaterializeRejectedEnvelopeReleasesNotAbandons(t *testing.T) {
	tokenless := &creds.Credential{}
	tokenless.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli()
	rtBearing := cred("at-secret", "rt-secret")
	rtBearing.ClaudeAiOauth.ExpiresAt = tokenless.ClaudeAiOauth.ExpiresAt

	cases := map[string]struct {
		env       *creds.Credential
		wantErrIs error
	}{
		"tokenless envelope":  {tokenless, pool.ErrEnvelopeNoAccessToken},
		"RT-bearing envelope": {rtBearing, pool.ErrEnvelopeCarriesSecret},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, m, fk, rec := newMaterializeService(t)
			if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
				t.Fatal(err)
			}
			oauthAccount := json.RawMessage(`{"accountUuid":"u-rej","emailAddress":"r@example.com"}`)

			res, err := s.Materialize(context.Background(), materializeVal("u-rej", "r@example.com", oauthAccount), []string{"hostB"}, pullConst(tc.env), materializeManifest)
			if !errors.Is(err, tc.wantErrIs) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.wantErrIs)
			}
			if res != (MaterializeResult{}) {
				t.Fatalf("result = %+v, want zero on rejection", res)
			}
			// The dir is kept (release, not abandon) and no credential was written.
			if _, statErr := os.Stat(pool.AccountDir(1)); statErr != nil {
				t.Fatalf("account dir stat err = %v, want kept on rejection", statErr)
			}
			if _, ok := fk.Get(creds.ServiceName(pool.AccountDir(1)), creds.AccountLabel()); ok {
				t.Fatal("a rejected envelope landed in the keychain")
			}
			// Reservation released: the freed index is handed straight back.
			n, rerr := m.Store.ReserveAccountIndex()
			if rerr != nil {
				t.Fatal(rerr)
			}
			if n != 1 {
				t.Fatalf("next reserved index = %d, want the released 1", n)
			}
			accounts, lerr := m.Store.ListAccounts()
			if lerr != nil {
				t.Fatal(lerr)
			}
			if len(accounts) != 0 {
				t.Fatalf("accounts = %+v, want none after rejection", accounts)
			}
			if len(rec.calls) != 0 {
				t.Fatalf("nudge calls = %v, want none on rejection", rec.calls)
			}
		})
	}
}

// TestMaterializeNeverOverwritesRetainedCredential pins the interrupted-add
// guard: a kept dir whose slot retains a usable credential (from a prior
// ReleaseAdd) aborts before writing identity or pulling — the retained
// credential and identity survive intact and the reservation is released.
func TestMaterializeNeverOverwritesRetainedCredential(t *testing.T) {
	s, m, fk, rec := newMaterializeService(t)
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}

	// A kept dir from an interrupted `ccp add`: its own identity + owned credential.
	keptDir := pool.AccountDir(1)
	if err := os.MkdirAll(keptDir, 0o700); err != nil {
		t.Fatal(err)
	}
	const keptIdentity = `{"oauthAccount":{"accountUuid":"u-kept","emailAddress":"kept@example.com"}}`
	if err := os.WriteFile(filepath.Join(keptDir, ".claude.json"), []byte(keptIdentity), 0o600); err != nil {
		t.Fatal(err)
	}
	retained := cred("at-kept", "rt-kept")
	retained.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli()
	fk.Put(creds.ServiceName(keptDir), "claude-login-label", retained)

	pullFatal := PullCredential(func(context.Context, string, ChainStamp, []string) (*creds.Credential, error) {
		t.Fatal("pull invoked despite a retained slot credential")
		return nil, nil
	})
	oauthAccount := json.RawMessage(`{"accountUuid":"u-peer","emailAddress":"peer@example.com"}`)
	res, err := s.Materialize(context.Background(), materializeVal("u-peer", "peer@example.com", oauthAccount), []string{"hostB"}, pullFatal, materializeManifest)
	if err == nil || !strings.Contains(err.Error(), "retains a credential") {
		t.Fatalf("err = %v, want the retained-credential abort", err)
	}
	if res != (MaterializeResult{}) {
		t.Fatalf("result = %+v, want zero on abort", res)
	}

	// Retained credential intact, identity untouched (WriteIdentity never ran).
	got, ok := fk.Get(creds.ServiceName(keptDir), "claude-login-label")
	if !ok || got.ClaudeAiOauth.RefreshToken != "rt-kept" {
		t.Fatalf("retained credential = %+v ok=%v, want rt-kept intact", got, ok)
	}
	raw, err := os.ReadFile(filepath.Join(keptDir, ".claude.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != keptIdentity {
		t.Fatalf("kept identity mutated: %s", raw)
	}
	// Reservation released, no row, no nudge.
	n, rerr := m.Store.ReserveAccountIndex()
	if rerr != nil {
		t.Fatal(rerr)
	}
	if n != 1 {
		t.Fatalf("next reserved index = %d, want the released 1", n)
	}
	accounts, lerr := m.Store.ListAccounts()
	if lerr != nil {
		t.Fatal(lerr)
	}
	if len(accounts) != 0 {
		t.Fatalf("accounts = %+v, want none", accounts)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("nudge calls = %v, want none", rec.calls)
	}
}

// TestMaterializeEmptyOAuthDefers pins carry-forward #3: an entry with no
// oauthAccount is deferred — no dir, no reservation, no pull — never an error
// loop, so a later scan-publish backfill can supply the identity.
func TestMaterializeEmptyOAuthDefers(t *testing.T) {
	cases := map[string]json.RawMessage{
		"absent": nil,
		"null":   json.RawMessage("null"),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			s, m, _, rec := newMaterializeService(t)
			pullFatal := PullCredential(func(context.Context, string, ChainStamp, []string) (*creds.Credential, error) {
				t.Fatal("pull invoked for a deferred (empty-oauth) entry")
				return nil, nil
			})

			res, err := s.Materialize(context.Background(), materializeVal("u-empty", "e@example.com", raw), []string{"hostB"}, pullFatal, materializeManifest)
			if err != nil {
				t.Fatalf("Materialize: %v, want a clean deferral", err)
			}
			if !res.Deferred || res.UUID != "u-empty" || res.AccountID != 0 {
				t.Fatalf("result = %+v, want Deferred true / u-empty / no acct", res)
			}

			// Nothing materialized: no dir, no reservation, no row, no nudge.
			if _, statErr := os.Stat(pool.AccountDir(1)); !os.IsNotExist(statErr) {
				t.Fatalf("account dir stat err = %v, want not-exist (nothing created)", statErr)
			}
			n, rerr := m.Store.ReserveAccountIndex()
			if rerr != nil {
				t.Fatal(rerr)
			}
			if n != 1 {
				t.Fatalf("next reserved index = %d, want 1 (no reservation was taken)", n)
			}
			accounts, lerr := m.Store.ListAccounts()
			if lerr != nil {
				t.Fatal(lerr)
			}
			if len(accounts) != 0 {
				t.Fatalf("accounts = %+v, want none for a deferred entry", accounts)
			}
			if len(rec.calls) != 0 {
				t.Fatalf("nudge calls = %v, want none for a deferred entry", rec.calls)
			}
		})
	}
}

// recorded reports whether rec captured an exact command invocation.
func recorded(rec *runRecorder, want []string) bool {
	for _, c := range rec.calls {
		if reflect.DeepEqual(c, want) {
			return true
		}
	}
	return false
}

// keysOf returns a map's keys for a readable failure message.
func keysOf(m map[string]json.RawMessage) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}
