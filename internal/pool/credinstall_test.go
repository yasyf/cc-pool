package pool

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/store"
)

// syncCred builds a distinct owned credential per suffix with the given expiry.
func syncCred(suffix string, expiresAt int64) *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.AccessToken = "at-" + suffix
	c.ClaudeAiOauth.RefreshToken = "rt-" + suffix
	c.ClaudeAiOauth.ExpiresAt = expiresAt
	return c
}

// envCred builds a synced envelope (no refresh token) per suffix.
func envCred(suffix string, expiresAt int64) *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.AccessToken = "at-" + suffix
	c.ClaudeAiOauth.ExpiresAt = expiresAt
	return c
}

// installFixture is a Manager over a fake credential seam with a counting
// OnCredWrite hook, plus the account under test.
type installFixture struct {
	m         *Manager
	fk        *credstest.Fake
	a         store.Account
	hookCalls int
	hookCred  *creds.Credential
}

func newInstallFixture(t *testing.T) *installFixture {
	t.Helper()
	f := &installFixture{
		fk: credstest.NewFake(),
		a:  store.Account{ID: 5, ConfigDir: t.TempDir(), KeychainService: "svc-install", KeychainAccount: "user"},
	}
	st := openTestStore(t)
	if err := st.UpsertAccount(f.a); err != nil {
		t.Fatal(err)
	}
	f.m = &Manager{Store: st, Creds: f.fk, LockDir: t.TempDir()}
	f.m.OnCredWrite = func(_ store.Account, cr *creds.Credential) error {
		f.hookCalls++
		f.hookCred = cr
		return nil
	}
	return f
}

// TestInstallSyncedCredentialOwnedPrecedence pins the install gates: an owned
// local blob is NEVER overwritten (even long expired), an absent or
// tombstoned local always installs, a synced local yields only to a strictly
// fresher expiry, and an envelope that still carries a refresh token is
// refused outright.
func TestInstallSyncedCredentialOwnedPrecedence(t *testing.T) {
	const incomingExpiry = 5_000
	tombstone := `{"claudeAiOauth":{"accessToken":"","refreshToken":"","expiresAt":0,"subscriptionType":"max"}}`

	cases := map[string]struct {
		local         *creds.Credential // nil: no local credential
		localTomb     bool              // seed a claude tombstone in the file store
		incoming      *creds.Credential
		wantInstalled bool
		wantErrIs     error
	}{
		"absent local installs": {
			incoming: envCred("in", incomingExpiry), wantInstalled: true,
		},
		"tombstoned local installs": {
			localTomb: true,
			incoming:  envCred("in", incomingExpiry), wantInstalled: true,
		},
		"owned fresher local skips": {
			local:    syncCred("own", 9_000),
			incoming: envCred("in", incomingExpiry),
		},
		"owned local skips even when long expired": {
			local:    syncCred("own", 1), // 1970: provably expired
			incoming: envCred("in", incomingExpiry),
		},
		"synced staler local yields": {
			local:    envCred("old", 1_000),
			incoming: envCred("in", incomingExpiry), wantInstalled: true,
		},
		"synced equal-expiry local skips": {
			local:    envCred("old", incomingExpiry),
			incoming: envCred("in", incomingExpiry),
		},
		"synced fresher local skips": {
			local:    envCred("old", 9_000),
			incoming: envCred("in", incomingExpiry),
		},
		"refresh-token-bearing envelope refused": {
			local:     syncCred("own", 1_000),
			incoming:  syncCred("in", incomingExpiry),
			wantErrIs: ErrEnvelopeCarriesSecret,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newInstallFixture(t)
			if tc.local != nil {
				f.fk.Put(f.a.KeychainService, f.a.KeychainAccount, tc.local)
			}
			if tc.localTomb {
				if err := os.WriteFile(creds.FileCredentialPath(f.a.ConfigDir), []byte(tombstone), 0o600); err != nil {
					t.Fatal(err)
				}
			}

			installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, tc.incoming)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("err = %v, want errors.Is(%v)", err, tc.wantErrIs)
				}
				if installed || f.fk.WriteCount() != 0 || f.hookCalls != 0 {
					t.Fatalf("refused install acted (installed=%v writes=%d hooks=%d)", installed, f.fk.WriteCount(), f.hookCalls)
				}
				return
			}
			if err != nil {
				t.Fatalf("InstallSyncedCredential: %v", err)
			}
			if installed != tc.wantInstalled {
				t.Fatalf("installed = %v, want %v", installed, tc.wantInstalled)
			}

			got, ok := f.fk.Get(f.a.KeychainService, f.a.KeychainAccount)
			if tc.wantInstalled {
				if !ok || got.ClaudeAiOauth.AccessToken != tc.incoming.ClaudeAiOauth.AccessToken {
					t.Fatalf("keychain holds %+v, want the incoming credential", got)
				}
				if got.HasRefreshToken() {
					t.Fatal("installed blob carries a refresh token")
				}
				if f.hookCalls != 1 || f.hookCred != tc.incoming {
					t.Fatalf("OnCredWrite calls=%d cred=%p, want 1 with the installed credential", f.hookCalls, f.hookCred)
				}
				return
			}
			if f.hookCalls != 0 || f.fk.WriteCount() != 0 {
				t.Fatalf("skip acted (writes=%d hooks=%d), want none", f.fk.WriteCount(), f.hookCalls)
			}
			if tc.local != nil {
				if !ok || got.ClaudeAiOauth.AccessToken != tc.local.ClaudeAiOauth.AccessToken {
					t.Fatalf("keychain holds %+v after a skip, want the local credential untouched", got)
				}
				if got.ClaudeAiOauth.RefreshToken != tc.local.ClaudeAiOauth.RefreshToken {
					t.Fatalf("local refresh token = %q, want %q untouched", got.ClaudeAiOauth.RefreshToken, tc.local.ClaudeAiOauth.RefreshToken)
				}
			}
		})
	}
}

// TestInstallSyncedCredentialFollowsBackendResolution pins that the install
// writes to the backend resolution picks: a file-backed account gets the file
// write and the Keychain is never touched.
func TestInstallSyncedCredentialFollowsBackendResolution(t *testing.T) {
	f := newInstallFixture(t)
	local := envCred("local", 1_000)
	if err := (creds.FileStore{ConfigDir: f.a.ConfigDir}).Write(local); err != nil {
		t.Fatalf("seed file credential: %v", err)
	}
	incoming := envCred("incoming", 2_000)

	installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, incoming)
	if err != nil {
		t.Fatalf("InstallSyncedCredential: %v", err)
	}
	if !installed {
		t.Fatal("installed = false, want true")
	}
	got, err := (creds.FileStore{ConfigDir: f.a.ConfigDir}).Read()
	if err != nil {
		t.Fatalf("read file credential back: %v", err)
	}
	if got.ClaudeAiOauth.AccessToken != incoming.ClaudeAiOauth.AccessToken {
		t.Fatalf("file backend holds %q, want the incoming credential", got.ClaudeAiOauth.AccessToken)
	}
	if f.fk.WriteCount() != 0 {
		t.Fatalf("keychain writes = %d, want 0 (install must stay on the file backend)", f.fk.WriteCount())
	}
	if _, ok := f.fk.Get(f.a.KeychainService, f.a.KeychainAccount); ok {
		t.Fatal("keychain gained an item; the install must not mirror across backends")
	}
	if f.hookCalls != 1 {
		t.Fatalf("OnCredWrite fired %d times, want 1", f.hookCalls)
	}
}

// TestInstallSyncedCredentialRefusesUnknowableKeychain pins the fail-fast on
// creds.ErrUnavailable: no write, no hook — a hidden fresher chain is never shadowed.
func TestInstallSyncedCredentialRefusesUnknowableKeychain(t *testing.T) {
	f := newInstallFixture(t)
	f.fk.KeychainFaults = credstest.Faults{Read: creds.ErrUnavailable}
	incoming := envCred("incoming", 5_000)

	installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, incoming)
	if installed {
		t.Fatal("installed = true, want false")
	}
	if !errors.Is(err, creds.ErrUnavailable) {
		t.Fatalf("err = %v, want errors.Is creds.ErrUnavailable", err)
	}
	if f.hookCalls != 0 {
		t.Fatalf("OnCredWrite fired %d times, want 0", f.hookCalls)
	}
	if f.fk.WriteCount() != 0 {
		t.Fatalf("keychain writes = %d, want 0", f.fk.WriteCount())
	}
}

// TestInstallSyncedCredentialConcurrentRotationWins pins the owned-precedence
// re-check across the lock-free window: a local login landing before the
// install wins outright — the local chain is owned, so the pull is skipped.
func TestInstallSyncedCredentialConcurrentRotationWins(t *testing.T) {
	f := newInstallFixture(t)
	f.fk.Put(f.a.KeychainService, f.a.KeychainAccount, envCred("old", 1_000))
	// Verified lock-free against a synced expiry 1_000, so 2_000 looked strictly fresher.
	incoming := envCred("incoming", 2_000)
	rotated := syncCred("rotated", 3_000)

	release, err := f.m.lockAccount(context.Background(), f.a.ID)
	if err != nil {
		t.Fatalf("lockAccount: %v", err)
	}
	type result struct {
		installed bool
		err       error
	}
	done := make(chan result, 1)
	go func() {
		installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, incoming)
		done <- result{installed, err}
	}()
	// While the install is (or will be) blocked on the account lock, a local
	// login mints an owned chain.
	f.fk.Put(f.a.KeychainService, f.a.KeychainAccount, rotated)
	release()

	res := <-done
	if res.err != nil {
		t.Fatalf("InstallSyncedCredential: %v", res.err)
	}
	if res.installed {
		t.Fatal("installed = true; the concurrent login must win")
	}
	got, ok := f.fk.Get(f.a.KeychainService, f.a.KeychainAccount)
	if !ok || got.ClaudeAiOauth.AccessToken != rotated.ClaudeAiOauth.AccessToken {
		t.Fatalf("keychain holds %+v, want the rotated credential", got)
	}
	if f.hookCalls != 0 {
		t.Fatalf("OnCredWrite fired %d times, want 0 (nothing was installed)", f.hookCalls)
	}
}

// swapStore serves scripted keychain reads: the first read (the locked probe)
// returns old/oldErr, later reads return swapped — a `claude /login` landing
// between the probe and the CAS re-read, outside every lock cc-pool holds.
type swapStore struct {
	creds.Store
	old, swapped *creds.Credential
	oldErr       error
	reads        int
}

func (s *swapStore) Read() (*creds.Credential, error) {
	s.reads++
	if s.reads == 1 {
		return s.old, s.oldErr
	}
	return s.swapped, nil
}

// swapCreds routes the keychain source to one shared swapStore instance.
type swapCreds struct {
	*credstest.Fake
	ks creds.Store
}

func (c swapCreds) Store(a store.Account, src creds.Source) creds.Store {
	if src == creds.SourceKeychain {
		return c.ks
	}
	return c.Fake.Store(a, src)
}

func (c swapCreds) Stores(a store.Account) []creds.Store {
	return []creds.Store{c.Store(a, creds.SourceKeychain), c.Store(a, creds.SourceFile)}
}

// TestInstallSyncedCredentialCASAbortsOnUnderfootLogin pins the CAS discipline:
// a `claude /login` landing between the locked read and the write aborts the
// install as a clean skip, the login's chain untouched.
func TestInstallSyncedCredentialCASAbortsOnUnderfootLogin(t *testing.T) {
	f := newInstallFixture(t)
	old := envCred("old", 1_000) // synced, so the gate reaches the CAS
	login := syncCred("login", 2_000)
	f.fk.Put(f.a.KeychainService, f.a.KeychainAccount, old)
	ks := &swapStore{Store: f.fk.Store(f.a, creds.SourceKeychain), old: old, swapped: login}
	f.m.Creds = swapCreds{Fake: f.fk, ks: ks}

	incoming := envCred("incoming", 5_000) // strictly fresher than old: gate passes

	installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, incoming)
	if err != nil {
		t.Fatalf("a CAS abort must be a clean skip, got: %v", err)
	}
	if installed {
		t.Fatal("installed = true; the underfoot login must win")
	}
	if ks.reads < 2 {
		t.Fatalf("CAS re-read never happened (reads = %d)", ks.reads)
	}
	if got, ok := f.fk.Get(f.a.KeychainService, f.a.KeychainAccount); !ok || got.ClaudeAiOauth.AccessToken != "at-old" {
		t.Fatalf("backing store = %+v; an aborted install must write nothing", got)
	}
	if f.hookCalls != 0 {
		t.Fatalf("OnCredWrite fired %d times on an aborted install", f.hookCalls)
	}
}

// TestInstallSyncedCredentialAbortsOnLoginOverEmptySlot pins the empty-slot
// install race: the precedence read proves the slot empty (absent, or a claude
// tombstone), a `claude /login` lands an owned chain before the write, and the
// CAS re-read must refuse to bury it — a clean skip, nothing written.
func TestInstallSyncedCredentialAbortsOnLoginOverEmptySlot(t *testing.T) {
	tombstone := `{"claudeAiOauth":{"accessToken":"","refreshToken":"","expiresAt":0}}`
	cases := map[string]struct {
		seedTombstone bool
	}{
		"absent slot":     {},
		"tombstoned slot": {seedTombstone: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newInstallFixture(t)
			if tc.seedTombstone {
				if err := os.WriteFile(creds.FileCredentialPath(f.a.ConfigDir), []byte(tombstone), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			login := syncCred("login", 2_000)
			ks := &swapStore{Store: f.fk.Store(f.a, creds.SourceKeychain), oldErr: creds.ErrNotFound, swapped: login}
			f.m.Creds = swapCreds{Fake: f.fk, ks: ks}

			installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, envCred("incoming", 5_000))
			if err != nil {
				t.Fatalf("a CAS abort must be a clean skip, got: %v", err)
			}
			if installed {
				t.Fatal("installed = true; the underfoot login must win")
			}
			if ks.reads < 2 {
				t.Fatalf("CAS re-read never happened (reads = %d)", ks.reads)
			}
			if f.fk.WriteCount() != 0 || f.hookCalls != 0 {
				t.Fatalf("aborted install acted (writes=%d hooks=%d), want none", f.fk.WriteCount(), f.hookCalls)
			}
		})
	}
}

// TestWriteCredCASWritesThroughTombstone pins the CAS behavior the
// install-over-tombstone path depends on: a prior read that fails to parse
// (claude tombstone) or misses entirely must not block the write.
func TestWriteCredCASWritesThroughTombstone(t *testing.T) {
	f := newInstallFixture(t)
	tombstone := `{"claudeAiOauth":{"accessToken":"","refreshToken":"","expiresAt":0}}`
	if err := os.WriteFile(creds.FileCredentialPath(f.a.ConfigDir), []byte(tombstone), 0o600); err != nil {
		t.Fatal(err)
	}

	next := envCred("healed", 4_000)
	if err := f.m.writeCredCAS(f.a, creds.SourceFile, nil, next); err != nil {
		t.Fatalf("writeCredCAS over a tombstone = %v, want write-through", err)
	}
	got, err := (creds.FileStore{ConfigDir: f.a.ConfigDir}).Read()
	if err != nil {
		t.Fatalf("read back after tombstone overwrite: %v", err)
	}
	if got.ClaudeAiOauth.AccessToken != "at-healed" {
		t.Fatalf("file backend holds %q, want at-healed", got.ClaudeAiOauth.AccessToken)
	}
}
