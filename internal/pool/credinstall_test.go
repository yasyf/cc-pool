package pool

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

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
// exact credential-write settlement, plus the account under test.
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
	f.a = persistTestAccount(t, st, f.a)
	f.m = &Manager{Store: st, Creds: f.fk}
	bindTestWorkerAuthority(t, f.m, "install")
	f.m.BuildCredentialWritePublication = func(
		_ store.Account,
		credential *creds.Credential,
		_ store.CredentialOperationID,
		_ time.Time,
	) ([]byte, error) {
		copy := *credential
		f.hookCred = &copy
		return []byte(`{"test":"credential-write"}`), nil
	}
	f.m.SettleCredentialWrite = func(_ context.Context, settlement CredentialWriteSettlement) error {
		f.hookCalls++
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
		"tokenless envelope refused": {
			local:     envCred("old", 1_000),
			incoming:  &creds.Credential{ClaudeAiOauth: creds.OAuth{ExpiresAt: incomingExpiry}},
			wantErrIs: ErrEnvelopeNoAccessToken,
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
				if f.hookCalls != 1 || !sameTokens(f.hookCred, tc.incoming) {
					t.Fatalf("credential settlements=%d cred=%+v, want 1 with the installed credential", f.hookCalls, f.hookCred)
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
	if err := credstest.FileStore(f.a.ConfigDir).Write(t.Context(), local); err != nil {
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
	got, err := credstest.FileStore(f.a.ConfigDir).Read(t.Context())
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
		t.Fatalf("credential settlement fired %d times, want 1", f.hookCalls)
	}
}

// TestInstallSyncedCredentialHeadlessKeychainUnavailable pins the headless
// rotation path: an unsearchable login keychain (creds.ErrUnavailable) is a
// file-store fallback mirroring installEnvelope, never an ErrUnavailable
// abort pullAndInstall would defer forever — a headless peer must keep
// receiving rotations after the one-time materialize. Owned precedence and
// the freshness gate still hold against the readable file store.
func TestInstallSyncedCredentialHeadlessKeychainUnavailable(t *testing.T) {
	tombstone := `{"claudeAiOauth":{"accessToken":"","refreshToken":"","expiresAt":0}}`
	cases := map[string]struct {
		file          *creds.Credential // nil: no file credential
		fileTomb      bool
		wantInstalled bool
	}{
		"rotation over synced file blob installs": {
			file: envCred("old", 1_000), wantInstalled: true,
		},
		"empty file store installs":      {wantInstalled: true},
		"tombstoned file store installs": {fileTomb: true, wantInstalled: true},
		"stale rotation skips":           {file: envCred("old", 9_000)},
		"owned file chain skips":         {file: syncCred("own", 1_000)},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newInstallFixture(t)
			f.fk.KeychainFaults = credstest.Faults{Read: creds.ErrUnavailable}
			fileStore := credstest.FileStore(f.a.ConfigDir)
			if tc.file != nil {
				if err := fileStore.Write(t.Context(), tc.file); err != nil {
					t.Fatal(err)
				}
			}
			if tc.fileTomb {
				if err := os.WriteFile(creds.FileCredentialPath(f.a.ConfigDir), []byte(tombstone), 0o600); err != nil {
					t.Fatal(err)
				}
			}

			installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, envCred("incoming", 5_000))
			if err != nil {
				t.Fatalf("InstallSyncedCredential: %v (headless install must not abort)", err)
			}
			if installed != tc.wantInstalled {
				t.Fatalf("installed = %v, want %v", installed, tc.wantInstalled)
			}
			if f.fk.WriteCount() != 0 {
				t.Fatalf("keychain writes = %d, want 0 (headless install must stay on the file backend)", f.fk.WriteCount())
			}
			if !tc.wantInstalled {
				if f.hookCalls != 0 {
					t.Fatalf("credential settlement fired %d times on a skip, want 0", f.hookCalls)
				}
				got, rerr := fileStore.Read(t.Context())
				if rerr != nil || got.ClaudeAiOauth.AccessToken != tc.file.ClaudeAiOauth.AccessToken ||
					got.ClaudeAiOauth.RefreshToken != tc.file.ClaudeAiOauth.RefreshToken {
					t.Fatalf("file backend = (%+v, %v), want the local credential untouched", got, rerr)
				}
				return
			}
			got, rerr := fileStore.Read(t.Context())
			if rerr != nil || got.ClaudeAiOauth.AccessToken != "at-incoming" {
				t.Fatalf("file backend = (%+v, %v), want the incoming rotation", got, rerr)
			}
			if got.HasRefreshToken() {
				t.Fatal("installed blob carries a refresh token")
			}
			if f.hookCalls != 1 {
				t.Fatalf("credential settlement fired %d times, want 1", f.hookCalls)
			}
		})
	}
}

// swapStore serves scripted keychain reads: the durable operation's observation
// and the operation's first probe return old/oldErr, then later reads return
// swapped/swappedErr — a `claude` login or logout landing between the probe and
// any later re-read, outside every lane cc-pool owns.
type swapStore struct {
	creds.Store
	old, swapped       *creds.Credential
	oldErr, swappedErr error
	reads              int
}

func (s *swapStore) Read(context.Context) (*creds.Credential, error) {
	s.reads++
	if s.reads <= 2 {
		return s.old, s.oldErr
	}
	return s.swapped, s.swappedErr
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
// a `claude /login` landing between the initial read and the write aborts the
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
		t.Fatalf("credential settlement fired %d times on an aborted install", f.hookCalls)
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

// TestInstallSyncedCredentialSkipsOnOwnedOtherBackend pins the all-backend
// owned re-check: resolution picks the fresher synced file copy, but a staler
// OWNED chain on the keychain means this host owns the account — installing
// would hide the owned chain behind fresher-wins resolution and let cleanup
// delete it.
func TestInstallSyncedCredentialSkipsOnOwnedOtherBackend(t *testing.T) {
	f := newInstallFixture(t)
	f.fk.Put(f.a.KeychainService, f.a.KeychainAccount, syncCred("owned", 1_000))
	if err := credstest.FileStore(f.a.ConfigDir).Write(t.Context(), envCred("synced", 3_000)); err != nil {
		t.Fatal(err)
	}

	installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, envCred("incoming", 5_000))
	if err != nil {
		t.Fatalf("InstallSyncedCredential: %v", err)
	}
	if installed {
		t.Fatal("installed = true; an owned chain on any backend must win")
	}
	if f.fk.WriteCount() != 0 || f.hookCalls != 0 {
		t.Fatalf("skip acted (writes=%d hooks=%d), want none", f.fk.WriteCount(), f.hookCalls)
	}
	if got, ok := f.fk.Get(f.a.KeychainService, f.a.KeychainAccount); !ok || got.ClaudeAiOauth.RefreshToken != "rt-owned" {
		t.Fatalf("keychain holds %+v, want the owned chain untouched", got)
	}
	if got, err := credstest.FileStore(f.a.ConfigDir).Read(t.Context()); err != nil || got.ClaudeAiOauth.AccessToken != "at-synced" {
		t.Fatalf("file backend = (%+v, %v), want the synced copy untouched", got, err)
	}
}

// TestInstallSyncedCredentialFailsClosedOnUnverifiableBackend pins the
// fail-closed owned re-check: an opaque backend read error — not proven-absent
// (ErrNotFound), a tombstone (ErrNoTokens), or an unsearchable backend
// (ErrUnavailable, the headless file fallback) — may hide an owned chain, so
// the install aborts with ErrCredentialUnverifiable and writes nothing.
func TestInstallSyncedCredentialFailsClosedOnUnverifiableBackend(t *testing.T) {
	errOpaque := errors.New("backend query exploded")
	cases := map[string]struct {
		opaqueFile bool // opaque re-check read on the file store instead of the keychain
	}{
		"keychain degrading after the probe": {},
		"opaque file store":                  {opaqueFile: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newInstallFixture(t)
			if tc.opaqueFile {
				f.fk.Put(f.a.KeychainService, f.a.KeychainAccount, envCred("synced", 1_000))
				f.fk.FileFaults = credstest.Faults{Read: errOpaque}
			} else {
				if err := credstest.FileStore(f.a.ConfigDir).Write(t.Context(), envCred("synced", 1_000)); err != nil {
					t.Fatal(err)
				}
				ks := &swapStore{Store: f.fk.Store(f.a, creds.SourceKeychain), oldErr: creds.ErrNotFound, swappedErr: errOpaque}
				f.m.Creds = swapCreds{Fake: f.fk, ks: ks}
			}

			installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, envCred("incoming", 5_000))
			if installed {
				t.Fatal("installed = true; an unverifiable backend must abort the install")
			}
			if !errors.Is(err, ErrCredentialUnverifiable) {
				t.Fatalf("err = %v, want errors.Is(ErrCredentialUnverifiable)", err)
			}
			if !errors.Is(err, errOpaque) {
				t.Fatalf("err = %v, want errors.Is(%v)", err, errOpaque)
			}
			if f.fk.WriteCount() != 0 || f.hookCalls != 0 {
				t.Fatalf("aborted install acted (writes=%d hooks=%d), want none", f.fk.WriteCount(), f.hookCalls)
			}
		})
	}
}

// TestInstallSyncedCredentialAbortsOnLoginDuringInstall pins the pre-write
// re-probe against the racing login: the precedence read proves the keychain
// empty and picks the synced file copy, a `claude /login` lands an owned
// chain on the keychain before the write, and the file-store CAS alone would
// never see it.
func TestInstallSyncedCredentialAbortsOnLoginDuringInstall(t *testing.T) {
	f := newInstallFixture(t)
	if err := credstest.FileStore(f.a.ConfigDir).Write(t.Context(), envCred("synced", 1_000)); err != nil {
		t.Fatal(err)
	}
	login := syncCred("login", 2_000)
	ks := &swapStore{Store: f.fk.Store(f.a, creds.SourceKeychain), oldErr: creds.ErrNotFound, swapped: login}
	f.m.Creds = swapCreds{Fake: f.fk, ks: ks}

	installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, envCred("incoming", 5_000))
	if err != nil {
		t.Fatalf("an owned-underfoot skip must be clean, got: %v", err)
	}
	if installed {
		t.Fatal("installed = true; the underfoot login must win")
	}
	if ks.reads < 2 {
		t.Fatalf("owned re-probe never happened (reads = %d)", ks.reads)
	}
	if f.fk.WriteCount() != 0 || f.hookCalls != 0 {
		t.Fatalf("aborted install acted (writes=%d hooks=%d), want none", f.fk.WriteCount(), f.hookCalls)
	}
	if got, err := credstest.FileStore(f.a.ConfigDir).Read(t.Context()); err != nil || got.ClaudeAiOauth.AccessToken != "at-synced" {
		t.Fatalf("file backend = (%+v, %v), want the synced copy untouched", got, err)
	}
}
