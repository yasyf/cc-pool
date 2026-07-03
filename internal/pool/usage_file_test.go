package pool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/store"
)

// TestReadCredentialPrecedence pins backend resolution: when both backends hold
// a credential the FRESHER (later-expiring) one wins, with the Keychain breaking
// exact-expiry ties (so a fresh headless re-login in the file is never shadowed
// by a stale Keychain item); creds.ErrUnavailable (an unsearchable login
// keychain) must outrank creds.ErrNotFound so headless absence is never
// misreported as signed-out; and a hard Keychain error fails fast without
// consulting the file.
func TestReadCredentialPrecedence(t *testing.T) {
	errHard := errors.New("keychain exploded")
	cases := []struct {
		name        string
		keychain    bool          // seed the keychain item
		keychainErr error         // injected keychain read fault
		file        bool          // seed the file credential
		kcExp       time.Duration // keychain token expiry offset from now (0 => shared epoch tie)
		fileExp     time.Duration // file token expiry offset from now
		wantToken   string
		wantSrc     creds.Source
		wantErr     error // sentinel matched with errors.Is; nil means success
	}{
		{name: "equal expiry breaks to keychain", keychain: true, file: true, wantToken: "at-kc", wantSrc: creds.SourceKeychain},
		{name: "fresher file wins over a stale keychain shadow", keychain: true, file: true, kcExp: time.Hour, fileExp: 3 * time.Hour, wantToken: "at-file", wantSrc: creds.SourceFile},
		{name: "fresher keychain wins over a stale file", keychain: true, file: true, kcExp: 3 * time.Hour, fileExp: time.Hour, wantToken: "at-kc", wantSrc: creds.SourceKeychain},
		{name: "keychain miss falls through to file", file: true, wantToken: "at-file", wantSrc: creds.SourceFile},
		{name: "unavailable keychain falls through to file", keychainErr: creds.ErrUnavailable, file: true, wantToken: "at-file", wantSrc: creds.SourceFile},
		{name: "unavailable keychain and no file surfaces unavailable", keychainErr: creds.ErrUnavailable, wantErr: creds.ErrUnavailable},
		{name: "both missing is not-found", wantErr: creds.ErrNotFound},
		{name: "hard keychain error fails fast before the file", keychainErr: errHard, file: true, wantErr: errHard},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			a := store.Account{ID: 1, ConfigDir: dir, KeychainService: "svc", KeychainAccount: "user"}
			fk := credstest.NewFake()
			fk.KeychainFaults = credstest.Faults{Read: tc.keychainErr}
			if tc.keychain {
				kc := &creds.Credential{}
				kc.ClaudeAiOauth.AccessToken = "at-kc"
				if tc.kcExp != 0 {
					kc.ClaudeAiOauth.ExpiresAt = time.Now().Add(tc.kcExp).UnixMilli()
				}
				fk.Put(a.KeychainService, a.KeychainAccount, kc)
			}
			if tc.file {
				fc := &creds.Credential{}
				fc.ClaudeAiOauth.AccessToken = "at-file"
				if tc.fileExp != 0 {
					fc.ClaudeAiOauth.ExpiresAt = time.Now().Add(tc.fileExp).UnixMilli()
				}
				if err := creds.WriteFileCredential(dir, fc); err != nil {
					t.Fatal(err)
				}
			}
			m := &Manager{Creds: fk}

			cred, src, err := m.ReadCredential(a)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want errors.Is(%v)", err, tc.wantErr)
				}
				if errors.Is(tc.wantErr, creds.ErrUnavailable) && errors.Is(err, creds.ErrNotFound) {
					t.Fatalf("err = %v also matches ErrNotFound; unavailability must not read as signed-out", err)
				}
				if cred != nil {
					t.Fatalf("cred = %+v, want nil on error", cred)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := cred.ClaudeAiOauth.AccessToken; got != tc.wantToken {
				t.Errorf("token = %q, want %q", got, tc.wantToken)
			}
			if src != tc.wantSrc {
				t.Errorf("source = %v, want %v", src, tc.wantSrc)
			}
		})
	}
}

// TestWriteCredRoutesBySource pins that the paired write stays on the backend
// the read resolved: a file-source write must never create a Keychain item and
// vice versa.
func TestWriteCredRoutesBySource(t *testing.T) {
	cases := []struct {
		name     string
		src      creds.Source
		wantFile bool // the credential must land in the file, else the keychain
	}{
		{name: "file source writes only the file", src: creds.SourceFile, wantFile: true},
		{name: "keychain source writes only the keychain", src: creds.SourceKeychain, wantFile: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			a := store.Account{ID: 1, ConfigDir: dir, KeychainService: "svc", KeychainAccount: "user"}
			fk := credstest.NewFake()
			m := &Manager{Creds: fk}
			cred := &creds.Credential{}
			cred.ClaudeAiOauth.AccessToken = "at-1"

			if err := m.writeCred(a, tc.src, cred); err != nil {
				t.Fatal(err)
			}
			if got := creds.FileCredentialExists(dir); got != tc.wantFile {
				t.Errorf("file credential exists = %v, want %v", got, tc.wantFile)
			}
			if _, inKeychain := fk.Get(a.KeychainService, a.KeychainAccount); inKeychain == tc.wantFile {
				t.Errorf("keychain item exists = %v, want %v", inKeychain, !tc.wantFile)
			}
		})
	}
}

// TestRefreshUsesFileBackendWhenKeychainEmpty pins the headless path: with the
// credential in claude's plaintext .credentials.json and the Keychain empty, a
// refresh reads and writes the file backend and never touches the Keychain.
func TestRefreshUsesFileBackendWhenKeychainEmpty(t *testing.T) {
	st := openTestStore(t)
	dir := t.TempDir()
	a := store.Account{ID: 1, ConfigDir: dir, KeychainService: creds.ServiceName(dir), KeychainAccount: "user"}

	seed := &creds.Credential{}
	seed.ClaudeAiOauth.AccessToken = "at-0"
	seed.ClaudeAiOauth.RefreshToken = "rt-0"
	seed.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Minute).UnixMilli()
	if err := creds.WriteFileCredential(dir, seed); err != nil {
		t.Fatal(err)
	}

	fk := credstest.NewFake()
	fo := &fakeOAuth{currentRT: "rt-0"}
	m := &Manager{Store: st, OAuth: fo, Creds: fk, LockDir: t.TempDir()}

	cred, refreshed, err := m.EnsureFreshToken(context.Background(), a, RefreshLeadTime, true)
	if err != nil {
		t.Fatal(err)
	}
	if !refreshed {
		t.Fatal("near-expiry file-backed credential was not refreshed")
	}
	if cred.ClaudeAiOauth.AccessToken != "at-1" {
		t.Fatalf("returned access token = %q, want at-1", cred.ClaudeAiOauth.AccessToken)
	}

	onDisk, err := creds.ReadFileCredential(dir)
	if err != nil {
		t.Fatal(err)
	}
	if onDisk.ClaudeAiOauth.AccessToken != "at-1" || onDisk.ClaudeAiOauth.RefreshToken != "rt-1" {
		t.Fatalf("file backend not updated by refresh: %+v", onDisk.ClaudeAiOauth)
	}

	if _, ok := fk.Get(a.KeychainService, a.KeychainAccount); ok {
		t.Fatal("refresh wrote the credential to the Keychain instead of the file")
	}
}
