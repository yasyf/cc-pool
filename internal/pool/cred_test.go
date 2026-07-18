package pool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
)

// moveCred builds the credential the move tests seed and expect back. It is
// expired on purpose: a move transfers the credential as-is (the refresh token
// is the asset, not the access token), and the Manager under test carries a
// nil OAuth client so any refresh attempt panics the test.
func moveCred() *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.AccessToken = "at-1"
	c.ClaudeAiOauth.RefreshToken = "rt-1"
	c.ClaudeAiOauth.ExpiresAt = 1_700_000_000_000
	return c
}

// TestMoveCredential drives every MoveCredential outcome: real moves in both
// directions, both no-ops, stray-file cleanup, the three refusals, and the
// unwind/rollback fault paths. No-op and refusal cases inject Write faults on
// BOTH backends so any write attempt fails the case.
func TestMoveCredential(t *testing.T) {
	var (
		errNoWrite    = errors.New("unexpected write: this case must not write")
		errKCDelete   = errors.New("keychain delete exploded")
		errFileDelete = errors.New("file delete exploded")
		errFileRead   = errors.New("file read exploded")
	)
	noWrites := credstest.Faults{Write: errNoWrite}

	cases := []struct {
		name        string
		seedKC      bool
		seedFile    bool
		corruptFile bool
		kcFaults    credstest.Faults
		fileFaults  credstest.Faults
		target      creds.Source

		want          *CredMove // nil: an error is expected
		wantErrIs     []error
		wantErrSubstr []string
		wantKC        bool // keychain item present afterwards
		wantFile      bool // file credential present afterwards
		wantKCWrites  int
	}{
		{
			name:     "keychain to file moves the item",
			seedKC:   true,
			target:   creds.SourceFile,
			want:     &CredMove{From: creds.SourceKeychain, To: creds.SourceFile, Moved: true},
			wantFile: true,
		},
		{
			name:         "file to keychain moves the file",
			seedFile:     true,
			target:       creds.SourceKeychain,
			want:         &CredMove{From: creds.SourceFile, To: creds.SourceKeychain, Moved: true},
			wantKC:       true,
			wantKCWrites: 1,
		},
		{
			name:       "already on keychain is a no-op",
			seedKC:     true,
			kcFaults:   noWrites,
			fileFaults: noWrites,
			target:     creds.SourceKeychain,
			want:       &CredMove{From: creds.SourceKeychain, To: creds.SourceKeychain},
			wantKC:     true,
		},
		{
			name:       "already on file is a no-op",
			seedFile:   true,
			kcFaults:   noWrites,
			fileFaults: noWrites,
			target:     creds.SourceFile,
			want:       &CredMove{From: creds.SourceFile, To: creds.SourceFile},
			wantFile:   true,
		},
		{
			name:       "already on keychain deletes a stray file copy",
			seedKC:     true,
			seedFile:   true,
			kcFaults:   noWrites,
			fileFaults: noWrites,
			target:     creds.SourceKeychain,
			want:       &CredMove{From: creds.SourceKeychain, To: creds.SourceKeychain, CleanedStray: true},
			wantKC:     true,
		},
		{
			name:        "corrupt stray file still counts as a stray",
			seedKC:      true,
			corruptFile: true,
			kcFaults:    noWrites,
			fileFaults:  noWrites,
			target:      creds.SourceKeychain,
			want:        &CredMove{From: creds.SourceKeychain, To: creds.SourceKeychain, CleanedStray: true},
			wantKC:      true,
		},
		{
			name:          "no credential anywhere refuses with a login hint",
			kcFaults:      noWrites,
			fileFaults:    noWrites,
			target:        creds.SourceFile,
			wantErrIs:     []error{creds.ErrNotFound},
			wantErrSubstr: []string{"ccp login 1"},
		},
		{
			name:          "unavailable keychain with no file refuses",
			kcFaults:      credstest.Faults{Read: creds.ErrUnavailable, Write: errNoWrite},
			fileFaults:    noWrites,
			target:        creds.SourceFile,
			wantErrIs:     []error{creds.ErrUnavailable},
			wantErrSubstr: []string{"safe move is impossible"},
		},
		{
			name:       "file-backed with unavailable keychain no-ops a file move",
			seedFile:   true,
			kcFaults:   credstest.Faults{Read: creds.ErrUnavailable, Write: errNoWrite},
			fileFaults: noWrites,
			target:     creds.SourceFile,
			want:       &CredMove{From: creds.SourceFile, To: creds.SourceFile},
			wantFile:   true,
		},
		{
			name:          "file-backed with unavailable keychain refuses a keychain move",
			seedFile:      true,
			kcFaults:      credstest.Faults{Read: creds.ErrUnavailable, Write: errNoWrite},
			fileFaults:    noWrites,
			target:        creds.SourceKeychain,
			wantErrIs:     []error{creds.ErrUnavailable},
			wantErrSubstr: []string{"could not be verified"},
			wantFile:      true,
		},
		{
			name:       "readback failure unwinds the target copy",
			seedKC:     true,
			fileFaults: credstest.Faults{Read: errFileRead},
			target:     creds.SourceFile,
			wantErrIs:  []error{errFileRead},
			wantKC:     true, // source untouched
		},
		{
			name:          "source delete failure rolls back the target",
			seedKC:        true,
			kcFaults:      credstest.Faults{Delete: errKCDelete},
			target:        creds.SourceFile,
			wantErrIs:     []error{errKCDelete},
			wantErrSubstr: []string{`fake keychain item "svc-move"`, ".credentials.json"},
			wantKC:        true, // exactly one copy remains: the source
		},
		{
			name:          "failed rollback names both locations",
			seedKC:        true,
			kcFaults:      credstest.Faults{Delete: errKCDelete},
			fileFaults:    credstest.Faults{Delete: errFileDelete},
			target:        creds.SourceFile,
			wantErrIs:     []error{errKCDelete, errFileDelete},
			wantErrSubstr: []string{`fake keychain item "svc-move"`, ".credentials.json"},
			wantKC:        true,
			wantFile:      true, // double fault: both copies remain, both named
		},
		{
			name:          "unknown backend target refused",
			seedKC:        true,
			kcFaults:      noWrites,
			fileFaults:    noWrites,
			target:        creds.Source(42),
			wantErrSubstr: []string{"unknown credential backend"},
			wantKC:        true,
		},
		{
			name:          "corrupt file with empty keychain fails resolution",
			corruptFile:   true,
			kcFaults:      noWrites,
			fileFaults:    noWrites,
			target:        creds.SourceKeychain,
			wantErrSubstr: []string{"parse credential blob"},
			wantFile:      true, // untouched
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			a := store.Account{ID: 1, ConfigDir: dir, KeychainService: "svc-move", KeychainAccount: "user"}
			fk := credstest.NewFake()
			fk.KeychainFaults = tc.kcFaults
			fk.FileFaults = tc.fileFaults
			if tc.seedKC {
				fk.Put(a.KeychainService, a.KeychainAccount, moveCred())
			}
			if tc.seedFile {
				if err := creds.WriteFileCredential(dir, moveCred()); err != nil {
					t.Fatal(err)
				}
			}
			if tc.corruptFile {
				if err := os.WriteFile(creds.FileCredentialPath(dir), []byte("{not json"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			m := &Manager{Creds: fk, LockDir: t.TempDir()}

			got, err := m.MoveCredential(context.Background(), a, tc.target)
			if tc.want == nil {
				if err == nil {
					t.Fatalf("MoveCredential = %+v, want error", got)
				}
				for _, sentinel := range tc.wantErrIs {
					if !errors.Is(err, sentinel) {
						t.Errorf("error %q does not match %q", err, sentinel)
					}
				}
				for _, sub := range tc.wantErrSubstr {
					if !strings.Contains(err.Error(), sub) {
						t.Errorf("error %q missing %q", err, sub)
					}
				}
			} else {
				if err != nil {
					t.Fatalf("MoveCredential: %v", err)
				}
				if *got != *tc.want {
					t.Errorf("MoveCredential = %+v, want %+v", *got, *tc.want)
				}
			}

			if _, ok := fk.Get(a.KeychainService, a.KeychainAccount); ok != tc.wantKC {
				t.Errorf("keychain item present = %v, want %v", ok, tc.wantKC)
			}
			if present := creds.FileCredentialExists(dir); present != tc.wantFile {
				t.Errorf("file credential present = %v, want %v", present, tc.wantFile)
			}
			if n := fk.WriteCount(); n != tc.wantKCWrites {
				t.Errorf("keychain writes = %d, want %d", n, tc.wantKCWrites)
			}

			if tc.want == nil || !tc.want.Moved {
				return
			}
			moved, rerr := readMovedCredential(fk, a, tc.target)
			if rerr != nil {
				t.Fatalf("read moved credential: %v", rerr)
			}
			seeded := moveCred()
			if moved.ClaudeAiOauth.AccessToken != seeded.ClaudeAiOauth.AccessToken ||
				moved.ClaudeAiOauth.RefreshToken != seeded.ClaudeAiOauth.RefreshToken ||
				moved.ClaudeAiOauth.ExpiresAt != seeded.ClaudeAiOauth.ExpiresAt {
				t.Errorf("moved credential = %+v, want tokens/expiry of %+v (moved as-is, never refreshed)",
					moved.ClaudeAiOauth, seeded.ClaudeAiOauth)
			}
		})
	}
}

// readMovedCredential fetches the credential from the backend a move landed
// on, bypassing the seam's fault injection.
func readMovedCredential(fk *credstest.Fake, a store.Account, target creds.Source) (*creds.Credential, error) {
	if target == creds.SourceFile {
		return creds.ReadFileCredential(a.ConfigDir)
	}
	c, ok := fk.Get(a.KeychainService, a.KeychainAccount)
	if !ok {
		return nil, errors.New("keychain item missing after the move")
	}
	return c, nil
}

// tamperStore corrupts every successful read, simulating a target backend
// that acknowledges the write but returns different bytes.
type tamperStore struct{ creds.Store }

func (s tamperStore) Read() (*creds.Credential, error) {
	c, err := s.Store.Read()
	if err != nil {
		return nil, err
	}
	c.ClaudeAiOauth.RefreshToken = "tampered-" + c.ClaudeAiOauth.RefreshToken
	return c, nil
}

// tamperCreds swaps a tamperStore in for one source of the wrapped seam.
type tamperCreds struct {
	Credentials
	src creds.Source
}

func (c tamperCreds) Store(a store.Account, src creds.Source) creds.Store {
	s := c.Credentials.Store(a, src)
	if src == c.src {
		return tamperStore{s}
	}
	return s
}

func (c tamperCreds) Stores(a store.Account) []creds.Store {
	return []creds.Store{c.Store(a, creds.SourceKeychain), c.Store(a, creds.SourceFile)}
}

// TestMoveCredentialReadbackMismatch pins the write-verify step: a target
// whose readback differs from what was written (a racing writer, a lying
// backend) is unwound, leaving the source as the only live credential.
func TestMoveCredentialReadbackMismatch(t *testing.T) {
	dir := t.TempDir()
	a := store.Account{ID: 1, ConfigDir: dir, KeychainService: "svc-move", KeychainAccount: "user"}
	fk := credstest.NewFake()
	fk.Put(a.KeychainService, a.KeychainAccount, moveCred())
	m := &Manager{Creds: tamperCreds{Credentials: fk, src: creds.SourceFile}, LockDir: t.TempDir()}

	_, err := m.MoveCredential(context.Background(), a, creds.SourceFile)
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("MoveCredential = %v, want a readback-mismatch error", err)
	}
	if _, ok := fk.Get(a.KeychainService, a.KeychainAccount); !ok {
		t.Error("source keychain item deleted after a failed verify")
	}
	if creds.FileCredentialExists(dir) {
		t.Error("unverified target copy not unwound")
	}
}

// TestMoveCredentialLockContention pins that MoveCredential runs under the
// per-account cross-process lock: with another holder on the flock and a
// short ctx, the lock error propagates and neither backend is touched.
func TestMoveCredentialLockContention(t *testing.T) {
	lockDir := t.TempDir()
	a := store.Account{ID: 7, ConfigDir: t.TempDir(), KeychainService: "svc-move", KeychainAccount: "user"}
	held, err := proc.Flock(context.Background(), filepath.Join(lockDir, AccountDirName(a.ID)+".lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()

	fk := credstest.NewFake()
	fk.Put(a.KeychainService, a.KeychainAccount, moveCred())
	m := &Manager{Creds: fk, LockDir: lockDir}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := m.MoveCredential(ctx, a, creds.SourceFile); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("MoveCredential = %v, want context.DeadlineExceeded", err)
	}
	if got := fk.TouchedServices(); len(got) != 0 {
		t.Errorf("keychain ops ran without the lock: %v", got)
	}
	if creds.FileCredentialExists(a.ConfigDir) {
		t.Error("file credential appeared under a refused lock")
	}
}

// datedCred builds a usable credential whose token names it and whose expiry is
// offset from now, so a test can tell the fresher copy from the stale one.
func datedCred(token string, exp time.Duration) *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.AccessToken = token
	c.ClaudeAiOauth.RefreshToken = "rt-" + token
	c.ClaudeAiOauth.ExpiresAt = time.Now().Add(exp).UnixMilli()
	return c
}

// TestMoveCredentialFresherWins is the anti-regression for the sign-out hazard:
// with a STALE keychain item and a FRESHER file copy (the shape a headless
// re-login of a keychain-backed account leaves), a move to the keychain must
// carry the FRESH credential onto the keychain and drop the file — never no-op
// and delete the fresher copy because the keychain happened to be probed first.
func TestMoveCredentialFresherWins(t *testing.T) {
	dir := t.TempDir()
	a := store.Account{ID: 1, ConfigDir: dir, KeychainService: "svc-move", KeychainAccount: "user"}
	fk := credstest.NewFake()
	fk.Put(a.KeychainService, a.KeychainAccount, datedCred("stale", time.Hour))
	if err := creds.WriteFileCredential(dir, datedCred("fresh", 4*time.Hour)); err != nil {
		t.Fatal(err)
	}
	m := &Manager{Creds: fk, LockDir: t.TempDir()}

	got, err := m.MoveCredential(context.Background(), a, creds.SourceKeychain)
	if err != nil {
		t.Fatalf("MoveCredential: %v", err)
	}
	if want := (&CredMove{From: creds.SourceFile, To: creds.SourceKeychain, Moved: true}); *got != *want {
		t.Errorf("MoveCredential = %+v, want %+v", *got, *want)
	}
	kc, ok := fk.Get(a.KeychainService, a.KeychainAccount)
	if !ok {
		t.Fatal("keychain item missing after the move")
	}
	if kc.ClaudeAiOauth.AccessToken != "fresh" {
		t.Errorf("keychain now holds %q, want the fresher \"fresh\" credential", kc.ClaudeAiOauth.AccessToken)
	}
	if creds.FileCredentialExists(dir) {
		t.Error("stale-superseding file copy not removed after the move")
	}
}

// TestReadCredentialRefreshOnlyKeychainWinsByExpiry pins that a refresh-only
// keychain blob (empty access token, live refresh token) still competes in the
// fresher-wins probe on its raw expiry: a LATER-expiring refresh-only keychain
// copy beats an EARLIER-expiring complete file copy and resolution returns the
// keychain — proof the ExpiresWithin empty-token fold never leaked into Expiry().
func TestReadCredentialRefreshOnlyKeychainWinsByExpiry(t *testing.T) {
	dir := t.TempDir()
	a := store.Account{ID: 1, ConfigDir: dir, KeychainService: "svc-heal", KeychainAccount: "user"}
	fk := credstest.NewFake()
	kcOnly := refreshOnly("rt-kc", time.Now().Add(4*time.Hour)) // later, but access-token-less
	fk.Put(a.KeychainService, a.KeychainAccount, kcOnly)
	fileCred := &creds.Credential{}
	fileCred.ClaudeAiOauth.AccessToken = "at-file"
	fileCred.ClaudeAiOauth.RefreshToken = "rt-file"
	fileCred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli() // earlier
	if err := creds.WriteFileCredential(dir, fileCred); err != nil {
		t.Fatal(err)
	}
	m := &Manager{Creds: fk, LockDir: t.TempDir()}

	cred, src, err := m.ReadCredential(a)
	if err != nil {
		t.Fatalf("ReadCredential: %v", err)
	}
	if src != creds.SourceKeychain {
		t.Errorf("source = %v, want keychain (the later-expiring refresh-only copy wins)", src)
	}
	if cred.ClaudeAiOauth.AccessToken != "" || cred.ClaudeAiOauth.RefreshToken != "rt-kc" {
		t.Errorf("resolved cred = %+v, want the refresh-only keychain copy", cred.ClaudeAiOauth)
	}
}

// syncedDatedCred builds a synced (refresh-token-free) credential whose token
// names it and whose expiry is offset from now.
func syncedDatedCred(token string, exp time.Duration) *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.AccessToken = token
	c.ClaudeAiOauth.ExpiresAt = time.Now().Add(exp).UnixMilli()
	return c
}

// TestReadCredentialOwnershipFirst pins ownership-first resolution: an OWNED
// credential beats a SYNCED replica regardless of expiry — a later-expiring
// synced copy must never out-resolve the owned chain, or DropDivergentCopy
// would delete it — while expiry still breaks ties within an ownership class.
// A dead-but-owned blob transiently outranking a live synced copy self-heals
// (the next refresh's invalid_grant tombstones it and the synced copy takes
// over), so it is no permanent wedge and gets no special case.
func TestReadCredentialOwnershipFirst(t *testing.T) {
	cases := []struct {
		name     string
		kc, file *creds.Credential
		wantSrc  creds.Source
		wantAT   string
	}{
		{
			name:    "owned keychain beats later-expiring synced file",
			kc:      datedCred("own-kc", time.Hour),
			file:    syncedDatedCred("sync-file", 4*time.Hour),
			wantSrc: creds.SourceKeychain,
			wantAT:  "own-kc",
		},
		{
			name:    "owned file beats later-expiring synced keychain",
			kc:      syncedDatedCred("sync-kc", 4*time.Hour),
			file:    datedCred("own-file", time.Hour),
			wantSrc: creds.SourceFile,
			wantAT:  "own-file",
		},
		{
			name:    "owned vs owned picks the later expiry",
			kc:      datedCred("own-kc", time.Hour),
			file:    datedCred("own-file", 4*time.Hour),
			wantSrc: creds.SourceFile,
			wantAT:  "own-file",
		},
		{
			name:    "synced vs synced picks the later expiry",
			kc:      syncedDatedCred("sync-kc", 4*time.Hour),
			file:    syncedDatedCred("sync-file", time.Hour),
			wantSrc: creds.SourceKeychain,
			wantAT:  "sync-kc",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			a := store.Account{ID: 1, ConfigDir: dir, KeychainService: "svc-rank", KeychainAccount: "user"}
			fk := credstest.NewFake()
			fk.Put(a.KeychainService, a.KeychainAccount, tc.kc)
			if err := creds.WriteFileCredential(dir, tc.file); err != nil {
				t.Fatal(err)
			}
			m := &Manager{Creds: fk, LockDir: t.TempDir()}

			cred, src, err := m.ReadCredential(a)
			if err != nil {
				t.Fatalf("ReadCredential: %v", err)
			}
			if src != tc.wantSrc {
				t.Errorf("source = %v, want %v", src, tc.wantSrc)
			}
			if cred.ClaudeAiOauth.AccessToken != tc.wantAT {
				t.Errorf("resolved access token = %q, want %q", cred.ClaudeAiOauth.AccessToken, tc.wantAT)
			}
		})
	}
}

// TestDropDivergentCopy pins relogin's consolidation: the backend other than the
// one resolution prefers (owned first, then fresher) is dropped, an unreachable
// headless keychain is left alone, and a single-backend or empty account is a no-op.
func TestDropDivergentCopy(t *testing.T) {
	errDelete := errors.New("file delete exploded")
	cases := []struct {
		name       string
		seedKC     *creds.Credential
		seedFile   *creds.Credential
		kcFaults   credstest.Faults
		fileFaults credstest.Faults
		wantKC     bool
		wantFile   bool
		wantErrIs  error
	}{
		{
			name:     "fresher keychain drops the stale file",
			seedKC:   datedCred("kc", 4*time.Hour),
			seedFile: datedCred("file", time.Hour),
			wantKC:   true, wantFile: false,
		},
		{
			name:     "fresher file drops the stale keychain",
			seedKC:   datedCred("kc", time.Hour),
			seedFile: datedCred("file", 4*time.Hour),
			wantKC:   false, wantFile: true,
		},
		{
			name:     "owned keychain survives a later-expiring synced file",
			seedKC:   datedCred("kc", time.Hour),
			seedFile: syncedDatedCred("file", 4*time.Hour),
			wantKC:   true, wantFile: false,
		},
		{
			name:     "owned file survives a later-expiring synced keychain",
			seedKC:   syncedDatedCred("kc", 4*time.Hour),
			seedFile: datedCred("file", time.Hour),
			wantKC:   false, wantFile: true,
		},
		{
			name:   "keychain only is a no-op",
			seedKC: datedCred("kc", time.Hour),
			wantKC: true, wantFile: false,
		},
		{
			name:     "file only is a no-op",
			seedFile: datedCred("file", time.Hour),
			wantKC:   false, wantFile: true,
		},
		{
			name:     "an unreachable keychain shadow is left untouched",
			seedFile: datedCred("file", time.Hour),
			kcFaults: credstest.Faults{Read: creds.ErrUnavailable},
			wantKC:   false, wantFile: true,
		},
		{
			name:       "a delete fault surfaces",
			seedKC:     datedCred("kc", 4*time.Hour),
			seedFile:   datedCred("file", time.Hour),
			fileFaults: credstest.Faults{Delete: errDelete},
			wantErrIs:  errDelete,
			wantKC:     true, wantFile: true,
		},
		{
			name:   "no credential anywhere is a no-op",
			wantKC: false, wantFile: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			a := store.Account{ID: 1, ConfigDir: dir, KeychainService: "svc-move", KeychainAccount: "user"}
			fk := credstest.NewFake()
			fk.KeychainFaults = tc.kcFaults
			fk.FileFaults = tc.fileFaults
			if tc.seedKC != nil {
				fk.Put(a.KeychainService, a.KeychainAccount, tc.seedKC)
			}
			if tc.seedFile != nil {
				if err := creds.WriteFileCredential(dir, tc.seedFile); err != nil {
					t.Fatal(err)
				}
			}
			m := &Manager{Creds: fk, LockDir: t.TempDir()}

			err := m.DropDivergentCopy(context.Background(), a)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("DropDivergentCopy = %v, want errors.Is(%v)", err, tc.wantErrIs)
				}
			} else if err != nil {
				t.Fatalf("DropDivergentCopy: %v", err)
			}
			if _, ok := fk.Get(a.KeychainService, a.KeychainAccount); ok != tc.wantKC {
				t.Errorf("keychain item present = %v, want %v", ok, tc.wantKC)
			}
			if got := creds.FileCredentialExists(dir); got != tc.wantFile {
				t.Errorf("file credential present = %v, want %v", got, tc.wantFile)
			}
		})
	}
}
