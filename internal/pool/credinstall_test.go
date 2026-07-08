package pool

import (
	"context"
	"errors"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/store"
)

// syncCred builds a distinct credential per suffix with the given expiry.
func syncCred(suffix string, expiresAt int64) *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.AccessToken = "at-" + suffix
	c.ClaudeAiOauth.RefreshToken = "rt-" + suffix
	c.ClaudeAiOauth.ExpiresAt = expiresAt
	return c
}

// installFixture is a Manager over a fake credential seam with a counting
// OnCredWrite hook, plus the account under test.
type installFixture struct {
	m          *Manager
	fk         *credstest.Fake
	a          store.Account
	hookCalls  int
	hookCred   *creds.Credential
	hookParent string
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
	f.m.OnCredWrite = func(_ store.Account, cr *creds.Credential, parentHash string) error {
		f.hookCalls++
		f.hookCred = cr
		f.hookParent = parentHash
		return nil
	}
	return f
}

// TestInstallSyncedCredentialFresherWins pins the locked fresher-wins
// re-check: only strictly later expiries install, equal/earlier skip with no
// write or hook, and a credential-less account installs to the Keychain.
func TestInstallSyncedCredentialFresherWins(t *testing.T) {
	cases := map[string]struct {
		localExpiry    int64 // 0: no local credential seeded
		incomingExpiry int64
		wantInstalled  bool
	}{
		"strictly fresher installs":     {localExpiry: 1_000, incomingExpiry: 1_001, wantInstalled: true},
		"equal expiry skips":            {localExpiry: 1_000, incomingExpiry: 1_000},
		"staler skips":                  {localExpiry: 1_000, incomingExpiry: 999},
		"no local credential installs":  {incomingExpiry: 1, wantInstalled: true},
		"much fresher over stale local": {localExpiry: 1, incomingExpiry: 9_000_000, wantInstalled: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newInstallFixture(t)
			local := syncCred("local", tc.localExpiry)
			if tc.localExpiry != 0 {
				f.fk.Put(f.a.KeychainService, f.a.KeychainAccount, local)
			}
			incoming := syncCred("incoming", tc.incomingExpiry)

			installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, incoming, "")
			if err != nil {
				t.Fatalf("InstallSyncedCredential: %v", err)
			}
			if installed != tc.wantInstalled {
				t.Fatalf("installed = %v, want %v", installed, tc.wantInstalled)
			}

			got, ok := f.fk.Get(f.a.KeychainService, f.a.KeychainAccount)
			if tc.wantInstalled {
				if !ok || got.ClaudeAiOauth.AccessToken != incoming.ClaudeAiOauth.AccessToken {
					t.Fatalf("keychain holds %+v, want the incoming credential", got)
				}
				if f.hookCalls != 1 {
					t.Fatalf("OnCredWrite fired %d times, want 1", f.hookCalls)
				}
				if f.hookCred != incoming {
					t.Fatalf("OnCredWrite got credential %p, want the installed one %p", f.hookCred, incoming)
				}
				return
			}
			if f.hookCalls != 0 {
				t.Fatalf("OnCredWrite fired %d times on a skip, want 0", f.hookCalls)
			}
			if tc.localExpiry != 0 {
				if f.fk.WriteCount() != 0 {
					t.Fatalf("keychain writes = %d on a skip, want 0", f.fk.WriteCount())
				}
				if !ok || got.ClaudeAiOauth.AccessToken != local.ClaudeAiOauth.AccessToken {
					t.Fatalf("keychain holds %+v after a skip, want the local credential untouched", got)
				}
			}
		})
	}
}

// TestInstallSyncedCredentialLineage pins the lineage arms under clock skew: a
// pull matching the recorded parent is refused despite a later expiry, a child
// installs despite an earlier one, and an identical chain is a clean skip.
func TestInstallSyncedCredentialLineage(t *testing.T) {
	c2 := syncCred("c2", 2_000) // the spent parent, expiry skewed AHEAD
	c3 := syncCred("c3", 1_500) // the live child, expiry skewed behind

	t.Run("refuses re-install of the recorded parent despite later expiry", func(t *testing.T) {
		f := newInstallFixture(t)
		f.fk.Put(f.a.KeychainService, f.a.KeychainAccount, c3)
		if err := f.m.Store.SetChainHashes(f.a.ID, creds.CredentialHash(c3), creds.CredentialHash(c2)); err != nil {
			t.Fatal(err)
		}

		installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, c2, "")
		if err != nil {
			t.Fatalf("InstallSyncedCredential: %v", err)
		}
		if installed {
			t.Fatal("installed = true; the pull is our own parent — expiry skew must not resurrect it")
		}
		if f.fk.WriteCount() != 0 || f.hookCalls != 0 {
			t.Fatalf("refused install wrote (writes=%d hooks=%d), want none", f.fk.WriteCount(), f.hookCalls)
		}
		if got, _ := f.fk.Get(f.a.KeychainService, f.a.KeychainAccount); got.ClaudeAiOauth.AccessToken != c3.ClaudeAiOauth.AccessToken {
			t.Fatalf("keychain holds %+v, want the live child untouched", got)
		}
	})

	t.Run("installs a child of the current chain despite earlier expiry", func(t *testing.T) {
		f := newInstallFixture(t)
		f.fk.Put(f.a.KeychainService, f.a.KeychainAccount, c2)

		installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, c3, creds.CredentialHash(c2))
		if err != nil {
			t.Fatalf("InstallSyncedCredential: %v", err)
		}
		if !installed {
			t.Fatal("installed = false; a child of the current chain must land despite expiry skew")
		}
		if got, _ := f.fk.Get(f.a.KeychainService, f.a.KeychainAccount); got.ClaudeAiOauth.AccessToken != c3.ClaudeAiOauth.AccessToken {
			t.Fatalf("keychain holds %+v, want the child", got)
		}
		if f.hookCalls != 1 || f.hookParent != creds.CredentialHash(c2) {
			t.Fatalf("hook calls=%d parent=%q, want 1 with hash(c2)", f.hookCalls, f.hookParent)
		}
		// The install recorded its own lineage.
		row, err := f.m.Store.GetAccount(f.a.ID)
		if err != nil {
			t.Fatal(err)
		}
		if row.CredHash != creds.CredentialHash(c3) || row.CredParentHash != creds.CredentialHash(c2) {
			t.Fatalf("chain columns = (%q,%q), want (hash(c3),hash(c2))", row.CredHash, row.CredParentHash)
		}
	})

	t.Run("identical chain skips", func(t *testing.T) {
		f := newInstallFixture(t)
		f.fk.Put(f.a.KeychainService, f.a.KeychainAccount, c3)

		same := syncCred("c3", 1_500)
		installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, same, creds.CredentialHash(c2))
		if err != nil {
			t.Fatalf("InstallSyncedCredential: %v", err)
		}
		if installed || f.fk.WriteCount() != 0 || f.hookCalls != 0 {
			t.Fatalf("identical chain re-installed (installed=%v writes=%d hooks=%d)", installed, f.fk.WriteCount(), f.hookCalls)
		}
	})
}

// TestInstallSyncedCredentialFollowsBackendResolution pins that the install
// writes to the backend resolution picks: a file-backed account gets the file
// write and the Keychain is never touched.
func TestInstallSyncedCredentialFollowsBackendResolution(t *testing.T) {
	f := newInstallFixture(t)
	local := syncCred("local", 1_000)
	if err := (creds.FileStore{ConfigDir: f.a.ConfigDir}).Write(local); err != nil {
		t.Fatalf("seed file credential: %v", err)
	}
	incoming := syncCred("incoming", 2_000)

	installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, incoming, "")
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
	incoming := syncCred("incoming", 5_000)

	installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, incoming, "")
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

// TestInstallSyncedCredentialConcurrentRotationWins pins the never-write-staler
// re-check across the lock-free window: a local rotation landing before the
// install wins, and the now-staler pull is not written.
func TestInstallSyncedCredentialConcurrentRotationWins(t *testing.T) {
	f := newInstallFixture(t)
	f.fk.Put(f.a.KeychainService, f.a.KeychainAccount, syncCred("old", 1_000))
	// Verified lock-free against expiry 1_000, so 2_000 looked strictly fresher.
	incoming := syncCred("incoming", 2_000)
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
		installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, incoming, "")
		done <- result{installed, err}
	}()
	// While the install is (or will be) blocked on the account lock, a live
	// session rotates the chain to something fresher than the incoming pull.
	f.fk.Put(f.a.KeychainService, f.a.KeychainAccount, rotated)
	release()

	res := <-done
	if res.err != nil {
		t.Fatalf("InstallSyncedCredential: %v", res.err)
	}
	if res.installed {
		t.Fatal("installed = true; the concurrent rotation must win")
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
// returns old, later reads return swapped — a `claude /login` landing between
// the probe and the CAS re-read, outside every lock cc-pool holds.
type swapStore struct {
	creds.Store
	old, swapped *creds.Credential
	reads        int
}

func (s *swapStore) Read() (*creds.Credential, error) {
	s.reads++
	if s.reads == 1 {
		return s.old, nil
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
	old := syncCred("old", 1_000)
	login := syncCred("login", 2_000)
	f.fk.Put(f.a.KeychainService, f.a.KeychainAccount, old)
	ks := &swapStore{Store: f.fk.Store(f.a, creds.SourceKeychain), old: old, swapped: login}
	f.m.Creds = swapCreds{Fake: f.fk, ks: ks}

	incoming := syncCred("incoming", 5_000) // strictly fresher than old: gate passes

	installed, err := f.m.InstallSyncedCredential(context.Background(), f.a, incoming, "")
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
