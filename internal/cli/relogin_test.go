package cli

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

func cred(token, refresh string, expiresAtMillis int64) *creds.Credential {
	return &creds.Credential{ClaudeAiOauth: creds.OAuth{
		AccessToken:  token,
		RefreshToken: refresh,
		ExpiresAt:    expiresAtMillis,
	}}
}

// TestNewReloginProbe: completion keys on the credential turning fresh and usable
// (refresh-token-bearing, unexpired), not on mere presence the account already
// carries before re-login.
func TestNewReloginProbe(t *testing.T) {
	future := time.Now().Add(time.Hour).UnixMilli()
	past := time.Now().Add(-time.Hour).UnixMilli()
	brokenErr := errors.New("security: keychain locked")

	cases := map[string]struct {
		baseline string
		read     credReader
		want     bool
		wantErr  error
	}{
		// Claude clears the refresh token to "" on a dead token; a still-revoked
		// credential is not a completed login even though its access token is new.
		"revoked stays revoked": {
			baseline: "tok-old",
			read:     func() (*creds.Credential, error) { return cred("tok-old", "", future), nil },
		},
		"revoked to fresh valid": {
			baseline: "tok-old",
			read:     func() (*creds.Credential, error) { return cred("tok-new", "rt", future), nil },
			want:     true,
		},
		"same valid token no change": {
			baseline: "tok-A",
			read:     func() (*creds.Credential, error) { return cred("tok-A", "rt", future), nil },
		},
		"valid token changes": {
			baseline: "tok-A",
			read:     func() (*creds.Credential, error) { return cred("tok-B", "rt", future), nil },
			want:     true,
		},
		// A fresh-but-expired credential is not usable: re-login did not land yet.
		"new credential valid but expired": {
			baseline: "tok-old",
			read:     func() (*creds.Credential, error) { return cred("tok-new", "rt", past), nil },
		},
		// ErrNotFound means "not yet": the wait continues without erroring.
		"no credential yet": {
			baseline: "tok-old",
			read:     func() (*creds.Credential, error) { return nil, creds.ErrNotFound },
		},
		// Any read error means "not yet": a transient backend hiccup must not
		// abort the watch and force-close the live login.
		"transient read error keeps waiting": {
			baseline: "tok-old",
			read:     func() (*creds.Credential, error) { return nil, brokenErr },
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			probe := newReloginProbe(tc.read, tc.baseline)
			done, err := probe()
			if done != tc.want || !errors.Is(err, tc.wantErr) {
				t.Errorf("probe() = %v, %v; want %v, %v", done, err, tc.want, tc.wantErr)
			}
		})
	}
}

// TestFinishRelogin pins that the post-login credential gate resolves through
// m.ReadCredential — both backends in resolution order — so a headless session
// surfaces the Keychain's unknown state (creds.ErrUnavailable) instead of a
// bogus not-found, and only a usable credential clears the needs-login flag.
func TestFinishRelogin(t *testing.T) {
	// Zero grace = the pre-grace single-read semantics; the grace loop itself
	// is covered by TestAwaitFreshCred.
	old := finishReloginGrace
	finishReloginGrace = 0
	t.Cleanup(func() { finishReloginGrace = old })

	future := time.Now().Add(time.Hour).UnixMilli()
	past := time.Now().Add(-time.Hour).UnixMilli()

	cases := map[string]struct {
		keychain     *creds.Credential
		file         *creds.Credential
		keychainRead error  // injected keychain Read fault
		baseline     string // pre-login access token
		wantErr      error  // errors.Is target; nil with empty wantContains = success
		wantContains []string
		wantOmits    []string
		wantWrites   int // seam keychain writes (the ACL re-assertion)
	}{
		"keychain-backed login lands and re-asserts the ACL": {
			keychain:   cred("at-new", "rt", future),
			baseline:   "at-old",
			wantWrites: 1,
		},
		// No prior credential (first login after revocation cleared it): an
		// empty baseline never matches a real token.
		"file-backed login lands with no prior credential": {
			file: cred("at-new", "rt", future),
		},
		// Quitting claude without logging in leaves the pre-login credential
		// byte-identical; reporting success would clear a correct needs-login
		// flag off a dead chain.
		"unchanged credential after quit fails closed": {
			keychain:     cred("at-old", "rt", future),
			baseline:     "at-old",
			wantContains: []string{"no new login landed", "credential unchanged", "ccp login 3"},
		},
		"headless unsearchable keychain surfaces unknown state, not absence": {
			keychainRead: creds.ErrUnavailable,
			wantErr:      creds.ErrUnavailable,
			wantContains: []string{"login keychain not in the security search list", "ccp login 3"},
			wantOmits:    []string{"not found"},
		},
		"no credential in either backend": {
			wantErr:      creds.ErrNotFound,
			wantContains: []string{"ccp login 3"},
		},
		"revoked credential (no refresh token) fails closed": {
			keychain:     cred("at-new", "", future),
			wantContains: []string{"no usable credential", "ccp login 3"},
		},
		"expired credential fails closed": {
			keychain:     cred("at-new", "rt", past),
			wantContains: []string{"no usable credential"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			st, err := store.Open(filepath.Join(home, "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			a := store.Account{ID: 3, ConfigDir: filepath.Join(home, "acct-03"), KeychainService: "svc-03", KeychainAccount: "user", Label: "bob@example.com"}
			if err := st.UpsertAccount(a); err != nil {
				t.Fatal(err)
			}
			if _, err := st.SetNeedsLogin(a.ID, time.Now(), "revoked"); err != nil {
				t.Fatal(err)
			}
			fk := credstest.NewFake()
			fk.KeychainFaults = credstest.Faults{Read: tc.keychainRead}
			if tc.keychain != nil {
				fk.Put(a.KeychainService, a.KeychainAccount, tc.keychain)
			}
			if tc.file != nil {
				if err := creds.WriteFileCredential(a.ConfigDir, tc.file); err != nil {
					t.Fatal(err)
				}
			}
			m := &pool.Manager{Store: st, Creds: fk, LockDir: t.TempDir()}

			ferr := finishRelogin(context.Background(), m, a, tc.baseline)

			h, herr := st.GetAuthHealth(a.ID)
			if herr != nil {
				t.Fatal(herr)
			}
			if wantOK := tc.wantErr == nil && len(tc.wantContains) == 0; wantOK {
				if ferr != nil {
					t.Fatalf("finishRelogin: %v", ferr)
				}
				if h.NeedsLogin {
					t.Error("needs-login flag not cleared by a successful re-login")
				}
				if got := fk.WriteCount(); got != tc.wantWrites {
					t.Errorf("keychain writes = %d, want %d", got, tc.wantWrites)
				}
				return
			}
			if ferr == nil {
				t.Fatal("finishRelogin succeeded, want failure")
			}
			if tc.wantErr != nil && !errors.Is(ferr, tc.wantErr) {
				t.Errorf("err = %v, want errors.Is %v", ferr, tc.wantErr)
			}
			for _, frag := range tc.wantContains {
				if !strings.Contains(ferr.Error(), frag) {
					t.Errorf("err %q missing %q", ferr, frag)
				}
			}
			for _, frag := range tc.wantOmits {
				if strings.Contains(ferr.Error(), frag) {
					t.Errorf("err %q must not contain %q", ferr, frag)
				}
			}
			if !h.NeedsLogin {
				t.Error("needs-login flag cleared by a failed re-login")
			}
		})
	}
}

// TestAwaitFreshCred: the post-exit read tolerates the claude-exit/credential-
// write race for the grace window — including a pre-login credential that
// still looks usable while the fresh write is in flight — but unknown Keychain
// state and cancellation abort immediately.
func TestAwaitFreshCred(t *testing.T) {
	future := time.Now().Add(time.Hour).UnixMilli()

	t.Run("fresh and usable immediately", func(t *testing.T) {
		got, err := awaitFreshCred(context.Background(), func() (*creds.Credential, error) {
			return cred("at", "rt", future), nil
		}, "at-old", 0, time.Millisecond)
		if err != nil || got.ClaudeAiOauth.AccessToken != "at" {
			t.Fatalf("= %v, %v; want the credential", got, err)
		}
	})

	t.Run("lands within the grace window", func(t *testing.T) {
		var calls int
		got, err := awaitFreshCred(context.Background(), func() (*creds.Credential, error) {
			calls++
			if calls < 3 {
				return nil, creds.ErrNotFound
			}
			return cred("at", "rt", future), nil
		}, "at-old", time.Second, time.Millisecond)
		if err != nil || got == nil {
			t.Fatalf("= %v, %v; want the late credential", got, err)
		}
		if calls != 3 {
			t.Fatalf("reads = %d, want 3", calls)
		}
	})

	t.Run("stale-but-usable baseline credential keeps waiting for the fresh one", func(t *testing.T) {
		var calls int
		got, err := awaitFreshCred(context.Background(), func() (*creds.Credential, error) {
			calls++
			if calls < 3 {
				return cred("at-old", "rt", future), nil // pre-login credential, still usable
			}
			return cred("at-new", "rt", future), nil
		}, "at-old", time.Second, time.Millisecond)
		if err != nil || got.ClaudeAiOauth.AccessToken != "at-new" {
			t.Fatalf("= %v, %v; want the fresh credential, not the baseline one", got, err)
		}
	})

	t.Run("baseline-unchanged at the deadline returns it for the caller to judge", func(t *testing.T) {
		stale := cred("at-old", "rt", future)
		got, err := awaitFreshCred(context.Background(), func() (*creds.Credential, error) {
			return stale, nil
		}, "at-old", 10*time.Millisecond, time.Millisecond)
		if err != nil || got != stale {
			t.Fatalf("= %v, %v; want the unchanged credential back", got, err)
		}
	})

	t.Run("never lands returns the last error", func(t *testing.T) {
		_, err := awaitFreshCred(context.Background(), func() (*creds.Credential, error) {
			return nil, creds.ErrNotFound
		}, "", 10*time.Millisecond, time.Millisecond)
		if !errors.Is(err, creds.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})

	t.Run("unusable but readable returns the credential for the caller to judge", func(t *testing.T) {
		revoked := cred("at", "", future)
		got, err := awaitFreshCred(context.Background(), func() (*creds.Credential, error) {
			return revoked, nil
		}, "", 10*time.Millisecond, time.Millisecond)
		if err != nil || got != revoked {
			t.Fatalf("= %v, %v; want the revoked credential back", got, err)
		}
	})

	t.Run("unknown keychain state aborts without retrying", func(t *testing.T) {
		var calls int
		_, err := awaitFreshCred(context.Background(), func() (*creds.Credential, error) {
			calls++
			return nil, creds.ErrUnavailable
		}, "", time.Hour, time.Millisecond)
		if !errors.Is(err, creds.ErrUnavailable) || calls != 1 {
			t.Fatalf("err = %v after %d reads, want immediate ErrUnavailable", err, calls)
		}
	})

	t.Run("cancellation aborts the wait", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := awaitFreshCred(ctx, func() (*creds.Credential, error) {
			return nil, creds.ErrNotFound
		}, "", time.Hour, time.Millisecond)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	})
}

type fakeOAuth struct {
	refreshErr error
	calls      int
}

func (f *fakeOAuth) Refresh(context.Context, string, string) (*oauth.TokenResponse, error) {
	f.calls++
	if f.refreshErr != nil {
		return nil, f.refreshErr
	}
	return &oauth.TokenResponse{AccessToken: "at-rotated", RefreshToken: "rt-rotated", ExpiresIn: 3600}, nil
}

func (f *fakeOAuth) Usage(context.Context, string) (*oauth.Usage, error) {
	return nil, errors.New("usage must not be called")
}

// TestShortCircuitRelogin: only a store-flagged account whose refresh chain
// survives a forced rotate-and-persist clears without a login. Access-token
// life proves nothing — the daemon flags on proactive refresh failure, so an
// unexpired token can sit on a dead chain.
func TestShortCircuitRelogin(t *testing.T) {
	future := time.Now().Add(time.Hour).UnixMilli()
	past := time.Now().Add(-time.Hour).UnixMilli()

	cases := map[string]struct {
		flagged          bool
		keychain         *creds.Credential
		refreshErr       error
		want             bool
		wantRefreshCalls int
		wantWrites       int
	}{
		"stale flag with live refresh chain clears": {
			flagged: true, keychain: cred("at", "rt", future),
			want: true, wantRefreshCalls: 1, wantWrites: 1, // the refresh's own rotate-persist
		},
		// The counterexample that killed the usage-probe design: hours of
		// access-token life on a chain the daemon already proved dead.
		"dead refresh chain behind an unexpired access token keeps the login": {
			flagged: true, keychain: cred("at", "rt", future),
			refreshErr:       &oauth.RefreshError{Status: 400, Body: `{"error": "invalid_grant"}`},
			wantRefreshCalls: 1,
		},
		"transient refresh failure keeps the login": {
			flagged: true, keychain: cred("at", "rt", future),
			refreshErr:       errors.New("dial tcp: connection refused"),
			wantRefreshCalls: 1,
		},
		"expired access token with live chain still clears": {
			flagged: true, keychain: cred("at", "rt", past),
			want: true, wantRefreshCalls: 1, wantWrites: 1,
		},
		"revoked credential (no refresh token) keeps the login": {
			flagged: true, keychain: cred("at", "", future),
		},
		"absent credential keeps the login": {
			flagged: true,
		},
		"unflagged account never short-circuits": {
			flagged: false, keychain: cred("at", "rt", future),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			st, err := store.Open(filepath.Join(home, "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			a := store.Account{ID: 3, ConfigDir: filepath.Join(home, "acct-03"), KeychainService: "svc-03", KeychainAccount: "user", Label: "bob@example.com"}
			if err := st.UpsertAccount(a); err != nil {
				t.Fatal(err)
			}
			if tc.flagged {
				if _, err := st.SetNeedsLogin(a.ID, time.Now(), "revoked"); err != nil {
					t.Fatal(err)
				}
			}
			fk := credstest.NewFake()
			if tc.keychain != nil {
				fk.Put(a.KeychainService, a.KeychainAccount, tc.keychain)
			}
			fo := &fakeOAuth{refreshErr: tc.refreshErr}
			m := &pool.Manager{Store: st, OAuth: fo, Creds: fk, LockDir: t.TempDir()}

			got, scErr := shortCircuitRelogin(context.Background(), m, a)
			if scErr != nil {
				t.Fatalf("shortCircuitRelogin: %v", scErr)
			}
			if got != tc.want {
				t.Errorf("cleared = %v, want %v", got, tc.want)
			}
			if fo.calls != tc.wantRefreshCalls {
				t.Errorf("forced refreshes = %d, want %d", fo.calls, tc.wantRefreshCalls)
			}
			if got := fk.WriteCount(); got != tc.wantWrites {
				t.Errorf("keychain writes = %d, want %d", got, tc.wantWrites)
			}
			if tc.want {
				// The spent single-use token's successor must be what persisted.
				stored, ok := fk.Get(a.KeychainService, a.KeychainAccount)
				if !ok || stored.ClaudeAiOauth.RefreshToken != "rt-rotated" {
					t.Errorf("persisted credential = %+v, want the rotated chain", stored)
				}
			}
			h, err := st.GetAuthHealth(a.ID)
			if err != nil {
				t.Fatal(err)
			}
			if wantFlag := tc.flagged && !tc.want; h.NeedsLogin != wantFlag {
				t.Errorf("needs-login = %v, want %v", h.NeedsLogin, wantFlag)
			}
		})
	}
}

// TestFinishReloginAndPublish pins the relogin sync hook to the fail-closed
// token-changed discipline: only a landed login (credential differing from the
// pre-login baseline) publishes to the shared registry; a login that never
// landed generates zero registry traffic.
func TestFinishReloginAndPublish(t *testing.T) {
	old := finishReloginGrace
	finishReloginGrace = 0
	t.Cleanup(func() { finishReloginGrace = old })
	future := time.Now().Add(time.Hour).UnixMilli()

	t.Run("unchanged credential fails closed and publishes nothing", func(t *testing.T) {
		m, fk := syncTestEnv(t)
		m.LockDir = t.TempDir()
		calls := stubSynckitdRun(t)
		if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
			t.Fatal(err)
		}
		a := addSyncTestAccount(t, m, fk, 3, "u-3", "bob@x.y", "bob", cred("at-old", "rt", future))
		cmd, _ := syncCmdBuf(t)

		if err := finishReloginAndPublish(cmd, m, a, "at-old"); err == nil {
			t.Fatal("want the fail-closed error for an unchanged credential")
		}
		if _, err := os.Stat(pool.SyncDir()); !os.IsNotExist(err) {
			t.Fatalf("sync dir exists (stat err %v); a failed relogin must not advertise a chain", err)
		}
		if len(*calls) != 0 {
			t.Errorf("synckitd calls = %v, want none", *calls)
		}
	})

	t.Run("landed login publishes the fresh chain and tags the row", func(t *testing.T) {
		m, fk := syncTestEnv(t)
		m.LockDir = t.TempDir()
		stubSynckitdRun(t)
		writeMeshState(t, `{"self": "me@host-a"}`)
		if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
			t.Fatal(err)
		}
		fresh := cred("at-new", "rt-new", future)
		a := addSyncTestAccount(t, m, fk, 3, "u-3", "bob@x.y", "bob", fresh)
		cmd, _ := syncCmdBuf(t)

		if err := finishReloginAndPublish(cmd, m, a, "at-old"); err != nil {
			t.Fatalf("finishReloginAndPublish: %v", err)
		}
		reg, err := syncRegistryFile().Load()
		if err != nil {
			t.Fatal(err)
		}
		entry, ok := reg["u-3"]
		if !ok || !entry.Present() {
			t.Fatalf("entry = %+v, want the re-login published", entry)
		}
		if want := hostsync.CredentialHash(fresh); entry.Value.Chain.Hash != want {
			t.Errorf("chain hash = %q, want %q (the landed credential)", entry.Value.Chain.Hash, want)
		}
		if entry.Value.Chain.Holder != "me@host-a" {
			t.Errorf("holder = %q, want the mesh self", entry.Value.Chain.Holder)
		}
		row, err := m.Store.GetAccount(a.ID)
		if err != nil {
			t.Fatal(err)
		}
		if row.AccountUUID != "u-3" {
			t.Errorf("row uuid = %q, want u-3", row.AccountUUID)
		}
	})
}

// TestRunReloginShortCircuitPublishes pins runRelogin's cleared branch to the
// publish tail — dropping the afterRelogin call would strand peers on a stale
// chain with no test failing.
func TestRunReloginShortCircuitPublishes(t *testing.T) {
	future := time.Now().Add(time.Hour).UnixMilli()
	m, fk := syncTestEnv(t)
	m.LockDir = t.TempDir()
	m.OAuth = &fakeOAuth{}
	stubSynckitdRun(t)
	writeMeshState(t, `{"self": "me@host-a"}`)
	if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
		t.Fatal(err)
	}
	a := addSyncTestAccount(t, m, fk, 3, "u-3", "bob@x.y", "bob", cred("at", "rt", future))
	if _, err := m.Store.SetNeedsLogin(a.ID, time.Now(), "revoked"); err != nil {
		t.Fatal(err)
	}
	cmd, _ := syncCmdBuf(t)

	if err := runRelogin(cmd, m, "3"); err != nil {
		t.Fatalf("runRelogin: %v", err)
	}
	reg, err := syncRegistryFile().Load()
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := reg["u-3"]
	if !ok || !entry.Present() {
		t.Fatalf("entry = %+v, want the short-circuit clear published", entry)
	}
	// The published chain is the forced refresh's rotated successor.
	stored, ok := fk.Get(a.KeychainService, a.KeychainAccount)
	if !ok {
		t.Fatal("rotated credential missing from the keychain fake")
	}
	if want := hostsync.CredentialHash(stored); entry.Value.Chain.Hash != want {
		t.Errorf("chain hash = %q, want %q (the rotated chain)", entry.Value.Chain.Hash, want)
	}
}

// TestTUIReloginSeamsPublish pins the status TUI's re-login paths to the same
// publish tail as `ccp login` — a TUI-cleared account must not stay flagged
// dead on every peer.
func TestTUIReloginSeamsPublish(t *testing.T) {
	future := time.Now().Add(time.Hour).UnixMilli()
	setup := func(t *testing.T) (*pool.Manager, store.Account) {
		t.Helper()
		m, fk := syncTestEnv(t)
		m.LockDir = t.TempDir()
		m.OAuth = &fakeOAuth{}
		stubSynckitdRun(t)
		writeMeshState(t, `{"self": "me@host-a"}`)
		if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
			t.Fatal(err)
		}
		return m, addSyncTestAccount(t, m, fk, 3, "u-3", "bob@x.y", "bob", cred("at", "rt", future))
	}
	assertPublished := func(t *testing.T) {
		t.Helper()
		reg, err := syncRegistryFile().Load()
		if err != nil {
			t.Fatal(err)
		}
		if e, ok := reg["u-3"]; !ok || !e.Present() {
			t.Fatalf("entry = %+v, want published", e)
		}
	}

	t.Run("checkFresh clear publishes", func(t *testing.T) {
		m, a := setup(t)
		if _, err := m.Store.SetNeedsLogin(a.ID, time.Now(), "revoked"); err != nil {
			t.Fatal(err)
		}
		cleared, err := tuiCheckFresh(context.Background(), m, a)
		if err != nil || !cleared {
			t.Fatalf("cleared = %v, err = %v; want a clean clear", cleared, err)
		}
		assertPublished(t)
	})

	t.Run("finish login publishes", func(t *testing.T) {
		old := finishReloginGrace
		finishReloginGrace = 0
		t.Cleanup(func() { finishReloginGrace = old })
		m, a := setup(t)
		if err := tuiFinishRelogin(context.Background(), m, a, "at-old"); err != nil {
			t.Fatalf("tuiFinishRelogin: %v", err)
		}
		assertPublished(t)
	})

	t.Run("uncleared checkFresh publishes nothing", func(t *testing.T) {
		m, a := setup(t) // not flagged: never short-circuits
		cleared, err := tuiCheckFresh(context.Background(), m, a)
		if err != nil || cleared {
			t.Fatalf("cleared = %v, err = %v; want no clear", cleared, err)
		}
		if _, err := os.Stat(pool.SyncDir()); !os.IsNotExist(err) {
			t.Fatalf("sync dir exists (stat err %v); an uncleared account must publish nothing", err)
		}
	})
}

// TestWatchedLoginRun drives watchedLogin.Run() against real child processes
// (no claude): a fresh credential mid-flight closes claude, a manual exit
// returns without a force-kill. The injected read never touches the real credential backends.
func TestWatchedLoginRun(t *testing.T) {
	future := time.Now().Add(time.Hour).UnixMilli()

	t.Run("fresh credential closes claude", func(t *testing.T) {
		c := exec.Command("/bin/sleep", "60")
		// Baseline read (Run's first call) is revoked; a later poll turns fresh — the close signal.
		var calls int
		read := func() (*creds.Credential, error) {
			calls++
			if calls <= 2 {
				return cred("tok-old", "", future), nil // revoked: no refresh token
			}
			return cred("tok-new", "rt", future), nil // fresh + usable
		}
		wl := &watchedLogin{ctx: context.Background(), cmd: c, read: read}

		done := make(chan error, 1)
		go func() { done <- wl.Run() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run returned %v, want nil", err)
			}
		case <-time.After(killGrace + 2*time.Second):
			t.Fatal("Run did not return after a fresh credential landed")
		}
		// A fresh credential must close claude (signaled or killed), not run to a clean exit.
		if c.ProcessState == nil || c.ProcessState.Exited() && c.ProcessState.Success() {
			t.Fatalf("process state = %v, want signaled/killed", c.ProcessState)
		}
		// Run must capture the pre-login token for the finish gate.
		if wl.baseline != "tok-old" {
			t.Fatalf("baseline = %q, want the pre-login token", wl.baseline)
		}
	})

	t.Run("transient read errors then a fresh credential still closes claude", func(t *testing.T) {
		c := exec.Command("/bin/sleep", "60")
		// A transient read error must not abort the watch — claude must still
		// close once the credential lands.
		brokenErr := errors.New("security: keychain locked")
		var calls int
		read := func() (*creds.Credential, error) {
			calls++
			switch {
			case calls == 1:
				return cred("tok-old", "", future), nil // baseline: revoked
			case calls <= 3:
				return nil, brokenErr // transient backend hiccup
			default:
				return cred("tok-new", "rt", future), nil // fresh + usable
			}
		}
		wl := &watchedLogin{ctx: context.Background(), cmd: c, read: read}

		done := make(chan error, 1)
		go func() { done <- wl.Run() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run returned %v, want nil", err)
			}
		case <-time.After(killGrace + 2*time.Second):
			t.Fatal("Run did not return after a fresh credential landed")
		}
		if c.ProcessState == nil || c.ProcessState.Exited() && c.ProcessState.Success() {
			t.Fatalf("process state = %v, want signaled/killed", c.ProcessState)
		}
	})

	t.Run("manual exit needs no kill", func(t *testing.T) {
		c := exec.Command("/usr/bin/true")
		// Always revoked: the probe never fires, so Run returns on the child's own exit (awaitExited), no force-kill.
		read := func() (*creds.Credential, error) { return cred("tok-old", "", future), nil }
		wl := &watchedLogin{ctx: context.Background(), cmd: c, read: read}

		done := make(chan error, 1)
		go func() { done <- wl.Run() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run returned %v, want nil", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Run hung on a self-exiting child")
		}
		if c.ProcessState == nil || !c.ProcessState.Success() {
			t.Fatalf("process state = %v, want clean self-exit (no kill)", c.ProcessState)
		}
	})
}
