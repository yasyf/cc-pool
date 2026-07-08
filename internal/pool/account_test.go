package pool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// detectSymlink injects a deterministic symlink verdict so Init never probes
// (or, in a fuse build, spawns) a mount holder.
func detectSymlink() (fkoverlay.Backend, string) { return fkoverlay.BackendSymlink, "" }

func TestDuplicateIdentity(t *testing.T) {
	st := openTestStore(t)
	m := &Manager{Store: st}

	mkAccount := func(t *testing.T, id int, uuid, email string) store.Account {
		t.Helper()
		dir := t.TempDir()
		if uuid != "" {
			body := `{"oauthAccount":{"accountUuid":"` + uuid + `","emailAddress":"` + email + `"}}`
			if err := os.WriteFile(filepath.Join(dir, ".claude.json"), []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		a := store.Account{ID: id, ConfigDir: dir, KeychainService: creds.ServiceName(dir), KeychainAccount: "user", OverlayKind: "symlink"}
		if err := st.UpsertAccount(a); err != nil {
			t.Fatal(err)
		}
		return a
	}

	a1 := mkAccount(t, 1, "u-1", "a@example.com")
	mkAccount(t, 2, "u-2", "b@example.com")

	t.Run("matches an already-pooled subscription", func(t *testing.T) {
		dup, err := m.DuplicateIdentity(Identity{AccountUUID: "u-1", EmailAddress: "a@example.com"})
		if err != nil {
			t.Fatal(err)
		}
		if dup == nil || dup.ID != a1.ID {
			t.Fatalf("DuplicateIdentity(u-1) = %+v, want acct %d", dup, a1.ID)
		}
	})

	t.Run("a new subscription returns nil", func(t *testing.T) {
		dup, err := m.DuplicateIdentity(Identity{AccountUUID: "u-3"})
		if err != nil {
			t.Fatal(err)
		}
		if dup != nil {
			t.Fatalf("DuplicateIdentity(u-3) = %+v, want nil", dup)
		}
	})

	t.Run("an account with no readable identity is skipped, not matched", func(t *testing.T) {
		mkAccount(t, 3, "", "")
		dup, err := m.DuplicateIdentity(Identity{AccountUUID: "u-1"})
		if err != nil {
			t.Fatal(err)
		}
		if dup == nil || dup.ID != 1 {
			t.Fatalf("got %+v, want acct 1 (the broken acct must be skipped, not error)", dup)
		}
	})
}

func TestInitIdempotentAndMarker(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	st := openTestStore(t)
	m := &Manager{Store: st, DetectOverlay: detectSymlink}

	if ok, err := m.Initialized(); err != nil || ok {
		t.Fatalf("fresh manager Initialized() = %v err=%v, want false", ok, err)
	}
	res, err := m.Init()
	if err != nil {
		t.Fatal(err)
	}
	if res.Already {
		t.Fatal("first Init reported Already")
	}
	if res.OverlayKind == "" {
		t.Fatal("Init did not record an overlay kind")
	}
	if ok, _ := m.Initialized(); !ok {
		t.Fatal("Initialized() false after Init")
	}

	res2, err := m.Init()
	if err != nil {
		t.Fatal(err)
	}
	if !res2.Already {
		t.Fatal("second Init did not report Already")
	}
	if res2.OverlayKind != res.OverlayKind {
		t.Fatalf("re-init flipped overlay kind %q -> %q", res.OverlayKind, res2.OverlayKind)
	}
}

// TestPrepareAddRepairsHalfAddedDir proves PrepareAdd repairs a dir left by an
// add that died mid-onboarding: index reused, stub overwritten with the seeded
// config.
func TestPrepareAddRepairsHalfAddedDir(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	base := ClaudeDir()
	for _, d := range []string{"projects", "backups"} {
		if err := os.MkdirAll(filepath.Join(base, d), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(base, "backups", ".claude.json.backup.1"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ClaudeJSONPath(), []byte(`{"hasCompletedOnboarding": true, "oauthAccount": {"accountUuid": "main"}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	acct := AccountDir(1)
	if err := os.MkdirAll(acct, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"daemon", "ide", "backups"} {
		if err := os.MkdirAll(filepath.Join(acct, d), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(base, "projects"), filepath.Join(acct, "projects")); err != nil {
		t.Fatal(err)
	}
	stub := `{"firstStartTime": "2026-06-06T07:57:05.707Z", "userID": "fresh"}`
	if err := os.WriteFile(filepath.Join(acct, ".claude.json"), []byte(stub), 0o600); err != nil {
		t.Fatal(err)
	}

	st := openTestStore(t)
	m := &Manager{Store: st, Creds: credstest.NewFake(), DetectOverlay: detectSymlink}
	if _, err := m.Init(); err != nil {
		t.Fatal(err)
	}
	pending, err := m.PrepareAdd()
	if err != nil {
		t.Fatal(err)
	}

	if pending.Index != 1 {
		t.Fatalf("index = %d, want 1 (no row exists, the dir is reused)", pending.Index)
	}
	if pending.ClaudeJSONSeed != SeedCopied {
		t.Fatalf("seed outcome = %q, want %q (stub must be overwritten)", pending.ClaudeJSONSeed, SeedCopied)
	}
	// Login must pin the plugin root to the shared base, else it stamps
	// acct-anchored paths into shared plugin state.
	wantLogin := fmt.Sprintf("CLAUDE_CODE_PLUGIN_CACHE_DIR=%s CLAUDE_CONFIG_DIR=%s claude /login",
		filepath.Join(base, "plugins"), acct)
	if pending.LoginCommand != wantLogin {
		t.Fatalf("LoginCommand = %q, want %q", pending.LoginCommand, wantLogin)
	}
	fi, err := os.Lstat(filepath.Join(acct, "backups"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode()&os.ModeSymlink != 0 || !fi.IsDir() {
		t.Fatalf("backups is not a private dir (mode %v)", fi.Mode())
	}
	if _, err := os.Stat(filepath.Join(base, "backups", ".claude.json.backup.1")); err != nil {
		t.Fatalf("base backups damaged: %v", err)
	}
	var seeded map[string]any
	if err := json.Unmarshal(readFile(t, filepath.Join(acct, ".claude.json")), &seeded); err != nil {
		t.Fatal(err)
	}
	if seeded["hasCompletedOnboarding"] != true {
		t.Fatalf("seeded config missing onboarding state: %v", seeded)
	}
	if _, ok := seeded["oauthAccount"]; ok {
		t.Fatal("seeded config leaked the main account's oauthAccount")
	}
}

// TestPrepareAddRequiresInit pins the defense-in-depth check behind add's
// auto-init.
func TestPrepareAddRequiresInit(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	st := openTestStore(t)
	m := &Manager{Store: st}
	if _, err := m.PrepareAdd(); err == nil || !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("PrepareAdd on fresh pool = %v, want ErrNotInitialized", err)
	}
}

// TestPrepareAddPurgesStaleCredentials pins that PrepareAdd purges stale
// credentials under a reused index — the Keychain item under its service name
// (else the login watcher false-positives) and the plaintext file a dead
// headless attempt left behind — except on the SeedKeptExisting path.
func TestPrepareAddPurgesStaleCredentials(t *testing.T) {
	setup := func(t *testing.T) (*Manager, *credstest.Fake, string) {
		t.Helper()
		t.Setenv("HOME", t.TempDir())
		t.Setenv("USER", "tester")
		if err := os.MkdirAll(ClaudeDir(), 0o700); err != nil {
			t.Fatal(err)
		}
		fk := credstest.NewFake()
		m := &Manager{Store: openTestStore(t), Creds: fk, DetectOverlay: detectSymlink}
		if _, err := m.Init(); err != nil {
			t.Fatal(err)
		}
		svc := creds.ServiceName(AccountDir(1))
		stale := &creds.Credential{}
		stale.ClaudeAiOauth.AccessToken = "at-stale"
		fk.Put(svc, "tester", stale)
		return m, fk, svc
	}

	t.Run("fresh dir purges the leftover", func(t *testing.T) {
		m, fk, svc := setup(t)
		if _, err := m.PrepareAdd(); err != nil {
			t.Fatal(err)
		}
		if _, ok := fk.Get(svc, "tester"); ok {
			t.Error("stale item survived")
		}
		if del := fk.DeletedServices(); len(del) != 1 || del[0] != svc {
			t.Errorf("deletes = %v, want exactly [%q]", del, svc)
		}
	})

	t.Run("purges an item stored under a different -a label", func(t *testing.T) {
		// The stale item's label is whatever claude wrote at login; the purge must
		// find it by service, not today's label.
		m, fk, svc := setup(t)
		fk.Remove(svc, "tester")
		stale := &creds.Credential{}
		stale.ClaudeAiOauth.AccessToken = "at-stale"
		fk.Put(svc, "someone-else", stale)
		if _, err := m.PrepareAdd(); err != nil {
			t.Fatal(err)
		}
		if _, ok := fk.Get(svc, "someone-else"); ok {
			t.Error("label-mismatched stale item survived")
		}
	})

	t.Run("fresh dir purges a stale file credential", func(t *testing.T) {
		// A dead headless attempt leaves .credentials.json instead of a Keychain
		// item; a reused index must not inherit it.
		m, _, _ := setup(t)
		acct := AccountDir(1)
		if err := os.MkdirAll(acct, 0o700); err != nil {
			t.Fatal(err)
		}
		stale := &creds.Credential{}
		stale.ClaudeAiOauth.AccessToken = "at-stale-file"
		if err := creds.WriteFileCredential(acct, stale); err != nil {
			t.Fatal(err)
		}
		if _, err := m.PrepareAdd(); err != nil {
			t.Fatal(err)
		}
		if creds.FileCredentialExists(acct) {
			t.Error("stale file credential survived")
		}
	})

	t.Run("kept-existing dir keeps both credentials", func(t *testing.T) {
		m, fk, svc := setup(t)
		acct := AccountDir(1)
		if err := os.MkdirAll(acct, 0o700); err != nil {
			t.Fatal(err)
		}
		loggedIn := `{"oauthAccount": {"accountUuid": "u-prior"}}`
		if err := os.WriteFile(filepath.Join(acct, ".claude.json"), []byte(loggedIn), 0o600); err != nil {
			t.Fatal(err)
		}
		kept := &creds.Credential{}
		kept.ClaudeAiOauth.AccessToken = "at-kept-file"
		if err := creds.WriteFileCredential(acct, kept); err != nil {
			t.Fatal(err)
		}
		pending, err := m.PrepareAdd()
		if err != nil {
			t.Fatal(err)
		}
		if pending.ClaudeJSONSeed != SeedKeptExisting {
			t.Fatalf("seed outcome = %q, want %q", pending.ClaudeJSONSeed, SeedKeptExisting)
		}
		if _, ok := fk.Get(svc, "tester"); !ok {
			t.Error("kept keychain credential was purged")
		}
		if !creds.FileCredentialExists(acct) {
			t.Error("kept file credential was purged")
		}
	})
}

// TestFinalizeAddRequiresIdentity pins the anti-adoption gate: FinalizeAdd
// refuses to register an account whose login wrote no oauthAccount rather than
// pool a copy of plain claude's adopted secret.
func TestFinalizeAddRequiresIdentity(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if err := os.MkdirAll(ClaudeDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	m := &Manager{Store: openTestStore(t), Creds: credstest.NewFake(), DetectOverlay: detectSymlink}
	if _, err := m.Init(); err != nil {
		t.Fatal(err)
	}
	pending, err := m.PrepareAdd() // no ~/.claude.json to seed → no identity
	if err != nil {
		t.Fatal(err)
	}
	acct, err := m.FinalizeAdd(context.Background(), pending, "")
	if acct != nil {
		t.Fatalf("FinalizeAdd returned acct %+v, want nil when no identity was written", acct)
	}
	if !errors.Is(err, ErrNoIdentity) {
		t.Fatalf("FinalizeAdd err = %v, want ErrNoIdentity", err)
	}
}

// TestAbandonAddDeletesBothStores pins that rolling back a half-added account
// rolls back whatever credential its login wrote — the Keychain item AND the
// plaintext file — explicitly, not as a side effect of dir removal.
func TestAbandonAddDeletesBothStores(t *testing.T) {
	setup := func(t *testing.T) (*Manager, *credstest.Fake, *PendingAdd) {
		t.Helper()
		t.Setenv("HOME", t.TempDir())
		t.Setenv("USER", "tester")
		if err := os.MkdirAll(ClaudeDir(), 0o700); err != nil {
			t.Fatal(err)
		}
		fk := credstest.NewFake()
		m := &Manager{Store: openTestStore(t), Creds: fk, DetectOverlay: detectSymlink}
		if _, err := m.Init(); err != nil {
			t.Fatal(err)
		}
		pending, err := m.PrepareAdd()
		if err != nil {
			t.Fatal(err)
		}
		// The login lands a Keychain credential under a label differing from
		// today's AccountLabel() (the rollback must discover it by service) and,
		// headless, a plaintext file too.
		cred := &creds.Credential{}
		cred.ClaudeAiOauth.AccessToken = "at-login"
		fk.Put(pending.KeychainService, "claude-wrote-this", cred)
		if err := creds.WriteFileCredential(pending.ConfigDir, cred); err != nil {
			t.Fatal(err)
		}
		return m, fk, pending
	}

	t.Run("rollback deletes both stores and the dir", func(t *testing.T) {
		m, fk, pending := setup(t)
		if err := m.AbandonAdd(pending); err != nil {
			t.Fatalf("AbandonAdd: %v", err)
		}
		if _, ok := fk.Get(pending.KeychainService, "claude-wrote-this"); ok {
			t.Error("keychain credential survived the rollback")
		}
		if creds.FileCredentialExists(pending.ConfigDir) {
			t.Error("file credential survived the rollback")
		}
		if _, err := os.Stat(pending.ConfigDir); !os.IsNotExist(err) {
			t.Errorf("account dir survived the rollback: %v", err)
		}
	})

	t.Run("credentials are purged even when dir removal cannot run", func(t *testing.T) {
		m, fk, pending := setup(t)
		m.OverlayFor = func(fkoverlay.Backend) (fkoverlay.Provider, error) {
			return nil, errors.New("holder gone")
		}
		if err := m.AbandonAdd(pending); err == nil {
			t.Fatal("AbandonAdd succeeded despite the overlay provider failing")
		}
		if _, ok := fk.Get(pending.KeychainService, "claude-wrote-this"); ok {
			t.Error("keychain credential survived")
		}
		if creds.FileCredentialExists(pending.ConfigDir) {
			t.Error("file credential survived (rollback depended on dir removal)")
		}
		if _, err := os.Stat(pending.ConfigDir); err != nil {
			t.Errorf("account dir unexpectedly gone despite the failed teardown: %v", err)
		}
	})
}

// TestFinalizeAddResolvesBackend pins backend resolution at registration:
// Keychain first (with the ACL-owning write-back), else the plaintext file
// under the computed label, else a refusal naming the incomplete login.
func TestFinalizeAddResolvesBackend(t *testing.T) {
	cases := []struct {
		name        string
		keychain    bool // login wrote a Keychain item (under its own label)
		file        bool // login wrote .credentials.json
		wantErr     string
		wantAccount string
		wantWrites  int // keychain writes: exactly the ACL re-assert
	}{
		{name: "keychain credential wins and is re-asserted", keychain: true, file: true, wantAccount: "claude-wrote", wantWrites: 1},
		{name: "file credential registers with the computed label", file: true, wantAccount: "tester", wantWrites: 0},
		{name: "no credential refuses", wantErr: "no credential found"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			t.Setenv("USER", "tester")
			if err := os.MkdirAll(ClaudeDir(), 0o700); err != nil {
				t.Fatal(err)
			}
			fk := credstest.NewFake()
			m := &Manager{Store: openTestStore(t), Creds: fk, OAuth: &fakeOAuth{currentRT: "rt-0"}, DetectOverlay: detectSymlink, LockDir: t.TempDir()}
			if _, err := m.Init(); err != nil {
				t.Fatal(err)
			}
			pending, err := m.PrepareAdd()
			if err != nil {
				t.Fatal(err)
			}
			// Simulate the interactive login: identity lands in .claude.json plus a
			// credential in one (or both) backends.
			identity := `{"oauthAccount": {"accountUuid": "u-new", "emailAddress": "new@example.com"}}`
			if err := os.WriteFile(filepath.Join(pending.ConfigDir, ".claude.json"), []byte(identity), 0o600); err != nil {
				t.Fatal(err)
			}
			cred := &creds.Credential{}
			cred.ClaudeAiOauth.AccessToken = "at-0"
			cred.ClaudeAiOauth.RefreshToken = "rt-0"
			cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli() // fresh: no refresh write
			if tc.keychain {
				fk.Put(pending.KeychainService, "claude-wrote", cred)
			}
			if tc.file {
				if err := creds.WriteFileCredential(pending.ConfigDir, cred); err != nil {
					t.Fatal(err)
				}
			}

			acct, err := m.FinalizeAdd(context.Background(), pending, "note")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("FinalizeAdd = %v, want error containing %q", err, tc.wantErr)
				}
				if acct != nil {
					t.Fatalf("FinalizeAdd returned acct %+v with the refusal", acct)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if acct.KeychainAccount != tc.wantAccount {
				t.Errorf("KeychainAccount = %q, want %q", acct.KeychainAccount, tc.wantAccount)
			}
			if got := fk.WriteCount(); got != tc.wantWrites {
				t.Errorf("keychain writes = %d, want %d (exactly the ACL write-back)", got, tc.wantWrites)
			}
			row, gerr := m.Store.GetAccount(pending.Index)
			if gerr != nil {
				t.Fatalf("registered row missing: %v", gerr)
			}
			if row.KeychainAccount != tc.wantAccount {
				t.Errorf("row KeychainAccount = %q, want %q", row.KeychainAccount, tc.wantAccount)
			}
		})
	}
}

// TestRemoveKeepCredential pins the --keep-credential guard: a file-backed
// credential lives inside the account dir and cannot survive removal, so
// Remove must refuse (naming the cred-move escape hatch); a keychain-backed or
// absent credential proceeds, keeping the Keychain item untouched.
func TestRemoveKeepCredential(t *testing.T) {
	errHard := errors.New("keychain exploded")
	cases := []struct {
		name        string
		keychain    bool
		file        bool
		keychainErr error  // injected keychain read fault
		wantErr     string // substring; empty means the removal proceeds
		wantKept    bool   // keychain item still present afterwards
	}{
		{name: "file-backed refuses with the move hint", file: true, wantErr: "ccp cred move --to keychain --account 1"},
		{name: "keychain-backed removes and keeps the item", keychain: true, wantKept: true},
		{name: "no credential anywhere proceeds"},
		{name: "unavailable keychain proceeds", keychainErr: creds.ErrUnavailable},
		{name: "hard keychain error refuses", keychainErr: errHard, wantErr: "keychain exploded"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			st := openTestStore(t)
			dir := t.TempDir()
			a := store.Account{ID: 1, ConfigDir: dir, KeychainService: "svc", KeychainAccount: "user", OverlayKind: "symlink"}
			if err := st.UpsertAccount(a); err != nil {
				t.Fatal(err)
			}
			fk := credstest.NewFake()
			fk.KeychainFaults = credstest.Faults{Read: tc.keychainErr}
			cred := &creds.Credential{}
			cred.ClaudeAiOauth.AccessToken = "at-1"
			if tc.keychain {
				fk.Put(a.KeychainService, a.KeychainAccount, cred)
			}
			if tc.file {
				if err := creds.WriteFileCredential(dir, cred); err != nil {
					t.Fatal(err)
				}
			}
			m := &Manager{Store: st, Creds: fk}

			err := m.Remove(a.ID, false)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Remove = %v, want error containing %q", err, tc.wantErr)
				}
				if _, gerr := st.GetAccount(a.ID); gerr != nil {
					t.Fatalf("refused Remove deleted the row: %v", gerr)
				}
				if _, serr := os.Stat(dir); serr != nil {
					t.Fatalf("refused Remove damaged the account dir: %v", serr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, gerr := st.GetAccount(a.ID); gerr == nil {
				t.Fatal("row survived Remove")
			}
			if _, ok := fk.Get(a.KeychainService, a.KeychainAccount); ok != tc.wantKept {
				t.Errorf("keychain item present = %v, want %v", ok, tc.wantKept)
			}
		})
	}
}

// TestConcurrentPrepareAddIndexRace pins the pending-row reservation fix:
// concurrent PrepareAdds (still no accounts row until FinalizeAdd) must be
// handed distinct indices — and so distinct dirs and Keychain services —
// because ReserveAccountIndex allocates atomically.
func TestConcurrentPrepareAddIndexRace(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if err := os.MkdirAll(ClaudeDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	st := openTestStore(t)
	m := &Manager{Store: st, Creds: credstest.NewFake(), DetectOverlay: detectSymlink}
	if _, err := m.Init(); err != nil {
		t.Fatal(err)
	}

	type result struct {
		p   *PendingAdd
		err error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			p, err := m.PrepareAdd()
			results <- result{p, err}
		}()
	}
	close(start)
	var got []*PendingAdd
	for range 2 {
		r := <-results
		if r.err != nil {
			t.Fatalf("PrepareAdd: %v", r.err)
		}
		got = append(got, r.p)
	}

	if got[0].Index == got[1].Index {
		t.Fatalf("both PrepareAdds were handed index %d — the reservation must allocate distinct indices", got[0].Index)
	}
	if got[0].ConfigDir == got[1].ConfigDir {
		t.Fatalf("both PrepareAdds share config dir %s", got[0].ConfigDir)
	}
	if got[0].KeychainService == got[1].KeychainService {
		t.Fatalf("both PrepareAdds share Keychain service %s", got[0].KeychainService)
	}
}

// stubOverlay is an injectable fuse-kind provider whose Setup can be forced to
// fail.
type stubOverlay struct {
	backend  fkoverlay.Backend
	setupErr error
	setups   int
}

func (s *stubOverlay) Backend() fkoverlay.Backend { return s.backend }
func (s *stubOverlay) Sync(_, _ string) error     { return nil }
func (s *stubOverlay) Health(_, _ string) error   { return nil }
func (s *stubOverlay) Teardown(_, _ string) error { return nil }
func (s *stubOverlay) PrivateRoot(dir string) string {
	if s.backend.IsFuse() {
		return fkoverlay.FusePrivateRoot(dir)
	}
	return dir
}

func (s *stubOverlay) Setup(_, dir string) error {
	s.setups++
	if s.setupErr != nil {
		return s.setupErr
	}
	// Mimic the real fuse provider's footprint: mountpoint dir plus the private
	// backing dir beside it.
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return os.MkdirAll(fkoverlay.FusePrivateRoot(dir), 0o700)
}

// TestPrepareAddFuseFallback pins the fuse→symlink fallback: when the recorded
// kind is fuse but the provider can't establish it, PrepareAdd records the
// symlink overlay it actually established and carries the reason — never a silent
// substitution.
func TestPrepareAddFuseFallback(t *testing.T) {
	setup := func(t *testing.T, fuse fkoverlay.Provider) *Manager {
		t.Helper()
		t.Setenv("HOME", t.TempDir())
		if err := os.MkdirAll(filepath.Join(ClaudeDir(), "projects"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(ClaudeJSONPath(), []byte(`{"hasCompletedOnboarding":true}`), 0o600); err != nil {
			t.Fatal(err)
		}
		m := &Manager{Store: openTestStore(t), Creds: credstest.NewFake()}
		m.DetectOverlay = func() (fkoverlay.Backend, string) { return fkoverlay.BackendNFS, "" }
		m.OverlayFor = func(kind fkoverlay.Backend) (fkoverlay.Provider, error) {
			if kind.IsFuse() {
				return fuse, nil
			}
			return newSymlinkProvider(), nil
		}
		if _, err := m.Init(); err != nil {
			t.Fatal(err)
		}
		return m
	}

	t.Run("fuse setup failure falls back to symlinks and says why", func(t *testing.T) {
		m := setup(t, &stubOverlay{backend: fkoverlay.BackendNFS, setupErr: errors.New("mount holder did not start: boom")})
		// The holder created the backing dir before Setup failed; the fallback must
		// not leave it behind.
		if err := os.MkdirAll(fkoverlay.FusePrivateRoot(AccountDir(1)), 0o700); err != nil {
			t.Fatal(err)
		}
		pending, err := m.PrepareAdd()
		if err != nil {
			t.Fatalf("PrepareAdd: %v", err)
		}
		if pending.ConfigDir != AccountDir(1) {
			t.Fatalf("ConfigDir = %q, want %q (the account whose backing dir was pre-created)", pending.ConfigDir, AccountDir(1))
		}
		if pending.OverlayKind != fkoverlay.BackendSymlink {
			t.Fatalf("OverlayKind = %q, want symlink (the overlay actually established)", pending.OverlayKind)
		}
		if pending.FallbackReason != "mount holder did not start: boom" {
			t.Fatalf("FallbackReason = %q, want the fuse setup error", pending.FallbackReason)
		}
		if _, err := os.Readlink(filepath.Join(pending.ConfigDir, "projects")); err != nil {
			t.Fatalf("symlink overlay not established: %v", err)
		}
		if pending.ClaudeJSONSeed != SeedCopied {
			t.Fatalf("seed outcome = %q, want %q", pending.ClaudeJSONSeed, SeedCopied)
		}
		if _, err := os.Stat(filepath.Join(pending.ConfigDir, ".claude.json")); err != nil {
			t.Fatalf("seed not in the account dir: %v", err)
		}
		if _, err := os.Lstat(filepath.Join(fkoverlay.FusePrivateRoot(pending.ConfigDir), ".claude.json")); !os.IsNotExist(err) {
			t.Fatal("seed leaked into the fuse private root despite the fallback")
		}
		if _, err := os.Lstat(fkoverlay.FusePrivateRoot(pending.ConfigDir)); !os.IsNotExist(err) {
			t.Fatalf("empty fuse private root left behind after the fallback (lstat err = %v)", err)
		}
	})

	t.Run("fuse and fallback both failing reports both causes", func(t *testing.T) {
		fuseErr := errors.New("dir is a foreign mount carcass")
		symErr := errors.New("refusing to lay symlinks in a live mountpoint")
		m := setup(t, &stubOverlay{backend: fkoverlay.BackendNFS, setupErr: fuseErr})
		m.OverlayFor = func(kind fkoverlay.Backend) (fkoverlay.Provider, error) {
			if kind.IsFuse() {
				return &stubOverlay{backend: fkoverlay.BackendNFS, setupErr: fuseErr}, nil
			}
			return &stubOverlay{backend: fkoverlay.BackendSymlink, setupErr: symErr}, nil
		}
		pending, err := m.PrepareAdd()
		if pending != nil {
			t.Fatalf("PrepareAdd returned %+v despite both setups failing", pending)
		}
		if err == nil {
			t.Fatal("PrepareAdd succeeded, want a both-setups failure")
		}
		// Both causes must ride the chain (errors.Is), else the symlink complaint
		// masks the fuse failure that started the fallback.
		if !errors.Is(err, fuseErr) {
			t.Errorf("errors.Is(err, fuseErr) = false; err = %v", err)
		}
		if !errors.Is(err, symErr) {
			t.Errorf("errors.Is(err, symErr) = false; err = %v", err)
		}
		if !strings.Contains(err.Error(), fuseErr.Error()) || !strings.Contains(err.Error(), symErr.Error()) {
			t.Errorf("error text missing a cause: %v", err)
		}
	})

	t.Run("an unmitigated holder falls back to symlinks before any mount", func(t *testing.T) {
		// The gated provider is exactly what OverlayProviderFor hands every real
		// `ccp add`: a v0.36.0 pool whose recorded default is fuse but whose
		// cask still serves a pre-mitigation holder must fall back to symlink —
		// never mount on the nfs_vinvalbuf2 panic vector.
		stub := &stubOverlay{backend: fkoverlay.BackendNFS}
		m := setup(t, mitigationGate{Provider: stub, health: func() (string, error) { return "v0.22.1", nil }})
		pending, err := m.PrepareAdd()
		if err != nil {
			t.Fatalf("PrepareAdd: %v", err)
		}
		if stub.setups != 0 {
			t.Fatalf("fuse setups = %d, want 0 (no mount may be attempted on an unmitigated holder)", stub.setups)
		}
		if pending.OverlayKind != fkoverlay.BackendSymlink {
			t.Fatalf("OverlayKind = %q, want symlink", pending.OverlayKind)
		}
		if !strings.Contains(pending.FallbackReason, "brew upgrade --cask fusekit-holder") {
			t.Fatalf("FallbackReason = %q, want the cask-upgrade hint", pending.FallbackReason)
		}
		if _, err := os.Readlink(filepath.Join(pending.ConfigDir, "projects")); err != nil {
			t.Fatalf("symlink overlay not established: %v", err)
		}
	})

	t.Run("fuse setup success keeps fuse and carries no reason", func(t *testing.T) {
		stub := &stubOverlay{backend: fkoverlay.BackendNFS}
		m := setup(t, stub)
		pending, err := m.PrepareAdd()
		if err != nil {
			t.Fatalf("PrepareAdd: %v", err)
		}
		if !pending.OverlayKind.IsFuse() {
			t.Fatalf("OverlayKind = %q, want a fuse backend", pending.OverlayKind)
		}
		if pending.FallbackReason != "" {
			t.Fatalf("FallbackReason = %q, want empty", pending.FallbackReason)
		}
		if stub.setups != 1 {
			t.Fatalf("fuse setups = %d, want 1", stub.setups)
		}
		if _, err := os.Stat(filepath.Join(fkoverlay.FusePrivateRoot(pending.ConfigDir), ".claude.json")); err != nil {
			t.Fatalf("seed not in the private root: %v", err)
		}
	})

	t.Run("a non-fuse setup failure stays fatal", func(t *testing.T) {
		m := setup(t, &stubOverlay{backend: fkoverlay.BackendNFS})
		if err := m.SetDefaultOverlayKind(fkoverlay.BackendSymlink); err != nil {
			t.Fatal(err)
		}
		m.OverlayFor = func(fkoverlay.Backend) (fkoverlay.Provider, error) {
			return &stubOverlay{backend: fkoverlay.BackendSymlink, setupErr: errors.New("disk full")}, nil
		}
		_, err := m.PrepareAdd()
		if err == nil || !strings.Contains(err.Error(), "disk full") {
			t.Fatalf("PrepareAdd = %v, want the symlink setup failure propagated", err)
		}
	})
}

// TestPrepareAddSurfacesDetectReason pins the legacy-pool path (initialized
// marker but no recorded kind): detection runs inside PrepareAdd and a symlink
// verdict's reason rides out on PendingAdd.FallbackReason.
func TestPrepareAddSurfacesDetectReason(t *testing.T) {
	const reason = "this build cannot host fuse mounts; install fuse-t"
	t.Setenv("HOME", t.TempDir())
	if err := os.MkdirAll(filepath.Join(ClaudeDir(), "projects"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ClaudeJSONPath(), []byte(`{"hasCompletedOnboarding":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	if err := EnsureAccountsDir(); err != nil {
		t.Fatal(err)
	}
	m := &Manager{Store: openTestStore(t), Creds: credstest.NewFake()}
	m.DetectOverlay = func() (fkoverlay.Backend, string) { return fkoverlay.BackendSymlink, reason }
	if err := m.Store.SetMeta(metaInitialized, "1"); err != nil {
		t.Fatal(err)
	}

	pending, err := m.PrepareAdd()
	if err != nil {
		t.Fatalf("PrepareAdd: %v", err)
	}
	if pending.OverlayKind != fkoverlay.BackendSymlink {
		t.Fatalf("OverlayKind = %q, want symlink", pending.OverlayKind)
	}
	if pending.FallbackReason != reason {
		t.Fatalf("FallbackReason = %q, want the detection reason %q", pending.FallbackReason, reason)
	}
}

// TestInitSurfacesOverlayFallbackReason pins that the Init running detection
// reports why fuse was ruled out, while later Inits (kind already recorded) skip
// detection and report nothing.
func TestInitSurfacesOverlayFallbackReason(t *testing.T) {
	const reason = "probe mount declined (fuse-t missing or Network Volumes access denied)"
	t.Setenv("HOME", t.TempDir())
	m := &Manager{Store: openTestStore(t)}
	m.DetectOverlay = func() (fkoverlay.Backend, string) { return fkoverlay.BackendSymlink, reason }

	res, err := m.Init()
	if err != nil {
		t.Fatal(err)
	}
	if res.OverlayKind != fkoverlay.BackendSymlink {
		t.Fatalf("OverlayKind = %q, want symlink", res.OverlayKind)
	}
	if res.OverlayFallbackReason != reason {
		t.Fatalf("OverlayFallbackReason = %q, want %q", res.OverlayFallbackReason, reason)
	}

	m.DetectOverlay = func() (fkoverlay.Backend, string) {
		t.Error("re-init re-ran detection")
		return "", ""
	}
	res2, err := m.Init()
	if err != nil {
		t.Fatal(err)
	}
	if !res2.Already || res2.OverlayKind != fkoverlay.BackendSymlink || res2.OverlayFallbackReason != "" {
		t.Fatalf("re-init = %+v, want already/symlink/no reason", res2)
	}
}

// TestInitFuseVerdictCarriesNoReason pins that a fuse verdict carries no reason.
func TestInitFuseVerdictCarriesNoReason(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	m := &Manager{Store: openTestStore(t)}
	m.DetectOverlay = func() (fkoverlay.Backend, string) { return fkoverlay.BackendNFS, "" }
	res, err := m.Init()
	if err != nil {
		t.Fatal(err)
	}
	if !res.OverlayKind.IsFuse() || res.OverlayFallbackReason != "" {
		t.Fatalf("res = %+v, want fuse with no reason", res)
	}
}
