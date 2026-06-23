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

	"github.com/yasyf/cc-pool/internal/keychain"
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

// detectSymlink is the DetectOverlay injection for tests whose semantics are
// provider-independent: a deterministic symlink verdict, so Init never probes
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
		a := store.Account{ID: id, ConfigDir: dir, KeychainService: keychain.ServiceName(dir), KeychainAccount: "user", OverlayKind: "symlink"}
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
		mkAccount(t, 3, "", "") // no .claude.json
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

// TestPrepareAddRepairsHalfAddedDir reconstructs the forensic state of an add
// that died at the onboarding wizard (overlay symlinks, private daemon/ide/backups
// dirs, a pre-login .claude.json stub) and proves the normal PrepareAdd flow
// repairs it in place: index reused, stub overwritten with the seeded config.
func TestPrepareAddRepairsHalfAddedDir(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	// Plain claude's state: ~/.claude with shared entries, ~/.claude.json.
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

	// The half-added acct-01 dir from a previous broken run.
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
	// The repair semantics pinned here are provider-independent; inject a
	// symlink verdict so PrepareAdd never routes through a mount holder this
	// test can't host (a real fuse detection would move the private root to
	// acct-01.private).
	m := &Manager{Store: st, Keychain: newFakeKeychain(), DetectOverlay: detectSymlink}
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
	// The login command must pin claude's plugin root to the shared base, or
	// the login session stamps acct-anchored paths into shared plugin state.
	wantLogin := fmt.Sprintf("CLAUDE_CODE_PLUGIN_CACHE_DIR=%s CLAUDE_CONFIG_DIR=%s claude /login",
		filepath.Join(base, "plugins"), acct)
	if pending.LoginCommand != wantLogin {
		t.Fatalf("LoginCommand = %q, want %q", pending.LoginCommand, wantLogin)
	}
	// backups: the existing private dir is left intact; base is never touched.
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
	// Seeded config: onboarding inherited, identity stripped.
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

// TestPrepareAddPurgesStaleKeychainItem pins the stale-item fix: an abandoned
// add (or `ccp remove --keep-credential`) can leave a credential under a
// service name a later add at the same index reuses; PrepareAdd must purge it
// (else the login watcher false-positives and FinalizeAdd registers a stale
// credential) — EXCEPT for SeedKeptExisting, the documented reuse path.
func TestPrepareAddPurgesStaleKeychainItem(t *testing.T) {
	setup := func(t *testing.T) (*Manager, *fakeKeychain, string) {
		t.Helper()
		t.Setenv("HOME", t.TempDir())
		t.Setenv("USER", "tester")
		if err := os.MkdirAll(ClaudeDir(), 0o700); err != nil {
			t.Fatal(err)
		}
		fk := newFakeKeychain()
		// Provider-independent purge semantics; inject symlink for the same
		// reason as TestPrepareAddRepairsHalfAddedDir (no mounts in tests).
		m := &Manager{Store: openTestStore(t), Keychain: fk, DetectOverlay: detectSymlink}
		if _, err := m.Init(); err != nil {
			t.Fatal(err)
		}
		svc := keychain.ServiceName(AccountDir(1))
		stale := &keychain.Credential{}
		stale.ClaudeAiOauth.AccessToken = "at-stale"
		if err := fk.Write(svc, "tester", stale); err != nil {
			t.Fatal(err)
		}
		return m, fk, svc
	}

	t.Run("fresh dir purges the leftover", func(t *testing.T) {
		m, fk, svc := setup(t)
		if _, err := m.PrepareAdd(); err != nil {
			t.Fatal(err)
		}
		if _, err := fk.Read(svc, "tester"); !errors.Is(err, keychain.ErrNotFound) {
			t.Errorf("stale item survived: %v", err)
		}
		if del := fk.deletedServices(); len(del) != 1 || del[0] != svc {
			t.Errorf("deletes = %v, want exactly [%q]", del, svc)
		}
	})

	t.Run("purges an item stored under a different -a label", func(t *testing.T) {
		// The stale item carries whatever label claude derived at ITS login
		// (USER drift, fallback label); the purge must discover it by service,
		// like every consumer does, not recompute today's label.
		m, fk, svc := setup(t)
		if err := fk.Delete(svc, "tester"); err != nil {
			t.Fatal(err)
		}
		stale := &keychain.Credential{}
		stale.ClaudeAiOauth.AccessToken = "at-stale"
		if err := fk.Write(svc, "someone-else", stale); err != nil {
			t.Fatal(err)
		}
		if _, err := m.PrepareAdd(); err != nil {
			t.Fatal(err)
		}
		if _, err := fk.Read(svc, "someone-else"); !errors.Is(err, keychain.ErrNotFound) {
			t.Errorf("label-mismatched stale item survived: %v", err)
		}
	})

	t.Run("kept-existing dir keeps the credential", func(t *testing.T) {
		m, fk, svc := setup(t)
		// A logged-in .claude.json from a prior attempt → SeedKeptExisting.
		acct := AccountDir(1)
		if err := os.MkdirAll(acct, 0o700); err != nil {
			t.Fatal(err)
		}
		loggedIn := `{"oauthAccount": {"accountUuid": "u-prior"}}`
		if err := os.WriteFile(filepath.Join(acct, ".claude.json"), []byte(loggedIn), 0o600); err != nil {
			t.Fatal(err)
		}
		pending, err := m.PrepareAdd()
		if err != nil {
			t.Fatal(err)
		}
		if pending.ClaudeJSONSeed != SeedKeptExisting {
			t.Fatalf("seed outcome = %q, want %q", pending.ClaudeJSONSeed, SeedKeptExisting)
		}
		if _, err := fk.Read(svc, "tester"); err != nil {
			t.Errorf("kept credential was purged: %v", err)
		}
	})
}

// TestFinalizeAddRequiresIdentity pins the anti-adoption gate: a credential can
// land in an account dir without a real login — with a fresh CLAUDE_CONFIG_DIR
// claude adopts the global session's secret into the suffixed Keychain item (or
// the plaintext .credentials.json headless over SSH) at startup, writing no
// oauthAccount. FinalizeAdd must refuse to register such an account rather than
// pool a copy of plain claude's login. The gate precedes any credential read,
// so this is hermetic (no Keychain, no network).
func TestFinalizeAddRequiresIdentity(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if err := os.MkdirAll(ClaudeDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	m := &Manager{Store: openTestStore(t), Keychain: newFakeKeychain(), DetectOverlay: detectSymlink}
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

// TestAbandonAddDeletesKeychainItem pins that rolling back a half-added
// account also rolls back the credential its login wrote.
func TestAbandonAddDeletesKeychainItem(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("USER", "tester")
	if err := os.MkdirAll(ClaudeDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	fk := newFakeKeychain()
	m := &Manager{Store: openTestStore(t), Keychain: fk, DetectOverlay: detectSymlink}
	if _, err := m.Init(); err != nil {
		t.Fatal(err)
	}
	pending, err := m.PrepareAdd()
	if err != nil {
		t.Fatal(err)
	}
	// Simulate the interactive login landing a credential — under a label that
	// differs from today's AccountLabel(), which the rollback must discover.
	cred := &keychain.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-login"
	if err := fk.Write(pending.KeychainService, "claude-wrote-this", cred); err != nil {
		t.Fatal(err)
	}

	if err := m.AbandonAdd(pending); err != nil {
		t.Fatalf("AbandonAdd: %v", err)
	}
	if _, err := fk.Read(pending.KeychainService, "claude-wrote-this"); !errors.Is(err, keychain.ErrNotFound) {
		t.Errorf("credential survived the rollback: %v", err)
	}
	if _, err := os.Stat(pending.ConfigDir); !os.IsNotExist(err) {
		t.Errorf("account dir survived the rollback: %v", err)
	}
}

// TestConcurrentPrepareAddIndexRace documents the known index-reservation gap:
// two concurrent PrepareAdds (no row until FinalizeAdd) are handed the same
// index and thus the same dir + derived Keychain service. Fixing it needs a
// pending-row reservation; until then this pins the current behavior so the
// decision stays explicit.
func TestConcurrentPrepareAddIndexRace(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if err := os.MkdirAll(ClaudeDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	st := openTestStore(t)
	m := &Manager{Store: st, Keychain: newFakeKeychain(), DetectOverlay: detectSymlink}
	if _, err := m.Init(); err != nil {
		t.Fatal(err)
	}
	p1, err := m.PrepareAdd()
	if err != nil {
		t.Fatal(err)
	}
	p2, err := m.PrepareAdd()
	if err != nil {
		t.Fatal(err)
	}
	if p1.Index != p2.Index {
		t.Fatalf("indexes %d vs %d — the reservation gap closed; update this test and the AGENTS.md follow-up note", p1.Index, p2.Index)
	}
}

// stubOverlay is a minimal injectable provider for PrepareAdd's fuse-fallback
// tests: a fuse-kind provider whose Setup can be forced to fail.
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
	// Mimic the real fuse provider's footprint: a mountpoint dir plus the
	// private backing dir beside it.
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return os.MkdirAll(fkoverlay.FusePrivateRoot(dir), 0o700)
}

// TestPrepareAddFuseFallback pins the explicit fuse→symlink fallback: when the
// recorded kind is fuse but the provider cannot establish the overlay (holder
// unavailable), PrepareAdd records the symlink overlay it actually established
// and carries the reason for the CLI to print — never a silent substitution,
// never a row promising a mirror the dir doesn't have.
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
		m := &Manager{Store: openTestStore(t), Keychain: newFakeKeychain()}
		m.DetectOverlay = func() (fkoverlay.Backend, string) { return FuseBackend(), "" }
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
		// Simulate the holder getting as far as creating the backing dir
		// before Setup failed: the fallback must not leave it behind.
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
		// The dir really is a symlink overlay: shared entries are linked and
		// the seed landed in the dir itself, not in a fuse private root.
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
		// The empty backing dir the fuse attempt left behind was cleaned up —
		// a symlink account must not accrete an inert acct-NN.private.
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
		// Both causes ride the chain: the symlink complaint alone would mask
		// the fuse failure that started the fallback, and callers must be able
		// to match either with errors.Is.
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
		// The seed lands in the fuse private root, never through a mount.
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

// TestPrepareAddSurfacesDetectReason pins the legacy-pool path where no
// overlay kind was recorded at init (initialized marker only — Init normally
// writes the kind first): detection runs inside PrepareAdd, and a symlink
// verdict's reason must ride out on PendingAdd.FallbackReason, not vanish.
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
	m := &Manager{Store: openTestStore(t), Keychain: newFakeKeychain()}
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

// TestInitSurfacesOverlayFallbackReason pins the detect-reason plumbing: the
// Init that runs detection reports why fuse was ruled out; later Inits (kind
// already recorded) re-detect nothing and report nothing.
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

	// Re-init must not re-detect: the recorded kind wins and no reason leaks.
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

// TestInitFuseVerdictCarriesNoReason is the negative twin: a fuse verdict has
// nothing to explain.
func TestInitFuseVerdictCarriesNoReason(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	m := &Manager{Store: openTestStore(t)}
	m.DetectOverlay = func() (fkoverlay.Backend, string) { return FuseBackend(), "" }
	res, err := m.Init()
	if err != nil {
		t.Fatal(err)
	}
	if !res.OverlayKind.IsFuse() || res.OverlayFallbackReason != "" {
		t.Fatalf("res = %+v, want fuse with no reason", res)
	}
}
