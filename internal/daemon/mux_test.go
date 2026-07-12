package daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// flipFuseRows marks the given store rows fuse-backed so fuseAccounts() sees them.
func flipFuseRows(t *testing.T, s *Server, ids ...int) {
	t.Helper()
	for _, id := range ids {
		a, err := s.m.Store.GetAccount(id)
		if err != nil {
			t.Fatal(err)
		}
		a.OverlayKind = "nfs"
		if err := s.m.Store.UpsertAccount(a); err != nil {
			t.Fatal(err)
		}
	}
}

// muxSetupSim models the fuse provider's setupMux for the fake fuse provider: an
// empty account dir is replaced by the bridge symlink, a non-empty one is refused
// (ErrAccountDirOccupied), and an existing symlink is left in place.
func muxSetupSim(_, dir string) error {
	fi, err := os.Lstat(dir)
	switch {
	case os.IsNotExist(err):
	case err != nil:
		return err
	case fi.Mode()&os.ModeSymlink != 0:
		return nil
	case !fi.IsDir():
		return fmt.Errorf("%w: %s is a file", fkoverlay.ErrAccountDirOccupied, dir)
	default:
		entries, rerr := os.ReadDir(dir)
		if rerr != nil {
			return rerr
		}
		if len(entries) > 0 {
			return fmt.Errorf("%w: %s holds %d entries", fkoverlay.ErrAccountDirOccupied, dir, len(entries))
		}
		if err := os.Remove(dir); err != nil {
			return err
		}
	}
	return os.Symlink(filepath.Join(pool.MuxRootDir(), filepath.Base(dir)), dir)
}

// TestHolderStateRefreshTranslatesMuxDir pins the one wire→ConfigDir translation
// in refresh: a mux subtree the holder lists is keyed by its account ConfigDir, a
// legacy per-dir mount passes through unchanged, and neither leaves the raw
// subtree path in the cache.
func TestHolderStateRefreshTranslatesMuxDir(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	muxSubtree := filepath.Join(pool.MuxRootDir(), "acct-01")
	muxConfig := filepath.Join(pool.AccountsDir(), "acct-01")
	legacyDir := filepath.Join(pool.AccountsDir(), "acct-02") // a per-dir mount: Dir == ConfigDir

	var h holderState
	h.refresh(mountd.NewClient(startCannedHolder(t, []mountd.MountInfo{
		{Dir: muxSubtree, Base: "/base", Live: true, MuxRoot: pool.MuxRootDir()},
		{Dir: legacyDir, Base: "/base", Live: true},
	})))

	if !h.ready(muxConfig) {
		t.Fatalf("refresh did not key the mux mount by its ConfigDir %s", muxConfig)
	}
	if h.ready(muxSubtree) {
		t.Fatalf("refresh keyed the mount by the raw subtree %s, want the ConfigDir", muxSubtree)
	}
	if !h.ready(legacyDir) {
		t.Fatalf("refresh did not pass a legacy per-dir mount %s through unchanged", legacyDir)
	}
}

// TestReconcileMigratesLegacyFuseRowToBridge pins the one-time legacy→mux
// migration and its idempotence: a fuse row whose ConfigDir is a real dir is
// drained (shared orphans to base, private left in the backing root) and replaced
// by the bridge symlink; a re-run adopts the now-bridged account without a second
// Setup.
func TestReconcileMigratesLegacyFuseRowToBridge(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	fake.setupFn = muxSetupSim // simulate setupMux: drain the empty dir + bridge symlink
	dir, base := dirs[1], pool.ClaudeDir()

	// Legacy fuse rest state: a bare per-dir mountpoint with a shared orphan claude
	// wrote after unmount, plus the account identity in its private backing root.
	if err := os.WriteFile(filepath.Join(dir, "history.jsonl"), []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	priv := fkoverlay.FusePrivateRoot(dir)
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(priv, ".claude.json"), []byte("identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	s.reconcileAccount(t.Context(), a)

	if !pool.IsBridgeSymlink(dir) {
		t.Fatal("migration did not lay the bridge symlink")
	}
	if got, err := os.ReadFile(filepath.Join(base, "history.jsonl")); err != nil || string(got) != "orphan" { //nolint:gosec // G304: base is a test-owned temp dir, not user input.
		t.Fatalf("shared orphan not swept to base: %q err=%v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(priv, ".claude.json")); err != nil || string(got) != "identity" { //nolint:gosec // G304: priv is under the test's temp home
		t.Fatalf("private identity disturbed by migration: %q err=%v", got, err)
	}
	if got := kindOf(t, s, 1); got != "nfs" {
		t.Fatalf("row demoted during migration: %q", got)
	}
	if !s.holder.ready(dir) {
		t.Fatal("migrated mount not vouched in the holder cache")
	}
	setups := fake.setupCount()

	// Idempotent re-run: the dir is a bridge symlink now, so reconcile adopts it
	// (Health nil) instead of migrating again.
	s.reconcileAccount(t.Context(), a)
	if got := fake.setupCount(); got != setups {
		t.Fatalf("re-reconcile ran Setup again (%d, was %d): an already-bridged account was re-migrated", got, setups)
	}
	if !pool.IsBridgeSymlink(dir) {
		t.Fatal("re-reconcile disturbed the bridge symlink")
	}
}

// TestReconcileAccountRaceSkipsFuseArmForConvertedRow is the finding-2 regression: an
// OpSelect/OpMigrate converts an account fuse->symlink during startup reconcile,
// between the shared account listing and this account's poll claim. reconcileAccount
// must re-read the row under the claim and branch on the fresh backend — never run the
// destructive fuse arm (migrateLegacyFuseRow: drainDirForBridge + a fuse mount) on a
// row that is now symlink in SQLite.
func TestReconcileAccountRaceSkipsFuseArmForConvertedRow(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	dir := dirs[1]

	// The stale snapshot the shared listing handed reconcileAccount says nfs — a legacy
	// per-dir mount needing the mux migration (its dir is still a real dir).
	stale, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	stale.OverlayKind = "nfs"

	// The race: the row is symlink in SQLite by the time the poll claim lands.
	setRowKind(t, s, 1, fkoverlay.BackendSymlink)

	s.reconcileAccount(t.Context(), stale)

	if got := fake.setupCount(); got != 0 {
		t.Fatalf("reconcile ran the fuse arm (mount) on a row converted to symlink: fuse setups=%d, want 0", got)
	}
	if got := fake.teardownCount(); got != 0 {
		t.Fatalf("reconcile ran the fuse arm (teardown) on a converted row: fuse teardowns=%d, want 0", got)
	}
	if pool.IsBridgeSymlink(dir) {
		t.Fatal("reconcile migrated a converted row: a mux bridge symlink was laid")
	}
	if got := kindOf(t, s, 1); got != "symlink" {
		t.Fatalf("reconcile disturbed the converted row: kind=%q, want symlink", got)
	}
}

// TestReconcileMigrationRefusesUnmovableContent pins the fail-closed refusal: when
// the drain cannot classify-and-move the account dir clean (a file colliding with
// a directory in base), the migration refuses loudly — content intact, no bridge
// laid, no Setup attempted, row unchanged.
func TestReconcileMigrationRefusesUnmovableContent(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	fake.setupFn = muxSetupSim
	dir, base := dirs[1], pool.ClaudeDir()

	if err := os.WriteFile(filepath.Join(dir, "clash"), []byte("acct-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(base, "clash"), 0o700); err != nil {
		t.Fatal(err)
	}
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	s.reconcileAccount(t.Context(), a)

	if pool.IsBridgeSymlink(dir) {
		t.Fatal("bridge symlink laid over an un-drainable account dir")
	}
	if fake.setupCount() != 0 {
		t.Fatal("Setup ran despite a failed drain")
	}
	if got, err := os.ReadFile(filepath.Join(dir, "clash")); err != nil || string(got) != "acct-data" { //nolint:gosec // G304: dir is a test-owned temp dir, not user input.
		t.Fatalf("unmovable content clobbered: %q err=%v", got, err)
	}
	if got := kindOf(t, s, 1); got != "nfs" {
		t.Fatalf("row demoted despite a refused migration: %q", got)
	}
}

// TestReconcileLegacyMountMigrationSessionGated pins the per-account session gate
// on the legacy migration: a live legacy mountpoint under a session defers (never
// force-unmounted — that panics a busy NFS mirror), while an idle one is
// force-unmounted and migrated.
func TestReconcileLegacyMountMigrationSessionGated(t *testing.T) {
	cases := map[string]struct {
		teardownErr error
		wantBridge  bool
	}{
		"a held-lease legacy mount defers":   {teardownErr: mountd.ErrBusy, wantBridge: false},
		"a free-lease legacy mount migrates": {teardownErr: nil, wantBridge: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, dirs, fake := newMigrateServer(t)
			fake.setupFn = muxSetupSim
			fake.teardownErr = tc.teardownErr
			dir := dirs[1]
			// The account dir reads as a live legacy mountpoint; the mux root does not.
			fakeOverlayMounted(t, func(d string) bool { return d == dir })

			a, err := s.m.Store.GetAccount(1)
			if err != nil {
				t.Fatal(err)
			}
			a.OverlayKind = "nfs"
			if err := s.m.Store.UpsertAccount(a); err != nil {
				t.Fatal(err)
			}

			s.reconcileAccount(t.Context(), a)

			if got := pool.IsBridgeSymlink(dir); got != tc.wantBridge {
				t.Fatalf("bridge symlink laid = %v, want %v", got, tc.wantBridge)
			}
			if got := kindOf(t, s, 1); got != "nfs" {
				t.Fatalf("row demoted: %q, want it left on fuse", got)
			}
		})
	}
}

// TestMigrateLegacyClaimAtomicAgainstSelect pins the migration's claim-first
// discipline: the destructive drain runs under a convert claim (released after),
// and the holder-cache vouch is dropped the instant the legacy mount comes down.
func TestMigrateLegacyClaimAtomicAgainstSelect(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	fake.setupFn = muxSetupSim
	dir := dirs[1]
	a := flipToFuse(t, s, 1)
	// A legacy per-dir mount the holder still vouches for.
	fakeOverlayMounted(t, func(d string) bool { return d == dir })
	s.holder.noteMounted(dir)

	s.reconcileAccount(t.Context(), a)

	if !pool.IsBridgeSymlink(dir) {
		t.Fatal("bridge symlink not laid after the migration")
	}
	if s.cl.held(1) {
		t.Fatal("migration leaked its converting claim")
	}
}

// TestMigrateLegacyDropsCacheVouchOnDrainFailure pins the cache invalidation: a
// drain that fails after the holder teardown must leave the holder cache NOT
// vouching for the torn-down mount (else a select launches onto it).
func TestMigrateLegacyDropsCacheVouchOnDrainFailure(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	fake.setupFn = muxSetupSim
	dir, base := dirs[1], pool.ClaudeDir()
	a := flipToFuse(t, s, 1)
	// Unmovable content: a file colliding with a base dir fails the drain after the
	// teardown already happened.
	if err := os.WriteFile(filepath.Join(dir, "clash"), []byte("acct-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(base, "clash"), 0o700); err != nil {
		t.Fatal(err)
	}
	fakeOverlayMounted(t, func(d string) bool { return d == dir })
	s.holder.noteMounted(dir)

	s.reconcileAccount(t.Context(), a)

	if s.holder.ready(dir) {
		t.Fatal("holder cache still vouches for the torn-down legacy mount after a failed drain")
	}
	if pool.IsBridgeSymlink(dir) {
		t.Fatal("bridge symlink laid despite a failed drain")
	}
	if fake.setupCount() != 0 {
		t.Fatal("Setup ran despite a failed drain")
	}
	if kindOf(t, s, 1) != "nfs" {
		t.Fatal("row demoted on a failed drain")
	}
}

// TestEscalateWedgedRowRouting pins the v2 wedged-row recovery: a mux subtree
// tripping the remount breaker retreats to symlink (the holder owns any force
// path now), and a held session lease (teardown answers ErrBusy) leaves it fuse.
func TestEscalateWedgedRowRouting(t *testing.T) {
	cases := map[string]struct {
		teardownErr error
		wantKind    string
	}{
		"a free-lease wedged row retreats to symlink": {teardownErr: nil, wantKind: "symlink"},
		"a held-lease wedged row stays fuse":          {teardownErr: mountd.ErrBusy, wantKind: "nfs"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, dirs, fake := newMigrateServer(t)
			flipFuseRows(t, s, 1)
			fake.teardownErr = tc.teardownErr
			// A legacy mounted dir: the retreat routes through the holder's
			// lease-gated teardown.
			fakeOverlayMounted(t, func(d string) bool { return d == dirs[1] })

			a1, err := s.m.Store.GetAccount(1)
			if err != nil {
				t.Fatal(err)
			}
			s.escalateWedgedRow(t.Context(), a1)

			if got := kindOf(t, s, 1); got != tc.wantKind {
				t.Fatalf("row kind after escalation = %q, want %q", got, tc.wantKind)
			}
		})
	}
}
