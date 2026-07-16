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

// muxReconcileSim models the fuse provider's reconcileMux for the fake fuse provider: an
// empty account dir is replaced by the bridge symlink, a non-empty one is refused
// (ErrAccountDirOccupied), and an existing symlink is left in place.
func muxReconcileSim(_, dir string) error {
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

// TestReconcileAccountRaceSkipsFuseArmForConvertedRow is the finding-2 regression: an
// OpSelect/OpMigrate converts an account fuse->symlink during startup reconcile,
// between the shared account listing and this account's poll claim. reconcileAccount
// must re-read the row under the claim and branch on the fresh backend — never run the
// destructive fuse arm (healFuse: a fuse mount) on a row that is now symlink in SQLite.
func TestReconcileAccountRaceSkipsFuseArmForConvertedRow(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	dir := dirs[1]

	// The stale snapshot the shared listing handed reconcileAccount says nfs.
	stale, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	stale.OverlayKind = "nfs"

	// The race: the row is symlink in SQLite by the time the poll claim lands.
	setRowKind(t, s, 1, fkoverlay.BackendSymlink)

	s.reconcileAccount(t.Context(), stale)

	if got := fake.reconcileCount(); got != 0 {
		t.Fatalf("reconcile ran the fuse arm (mount) on a row converted to symlink: fuse reconciles=%d, want 0", got)
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
