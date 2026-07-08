package daemon

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
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
		sessions      []procscan.Session
		wantUnmounted bool
		wantBridge    bool
	}{
		"busy legacy mount defers":   {wantUnmounted: false, wantBridge: false},
		"idle legacy mount migrates": {wantUnmounted: true, wantBridge: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, dirs, fake := newMigrateServer(t)
			fake.setupFn = muxSetupSim
			dir := dirs[1]
			// The account dir reads as a live legacy mountpoint; the mux root does not.
			fakeOverlayMounted(t, func(d string) bool { return d == dir })
			if name == "busy legacy mount defers" {
				s.scanSessions = func(context.Context) ([]procscan.Session, error) {
					return []procscan.Session{{PID: 4242, ConfigDir: dir}}, nil
				}
			}
			var unmounted []string
			swapForceUnmount(t, func(d string) error { unmounted = append(unmounted, d); return nil })

			a, err := s.m.Store.GetAccount(1)
			if err != nil {
				t.Fatal(err)
			}
			a.OverlayKind = "nfs"
			if err := s.m.Store.UpsertAccount(a); err != nil {
				t.Fatal(err)
			}

			s.reconcileAccount(t.Context(), a)

			gotUnmounted := len(unmounted) == 1 && unmounted[0] == dir
			if gotUnmounted != tc.wantUnmounted {
				t.Fatalf("force-unmounted %v, want unmount=%v of the legacy mountpoint", unmounted, tc.wantUnmounted)
			}
			if got := pool.IsBridgeSymlink(dir); got != tc.wantBridge {
				t.Fatalf("bridge symlink laid = %v, want %v", got, tc.wantBridge)
			}
			if got := kindOf(t, s, 1); got != "nfs" {
				t.Fatalf("row demoted: %q, want it left on fuse", got)
			}
		})
	}
}

// TestNativeMountWedged pins the native-vs-subtree discrimination: a lone wedged
// subtree (siblings healthy) is isolated; a wedged sibling means the shared native
// mount is the fault.
func TestNativeMountWedged(t *testing.T) {
	s, dirs, _ := newMigrateServer(t)
	flipFuseRows(t, s, 1, 2)
	a1, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if s.nativeMountWedged(a1) {
		t.Fatal("a lone wedged subtree read as a native-mount wedge (siblings are healthy)")
	}
	s.holder.markDeepWedged(dirs[2])
	if !s.nativeMountWedged(a1) {
		t.Fatal("a wedged sibling did not read as a native-mount wedge")
	}
}

// TestSweepOrphanMuxRoot pins the orphan native-mount sweep: a mounted root with
// no reachable holder is force-unmounted when idle, but a live holder owns the
// root (left alone), an unmounted root is a no-op, and a live fuse session defers
// the pool-wide force-unmount.
func TestSweepOrphanMuxRoot(t *testing.T) {
	cases := map[string]struct {
		peerAlive bool
		mounted   bool
		busy      bool
		wantSwept bool
	}{
		"orphaned root idle is swept":            {peerAlive: false, mounted: true, wantSwept: true},
		"a live holder owns the root, left":      {peerAlive: true, mounted: true, wantSwept: false},
		"an unmounted root is a no-op":           {peerAlive: false, mounted: false, wantSwept: false},
		"orphaned root under a session deferred": {peerAlive: false, mounted: true, busy: true, wantSwept: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, dirs, _ := newMigrateServer(t)
			root := pool.MuxRootDir()
			s.peerAlive = func(string) bool { return tc.peerAlive }
			fakeOverlayMounted(t, func(d string) bool { return tc.mounted && d == root })
			if tc.busy {
				s.scanSessions = func(context.Context) ([]procscan.Session, error) {
					return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
				}
			}
			var unmounted []string
			swapForceUnmount(t, func(d string) error { unmounted = append(unmounted, d); return nil })

			s.sweepOrphanMuxRoot(t.Context(), []store.Account{{ID: 1, ConfigDir: dirs[1], OverlayKind: "nfs"}})

			swept := len(unmounted) == 1 && unmounted[0] == root
			if swept != tc.wantSwept {
				t.Fatalf("force-unmounted %v, want swept=%v of the mux root %s", unmounted, tc.wantSwept, root)
			}
		})
	}
}

// TestMigrateLegacyClaimAtomicAgainstSelect pins the migration's claim-first
// discipline (finding 1): the destructive drain runs under a convert claim, so a
// select cannot reserve the account between the gate and the force-unmount, and the
// holder-cache vouch is dropped the instant the legacy mount comes down.
func TestMigrateLegacyClaimAtomicAgainstSelect(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	fake.setupFn = muxSetupSim
	dir := dirs[1]
	a := flipToFuse(t, s, 1)
	// A legacy per-dir mount the holder still vouches for.
	fakeOverlayMounted(t, func(d string) bool { return d == dir })
	s.holder.noteMounted(dir)
	reservedMidMigration := false
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		reservedMidMigration = s.cl.reserve(1)
		return nil, nil
	}
	var unmounted []string
	swapForceUnmount(t, func(d string) error { unmounted = append(unmounted, d); return nil })

	s.reconcileAccount(t.Context(), a)

	if reservedMidMigration {
		t.Fatal("a select reserved the account between the migration claim and the drain")
	}
	if len(unmounted) != 1 || unmounted[0] != dir {
		t.Fatalf("legacy mount force-unmounted %v, want exactly [%s]", unmounted, dir)
	}
	if !pool.IsBridgeSymlink(dir) {
		t.Fatal("bridge symlink not laid after the migration")
	}
	if s.cl.held(1) {
		t.Fatal("migration leaked its converting claim")
	}
}

// TestMigrateLegacyDropsCacheVouchOnDrainFailure pins finding 1's cache
// invalidation: a drain that fails after the force-unmount must leave the holder
// cache NOT vouching for the torn-down mount (else a select launches onto it).
func TestMigrateLegacyDropsCacheVouchOnDrainFailure(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	fake.setupFn = muxSetupSim
	dir, base := dirs[1], pool.ClaudeDir()
	a := flipToFuse(t, s, 1)
	// Unmovable content: a file colliding with a base dir fails the drain after the
	// force-unmount already happened.
	if err := os.WriteFile(filepath.Join(dir, "clash"), []byte("acct-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(base, "clash"), 0o700); err != nil {
		t.Fatal(err)
	}
	fakeOverlayMounted(t, func(d string) bool { return d == dir })
	s.holder.noteMounted(dir)
	s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil }
	swapForceUnmount(t, func(string) error { return nil })

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

// TestMigrateLegacyDefersOnUnmitigatedHolder pins finding 6: a reachable holder
// predating MinHolderVersion defers the WHOLE migration — the working legacy mount
// is never torn down (which would strand the account until the cask upgrade).
func TestMigrateLegacyDefersOnUnmitigatedHolder(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	fake.setupFn = muxSetupSim
	dir := dirs[1]
	a := flipToFuse(t, s, 1)
	fakeOverlayMounted(t, func(d string) bool { return d == dir })
	s.holder.mu.Lock()
	s.holder.healthy, s.holder.version = true, "v0.28.0"
	s.holder.mu.Unlock()
	var unmounted []string
	swapForceUnmount(t, func(d string) error { unmounted = append(unmounted, d); return nil })
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)

	s.reconcileAccount(t.Context(), a)

	if len(unmounted) != 0 {
		t.Fatalf("tore down the working legacy mount %v on an unmitigated holder", unmounted)
	}
	if pool.IsBridgeSymlink(dir) {
		t.Fatal("migrated onto an unmitigated holder")
	}
	if fake.setupCount() != 0 {
		t.Fatal("Setup attempted on an unmitigated holder")
	}
	if kindOf(t, s, 1) != "nfs" {
		t.Fatal("row demoted while deferring the migration")
	}
	if !strings.Contains(buf.String(), "brew upgrade --cask fusekit-holder") {
		t.Fatalf("cask-upgrade deferral not surfaced:\n%s", buf.String())
	}
}

// TestMigrateLegacyBareDirDefersUnderLiveSession pins finding 8: the live-session
// gate is unconditional, so a re-run on a bare half-migrated dir (not a mountpoint,
// not yet a bridge) under a live session defers instead of draining + bridging the
// account's CLAUDE_CONFIG_DIR out from under the running claude.
func TestMigrateLegacyBareDirDefersUnderLiveSession(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	fake.setupFn = muxSetupSim
	dir := dirs[1]
	a := flipToFuse(t, s, 1)
	// Bare, half-migrated: not a mountpoint, not a bridge symlink.
	fakeOverlayMounted(t, func(string) bool { return false })
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dir}}, nil
	}
	var unmounted []string
	swapForceUnmount(t, func(d string) error { unmounted = append(unmounted, d); return nil })

	s.reconcileAccount(t.Context(), a)

	if pool.IsBridgeSymlink(dir) {
		t.Fatal("drained + bridged a bare dir under a live session (CLAUDE_CONFIG_DIR swapped out)")
	}
	if fake.setupCount() != 0 {
		t.Fatal("Setup ran under a live session on a bare dir")
	}
	if len(unmounted) != 0 {
		t.Fatalf("force-unmounted %v on a bare dir", unmounted)
	}
	if kindOf(t, s, 1) != "nfs" {
		t.Fatal("row demoted while deferring")
	}
}

// TestAnyLiveFuseSessionCountsRowlessBridge pins finding 2: the pool-idle gate
// derives busy from the scan itself, so a pre-FinalizeAdd `ccp add` session (a
// bridge symlink under accounts/ with no account row yet) still defers a
// pool-wide force-unmount; a rowless real dir or a session elsewhere does not.
func TestAnyLiveFuseSessionCountsRowlessBridge(t *testing.T) {
	s, dirs, _ := newMigrateServer(t)
	rowlessBridge := filepath.Join(pool.AccountsDir(), "acct-07")
	makeBridge(t, rowlessBridge)
	rowlessReal := filepath.Join(pool.AccountsDir(), "acct-08")
	if err := os.MkdirAll(rowlessReal, 0o700); err != nil {
		t.Fatal(err)
	}

	cases := map[string]struct {
		sessions []procscan.Session
		fuse     []store.Account
		wantBusy bool
		wantDir  string
	}{
		"rowless bridge session defers": {
			sessions: []procscan.Session{{PID: 1, ConfigDir: rowlessBridge}},
			wantBusy: true, wantDir: rowlessBridge,
		},
		"rowless real-dir session is idle": {
			sessions: []procscan.Session{{PID: 1, ConfigDir: rowlessReal}},
			wantBusy: false,
		},
		"session on an unrelated dir is idle": {
			sessions: []procscan.Session{{PID: 1, ConfigDir: "/somewhere/else"}},
			wantBusy: false,
		},
		"row-backed fuse session still defers": {
			sessions: []procscan.Session{{PID: 1, ConfigDir: dirs[1]}},
			fuse:     []store.Account{{ID: 1, ConfigDir: dirs[1], OverlayKind: "nfs"}},
			wantBusy: true, wantDir: dirs[1],
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s.scanSessions = func(context.Context) ([]procscan.Session, error) { return tc.sessions, nil }
			busy, dir, n := s.anyLiveFuseSession(t.Context(), tc.fuse)
			if busy != tc.wantBusy {
				t.Fatalf("busy = %v, want %v", busy, tc.wantBusy)
			}
			if tc.wantBusy && (dir != tc.wantDir || n != 1) {
				t.Fatalf("busy on (%q, %d), want (%q, 1)", dir, n, tc.wantDir)
			}
		})
	}
}

// TestNativeRecoveryBlocksReservations pins finding 5's pool-wide claim: while a
// native-root force-unmount is in flight no account may be reserved by a select,
// and a live reservation refuses a fresh recovery.
func TestNativeRecoveryBlocksReservations(t *testing.T) {
	s, dirs, _ := newMigrateServer(t)
	fuse := []store.Account{{ID: 1, ConfigDir: dirs[1], OverlayKind: "nfs"}}
	if !s.cl.ownPool(fuse) {
		t.Fatal("beginNativeRecovery failed on a free pool")
	}
	if s.cl.reserve(1) {
		t.Fatal("tryReserve succeeded during native recovery")
	}
	if s.cl.reserve(2) {
		t.Fatal("tryReserve succeeded for a sibling during native recovery")
	}
	s.cl.disownPool()
	if !s.cl.reserve(1) {
		t.Fatal("tryReserve failed after native recovery ended")
	}
	if s.cl.ownPool(fuse) {
		t.Fatal("beginNativeRecovery succeeded over a live reservation")
	}
}

// TestSweepOrphanMuxRootClaimAtomicAgainstSelect pins finding 5's scan→unmount
// window: the reservation claim is set before the session scan, so a select cannot
// reserve a fuse account between the scan and the shared-root force-unmount.
func TestSweepOrphanMuxRootClaimAtomicAgainstSelect(t *testing.T) {
	s, dirs, _ := newMigrateServer(t)
	root := pool.MuxRootDir()
	flipFuseRows(t, s, 1)
	makeBridge(t, dirs[1])
	s.peerAlive = func(string) bool { return false } // dead holder → carcass
	fakeOverlayMounted(t, func(d string) bool { return d == root })
	reservedMidSweep := false
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		reservedMidSweep = s.cl.reserve(1)
		return nil, nil
	}
	swapForceUnmount(t, func(string) error { return nil })

	s.sweepOrphanMuxRoot(t.Context(), []store.Account{{ID: 1, ConfigDir: dirs[1], OverlayKind: "nfs"}})

	if reservedMidSweep {
		t.Fatal("a select reserved a fuse account between the session scan and the root force-unmount")
	}
}

// TestSweepOrphanMuxRootHolderOwnership pins finding 7: a live peer is not enough —
// a freshly respawned empty-registry holder does NOT own a root a dead predecessor
// left mounted, so that carcass is swept; a holder actually serving a subtree of the
// root owns it and is left alone.
func TestSweepOrphanMuxRootHolderOwnership(t *testing.T) {
	cases := map[string]struct {
		listedKind string // "mux" = a subtree of our root, "plain" = a non-mux row, "empty" = none
		wantSwept  bool
	}{
		"holder serving a subtree owns the root, left":                    {listedKind: "mux", wantSwept: false},
		"live-but-empty holder over a carcass is swept":                   {listedKind: "empty", wantSwept: true},
		"holder serving only a plain (non-mux) row does not own the root": {listedKind: "plain", wantSwept: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, dirs, _ := newMigrateServer(t)
			root := pool.MuxRootDir() // computed AFTER newMigrateServer swaps HOME
			var listed []mountd.MountInfo
			switch tc.listedKind {
			case "mux":
				listed = []mountd.MountInfo{{Dir: filepath.Join(root, "acct-01"), Base: "/base", Live: true, MuxRoot: root}}
			case "plain":
				listed = []mountd.MountInfo{{Dir: "/elsewhere", Base: "/base", Live: true}}
			}
			s.peerAlive = func(string) bool { return true } // holder alive in every case
			s.holderSocket = startCannedHolder(t, listed)
			fakeOverlayMounted(t, func(d string) bool { return d == root })
			s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil }
			var unmounted []string
			swapForceUnmount(t, func(d string) error { unmounted = append(unmounted, d); return nil })

			s.sweepOrphanMuxRoot(t.Context(), []store.Account{{ID: 1, ConfigDir: dirs[1], OverlayKind: "nfs"}})

			swept := len(unmounted) == 1 && unmounted[0] == root
			if swept != tc.wantSwept {
				t.Fatalf("swept = %v (force-unmount calls %v), want %v", swept, unmounted, tc.wantSwept)
			}
		})
	}
}

// TestEscalateWedgedRowRouting pins the two-tier recovery a mux subtree tripping
// the remount breaker takes: an isolated subtree wedge never force-unmounts the
// shared root (a kernel-free per-row retreat), a pool-wide wedge force-unmounts
// the root when idle, and defers it under a live session.
func TestEscalateWedgedRowRouting(t *testing.T) {
	cases := map[string]struct {
		siblingWedged   bool
		session         bool
		wantRootUnmount bool
	}{
		"isolated subtree wedge leaves the native root mounted": {siblingWedged: false, wantRootUnmount: false},
		"native wedge pool-idle force-unmounts the root":        {siblingWedged: true, wantRootUnmount: true},
		"native wedge under a live session defers":              {siblingWedged: true, session: true, wantRootUnmount: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, dirs, _ := newMigrateServer(t)
			flipFuseRows(t, s, 1, 2)
			makeBridge(t, dirs[1])
			makeBridge(t, dirs[2])
			root := pool.MuxRootDir()
			// The account dirs are bridge symlinks (not mountpoints); only the shared
			// native root reads mounted.
			fakeOverlayMounted(t, func(d string) bool { return d == root })
			if tc.siblingWedged {
				s.holder.markDeepWedged(dirs[2])
			}
			if tc.session {
				s.scanSessions = func(context.Context) ([]procscan.Session, error) {
					return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
				}
			}
			var unmounted []string
			swapForceUnmount(t, func(d string) error { unmounted = append(unmounted, d); return nil })

			a1, err := s.m.Store.GetAccount(1)
			if err != nil {
				t.Fatal(err)
			}
			s.escalateWedgedRow(t.Context(), a1)

			rootUnmounted := false
			for _, d := range unmounted {
				if d == root {
					rootUnmounted = true
				}
			}
			if rootUnmounted != tc.wantRootUnmount {
				t.Fatalf("root force-unmounted = %v (calls %v), want %v", rootUnmounted, unmounted, tc.wantRootUnmount)
			}
		})
	}
}

// TestSweepHolderOrphans pins the dead-holder recovery (2026-07 incident): the
// sweep reaps the orphaned go-nfsv4 a crashed holder left bound to cc-pool mounts
// (always, carcass-gated in fusekit) and force-unmounts the mux root only when no
// live session rides it — a session defers the unmount (kernel-panic class) but
// never the reap.
func TestSweepHolderOrphans(t *testing.T) {
	cases := map[string]struct {
		session       bool
		wantUnmounted bool
	}{
		"idle: reap the orphan and force-unmount the mux root":                {session: false, wantUnmounted: true},
		"live session: reap but defer the force-unmount (kernel-panic class)": {session: true, wantUnmounted: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, dirs, _ := newMigrateServer(t)
			flipFuseRows(t, s, 1)
			makeBridge(t, dirs[1])
			root := pool.MuxRootDir()
			s.peerAlive = func(string) bool { return false } // dead holder → orphan carcass
			fakeOverlayMounted(t, func(d string) bool { return d == root })
			if tc.session {
				s.scanSessions = func(context.Context) ([]procscan.Session, error) {
					return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
				}
			}
			var reapedRoots [][]string
			swapReapOrphans(t, func(roots []string) []int {
				reapedRoots = append(reapedRoots, roots)
				return []int{909}
			})
			var unmounted []string
			swapForceUnmount(t, func(d string) error { unmounted = append(unmounted, d); return nil })

			s.sweepHolderOrphans(t.Context())

			// The reap is unconditional (carcass-gated in fusekit); roots cover the mux
			// go-nfsv4 (bound to the mux root) and legacy per-dir servers (under accounts/).
			wantRoots := []string{root, pool.AccountsDir()}
			if len(reapedRoots) != 1 || !reflect.DeepEqual(reapedRoots[0], wantRoots) {
				t.Fatalf("reaped roots = %v, want exactly one call with %v", reapedRoots, wantRoots)
			}
			gotUnmount := len(unmounted) == 1 && unmounted[0] == root
			if gotUnmount != tc.wantUnmounted {
				t.Fatalf("force-unmounted %v (mux-root unmount = %v), want unmount = %v", unmounted, gotUnmount, tc.wantUnmounted)
			}
		})
	}
}

// TestHolderDeathSchedulesOrphanSweep pins the full item-1 wiring: markUnhealthy's
// transition, wired as serve() wires it, schedules the async orphan sweep the
// instant a healthy holder serving mounts goes unreachable — no 5-strike wait.
func TestHolderDeathSchedulesOrphanSweep(t *testing.T) {
	s, dirs, _ := newMigrateServer(t)
	flipFuseRows(t, s, 1)
	makeBridge(t, dirs[1])
	root := pool.MuxRootDir()
	s.peerAlive = func(string) bool { return false }
	fakeOverlayMounted(t, func(d string) bool { return d == root })

	var reaped int
	swapReapOrphans(t, func([]string) []int { reaped++; return nil })
	var unmounted []string
	swapForceUnmount(t, func(d string) error { unmounted = append(unmounted, d); return nil })

	// Wire the hook and serve context exactly as serve() does.
	s.serveCtx = t.Context()
	s.holder.onLostWithMounts = s.scheduleHolderLostSweep

	// The holder was healthy and serving a mount, then its socket goes dead.
	s.holder.mu.Lock()
	s.holder.healthy = true
	s.holder.mounts = map[string]bool{dirs[1]: true}
	s.holder.mu.Unlock()
	s.holder.markUnhealthy() // schedules the tracked sweep goroutine (Add ran synchronously)

	s.wg.Wait() // the sweep goroutine is tracked; Wait synchronizes with its Done

	if reaped != 1 {
		t.Fatalf("holder death reaped %d time(s), want exactly one orphan sweep", reaped)
	}
	if len(unmounted) != 1 || unmounted[0] != root {
		t.Fatalf("holder death force-unmounted %v, want exactly [%s]", unmounted, root)
	}
}

// TestHolderDeathThroughDegradedSchedulesOrphanSweep pins the incident-shaped
// crash at the server level: the holder degrades first (Health up, List failing
// mid-teardown), THEN goes unreachable. The degraded poll must neither swallow
// the loss memory nor trigger any sweep while the holder is still reachable.
func TestHolderDeathThroughDegradedSchedulesOrphanSweep(t *testing.T) {
	s, dirs, _ := newMigrateServer(t)
	flipFuseRows(t, s, 1)
	makeBridge(t, dirs[1])
	root := pool.MuxRootDir()
	s.peerAlive = func(string) bool { return false }
	fakeOverlayMounted(t, func(d string) bool { return d == root })

	var reaped int
	swapReapOrphans(t, func([]string) []int { reaped++; return nil })
	var unmounted []string
	swapForceUnmount(t, func(d string) error { unmounted = append(unmounted, d); return nil })

	s.serveCtx = t.Context()
	s.holder.onLostWithMounts = s.scheduleHolderLostSweep

	s.holder.mu.Lock()
	s.holder.healthy = true
	s.holder.mounts = map[string]bool{dirs[1]: true}
	s.holder.mu.Unlock()

	s.holder.markDegraded("v9") // Health answers, List fails: still reachable
	s.wg.Wait()
	if reaped != 0 || len(unmounted) != 0 {
		t.Fatalf("degraded holder triggered reap=%d unmount=%v; a reachable holder may still own its mounts", reaped, unmounted)
	}

	s.holder.markUnhealthy() // the socket goes dead
	s.wg.Wait()
	if reaped != 1 {
		t.Fatalf("death through the degraded step reaped %d time(s), want exactly 1", reaped)
	}
	if len(unmounted) != 1 || unmounted[0] != root {
		t.Fatalf("death through the degraded step force-unmounted %v, want exactly [%s]", unmounted, root)
	}
}

// TestReconcileOverlaysReapsOrphansAtStartup pins the cold-start recovery: a
// fresh daemon over an already-dead holder sees no healthy→unreachable
// transition, so the startup reconcile itself must reap — unconditionally,
// before any per-account decision (carcass-gated in fusekit, so a live
// holder's servers are never touched).
func TestReconcileOverlaysReapsOrphansAtStartup(t *testing.T) {
	s, _, _ := newHealServer(t) // holder socket starts dead: the cold-start shape
	fakeOverlayMounted(t, func(string) bool { return false })

	var reapedRoots [][]string
	swapReapOrphans(t, func(roots []string) []int {
		reapedRoots = append(reapedRoots, roots)
		return []int{909}
	})

	s.reconcileOverlays(t.Context())

	wantRoots := []string{pool.MuxRootDir(), pool.AccountsDir()}
	if len(reapedRoots) != 1 || !reflect.DeepEqual(reapedRoots[0], wantRoots) {
		t.Fatalf("startup reconcile reaped roots %v, want exactly one call with %v", reapedRoots, wantRoots)
	}
}

// TestBeginNativeRecoveryExcludesOverlappingSweeps pins sweep coalescing: a
// holder-loss sweep and a startup/periodic sweep of the same mux root must not
// interleave — the second claimant defers instead of double-unmounting and
// releasing the first one's claim early.
func TestBeginNativeRecoveryExcludesOverlappingSweeps(t *testing.T) {
	s := &Server{cl: newClaims()}
	if !s.cl.ownPool(nil) {
		t.Fatal("first native-recovery claim refused on an idle pool")
	}
	if s.cl.ownPool(nil) {
		t.Fatal("second claim granted while a native recovery is in flight")
	}
	s.cl.disownPool()
	if !s.cl.ownPool(nil) {
		t.Fatal("claim refused after the in-flight recovery released")
	}
}
