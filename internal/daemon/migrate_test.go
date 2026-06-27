package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/keychain"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// fakeFuseProv stands in for the fuse provider so handler-level conversion
// runs without a live mount. Mechanics (file moves, identity verification,
// rollback ordering) are pinned by internal/pool's convert tests; these tests
// pin the daemon's gating and wiring. The fn seams override the static errors
// when set and run outside the fake's lock so they may inspect the fake.
type fakeFuseProv struct {
	mu          sync.Mutex
	calls       []string // "setup"/"teardown" in invocation order
	setups      int
	teardowns   int
	healths     int
	setupErr    error
	teardownErr error
	healthErr   error
	setupFn     func(base, dir string) error
	teardownFn  func(base, dir string) error
}

func (f *fakeFuseProv) Backend() fkoverlay.Backend { return fkoverlay.BackendNFS }
func (f *fakeFuseProv) Sync(_, _ string) error     { return nil }

func (f *fakeFuseProv) Health(_, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.healths++
	return f.healthErr
}

func (f *fakeFuseProv) healthCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.healths
}

func (f *fakeFuseProv) PrivateRoot(dir string) string { return fkoverlay.FusePrivateRoot(dir) }

func (f *fakeFuseProv) Setup(base, dir string) error {
	f.mu.Lock()
	f.setups++
	f.calls = append(f.calls, "setup")
	fn, err := f.setupFn, f.setupErr
	f.mu.Unlock()
	if fn != nil {
		return fn(base, dir)
	}
	return err
}

func (f *fakeFuseProv) Teardown(base, dir string) error {
	f.mu.Lock()
	f.teardowns++
	f.calls = append(f.calls, "teardown")
	fn, err := f.teardownFn, f.teardownErr
	f.mu.Unlock()
	if fn != nil {
		return fn(base, dir)
	}
	return err
}

func (f *fakeFuseProv) setupCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.setups
}

func (f *fakeFuseProv) teardownCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.teardowns
}

func (f *fakeFuseProv) callOrder() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// fakeOverlayMounted overrides the daemon's kernel-mountpoint seam for one
// test, restoring it after. Tests using it must not run in parallel.
func fakeOverlayMounted(t *testing.T, fn func(dir string) bool) {
	t.Helper()
	prev := overlayMounted
	overlayMounted = fn
	t.Cleanup(func() { overlayMounted = prev })
}

// newMigrateServer wires newTestServer for migration: hermetic HOME, account
// dirs that exist on disk, the fake fuse provider behind the Manager seam, and
// an open fuse gate.
func newMigrateServer(t *testing.T) (*Server, map[int]string, *fakeFuseProv) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	s, dirs := newTestServer(t)
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	fake := &fakeFuseProv{}
	s.m.OverlayFor = func(backend fkoverlay.Backend) (fkoverlay.Provider, error) {
		if backend.IsFuse() {
			return fake, nil
		}
		return &fkoverlay.SymlinkProvider{Spec: s.m.OverlaySpec()}, nil
	}
	s.fuseGateFn = func() (fkoverlay.Backend, string) { return fkoverlay.BackendNFS, "" }
	// SetDefaultOverlayKind's fence keys on real fuse-hosting capability
	// (pool.CanHostFuse); these conversions run on fakes, so vouch for
	// hosting explicitly or the post-migrate default recording would fail in
	// a pure-build test run.
	s.m.CanHostFuse = func() bool { return true }
	return s, dirs, fake
}

func migrateReq(account *int, to string) Request {
	return Request{Op: OpMigrate, Account: account, To: to}
}

func outcomes(resp Response) map[int]MigrationOutcome {
	m := map[int]MigrationOutcome{}
	for _, r := range resp.Migrations {
		m[r.ID] = r.Outcome
	}
	return m
}

func kindOf(t *testing.T, s *Server, id int) string {
	t.Helper()
	a, err := s.m.Store.GetAccount(id)
	if err != nil {
		t.Fatal(err)
	}
	return a.OverlayKind
}

func TestHandleMigrateConvertsIdleAccounts(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)

	resp := s.handleMigrate(t.Context(), migrateReq(nil, "fuse"))
	if !resp.OK {
		t.Fatalf("migrate failed: %s", resp.Error)
	}
	got := outcomes(resp)
	if got[1] != MigrationDone || got[2] != MigrationDone {
		t.Fatalf("outcomes = %v, want both done", got)
	}
	if kindOf(t, s, 1) != "nfs" || kindOf(t, s, 2) != "nfs" {
		t.Fatal("rows not flipped to fuse")
	}
	if fake.setupCount() != 2 {
		t.Fatalf("fuse setups = %d, want 2", fake.setupCount())
	}
	// The fresh mounts are visible to selection immediately — the next cache
	// refresh is up to a full poll away, and an excluded freshly-converted
	// account would refuse every `ccp run` until then.
	if !s.holder.ready(dirs[1]) || !s.holder.ready(dirs[2]) {
		t.Fatal("converted accounts not vouched for in the holder cache")
	}
	// The new-account default follows the migration.
	v, ok, err := s.m.Store.GetMeta("overlay_kind")
	if err != nil || !ok || v != "nfs" {
		t.Fatalf("meta overlay_kind = %q ok=%v err=%v, want fuse", v, ok, err)
	}

	// Re-running is free and truthful.
	resp = s.handleMigrate(t.Context(), migrateReq(nil, "fuse"))
	if !resp.OK {
		t.Fatalf("re-run failed: %s", resp.Error)
	}
	got = outcomes(resp)
	if got[1] != MigrationAlready || got[2] != MigrationAlready {
		t.Fatalf("re-run outcomes = %v, want both already", got)
	}
	if fake.setupCount() != 2 {
		t.Fatalf("re-run mounted again: setups = %d", fake.setupCount())
	}
}

func TestHandleMigrateReverse(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	if resp := s.handleMigrate(t.Context(), migrateReq(nil, "fuse")); !resp.OK {
		t.Fatalf("forward migrate failed: %s", resp.Error)
	}

	resp := s.handleMigrate(t.Context(), migrateReq(nil, "symlink"))
	if !resp.OK {
		t.Fatalf("reverse migrate failed: %s", resp.Error)
	}
	got := outcomes(resp)
	if got[1] != MigrationDone || got[2] != MigrationDone {
		t.Fatalf("outcomes = %v, want both done", got)
	}
	if kindOf(t, s, 1) != "symlink" || kindOf(t, s, 2) != "symlink" {
		t.Fatal("rows not flipped back to symlink")
	}
	if fake.teardownCount() != 2 {
		t.Fatalf("fuse teardowns = %d, want 2", fake.teardownCount())
	}
	if v, _, _ := s.m.Store.GetMeta("overlay_kind"); v != "symlink" {
		t.Fatalf("meta overlay_kind = %q, want symlink after retreat", v)
	}
	// The retreat dropped the cache entries: HolderStatus.Mounts must not keep
	// counting dismounted mirrors.
	if got := s.holder.wireStatus().Mounts; got != 0 {
		t.Fatalf("holder cache still counts %d mount(s) after the retreat", got)
	}
	// The dirs are usable symlink accounts again.
	for _, dir := range dirs {
		if _, err := os.Readlink(filepath.Join(dir, "plans")); err != nil {
			t.Fatalf("symlink overlay not re-asserted in %s: %v", dir, err)
		}
	}
}

// TestHandleMigrateToFuseRecordsDefaultWithoutConversions pins the
// `ccp fuse enable` empty/already-fuse case: migrating toward fuse records fuse
// as the new-account default even when no account needs converting, because the
// fuse gate passing already proves this machine can mount. (A symlink retreat,
// by contrast, only flips the default when an account actually converts back —
// see TestHandleMigrateReverse.)
func TestHandleMigrateToFuseRecordsDefaultWithoutConversions(t *testing.T) {
	s, _, _ := newMigrateServer(t)

	// First migrate converts both accounts and records fuse as the default.
	if resp := s.handleMigrate(t.Context(), migrateReq(nil, "fuse")); !resp.OK {
		t.Fatalf("initial migrate failed: %s", resp.Error)
	}
	// Drift the recorded default back to symlink while the accounts stay fuse —
	// standing in for a fresh pool whose fuse migrate converts nothing.
	if err := s.m.Store.SetMeta("overlay_kind", "symlink"); err != nil {
		t.Fatal(err)
	}

	resp := s.handleMigrate(t.Context(), migrateReq(nil, "fuse"))
	if !resp.OK {
		t.Fatalf("re-migrate failed: %s", resp.Error)
	}
	for id, oc := range outcomes(resp) {
		if oc != MigrationAlready {
			t.Fatalf("acct %d outcome = %v, want already (no conversion expected)", id, oc)
		}
	}
	if v, _, _ := s.m.Store.GetMeta("overlay_kind"); v != "nfs" {
		t.Fatalf("meta overlay_kind = %q, want fuse — a zero-conversion fuse migrate must still record the default", v)
	}
}

func TestHandleMigrateFuseGateBlocks(t *testing.T) {
	s, _, fake := newMigrateServer(t)
	s.fuseGateFn = func() (fkoverlay.Backend, string) { return "", "grant Network Volumes access" }

	resp := s.handleMigrate(t.Context(), migrateReq(nil, "fuse"))
	if resp.OK || !strings.Contains(resp.Error, "grant Network Volumes access") {
		t.Fatalf("resp = %+v, want gate error", resp)
	}
	if len(resp.Migrations) != 0 || fake.setupCount() != 0 {
		t.Fatal("gate failure disturbed accounts")
	}
	if kindOf(t, s, 1) != "symlink" {
		t.Fatal("row changed despite gate failure")
	}
}

func TestHandleMigrateValidation(t *testing.T) {
	s, _, _ := newMigrateServer(t)

	if resp := s.handleMigrate(t.Context(), migrateReq(nil, "zfs")); resp.OK || !strings.Contains(resp.Error, "unknown overlay target") {
		t.Fatalf("unknown backend: %+v", resp)
	}
	// The wire vocabulary is the user's coarse words (fuse/symlink); a concrete
	// fuse backend name is never sent by the CLI and is rejected.
	if resp := s.handleMigrate(t.Context(), migrateReq(nil, "nfs")); resp.OK || !strings.Contains(resp.Error, "unknown overlay target") {
		t.Fatalf(`concrete backend "nfs" must be rejected on the wire: %+v`, resp)
	}
	nine := 9
	if resp := s.handleMigrate(t.Context(), migrateReq(&nine, "fuse")); resp.OK || !strings.Contains(resp.Error, "account 9 not found") {
		t.Fatalf("unknown account: %+v", resp)
	}
}

func TestHandleMigrateSingleAccount(t *testing.T) {
	s, _, _ := newMigrateServer(t)
	two := 2
	resp := s.handleMigrate(t.Context(), migrateReq(&two, "fuse"))
	if !resp.OK || len(resp.Migrations) != 1 || resp.Migrations[0].ID != 2 || resp.Migrations[0].Outcome != MigrationDone {
		t.Fatalf("resp = %+v, want acct-2 done only", resp)
	}
	if kindOf(t, s, 1) != "symlink" || kindOf(t, s, 2) != "nfs" {
		t.Fatal("wrong rows flipped")
	}
}

func TestHandleMigrateBusyWhenReserved(t *testing.T) {
	s, _, fake := newMigrateServer(t)
	if !s.tryReserve(1) {
		t.Fatal("tryReserve failed on a free account")
	}

	resp := s.handleMigrate(t.Context(), migrateReq(nil, "fuse"))
	if !resp.OK {
		t.Fatalf("migrate failed: %s", resp.Error)
	}
	got := outcomes(resp)
	if got[1] != MigrationBusy || got[2] != MigrationDone {
		t.Fatalf("outcomes = %v, want acct-1 busy, acct-2 done", got)
	}
	if kindOf(t, s, 1) != "symlink" {
		t.Fatal("reserved account was converted")
	}
	if fake.setupCount() != 1 {
		t.Fatalf("setups = %d, want 1", fake.setupCount())
	}
	if s.isConverting(1) {
		t.Fatal("busy refusal leaked a converting claim")
	}

	// Reservation expired: the sweep picks up the straggler — the rollout's
	// re-run-as-sessions-free-up loop.
	s.mu.Lock()
	s.reservations[1] = time.Now().Add(-reservationTTL - time.Second)
	s.mu.Unlock()
	resp = s.handleMigrate(t.Context(), migrateReq(nil, "fuse"))
	got = outcomes(resp)
	if got[1] != MigrationDone || got[2] != MigrationAlready {
		t.Fatalf("sweep outcomes = %v, want acct-1 done, acct-2 already", got)
	}
}

func TestConvertClaimExcludesReservations(t *testing.T) {
	s, _, _ := newMigrateServer(t)

	if !s.beginConvert(1) {
		t.Fatal("beginConvert failed on a free account")
	}
	if s.tryReserve(1) {
		t.Fatal("tryReserve succeeded on a converting account")
	}
	if s.beginConvert(1) {
		t.Fatal("double beginConvert succeeded")
	}
	s.endConvert(1)
	if !s.tryReserve(1) {
		t.Fatal("tryReserve failed after endConvert")
	}
	if s.beginConvert(1) {
		t.Fatal("beginConvert succeeded over a live reservation")
	}
	s.mu.Lock()
	s.reservations[1] = time.Now().Add(-reservationTTL - time.Second)
	s.mu.Unlock()
	if !s.beginConvert(1) {
		t.Fatal("beginConvert failed over an expired reservation")
	}
}

func TestSelectSkipsConvertingAccount(t *testing.T) {
	s, _, _ := newMigrateServer(t)

	// acct-1 is the emptier (better) account; converting must hide it.
	if !s.beginConvert(1) {
		t.Fatal("beginConvert failed")
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true})
	if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 2 {
		t.Fatalf("select = %+v, want acct-2 while acct-1 converts", resp)
	}

	one := 1
	resp = s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &one})
	if resp.OK || !strings.Contains(resp.Error, "migrating") {
		t.Fatalf("forced select = %+v, want migrating refusal", resp)
	}

	s.endConvert(1)
	resp = s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true})
	if !resp.OK || *resp.SelectedID != 1 {
		t.Fatalf("select after endConvert = %+v, want acct-1", resp)
	}
}

func TestSelectExcludesUnmountedFuseAccount(t *testing.T) {
	s, _, _ := newMigrateServer(t)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs" // row says fuse, but nothing is mounted at the dir
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true})
	if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 2 {
		t.Fatalf("select = %+v, want acct-2 while acct-1's mount is down", resp)
	}

	one := 1
	resp = s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &one})
	if resp.OK || !strings.Contains(resp.Error, "mount is not up") {
		t.Fatalf("forced select = %+v, want mount-not-up refusal", resp)
	}
}

// TestFallbackToSymlinkRestoresPrivateFiles is the regression test for the
// stranding bug: the pre-fix fallback flipped the row to symlink but left the
// account's .claude.json identity behind in the fuse private backing dir.
func TestFallbackToSymlinkRestoresPrivateFiles(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	priv := fkoverlay.FusePrivateRoot(dirs[1])
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(priv, ".claude.json"), []byte("identity"), 0o600); err != nil {
		t.Fatal(err)
	}

	s.fallbackToSymlink(t.Context(), a)

	got, err := os.ReadFile(filepath.Join(dirs[1], ".claude.json"))
	if err != nil || string(got) != "identity" {
		t.Fatalf("identity not restored to the account dir: %q err=%v", got, err)
	}
	if kindOf(t, s, 1) != "symlink" {
		t.Fatal("row not flipped to symlink")
	}
	if _, err := os.Lstat(priv); !os.IsNotExist(err) {
		t.Fatal("emptied private root not removed")
	}

	// Twin negative: a wedged unmount must abort the fallback untouched.
	a, _ = s.m.Store.GetAccount(2)
	a.OverlayKind = "nfs"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	priv2 := fkoverlay.FusePrivateRoot(dirs[2])
	if err := os.MkdirAll(priv2, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(priv2, ".claude.json"), []byte("identity2"), 0o600); err != nil {
		t.Fatal(err)
	}
	fake.teardownErr = errors.New("still mounted")

	s.fallbackToSymlink(t.Context(), a)

	if _, err := os.Stat(filepath.Join(priv2, ".claude.json")); err != nil {
		t.Fatalf("identity moved despite failed unmount: %v", err)
	}
	if kindOf(t, s, 2) != "nfs" {
		t.Fatal("row flipped despite failed unmount")
	}
	if _, err := os.Lstat(filepath.Join(dirs[2], "plans")); !os.IsNotExist(err) {
		t.Fatal("symlinks laid despite failed unmount")
	}
}

// TestFallbackToSymlinkClaimAtomicAgainstSelect pins the fallback's claim
// atomicity: the converting claim is taken BEFORE the idle scan (the migrate
// path's claim-first order), so a select cannot reserve the account between
// the gate and ConvertOverlay's force-unmount. The scan seam doubles as the
// mid-fallback select: a tryReserve issued from inside it must be refused.
func TestFallbackToSymlinkClaimAtomicAgainstSelect(t *testing.T) {
	s, _, _ := newMigrateServer(t)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	reservedMidFallback := false
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		reservedMidFallback = s.tryReserve(1)
		return nil, nil
	}

	s.fallbackToSymlink(t.Context(), a)

	if reservedMidFallback {
		t.Fatal("a select reserved the account between the idle gate and the conversion")
	}
	if kindOf(t, s, 1) != "symlink" {
		t.Fatal("idle fallback did not convert")
	}
	if s.isConverting(1) {
		t.Fatal("fallback leaked its converting claim")
	}
	if !s.tryReserve(1) {
		t.Fatal("account not reservable after the fallback completed")
	}
}

func TestReconcileOverlaysHealsStrandedAndFallsBack(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)

	// acct-1: symlink row with an identity stranded by an interrupted
	// conversion — startup must restore it.
	priv1 := fkoverlay.FusePrivateRoot(dirs[1])
	if err := os.MkdirAll(priv1, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(priv1, ".claude.json"), []byte("stranded"), 0o600); err != nil {
		t.Fatal(err)
	}

	// acct-2: fuse row whose mirror is down (Health fails) and whose mount
	// cannot come up — startup must fall back to a fully usable symlink
	// account rather than adopting.
	a2, err := s.m.Store.GetAccount(2)
	if err != nil {
		t.Fatal(err)
	}
	a2.OverlayKind = "nfs"
	if err := s.m.Store.UpsertAccount(a2); err != nil {
		t.Fatal(err)
	}
	priv2 := fkoverlay.FusePrivateRoot(dirs[2])
	if err := os.MkdirAll(priv2, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(priv2, ".claude.json"), []byte("identity2"), 0o600); err != nil {
		t.Fatal(err)
	}
	fake.healthErr = errors.New("not a mountpoint")
	fake.setupErr = errors.New("mount did not come up")

	s.reconcileOverlays(t.Context())

	if got, err := os.ReadFile(filepath.Join(dirs[1], ".claude.json")); err != nil || string(got) != "stranded" {
		t.Fatalf("acct-1 stranded identity not healed: %q err=%v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(dirs[2], ".claude.json")); err != nil || string(got) != "identity2" {
		t.Fatalf("acct-2 identity not restored by fallback: %q err=%v", got, err)
	}
	if kindOf(t, s, 2) != "symlink" {
		t.Fatal("acct-2 row not flipped by fallback")
	}
}

// TestConvertClaimExcludesPolling pins the two-sided scheduler exclusion: a
// poll iteration owns the dir against conversions, and vice versa — the
// check-then-act hole a plain isConverting test left open.
func TestConvertClaimExcludesPolling(t *testing.T) {
	s, _, _ := newMigrateServer(t)

	if !s.beginPoll(1) {
		t.Fatal("beginPoll failed on a free account")
	}
	if s.beginConvert(1) {
		t.Fatal("beginConvert succeeded while the scheduler holds the account")
	}
	if s.beginPoll(1) {
		t.Fatal("double beginPoll succeeded")
	}
	s.endPoll(1)
	if !s.beginConvert(1) {
		t.Fatal("beginConvert failed after endPoll")
	}
	if s.beginPoll(1) {
		t.Fatal("beginPoll succeeded while a conversion holds the account")
	}
	s.endConvert(1)
	if !s.beginPoll(1) {
		t.Fatal("beginPoll failed after endConvert")
	}
	s.endPoll(1)

	// A poll claim must NOT hide the account from select — sessions can land
	// on a dir that is merely being health-checked.
	if !s.beginPoll(2) {
		t.Fatal("beginPoll failed")
	}
	if !s.tryReserve(2) {
		t.Fatal("tryReserve refused a merely-polling account")
	}
	s.endPoll(2)
}

// TestMountFuseSweepsUnderlay pins crash recovery for a conversion killed
// between its file moves and its row flip: private files left in the mount
// underlay must be swept into the backing dir BEFORE mounting, or the mirror
// would shadow the identity and a session would mint a divergent one.
func TestMountFuseSweepsUnderlay(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs" // mountFuse resolves the provider from the row's kind
	if err := os.WriteFile(filepath.Join(dirs[1], ".claude.json"), []byte("underlay-identity"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := s.mountFuse(t.Context(), a); err != nil {
		t.Fatalf("mountFuse: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(fkoverlay.FusePrivateRoot(dirs[1]), ".claude.json"))
	if err != nil || string(got) != "underlay-identity" {
		t.Fatalf("identity not swept into backing dir: %q err=%v", got, err)
	}
	if _, err := os.Lstat(filepath.Join(dirs[1], ".claude.json")); !os.IsNotExist(err) {
		t.Fatal("identity left in the underlay")
	}
	if fake.setupCount() != 1 {
		t.Fatalf("setups = %d, want 1", fake.setupCount())
	}

	// A wrong-backend injected fake must be refused loudly, never mounted
	// through — the real resolver always yields a fuse provider, so the fence
	// guards against fakes that would run symlink code on fuse paths.
	s.m.OverlayFor = func(_ fkoverlay.Backend) (fkoverlay.Provider, error) {
		return &fkoverlay.SymlinkProvider{Spec: s.m.OverlaySpec()}, nil
	}
	if err := s.mountFuse(t.Context(), a); err == nil || !strings.Contains(err.Error(), "no fuse provider") {
		t.Fatalf("mountFuse with a wrong-backend provider = %v, want a backend refusal", err)
	}
}

// TestMountReady pins the readiness gate: a fuse row trusts ONLY the holder
// cache (never a filesystem stat — an lstat through a dead fuse-t mount can
// hang the select path), and a non-fuse row needs the absence of a mountpoint
// (one under a symlink row is an aborted rollback's wreckage).
func TestMountReady(t *testing.T) {
	const dir = "/pool/acct-01"
	fuse := store.Account{OverlayKind: "nfs", ConfigDir: dir}
	sym := store.Account{OverlayKind: "symlink", ConfigDir: dir}
	cases := map[string]struct {
		a             store.Account
		healthy       bool
		mounts        map[string]bool
		kernelMounted bool
		want          bool
	}{
		"fuse healthy and listed live": {
			a: fuse, healthy: true, mounts: map[string]bool{dir: true}, kernelMounted: true, want: true,
		},
		"fuse healthy but missing from the list": {
			a: fuse, healthy: true, mounts: map[string]bool{}, kernelMounted: true, want: false,
		},
		"fuse healthy but listed dead": {
			a: fuse, healthy: true, mounts: map[string]bool{dir: false}, kernelMounted: true, want: false,
		},
		// THE carcass case: the dir is still a mountpoint per the kernel, but
		// with the holder dead nothing serves it — selection must never trust
		// it (the old overlay.Mounted check did).
		"fuse unhealthy cache ignores a live-looking mountpoint": {
			a: fuse, healthy: false, mounts: map[string]bool{dir: true}, kernelMounted: true, want: false,
		},
		"symlink unmounted": {
			a: sym, healthy: false, kernelMounted: false, want: true,
		},
		"symlink mounted is rollback wreckage": {
			a: sym, healthy: true, mounts: map[string]bool{dir: true}, kernelMounted: true, want: false,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, _, _ := newMigrateServer(t)
			fakeOverlayMounted(t, func(string) bool { return tc.kernelMounted })
			s.holder.mu.Lock()
			s.holder.healthy, s.holder.mounts = tc.healthy, tc.mounts
			s.holder.mu.Unlock()
			if got := s.mountReady(tc.a); got != tc.want {
				t.Fatalf("mountReady = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestHandleMigrateBudgetExhausted pins the out-of-time path: a request whose
// budget is spent reports the remaining accounts busy instead of overrunning
// the conn deadline and leaving the client a dead socket.
func TestHandleMigrateBudgetExhausted(t *testing.T) {
	s, _, fake := newMigrateServer(t)
	s.migrateBudget = time.Nanosecond

	resp := s.handleMigrate(t.Context(), migrateReq(nil, "fuse"))
	if !resp.OK {
		t.Fatalf("migrate failed: %s", resp.Error)
	}
	for _, r := range resp.Migrations {
		if r.Outcome != MigrationBusy || !strings.Contains(r.Detail, "window elapsed") {
			t.Fatalf("result = %+v, want busy/window elapsed", r)
		}
	}
	if fake.setupCount() != 0 {
		t.Fatal("conversion ran despite an exhausted budget")
	}
	if kindOf(t, s, 1) != "symlink" {
		t.Fatal("row flipped despite an exhausted budget")
	}
}

// TestConvertAccountForceStillRespectsReservations pins --force's boundary:
// it skips only the live-session gate. A reservation means a claude is
// launching into the dir right now — force must not override it.
func TestConvertAccountForceStillRespectsReservations(t *testing.T) {
	s, _, fake := newMigrateServer(t)
	if !s.tryReserve(1) {
		t.Fatal("tryReserve failed on a free account")
	}
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	res := s.convertAccount(t.Context(), a, fkoverlay.BackendNFS, true)
	if res.Outcome != MigrationBusy {
		t.Fatalf("outcome = %s, want busy despite force", res.Outcome)
	}
	if fake.setupCount() != 0 {
		t.Fatal("forced conversion ran over a live reservation")
	}

	// Force flows through the wire: with the reservation expired, a forced
	// sweep converts both accounts.
	s.mu.Lock()
	s.reservations[1] = time.Now().Add(-reservationTTL - time.Second)
	s.mu.Unlock()
	resp := s.handleMigrate(t.Context(), Request{Op: OpMigrate, To: "fuse", Force: true})
	if !resp.OK {
		t.Fatalf("forced migrate failed: %s", resp.Error)
	}
	if got := outcomes(resp); got[1] != MigrationDone || got[2] != MigrationDone {
		t.Fatalf("outcomes = %v, want both done", got)
	}
}

// TestMigrateToSymlinkDefersUnderLiveSession pins the user-initiated retreat's
// live-session gate: convertAccount(force=false) on a fuse row with a live
// session refuses (MigrationBusy) and never tears the mirror down, while
// force=true — the user vouching the session is idle — converts. This is the
// migrate-path twin of the autonomous force-unmount gate; both refuse to yank a
// busy NFS mirror out from under a live claude.
func TestMigrateToSymlinkDefersUnderLiveSession(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	a := flipToFuse(t, s, 1)
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}

	// force=false: the live session defers the retreat untouched.
	res := s.convertAccount(t.Context(), a, fkoverlay.BackendSymlink, false)
	if res.Outcome != MigrationBusy {
		t.Fatalf("outcome = %s (%s), want busy under a live session", res.Outcome, res.Detail)
	}
	if got := kindOf(t, s, 1); got != "nfs" {
		t.Fatalf("row kind after a deferred migrate = %q, want fuse (unchanged)", got)
	}
	if fake.teardownCount() != 0 {
		t.Fatalf("teardowns under a deferred migrate = %d, want 0", fake.teardownCount())
	}

	// force=true: the user vouches the session is idle; the retreat proceeds.
	res = s.convertAccount(t.Context(), a, fkoverlay.BackendSymlink, true)
	if res.Outcome != MigrationDone {
		t.Fatalf("forced outcome = %s (%s), want done", res.Outcome, res.Detail)
	}
	if got := kindOf(t, s, 1); got != "symlink" {
		t.Fatalf("row kind after a forced migrate = %q, want symlink", got)
	}
}

// TestConvertAccountRefetchesRow pins the stale-snapshot fix: the row is
// re-read under the claim, so a kind that changed since the caller's list is
// honored instead of double-converting (or converting from a wrong source).
func TestConvertAccountRefetchesRow(t *testing.T) {
	s, _, fake := newMigrateServer(t)
	stale, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	fresh := stale
	fresh.OverlayKind = "nfs" // flipped after the caller's snapshot
	if err := s.m.Store.UpsertAccount(fresh); err != nil {
		t.Fatal(err)
	}

	res := s.convertAccount(t.Context(), stale, fkoverlay.BackendNFS, false) // stale still says symlink
	if res.Outcome != MigrationAlready {
		t.Fatalf("outcome = %s (%s), want already", res.Outcome, res.Detail)
	}
	if fake.setupCount() != 0 {
		t.Fatal("conversion ran against a stale row")
	}
}

// TestPollOnceSkipsConvertingAccount mirrors the reserved-account refresh test:
// an account mid-conversion must see no overlay sync, no refresh, no adoption.
func TestPollOnceSkipsConvertingAccount(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	s, _ := newTestServer(t)
	fo := s.m.OAuth.(*fakeOAuth)
	fo.mu.Lock()
	fo.currentRT = "rt-0"
	fo.mu.Unlock()
	fk := s.m.Keychain.(*fakeKeychain)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture's accounts share a keychain service; give acct-1 its own so
	// acct-2's (un-converting, untouched) poll can't read this credential.
	a.KeychainService = "svc-acct-1"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	cred := &keychain.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-0"
	// Near-expiry (< RefreshLeadTime) so an idle poll must refresh.
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Minute).UnixMilli()
	if err := fk.Write(a.KeychainService, a.KeychainAccount, cred); err != nil {
		t.Fatal(err)
	}
	seedWrites := fk.writeCount()

	if !s.beginConvert(a.ID) {
		t.Fatal("beginConvert failed")
	}
	s.pollOnce(t.Context())
	if got := fo.refreshCount(); got != 0 {
		t.Fatalf("converting account was refreshed %d time(s)", got)
	}
	if got := fk.writeCount(); got != seedWrites {
		t.Fatalf("converting account's credential was written %d time(s)", got-seedWrites)
	}

	s.endConvert(a.ID)
	s.pollOnce(t.Context())
	if got := fo.refreshCount(); got != 1 {
		t.Fatalf("idle near-expiry account refreshed %d time(s), want 1", got)
	}
}

// TestReconcileAdoptsLiveMount pins the daemon-restart common case: a fuse row
// whose mirror is already live (the detached holder survived the restart) is
// adopted untouched — no teardown, no sweep, no remount — and vouched for in
// the holder cache directly, not left to the startup refresh's snapshot.
func TestReconcileAdoptsLiveMount(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	// healthErr nil (the default): the mirror reads mounted and live.

	s.reconcileOverlays(t.Context())

	if got := fake.callOrder(); len(got) != 0 {
		t.Fatalf("adoption touched the mount: calls = %v, want none", got)
	}
	if !strings.Contains(buf.String(), fmt.Sprintf("acct-%02d adopted live mount", a.ID)) {
		t.Fatalf("adoption not logged: %q", buf.String())
	}
	// The adopt path vouches in place: the startup refresh ran against a dead
	// holder socket here (markUnhealthy), so only noteMounted can explain a
	// ready dir — a live mirror implies the holder serving it.
	if !s.holder.ready(dirs[1]) {
		t.Fatal("adopted mount not vouched for in the holder cache")
	}
}

// TestMountFuseClearsDeadMountThenSweepsThenMounts pins mountFuse's fixed
// order: a mounted-but-dead mirror comes down first, then the (now unmounted)
// underlay is swept, then Setup — and the fresh mount lands in the holder
// cache so a select before the next poll trusts it.
func TestMountFuseClearsDeadMountThenSweepsThenMounts(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs" // mountFuse resolves the provider from the row's kind
	if err := os.WriteFile(filepath.Join(dirs[1], ".claude.json"), []byte("underlay-identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	var mounted atomic.Bool
	mounted.Store(true)
	fakeOverlayMounted(t, func(string) bool { return mounted.Load() })
	fake.healthErr = errors.New("mirror is dead")
	fake.teardownFn = func(string, string) error { mounted.Store(false); return nil }
	fake.setupFn = func(string, string) error {
		// The sweep must complete before the mount: the identity must already
		// be in the backing dir, or the mirror would shadow it.
		if _, err := os.Stat(filepath.Join(fkoverlay.FusePrivateRoot(dirs[1]), ".claude.json")); err != nil {
			return fmt.Errorf("setup ran before the sweep: %w", err)
		}
		return nil
	}

	if err := s.mountFuse(t.Context(), a); err != nil {
		t.Fatalf("mountFuse: %v", err)
	}
	if got := fake.callOrder(); !reflect.DeepEqual(got, []string{"teardown", "setup"}) {
		t.Fatalf("call order = %v, want [teardown setup]", got)
	}
	if _, err := os.Lstat(filepath.Join(dirs[1], ".claude.json")); !os.IsNotExist(err) {
		t.Fatal("identity left in the underlay")
	}
	if !s.holder.ready(dirs[1]) {
		t.Fatal("fresh mount not recorded in the holder cache")
	}
}

// TestMountFuseWedgedPreClearAborts pins the never-through-a-wedge rule: when
// the dead mount will not come down, mountFuse returns the error without
// sweeping or mounting.
func TestMountFuseWedgedPreClearAborts(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs" // mountFuse resolves the provider from the row's kind
	if err := os.WriteFile(filepath.Join(dirs[1], ".claude.json"), []byte("underlay-identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	fakeOverlayMounted(t, func(string) bool { return true })
	fake.healthErr = errors.New("mirror is dead")
	fake.teardownErr = errors.New("umount: resource busy")

	merr := s.mountFuse(t.Context(), a)
	if merr == nil || !strings.Contains(merr.Error(), "clear dead mount") {
		t.Fatalf("mountFuse over a wedged unmount = %v, want a clear-dead-mount error", merr)
	}
	if fake.setupCount() != 0 {
		t.Fatal("mounted through a wedged pre-clear")
	}
	if _, err := os.Stat(filepath.Join(dirs[1], ".claude.json")); err != nil {
		t.Fatalf("underlay identity disturbed despite the abort: %v", err)
	}
	if s.holder.ready(dirs[1]) {
		t.Fatal("wedged dir recorded as ready")
	}
}

// TestMountFuseForeignCarcassClearedAndRetriedOnce pins the dead-holder
// carcass contract from step 4: Setup's foreign-mount refusal is answered with
// a Teardown (whose registry-miss path clears the carcass) and exactly one
// sweep+Setup retry.
func TestMountFuseForeignCarcassClearedAndRetriedOnce(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs" // mountFuse resolves the provider from the row's kind
	var foreign atomic.Bool
	foreign.Store(true)
	fake.setupFn = func(_, dir string) error {
		if foreign.Load() {
			return fmt.Errorf("mount %s: %w", dir, mountd.ErrForeignMount)
		}
		return nil
	}
	fake.teardownFn = func(string, string) error { foreign.Store(false); return nil }

	if err := s.mountFuse(t.Context(), a); err != nil {
		t.Fatalf("mountFuse: %v", err)
	}
	if got := fake.callOrder(); !reflect.DeepEqual(got, []string{"setup", "teardown", "setup"}) {
		t.Fatalf("call order = %v, want [setup teardown setup]", got)
	}
	if !s.holder.ready(dirs[1]) {
		t.Fatal("remounted carcass not recorded in the holder cache")
	}
}

// TestMountFuseBaseMismatchClearedAndRetriedOnce pins the ErrBaseMismatch
// routing: a holder registry row pinning a different base is registry state,
// not a mount verdict — it gets the same unmount-then-retry treatment as a
// foreign carcass (the holder's handleUnmount tears down by its registered
// base), never the gated symlink conversion.
func TestMountFuseBaseMismatchClearedAndRetriedOnce(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs" // mountFuse resolves the provider from the row's kind
	var mismatched atomic.Bool
	mismatched.Store(true)
	fake.setupFn = func(_, dir string) error {
		if mismatched.Load() {
			return fmt.Errorf("mount %s: %w", dir, mountd.ErrBaseMismatch)
		}
		return nil
	}
	fake.teardownFn = func(string, string) error { mismatched.Store(false); return nil }

	if err := s.mountFuse(t.Context(), a); err != nil {
		t.Fatalf("mountFuse: %v", err)
	}
	if got := fake.callOrder(); !reflect.DeepEqual(got, []string{"setup", "teardown", "setup"}) {
		t.Fatalf("call order = %v, want [setup teardown setup]", got)
	}
	if !s.holder.ready(dirs[1]) {
		t.Fatal("remounted mismatched dir not recorded in the holder cache")
	}
}

// TestMountFusePersistentForeignFailsAfterOneRetry is the twin negative: a dir
// that stays foreign after the teardown surfaces the error — exactly one
// retry, never a loop.
func TestMountFusePersistentForeignFailsAfterOneRetry(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs" // mountFuse resolves the provider from the row's kind
	fake.setupErr = fmt.Errorf("mount %s: %w", dirs[1], mountd.ErrForeignMount)

	merr := s.mountFuse(t.Context(), a)
	if !errors.Is(merr, mountd.ErrForeignMount) {
		t.Fatalf("mountFuse = %v, want errors.Is ErrForeignMount", merr)
	}
	if got := fake.callOrder(); !reflect.DeepEqual(got, []string{"setup", "teardown", "setup"}) {
		t.Fatalf("call order = %v, want exactly one teardown+retry", got)
	}
	if s.holder.ready(dirs[1]) {
		t.Fatal("failed mount recorded as ready")
	}
}

// TestHealFuseTaxonomy pins the mount-failure classification: transient holder
// conditions (unreachable holder, busy dir) and a mount blocked pending the
// TCC grant never convert — they retry next poll — and only a genuine mount
// failure falls back to symlink, gated on the account being idle (no live
// session, no reservation, and never on a failed scan).
func TestHealFuseTaxonomy(t *testing.T) {
	cases := map[string]struct {
		setupErr    error
		scanKind    string // "" = real scan (idle), "live" = session on the dir, "err" = scan failure
		reserve     bool
		wantOutcome healOutcome
		wantKind    string
		wantTCC     bool
	}{
		"holder unavailable retries next poll": {
			setupErr:    fmt.Errorf("mount: %w", mountd.ErrHolderUnavailable),
			wantOutcome: healRetry, wantKind: "nfs",
		},
		// The exact chain the production spawn leg produces (RemoteProvider.Setup
		// wrapping EnsureRunning's come-up timeout): a holder spawn blip must
		// land in the retry arm, never the conversion arm.
		"spawn timeout (holder unavailable chain) retries next poll": {
			setupErr: fmt.Errorf("mount /pool/acct-01: %w",
				fmt.Errorf("%w: mount holder did not come up on /tmp/m.sock within 5s; check /tmp/holder.log", mountd.ErrHolderUnavailable)),
			wantOutcome: healRetry, wantKind: "nfs",
		},
		"busy dir retries next poll": {
			setupErr:    fmt.Errorf("mount: %w", mountd.ErrBusy),
			wantOutcome: healRetry, wantKind: "nfs",
		},
		"tcc block recorded and retried": {
			setupErr:    fmt.Errorf("mount: %w", overlay.ErrMountNotLive),
			wantOutcome: healTCCBlocked, wantKind: "nfs", wantTCC: true,
		},
		// A wedged unmount is no more a mount verdict than ErrBusy — and the
		// fallback's ConvertOverlay would hit the same wedge, so converting
		// would fail closed every poll for nothing.
		"wedged unmount retries next poll": {
			setupErr:    fmt.Errorf("mount: %w", overlay.ErrUnmountWedged),
			wantOutcome: healRetry, wantKind: "nfs",
		},
		// The exact chain overlayClass produces for a mount-up timeout under a
		// proven "Network Volumes" grant: transient fuse-t slowness, never the
		// TCC condition. wantTCC false is the load-bearing negative — pre-fix,
		// every timeout recorded TCC guidance and waited on the grant copy.
		"mount timeout (proven grant) retries without recording TCC": {
			setupErr:    fmt.Errorf("mount: %w", fmt.Errorf("%w: %w", overlay.ErrMountTimeout, mountd.ErrMountTimeout)),
			wantOutcome: healRetry, wantKind: "nfs", wantTCC: false,
		},
		// Forward skew: a newer holder's error class this daemon predates is
		// unclassifiable — the protocol's sanctioned extension path must read
		// as retry, never as the mount failure that converts.
		"unknown holder error class retries next poll": {
			setupErr:    fmt.Errorf("mount: %w", fmt.Errorf("%w (quota-exceeded): per-account quota exhausted", mountd.ErrUnknownClass)),
			wantOutcome: healRetry, wantKind: "nfs",
		},
		// The skew matrix's degrade path: a pre-fix daemon receiving the new
		// "mount-timeout" class reads it as ErrUnknownClass — which the
		// additive policy routes to retry, never to the conversion arm.
		"mount-timeout class on a pre-fix daemon degrades to retry": {
			setupErr:    fmt.Errorf("mount: %w", fmt.Errorf("%w (mount-timeout): fuse mount did not come up in time", mountd.ErrUnknownClass)),
			wantOutcome: healRetry, wantKind: "nfs",
		},
		"genuine failure on an idle account converts": {
			setupErr:    errors.New("mount exploded"),
			wantOutcome: healFallback, wantKind: "symlink",
		},
		"genuine failure under a live session defers": {
			setupErr: errors.New("mount exploded"), scanKind: "live",
			wantOutcome: healFallback, wantKind: "nfs",
		},
		"genuine failure under a reservation defers": {
			setupErr: errors.New("mount exploded"), reserve: true,
			wantOutcome: healFallback, wantKind: "nfs",
		},
		"genuine failure with a failed scan fails closed": {
			setupErr: errors.New("mount exploded"), scanKind: "err",
			wantOutcome: healFallback, wantKind: "nfs",
		},
		"clean mount": {wantOutcome: healMounted, wantKind: "nfs"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, dirs, fake := newMigrateServer(t)
			a, err := s.m.Store.GetAccount(1)
			if err != nil {
				t.Fatal(err)
			}
			a.OverlayKind = "nfs"
			if err := s.m.Store.UpsertAccount(a); err != nil {
				t.Fatal(err)
			}
			fake.setupErr = tc.setupErr
			switch tc.scanKind {
			case "live":
				s.scanSessions = func(context.Context) ([]procscan.Session, error) {
					return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
				}
			case "err":
				s.scanSessions = func(context.Context) ([]procscan.Session, error) {
					return nil, errors.New("ps exploded")
				}
			}
			if tc.reserve && !s.tryReserve(1) {
				t.Fatal("tryReserve failed on a free account")
			}

			if got := s.healFuse(t.Context(), a); got != tc.wantOutcome {
				t.Fatalf("healFuse outcome = %d, want %d", got, tc.wantOutcome)
			}
			if got := kindOf(t, s, 1); got != tc.wantKind {
				t.Fatalf("row kind = %q, want %q", got, tc.wantKind)
			}
			if s.isConverting(1) {
				t.Fatal("heal leaked a converting claim")
			}
			if gotTCC := s.holder.wireStatus().TCCError != ""; gotTCC != tc.wantTCC {
				t.Fatalf("TCC recorded = %v, want %v", gotTCC, tc.wantTCC)
			}
			if tc.wantOutcome == healMounted && !s.holder.ready(dirs[1]) {
				t.Fatal("clean mount not recorded in the holder cache")
			}
		})
	}
}

// TestHealFuseSweepFailureRetries pins the errSweepStranded gate: a failure in
// mountFuse's pre-Setup sweep of stranded private files is not a mount verdict,
// so it retries next poll instead of irreversibly demoting the account to
// symlink. A file-vs-directory private-entry collision — which
// MovePrivateEntries refuses loudly, and which the symlink retreat would hit
// the same way — is the deterministic trigger.
func TestHealFuseSweepFailureRetries(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	fake.setupErr = nil // Setup must never be reached; the sweep fails first.

	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	// A private file in the underlay collides with a private DIR of the same
	// name in the backing root: moveEntry refuses the file-vs-dir clash, so the
	// sweep fails before Setup is attempted.
	dir := dirs[1]
	if err := os.WriteFile(filepath.Join(dir, ".credentials.json"), []byte("identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(fkoverlay.FusePrivateRoot(dir), ".credentials.json"), 0o700); err != nil {
		t.Fatal(err)
	}

	if got := s.healFuse(t.Context(), a); got != healRetry {
		t.Fatalf("healFuse outcome = %d, want healRetry (%d): a pre-Setup sweep failure must not convert", got, healRetry)
	}
	if got := kindOf(t, s, 1); got != "nfs" {
		t.Fatalf("row kind = %q, want \"fuse\": a sweep failure must not demote to symlink", got)
	}
	if s.isConverting(1) {
		t.Fatal("a sweep failure leaked a converting claim")
	}
	if s.holder.wireStatus().TCCError != "" {
		t.Fatal("a sweep failure wrongly recorded a TCC block")
	}
}

// TestSelectServesFuseAccountWhenHolderVouches is the positive arm of the
// carcass fix: a fuse account is selectable exactly when the holder cache
// vouches for its mirror.
func TestSelectServesFuseAccountWhenHolderVouches(t *testing.T) {
	s, dirs, _ := newMigrateServer(t)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	s.holder.noteMounted(dirs[1])

	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true})
	if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 1 {
		t.Fatalf("select = %+v, want vouched-for acct-1 (the emptier account)", resp)
	}
}

// fuseRowWithCannedHolder flips acct-1 to a fuse row and points the server's
// holder socket at a canned holder that lists acct-1's mirror live.
func fuseRowWithCannedHolder(t *testing.T, s *Server, dirs map[int]string) store.Account {
	t.Helper()
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	s.holderSocket = startCannedHolder(t, []mountd.MountInfo{{Dir: dirs[1], Base: "/base", Live: true}})
	return a
}

// TestSelectColdStartPrimesHolderCacheLazily pins the cold-start window: the
// daemon socket binds before the startup goroutine primes the holder cache,
// so a select landing in that window must lazily refresh (bounded socket RPC,
// no filesystem touch) instead of refusing every fuse account while the
// detached holder serves the mounts fine.
func TestSelectColdStartPrimesHolderCacheLazily(t *testing.T) {
	s, dirs, _ := newMigrateServer(t)
	fuseRowWithCannedHolder(t, s, dirs)

	// The cache is zero-valued — never refreshed — exactly the bind→prime gap.
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true})
	if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 1 {
		t.Fatalf("cold-start select = %+v, want lazily-primed acct-1 (the emptier account)", resp)
	}

	// The forced path flows through the same lazy prime — pre-fix it refused
	// with "fuse mount is not up yet" while the mount served fine.
	s2, dirs2, _ := newMigrateServer(t)
	fuseRowWithCannedHolder(t, s2, dirs2)
	one := 1
	resp = s2.handleSelect(t.Context(), Request{Op: OpSelect, Account: &one, NoMark: true})
	if !resp.OK || resp.Dir != dirs2[1] {
		t.Fatalf("cold-start forced select = %+v, want acct-1's dir", resp)
	}
}

// TestMountReadyRefreshesOnCacheMiss pins the outside-the-daemon mount edge: a
// fuse dir absent from a stale cache (a mirror `ccp add` just mounted from the
// CLI process) triggers one refresh, while a fresh cache rate-limits the
// round-trip — a genuinely down mount must not turn every select into holder
// RPCs.
func TestMountReadyRefreshesOnCacheMiss(t *testing.T) {
	s, dirs, _ := newMigrateServer(t)
	a := fuseRowWithCannedHolder(t, s, dirs)

	// Fresh cache missing the dir: inside the floor, no refresh — not ready.
	s.holder.mu.Lock()
	s.holder.healthy, s.holder.mounts, s.holder.refreshedAt = true, map[string]bool{}, time.Now()
	s.holder.mu.Unlock()
	if s.mountReady(a) {
		t.Fatal("a refresh fired inside the rate-limit floor")
	}

	// Stale cache: the miss refreshes and the fresh mount is picked up.
	s.holder.mu.Lock()
	s.holder.refreshedAt = time.Now().Add(-holderRefreshFloor - time.Second)
	s.holder.mu.Unlock()
	if !s.mountReady(a) {
		t.Fatal("a stale cache miss did not refresh")
	}
}
