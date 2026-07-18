package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/lease"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// fakeFuseProv: fn seams run outside the lock so they may inspect the fake.
type fakeFuseProv struct {
	mu           sync.Mutex
	calls        []string
	reconciles   int
	teardowns    int
	checks       int
	reconcileErr error
	teardownErr  error
	checkErr     error
	reconcileFn  func(base, dir string) error
	teardownFn   func(base, dir string) error
}

func (f *fakeFuseProv) Backend() fkoverlay.Backend { return fkoverlay.BackendNFS }

func (f *fakeFuseProv) Check(_ context.Context, _, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checks++
	return f.checkErr
}

func (f *fakeFuseProv) checkCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.checks
}

func (f *fakeFuseProv) PrivateRoot(dir string) string { return fkoverlay.FusePrivateRoot(dir) }

func (f *fakeFuseProv) Reconcile(_ context.Context, base, dir string) error {
	f.mu.Lock()
	f.reconciles++
	f.calls = append(f.calls, "reconcile")
	fn, err := f.reconcileFn, f.reconcileErr
	f.mu.Unlock()
	if fn != nil {
		return fn(base, dir)
	}
	return err
}

func (f *fakeFuseProv) Teardown(_ context.Context, base, dir string) (string, error) {
	f.mu.Lock()
	f.teardowns++
	f.calls = append(f.calls, "teardown")
	fn, err := f.teardownFn, f.teardownErr
	f.mu.Unlock()
	if fn != nil {
		return "", fn(base, dir)
	}
	return "", err
}

func (f *fakeFuseProv) reconcileCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reconciles
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

// fakeOverlayMounted mutates a package global; callers must not run in
// parallel.
func fakeOverlayMounted(t *testing.T, fn func(dir string) bool) {
	t.Helper()
	prev := overlayMounted
	overlayMounted = fn
	t.Cleanup(func() { overlayMounted = prev })
}

// makeBridge replaces an account dir with the mux bridge symlink into the shared
// mux root — the on-disk shape of a migrated fuse account (pool.IsBridgeSymlink
// reads true), so reconcileAccount adopts/heals it instead of migrating it.
func makeBridge(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(pool.MuxRootDir(), filepath.Base(dir)), dir); err != nil {
		t.Fatal(err)
	}
}

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
	// SetDefaultOverlayKind fences on pool.CanHostFuse; vouch explicitly or
	// recording the post-migrate default fails in pure-build runs.
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
	if fake.reconcileCount() != 2 {
		t.Fatalf("fuse reconciles = %d, want 2", fake.reconcileCount())
	}
	// Fresh mounts must be selectable immediately, not a poll away.
	if !s.holder.ready(dirs[1]) || !s.holder.ready(dirs[2]) {
		t.Fatal("converted accounts not vouched for in the holder cache")
	}
	v, ok, err := s.m.Store.GetMeta("overlay_kind")
	if err != nil || !ok || v != "nfs" {
		t.Fatalf("meta overlay_kind = %q ok=%v err=%v, want fuse", v, ok, err)
	}

	resp = s.handleMigrate(t.Context(), migrateReq(nil, "fuse"))
	if !resp.OK {
		t.Fatalf("re-run failed: %s", resp.Error)
	}
	got = outcomes(resp)
	if got[1] != MigrationAlready || got[2] != MigrationAlready {
		t.Fatalf("re-run outcomes = %v, want both already", got)
	}
	if fake.reconcileCount() != 2 {
		t.Fatalf("re-run mounted again: reconciles = %d", fake.reconcileCount())
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
	if got := s.holder.wireStatus().Mounts; got != 0 {
		t.Fatalf("holder cache still counts %d mount(s) after the retreat", got)
	}
	for _, dir := range dirs {
		if _, err := os.Readlink(filepath.Join(dir, "plans")); err != nil {
			t.Fatalf("symlink overlay not re-asserted in %s: %v", dir, err)
		}
	}
}

// TestHandleMigrateToFuseRecordsDefaultWithoutConversions: the passing gate
// proves the machine can mount, so zero conversions still records the default.
func TestHandleMigrateToFuseRecordsDefaultWithoutConversions(t *testing.T) {
	s, _, _ := newMigrateServer(t)

	if resp := s.handleMigrate(t.Context(), migrateReq(nil, "fuse")); !resp.OK {
		t.Fatalf("initial migrate failed: %s", resp.Error)
	}
	// Stands in for a fresh pool whose fuse migrate converts nothing.
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
	if len(resp.Migrations) != 0 || fake.reconcileCount() != 0 {
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
	// The wire carries only coarse fuse/symlink; concrete backend names are rejected.
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
	if !s.cl.reserve(1) {
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
	if fake.reconcileCount() != 1 {
		t.Fatalf("reconciles = %d, want 1", fake.reconcileCount())
	}
	if s.cl.held(1) {
		t.Fatal("busy refusal leaked a converting claim")
	}

	expireCommittedReservations(s.cl, 1)
	resp = s.handleMigrate(t.Context(), migrateReq(nil, "fuse"))
	got = outcomes(resp)
	if got[1] != MigrationDone || got[2] != MigrationAlready {
		t.Fatalf("sweep outcomes = %v, want acct-1 done, acct-2 already", got)
	}
}

func TestConvertClaimExcludesReservations(t *testing.T) {
	s, _, _ := newMigrateServer(t)

	if !s.cl.own(1) {
		t.Fatal("beginConvert failed on a free account")
	}
	if s.cl.reserve(1) {
		t.Fatal("tryReserve succeeded on a converting account")
	}
	if s.cl.own(1) {
		t.Fatal("double beginConvert succeeded")
	}
	s.cl.disownConvert(1)
	if !s.cl.reserve(1) {
		t.Fatal("tryReserve failed after endConvert")
	}
	if s.cl.own(1) {
		t.Fatal("beginConvert succeeded over a live reservation")
	}
	expireCommittedReservations(s.cl, 1)
	if !s.cl.own(1) {
		t.Fatal("beginConvert failed over an expired reservation")
	}
}

func TestSelectSkipsConvertingAccount(t *testing.T) {
	s, _, _ := newMigrateServer(t)

	// acct-1 is the emptier account; converting must hide it.
	if !s.cl.own(1) {
		t.Fatal("beginConvert failed")
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect})
	if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 2 {
		t.Fatalf("select = %+v, want acct-2 while acct-1 converts", resp)
	}

	one := 1
	resp = s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &one})
	if resp.OK || !strings.Contains(resp.Error, "migrating") {
		t.Fatalf("forced select = %+v, want migrating refusal", resp)
	}

	s.cl.disownConvert(1)
	resp = s.handleSelect(t.Context(), Request{Op: OpSelect})
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
	a.OverlayKind = "nfs"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	resp := s.handleSelect(t.Context(), Request{Op: OpSelect})
	if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 2 {
		t.Fatalf("select = %+v, want acct-2 while acct-1's mount is down", resp)
	}

	one := 1
	resp = s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &one})
	if resp.OK || !strings.Contains(resp.Error, "mount is not up") {
		t.Fatalf("forced select = %+v, want mount-not-up refusal", resp)
	}
}

// TestFallbackToSymlinkRestoresPrivateFiles: the row flip must not strand
// .claude.json in the fuse private backing dir.
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

// TestFallbackToSymlinkClaimAtomicAgainstSelect: the converting claim precedes
// the idle scan, so no select can reserve between the gate and force-unmount.
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
		reservedMidFallback = s.cl.reserve(1)
		return nil, nil
	}

	s.fallbackToSymlink(t.Context(), a)

	if reservedMidFallback {
		t.Fatal("a select reserved the account between the idle gate and the conversion")
	}
	if kindOf(t, s, 1) != "symlink" {
		t.Fatal("idle fallback did not convert")
	}
	if s.cl.held(1) {
		t.Fatal("fallback leaked its converting claim")
	}
	if !s.cl.reserve(1) {
		t.Fatal("account not reservable after the fallback completed")
	}
}

func TestReconcileOverlaysHealsStrandedAndFallsBack(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)

	// acct-1: identity stranded by an interrupted conversion.
	priv1 := fkoverlay.FusePrivateRoot(dirs[1])
	if err := os.MkdirAll(priv1, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(priv1, ".claude.json"), []byte("stranded"), 0o600); err != nil {
		t.Fatal(err)
	}

	// acct-2: mirror down and mount cannot come up — must fall back to a
	// usable symlink account, not adopt.
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
	fake.checkErr = errors.New("not a mountpoint")
	fake.reconcileErr = errors.New("mount did not come up")

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

// TestConvertClaimExcludesPolling: poll and convert claims exclude each other
// both ways — a bare isConverting check would be check-then-act racy.
func TestConvertClaimExcludesPolling(t *testing.T) {
	s, _, _ := newMigrateServer(t)

	if !s.cl.hold(1) {
		t.Fatal("beginPoll failed on a free account")
	}
	if s.cl.own(1) {
		t.Fatal("beginConvert succeeded while the scheduler holds the account")
	}
	if s.cl.hold(1) {
		t.Fatal("double beginPoll succeeded")
	}
	s.cl.disownHold(1)
	if !s.cl.own(1) {
		t.Fatal("beginConvert failed after endPoll")
	}
	if s.cl.hold(1) {
		t.Fatal("beginPoll succeeded while a conversion holds the account")
	}
	s.cl.disownConvert(1)
	if !s.cl.hold(1) {
		t.Fatal("beginPoll failed after endConvert")
	}
	s.cl.disownHold(1)

	// A poll claim must NOT hide the account from select — sessions can land
	// on a dir that is merely being health-checked.
	if !s.cl.hold(2) {
		t.Fatal("beginPoll failed")
	}
	if !s.cl.reserve(2) {
		t.Fatal("tryReserve refused a merely-polling account")
	}
	s.cl.disownHold(2)
}

// TestMountFuseSweepsUnderlay: underlay private files (a conversion killed
// pre-flip) must be swept before mounting or the mirror shadows the identity.
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
	if fake.reconcileCount() != 1 {
		t.Fatalf("reconciles = %d, want 1", fake.reconcileCount())
	}

	// The real resolver always yields a fuse provider; the fence refuses
	// injected fakes that would run symlink code on fuse paths.
	s.m.OverlayFor = func(_ fkoverlay.Backend) (fkoverlay.Provider, error) {
		return &fkoverlay.SymlinkProvider{Spec: s.m.OverlaySpec()}, nil
	}
	if err := s.mountFuse(t.Context(), a); err == nil || !strings.Contains(err.Error(), "no fuse provider") {
		t.Fatalf("mountFuse with a wrong-backend provider = %v, want a backend refusal", err)
	}
}

// TestMountReady: a fuse row trusts ONLY the holder cache (an lstat through a
// dead fuse-t mount can hang select); a non-fuse row needs no mountpoint (one
// is aborted-rollback wreckage).
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
		// Carcass: still a mountpoint per the kernel but the dead holder serves
		// nothing — selection must never trust it.
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

// TestHandleMigrateBudgetExhausted: a spent budget reports remaining accounts
// busy instead of overrunning the conn deadline and dead-socketing the client.
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
	if fake.reconcileCount() != 0 {
		t.Fatal("conversion ran despite an exhausted budget")
	}
	if kindOf(t, s, 1) != "symlink" {
		t.Fatal("row flipped despite an exhausted budget")
	}
}

// TestConvertAccountForceStillRespectsReservations: force skips only the
// live-session gate — a reservation means a claude is launching right now.
func TestConvertAccountForceStillRespectsReservations(t *testing.T) {
	s, _, fake := newMigrateServer(t)
	if !s.cl.reserve(1) {
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
	if fake.reconcileCount() != 0 {
		t.Fatal("forced conversion ran over a live reservation")
	}

	// Force must flow through the wire path too.
	expireCommittedReservations(s.cl, 1)
	resp := s.handleMigrate(t.Context(), Request{Op: OpMigrate, To: "fuse", Force: true})
	if !resp.OK {
		t.Fatalf("forced migrate failed: %s", resp.Error)
	}
	if got := outcomes(resp); got[1] != MigrationDone || got[2] != MigrationDone {
		t.Fatalf("outcomes = %v, want both done", got)
	}
}

// TestHandleMigrateConversionsNeverOverlap pins the sequential settle the fleet
// fan-out relies on: handleMigrate converts one account at a time, so a slow
// domain materialization never piles concurrent conversions onto the host — the
// load that crushed fileproviderd in the migrate storm. An entrancy-recording
// Reconcile (run outside the fake's lock) asserts the daemon never runs two
// conversions at once, even with a widened window a real overlap would land in.
func TestHandleMigrateConversionsNeverOverlap(t *testing.T) {
	s, _, fake := newMigrateServer(t)
	var inFlight, maxInFlight atomic.Int32
	fake.reconcileFn = func(string, string) error {
		n := inFlight.Add(1)
		for {
			old := maxInFlight.Load()
			if n <= old || maxInFlight.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
		return nil
	}

	resp := s.handleMigrate(t.Context(), migrateReq(nil, "fuse"))
	if !resp.OK {
		t.Fatalf("migrate failed: %s", resp.Error)
	}
	if got := outcomes(resp); got[1] != MigrationDone || got[2] != MigrationDone {
		t.Fatalf("outcomes = %v, want both done", got)
	}
	if fake.reconcileCount() != 2 {
		t.Fatalf("reconciles = %d, want 2 (both accounts converted)", fake.reconcileCount())
	}
	if got := maxInFlight.Load(); got != 1 {
		t.Fatalf("max concurrent conversions = %d, want 1 — the fleet settle must stay sequential", got)
	}
}

// TestConvertAccountForceStampsSessionCount pins the forced-migrate forensic
// line: a conversion forced past the live-session gate records how many sessions
// it happened under (the evidence this incident's diagnosis leaned on); an idle
// forced conversion carries no line, and a failed scan is dropped silently
// (observability, never a gate).
func TestConvertAccountForceStampsSessionCount(t *testing.T) {
	live := func(dir string) func(context.Context) ([]procscan.Session, error) {
		return func(context.Context) ([]procscan.Session, error) {
			return []procscan.Session{{PID: 4242, ConfigDir: dir}, {PID: 4243, ConfigDir: dir}}, nil
		}
	}
	cases := map[string]struct {
		scan       func(dir string) func(context.Context) ([]procscan.Session, error)
		wantDetail string
	}{
		"live sessions stamp the count": {scan: live, wantDetail: "converted under 2 live session(s)"},
		"idle carries no line": {
			scan: func(string) func(context.Context) ([]procscan.Session, error) {
				return func(context.Context) ([]procscan.Session, error) { return nil, nil }
			},
			wantDetail: "",
		},
		"scan failure is silent": {
			scan: func(string) func(context.Context) ([]procscan.Session, error) {
				return func(context.Context) ([]procscan.Session, error) { return nil, errors.New("ps exploded") }
			},
			wantDetail: "",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, dirs, _ := newMigrateServer(t)
			a, err := s.m.Store.GetAccount(1)
			if err != nil {
				t.Fatal(err)
			}
			s.scanSessions = tc.scan(dirs[1])

			res := s.convertAccount(t.Context(), a, fkoverlay.BackendNFS, true)
			if res.Outcome != MigrationDone {
				t.Fatalf("outcome = %s (%s), want done", res.Outcome, res.Detail)
			}
			if res.Detail != tc.wantDetail {
				t.Fatalf("Detail = %q, want %q", res.Detail, tc.wantDetail)
			}
		})
	}
}

// TestMigrateToSymlinkDefersUnderLiveSession: a retreat never yanks the mirror
// from under a live claude; force means the user vouches the session is idle.
func TestMigrateToSymlinkDefersUnderLiveSession(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	a := flipToFuse(t, s, 1)
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}

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

	res = s.convertAccount(t.Context(), a, fkoverlay.BackendSymlink, true)
	if res.Outcome != MigrationDone {
		t.Fatalf("forced outcome = %s (%s), want done", res.Outcome, res.Detail)
	}
	if got := kindOf(t, s, 1); got != "symlink" {
		t.Fatalf("row kind after a forced migrate = %q, want symlink", got)
	}
}

// TestConvertAccountRefetchesRow: the row is re-read under the claim, so a
// kind that changed since the caller's snapshot cannot double-convert.
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
	if fake.reconcileCount() != 0 {
		t.Fatal("conversion ran against a stale row")
	}
}

// TestPollOnceSkipsConvertingAccount: no sync, refresh, or adoption mid-conversion.
func TestPollOnceSkipsConvertingAccount(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	s, _ := newTestServer(t)
	fo := s.m.OAuth.(*fakeOAuth)
	fo.mu.Lock()
	fo.currentRT = "rt-0"
	fo.mu.Unlock()
	fk := s.m.Creds.(*credstest.Fake)
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
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-0"
	// Near-expiry (< RefreshLeadTime) so an idle poll must refresh.
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Minute).UnixMilli()
	fk.Put(a.KeychainService, a.KeychainAccount, cred)
	seedWrites := fk.WriteCount()

	if !s.cl.own(a.ID) {
		t.Fatal("beginConvert failed")
	}
	s.pollOnce(t.Context())
	if got := fo.refreshCount(); got != 0 {
		t.Fatalf("converting account was refreshed %d time(s)", got)
	}
	if got := fk.WriteCount(); got != seedWrites {
		t.Fatalf("converting account's credential was written %d time(s)", got-seedWrites)
	}

	s.cl.disownConvert(a.ID)
	s.pollOnce(t.Context())
	if got := fo.refreshCount(); got != 1 {
		t.Fatalf("idle near-expiry account refreshed %d time(s), want 1", got)
	}
}

// TestReconcileAdoptsLiveMount: a mirror the detached holder kept live across
// a daemon restart is adopted untouched and vouched in the holder cache.
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
	// A migrated fuse account's dir is a mux bridge symlink; with checkErr nil
	// (the default) the mirror reads live, so reconcile adopts it untouched rather
	// than running the one-time legacy migration.
	makeBridge(t, dirs[1])

	s.reconcileOverlays(t.Context())

	if got := fake.callOrder(); len(got) != 0 {
		t.Fatalf("adoption touched the mount: calls = %v, want none", got)
	}
	if !strings.Contains(buf.String(), fmt.Sprintf("acct-%02d adopted live mount", a.ID)) {
		t.Fatalf("adoption not logged: %q", buf.String())
	}
	// The startup refresh ran against a dead holder socket (markUnhealthy), so
	// only the adopt path's noteMounted can explain a ready dir.
	if !s.holder.ready(dirs[1]) {
		t.Fatal("adopted mount not vouched for in the holder cache")
	}
}

// TestMountFuseClearsDeadMountThenSweepsThenMounts: the fixed order, plus the
// fresh mount is vouched so a select before the next poll trusts it.
func TestMountFuseClearsDeadMountThenSweepsThenMounts(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	if err := os.WriteFile(filepath.Join(dirs[1], ".claude.json"), []byte("underlay-identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	var mounted atomic.Bool
	mounted.Store(true)
	fakeOverlayMounted(t, func(string) bool { return mounted.Load() })
	fake.checkErr = errors.New("mirror is dead")
	fake.teardownFn = func(string, string) error { mounted.Store(false); return nil }
	fake.reconcileFn = func(string, string) error {
		if _, err := os.Stat(filepath.Join(fkoverlay.FusePrivateRoot(dirs[1]), ".claude.json")); err != nil {
			return fmt.Errorf("reconcile ran before the sweep: %w", err)
		}
		return nil
	}

	if err := s.mountFuse(t.Context(), a); err != nil {
		t.Fatalf("mountFuse: %v", err)
	}
	if got := fake.callOrder(); !reflect.DeepEqual(got, []string{"teardown", "reconcile"}) {
		t.Fatalf("call order = %v, want [teardown reconcile]", got)
	}
	if _, err := os.Lstat(filepath.Join(dirs[1], ".claude.json")); !os.IsNotExist(err) {
		t.Fatal("identity left in the underlay")
	}
	if !s.holder.ready(dirs[1]) {
		t.Fatal("fresh mount not recorded in the holder cache")
	}
}

// TestMountFuseDetachesWedgedBridgeSubtree pins finding 3: a deep-wedged but
// shallow-live mux subtree (a bridge symlink) is DETACHED (Teardown) before the
// re-attach, so the holder's idempotent AddMount genuinely re-establishes the child
// and noteMounted's verdict-clear reflects a real re-attach — not a masked wedge.
func TestMountFuseDetachesWedgedBridgeSubtree(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	a := flipToFuse(t, s, 1)
	makeBridge(t, dirs[1]) // the account dir is a bridge symlink, not a real mountpoint
	fake.reconcileFn = muxReconcileSim
	// Shallow-live (Check passes) but deep-wedged: the branch must still detach.
	s.holder.markDeepWedged(dirs[1])
	fakeOverlayMounted(t, func(string) bool { return false })
	s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil }

	if err := s.mountFuse(t.Context(), a); err != nil {
		t.Fatalf("mountFuse: %v", err)
	}
	if got := fake.callOrder(); !reflect.DeepEqual(got, []string{"teardown", "reconcile"}) {
		t.Fatalf("call order = %v, want [teardown reconcile] — a deep-wedged bridge subtree must be detached before re-attach", got)
	}
	if !pool.IsBridgeSymlink(dirs[1]) {
		t.Fatal("bridge symlink not intact after the re-attach")
	}
	if !s.holder.ready(dirs[1]) {
		t.Fatal("re-attached subtree not vouched in the holder cache")
	}
	if s.holder.deepWedged(dirs[1]) {
		t.Fatal("deep-wedge verdict not cleared by a real detach/re-attach")
	}
}

// TestMountFuseWedgedPreClearAborts: never sweep or mount through a wedged unmount.
func TestMountFuseWedgedPreClearAborts(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	if err := os.WriteFile(filepath.Join(dirs[1], ".claude.json"), []byte("underlay-identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	fakeOverlayMounted(t, func(string) bool { return true })
	fake.checkErr = errors.New("mirror is dead")
	fake.teardownErr = errors.New("umount: resource busy")

	merr := s.mountFuse(t.Context(), a)
	if merr == nil || !strings.Contains(merr.Error(), "clear dead mount") {
		t.Fatalf("mountFuse over a wedged unmount = %v, want a clear-dead-mount error", merr)
	}
	if fake.reconcileCount() != 0 {
		t.Fatal("mounted through a wedged pre-clear")
	}
	if _, err := os.Stat(filepath.Join(dirs[1], ".claude.json")); err != nil {
		t.Fatalf("underlay identity disturbed despite the abort: %v", err)
	}
	if s.holder.ready(dirs[1]) {
		t.Fatal("wedged dir recorded as ready")
	}
}

// TestMountFuseForeignCarcassClearedAndRetriedOnce: Teardown's registry-miss
// path clears the carcass; exactly one sweep+Reconcile retry answers the refusal.
func TestMountFuseForeignCarcassClearedAndRetriedOnce(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	var foreign atomic.Bool
	foreign.Store(true)
	fake.reconcileFn = func(_, dir string) error {
		if foreign.Load() {
			return fmt.Errorf("mount %s: %w", dir, mountd.ErrForeignMount)
		}
		return nil
	}
	fake.teardownFn = func(string, string) error { foreign.Store(false); return nil }

	if err := s.mountFuse(t.Context(), a); err != nil {
		t.Fatalf("mountFuse: %v", err)
	}
	if got := fake.callOrder(); !reflect.DeepEqual(got, []string{"reconcile", "teardown", "reconcile"}) {
		t.Fatalf("call order = %v, want [reconcile teardown reconcile]", got)
	}
	if !s.holder.ready(dirs[1]) {
		t.Fatal("remounted carcass not recorded in the holder cache")
	}
}

// TestMountFuseBaseMismatchClearedAndRetriedOnce: a mismatched base is registry
// state, not a mount verdict — unmount-then-retry (the holder tears down by its
// registered base), never the gated symlink conversion.
func TestMountFuseBaseMismatchClearedAndRetriedOnce(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	var mismatched atomic.Bool
	mismatched.Store(true)
	fake.reconcileFn = func(_, dir string) error {
		if mismatched.Load() {
			return fmt.Errorf("mount %s: %w", dir, mountd.ErrBaseMismatch)
		}
		return nil
	}
	fake.teardownFn = func(string, string) error { mismatched.Store(false); return nil }

	if err := s.mountFuse(t.Context(), a); err != nil {
		t.Fatalf("mountFuse: %v", err)
	}
	if got := fake.callOrder(); !reflect.DeepEqual(got, []string{"reconcile", "teardown", "reconcile"}) {
		t.Fatalf("call order = %v, want [reconcile teardown reconcile]", got)
	}
	if !s.holder.ready(dirs[1]) {
		t.Fatal("remounted mismatched dir not recorded in the holder cache")
	}
}

// TestMountFusePersistentForeignFailsAfterOneRetry: exactly one retry, never a loop.
func TestMountFusePersistentForeignFailsAfterOneRetry(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	fake.reconcileErr = fmt.Errorf("mount %s: %w", dirs[1], mountd.ErrForeignMount)

	merr := s.mountFuse(t.Context(), a)
	if !errors.Is(merr, mountd.ErrForeignMount) {
		t.Fatalf("mountFuse = %v, want errors.Is ErrForeignMount", merr)
	}
	if got := fake.callOrder(); !reflect.DeepEqual(got, []string{"reconcile", "teardown", "reconcile"}) {
		t.Fatalf("call order = %v, want exactly one teardown+retry", got)
	}
	if s.holder.ready(dirs[1]) {
		t.Fatal("failed mount recorded as ready")
	}
}

// TestHealFuseTaxonomy: transient holder conditions and TCC blocks retry next
// poll; only a genuine mount failure converts, gated on provable idleness (a
// failed scan fails closed).
func TestHealFuseTaxonomy(t *testing.T) {
	cases := map[string]struct {
		reconcileErr error
		scanKind     string // "" = real scan (idle), "live" = session on the dir, "err" = scan failure
		reserve      bool
		wantOutcome  healOutcome
		wantKind     string
	}{
		"holder unavailable retries next poll": {
			reconcileErr: fmt.Errorf("mount: %w", mountd.ErrHolderUnavailable),
			wantOutcome:  healRetry, wantKind: "nfs",
		},
		// The exact chain RemoteProvider.Reconcile wrapping EnsureRunning's timeout
		// produces: a spawn blip retries, never converts.
		"spawn timeout (holder unavailable chain) retries next poll": {
			reconcileErr: fmt.Errorf("mount /pool/acct-01: %w",
				fmt.Errorf("%w: mount holder did not come up on /tmp/m.sock within 5s; check /tmp/holder.log", mountd.ErrHolderUnavailable)),
			wantOutcome: healRetry, wantKind: "nfs",
		},
		"busy mirror defers without a breaker strike": {
			reconcileErr: fmt.Errorf("mount: %w", mountd.ErrBusy),
			wantOutcome:  healDeferredBusy, wantKind: "nfs",
		},
		"tcc block classified and retried": {
			reconcileErr: fmt.Errorf("mount: %w", overlay.ErrMountNotLive),
			wantOutcome:  healTCCBlocked, wantKind: "nfs",
		},
		// A wedged unmount is no more a mount verdict than ErrBusy — and the
		// fallback's ConvertOverlay would hit the same wedge.
		"wedged unmount retries next poll": {
			reconcileErr: fmt.Errorf("mount: %w", overlay.ErrUnmountWedged),
			wantOutcome:  healRetry, wantKind: "nfs",
		},
		// The exact chain overlayClass produces for a timeout under a proven
		// grant.
		"mount timeout (proven grant) retries without recording TCC": {
			reconcileErr: fmt.Errorf("mount: %w", fmt.Errorf("%w: %w", overlay.ErrMountTimeout, mountd.ErrMountTimeout)),
			wantOutcome:  healRetry, wantKind: "nfs",
		},
		// Forward skew: an unknown class from a newer holder must read as
		// retry, never the mount failure that converts.
		"unknown holder error class retries next poll": {
			reconcileErr: fmt.Errorf("mount: %w", fmt.Errorf("%w (quota-exceeded): per-account quota exhausted", mountd.ErrUnknownClass)),
			wantOutcome:  healRetry, wantKind: "nfs",
		},
		// Skew degrade: an older daemon reads the new "mount-timeout" class as
		// ErrUnknownClass, which routes to retry.
		"mount-timeout class on a pre-fix daemon degrades to retry": {
			reconcileErr: fmt.Errorf("mount: %w", fmt.Errorf("%w (mount-timeout): fuse mount did not come up in time", mountd.ErrUnknownClass)),
			wantOutcome:  healRetry, wantKind: "nfs",
		},
		// The chain the provider's feature gate produces for a holder missing a
		// required capability: defer for the cask upgrade, never demote.
		"unsupported holder defers without demoting": {
			reconcileErr: fmt.Errorf("%w: holder v0.9.0 lacks feature \"mux\"; `brew upgrade --cask fusekit-holder`",
				pool.ErrHolderUnsupported),
			wantOutcome: healDeferredUnsupported, wantKind: "nfs",
		},
		// Mux registry state: a subtree could not join its shared root
		// (unmount-then-retry). Never a mount verdict — retry, never demote.
		"mux mismatch retries without demoting": {
			reconcileErr: fmt.Errorf("fuse mux reconcile: %w", mountd.ErrMuxMismatch),
			wantOutcome:  healRetry, wantKind: "nfs",
		},
		// A half-drained legacy account dir still holds unclassified state where
		// the bridge symlink must go: refuse to clobber it, retry, never demote.
		"occupied account dir retries without demoting": {
			reconcileErr: fmt.Errorf("fuse mux reconcile: %w", fkoverlay.ErrAccountDirOccupied),
			wantOutcome:  healRetry, wantKind: "nfs",
		},
		"genuine failure on an idle account converts": {
			reconcileErr: errors.New("mount exploded"),
			wantOutcome:  healFallback, wantKind: "symlink",
		},
		"genuine failure under a reservation defers": {
			reconcileErr: errors.New("mount exploded"), reserve: true,
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
			fake.reconcileErr = tc.reconcileErr
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
			if tc.reserve && !s.cl.reserve(1) {
				t.Fatal("tryReserve failed on a free account")
			}

			if got := s.healFuse(t.Context(), a); got != tc.wantOutcome {
				t.Fatalf("healFuse outcome = %d, want %d", got, tc.wantOutcome)
			}
			if got := kindOf(t, s, 1); got != tc.wantKind {
				t.Fatalf("row kind = %q, want %q", got, tc.wantKind)
			}
			if s.cl.held(1) {
				t.Fatal("heal leaked a converting claim")
			}
			if tc.wantOutcome == healMounted && !s.holder.ready(dirs[1]) {
				t.Fatal("clean mount not recorded in the holder cache")
			}
		})
	}
}

// TestHealFuseSweepFailureRetries: a pre-Reconcile sweep failure (errSweepStranded)
// is not a mount verdict — retry next poll, never demote to symlink.
func TestHealFuseSweepFailureRetries(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	fake.reconcileErr = nil // Reconcile must never be reached; the sweep fails first.

	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	// moveEntry refuses the file-vs-dir clash, so the sweep fails before Reconcile.
	dir := dirs[1]
	if err := os.WriteFile(filepath.Join(dir, ".credentials.json"), []byte("identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(fkoverlay.FusePrivateRoot(dir), ".credentials.json"), 0o700); err != nil {
		t.Fatal(err)
	}

	if got := s.healFuse(t.Context(), a); got != healRetry {
		t.Fatalf("healFuse outcome = %d, want healRetry (%d): a pre-Reconcile sweep failure must not convert", got, healRetry)
	}
	if got := kindOf(t, s, 1); got != "nfs" {
		t.Fatalf("row kind = %q, want \"fuse\": a sweep failure must not demote to symlink", got)
	}
	if s.cl.held(1) {
		t.Fatal("a sweep failure leaked a converting claim")
	}
}

// TestSelectServesFuseAccountWhenHolderVouches: the carcass gate's positive arm.
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

	resp := s.handleSelect(t.Context(), Request{Op: OpSelect})
	if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 1 {
		t.Fatalf("select = %+v, want vouched-for acct-1 (the emptier account)", resp)
	}
}

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

// TestSelectColdStartPrimesHolderCacheLazily: the daemon socket binds before
// the startup prime, so a select in that window lazily refreshes (bounded
// socket RPC, no filesystem touch) instead of refusing every fuse account.
func TestSelectColdStartPrimesHolderCacheLazily(t *testing.T) {
	s, dirs, _ := newMigrateServer(t)
	fuseRowWithCannedHolder(t, s, dirs)

	// The cache is zero-valued — never refreshed — exactly the bind→prime gap.
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect})
	if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 1 {
		t.Fatalf("cold-start select = %+v, want lazily-primed acct-1 (the emptier account)", resp)
	}

	// The forced path flows through the same lazy prime.
	s2, dirs2, _ := newMigrateServer(t)
	fuseRowWithCannedHolder(t, s2, dirs2)
	one := 1
	resp = s2.handleSelect(t.Context(), Request{Op: OpSelect, Account: &one})
	if !resp.OK || resp.Dir != dirs2[1] {
		t.Fatalf("cold-start forced select = %+v, want acct-1's dir", resp)
	}
}

// startVersionedHolder binds a canned mountd holder at socket answering every
// op OK: Probe succeeds (FuseOK) and Health reports version. It is listening
// before it returns, so any Select/EnsureRunning against the socket
// short-circuits on Available and never spawns a real holder.
func startVersionedHolder(t *testing.T, socket, version string) {
	t.Helper()
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go serveVersionedHolder(ln, version)
}

func serveVersionedHolder(ln net.Listener, version string) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		var req mountd.Request
		resp := mountd.Response{OK: true, Proto: mountd.MountProtoVersion, Version: version, Features: mountd.HolderFeatures}
		if err := json.NewDecoder(conn).Decode(&req); err == nil && req.Op == mountd.OpProbe {
			resp.FuseOK = true
		}
		_ = json.NewEncoder(conn).Encode(resp)
		_ = conn.Close()
	}
}

// gateHome points HOME at a short /tmp dir (macOS caps sun_path at 104 bytes;
// t.TempDir paths overflow it) and returns the shared holder socket path under
// it, parent dir created.
func gateHome(t *testing.T) string {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "ccp-gate")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	socket := mountd.DefaultHolderSocket()
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	return socket
}

// startFeaturedHolder serves a holder answering health/probe/hello with the given
// feature set (proto 2) on socket, for the production fuseGate feature handshake.
func startFeaturedHolder(t *testing.T, socket string, features []string) {
	t.Helper()
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			var req mountd.Request
			resp := mountd.Response{OK: true, Proto: mountd.MountProtoVersion, Version: "v1.0.0", Features: features, FuseOK: true}
			_ = json.NewDecoder(conn).Decode(&req)
			_ = json.NewEncoder(conn).Encode(resp)
			_ = conn.Close()
		}
	}()
}

// TestFuseGateRequiresFeatures drives the PRODUCTION fuseGate (no fuseGateFn seam)
// against a canned holder on the shared socket: a holder serving every required
// capability passes, one missing a required feature is refused with the
// cask-upgrade hint — the feature handshake, not version arithmetic.
func TestFuseGateRequiresFeatures(t *testing.T) {
	cases := map[string]struct {
		features []string
		wantPass bool
	}{
		"a holder serving all required features passes": {features: mountd.HolderFeatures, wantPass: true},
		"a holder missing the mux feature is refused":   {features: []string{mountd.FeatureBridge, mountd.FeatureLeaseGate}, wantPass: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			socket := gateHome(t)
			startFeaturedHolder(t, socket, tc.features)
			s := &Server{cl: newClaims(), holderSocket: socket}

			backend, reason := s.fuseGate(t.Context())
			if tc.wantPass {
				if reason != "" || !backend.IsFuse() {
					t.Fatalf("fuseGate = (%q, %q), want a fuse pass", backend, reason)
				}
				return
			}
			if backend != "" || reason == "" {
				t.Fatalf("fuseGate = (%q, %q), want a refusal", backend, reason)
			}
			for _, frag := range []string{"brew upgrade --cask fusekit-holder", "ccp migrate"} {
				if !strings.Contains(reason, frag) {
					t.Fatalf("refusal %q missing %q", reason, frag)
				}
			}
		})
	}
}

// TestFuseGateHealthFailureFailsSafe: a holder whose version cannot be
// observed is never assumed mitigated — the gate refuses with the probe fault
// rather than passing (or minting a version verdict). The cancelled ctx bounds
// awaitHolderHealth's retry loop to one attempt.
func TestFuseGateHealthFailureFailsSafe(t *testing.T) {
	socket := gateHome(t)
	startVersionedHolder(t, socket, "v0.25.0") // Select's probe arm passes
	deadDir, err := os.MkdirTemp("/tmp", "ccp-dead")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(deadDir) })
	// The holder died between the probe and the health read.
	s := &Server{cl: newClaims(), holderSocket: filepath.Join(deadDir, "gone.sock")}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	backend, reason := s.fuseGate(ctx)
	if backend != "" || !strings.Contains(reason, "mount holder health probe failed") {
		t.Fatalf("fuseGate = (%q, %q), want a fail-safe health-probe refusal", backend, reason)
	}
	if strings.Contains(reason, "predates") {
		t.Fatalf("refusal %q claims a version verdict for an unobservable holder", reason)
	}
}

// TestAwaitHolderHealth pins the migrate warm-up: a freshly spawned holder
// still binding its socket is ridden out (bounded by the holder's own spawn
// allowance), and a cancelled wait surfaces the real Health fault.
func TestAwaitHolderHealth(t *testing.T) {
	t.Run("rides out a holder still binding its socket", func(t *testing.T) {
		sockDir, err := os.MkdirTemp("/tmp", "ccp-warm")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
		socket := filepath.Join(sockDir, "m.sock")
		s := &Server{cl: newClaims(), holderSocket: socket}
		lnCh := make(chan net.Listener, 1)
		timer := time.AfterFunc(300*time.Millisecond, func() {
			ln, err := net.Listen("unix", socket)
			if err != nil {
				return
			}
			lnCh <- ln
			go serveVersionedHolder(ln, "v0.25.0")
		})
		t.Cleanup(func() {
			timer.Stop()
			select {
			case ln := <-lnCh:
				_ = ln.Close()
			default:
			}
		})

		ver, err := s.awaitHolderHealth(t.Context())
		if err != nil {
			t.Fatalf("awaitHolderHealth over a late-binding holder: %v", err)
		}
		if ver != "v0.25.0" {
			t.Fatalf("version = %q, want v0.25.0", ver)
		}
	})

	t.Run("cancelled wait surfaces the last health error", func(t *testing.T) {
		deadDir, err := os.MkdirTemp("/tmp", "ccp-nosock")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(deadDir) })
		s := &Server{cl: newClaims(), holderSocket: filepath.Join(deadDir, "gone.sock")}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		start := time.Now()
		ver, err := s.awaitHolderHealth(ctx)
		if !errors.Is(err, mountd.ErrHolderUnavailable) {
			t.Fatalf("error = %v, want errors.Is ErrHolderUnavailable (the real fault, not a bare ctx error)", err)
		}
		if ver != "" {
			t.Fatalf("version = %q on failure, want empty", ver)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("cancelled wait took %v; it must not sit out the spawn allowance", elapsed)
		}
	})
}

// TestHandleMigrateBudgetStartsAfterFuseGate: the gate's warm-up (holder
// cold-start absorbed in awaitHolderHealth) must not be paid out of the
// conversion budget — handleMigrate computes the budget deadline only after
// fuseGate returns.
func TestHandleMigrateBudgetStartsAfterFuseGate(t *testing.T) {
	s, _, _ := newMigrateServer(t)
	s.migrateBudget = time.Second
	s.fuseGateFn = func() (fkoverlay.Backend, string) {
		time.Sleep(2 * time.Second) // a holder cold-start ridden out inside the gate
		return fkoverlay.BackendNFS, ""
	}

	resp := s.handleMigrate(t.Context(), migrateReq(nil, "fuse"))
	if !resp.OK {
		t.Fatalf("migrate failed: %s", resp.Error)
	}
	got := outcomes(resp)
	if got[1] != MigrationDone || got[2] != MigrationDone {
		t.Fatalf("outcomes = %v, want both done — the gate's wait consumed the conversion budget (deadline computed before fuseGate?)", got)
	}
}

// TestHandleMigrateToSymlinkSkipsFuseGate: the mitigation gate guards HOSTING
// fuse; the symlink retreat must run without consulting it — an old holder is
// exactly when users need to retreat.
func TestHandleMigrateToSymlinkSkipsFuseGate(t *testing.T) {
	s, _, _ := newMigrateServer(t)
	if resp := s.handleMigrate(t.Context(), migrateReq(nil, "fuse")); !resp.OK {
		t.Fatalf("forward migrate failed: %s", resp.Error)
	}
	gateCalls := 0
	s.fuseGateFn = func() (fkoverlay.Backend, string) {
		gateCalls++
		return "", "fuse unavailable: mount holder v0.22.9 predates the NFS kernel-panic mitigations"
	}

	resp := s.handleMigrate(t.Context(), migrateReq(nil, "symlink"))
	if !resp.OK {
		t.Fatalf("symlink retreat blocked: %s", resp.Error)
	}
	got := outcomes(resp)
	if got[1] != MigrationDone || got[2] != MigrationDone {
		t.Fatalf("outcomes = %v, want both done", got)
	}
	if kindOf(t, s, 1) != "symlink" || kindOf(t, s, 2) != "symlink" {
		t.Fatal("rows not flipped back to symlink")
	}
	if gateCalls != 0 {
		t.Fatalf("fuse gate consulted %d time(s) on a symlink migrate, want 0", gateCalls)
	}
}

// cancellingSymlinkProv cancels a context at the top of Teardown — inside
// convertToFuse's strand window — then runs the real symlink teardown.
type cancellingSymlinkProv struct {
	*fkoverlay.SymlinkProvider
	cancel context.CancelFunc
}

func (p *cancellingSymlinkProv) Teardown(ctx context.Context, base, dir string) (string, error) {
	p.cancel()
	return p.SymlinkProvider.Teardown(ctx, base, dir)
}

// TestHandleMigrateShutdownMidConversionCompletesInFlight: convertAccount
// detaches the conversion from the request ctx (context.WithoutCancel), so a
// daemon shutdown landing inside the strand window finishes the in-flight
// account instead of abandoning it half-converted; the migrate loop still
// stops before the next account.
func TestHandleMigrateShutdownMidConversionCompletesInFlight(t *testing.T) {
	s, dirs, fake := newMigrateServer(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// The shutdown lands mid-window: private files moved, row still symlink.
	s.m.OverlayFor = func(backend fkoverlay.Backend) (fkoverlay.Provider, error) {
		if backend.IsFuse() {
			return fake, nil
		}
		return &cancellingSymlinkProv{
			SymlinkProvider: &fkoverlay.SymlinkProvider{Spec: s.m.OverlaySpec()},
			cancel:          cancel,
		}, nil
	}

	resp := s.handleMigrate(ctx, migrateReq(nil, "fuse"))
	if !resp.OK {
		t.Fatalf("migrate failed: %s", resp.Error)
	}
	if len(resp.Migrations) != 1 || resp.Migrations[0].ID != 1 || resp.Migrations[0].Outcome != MigrationDone {
		t.Fatalf("migrations = %+v, want exactly acct-1 done (finished despite the shutdown)", resp.Migrations)
	}
	if kindOf(t, s, 1) != "nfs" {
		t.Fatal("in-flight conversion abandoned inside the strand window (row not flipped)")
	}
	if fake.reconcileCount() != 1 {
		t.Fatalf("reconciles = %d, want acct-1's mount only", fake.reconcileCount())
	}
	if kindOf(t, s, 2) != "symlink" {
		t.Fatal("migrate loop continued past a cancelled ctx")
	}
	if _, err := os.Lstat(fkoverlay.FusePrivateRoot(dirs[2])); !os.IsNotExist(err) {
		t.Fatal("untouched account grew a private root")
	}
}

// TestMountReadyRefreshesOnCacheMiss: a fuse dir mounted outside the daemon (a
// mirror `ccp add`) misses the stale cache and triggers one refresh; a fresh
// cache rate-limits so a down mount cannot turn every select into holder RPCs.
func TestMountReadyRefreshesOnCacheMiss(t *testing.T) {
	s, dirs, _ := newMigrateServer(t)
	a := fuseRowWithCannedHolder(t, s, dirs)

	s.holder.mu.Lock()
	s.holder.healthy, s.holder.mounts, s.holder.refreshedAt = true, map[string]bool{}, time.Now()
	s.holder.mu.Unlock()
	if s.mountReady(a) {
		t.Fatal("a refresh fired inside the rate-limit floor")
	}

	s.holder.mu.Lock()
	s.holder.refreshedAt = time.Now().Add(-holderRefreshFloor - time.Second)
	s.holder.mu.Unlock()
	if !s.mountReady(a) {
		t.Fatal("a stale cache miss did not refresh")
	}
}

// swapFPGateSeams points fpGate's two seams at canned verdicts and returns a
// counter of capability-probe dials, restoring both on cleanup. capable is the
// throwaway-domain verdict returned when probeErr is nil.
func swapFPGateSeams(t *testing.T, available, capable bool, probeErr error) *int {
	t.Helper()
	prevAvail, prevProbe := fpAvailable, fpCapabilityProbe
	t.Cleanup(func() { fpAvailable, fpCapabilityProbe = prevAvail, prevProbe })
	probes := new(int)
	fpAvailable = func(fkoverlay.Spec) bool { return available }
	fpCapabilityProbe = func(context.Context, string) (bool, error) {
		*probes++
		return capable, probeErr
	}
	return probes
}

// TestFPGate drives the production fpGate through its seams: an absent extension
// is the root fault and refuses WITHOUT dialing the companion app; a dead control
// socket refuses with a launch hint; an enabled-but-not-serving capability verdict
// refuses with the System Settings hint (the reworded rung — an installed but
// unconsented provider that a Health ok:true ping would have passed); and a serving
// pair passes as BackendFileProvider.
func TestFPGate(t *testing.T) {
	cases := map[string]struct {
		available   bool
		capable     bool
		probeErr    error
		wantBackend fkoverlay.Backend
		wantFrags   []string
		wantProbes  int
	}{
		"extension absent refuses without dialing the app": {
			available: false,
			wantFrags: []string{"fileprovider unavailable", pool.FPExtensionBundleID, "ccp fp onboard", "re-run `ccp migrate`"},
		},
		"dead control socket refuses with a launch hint": {
			available:  true,
			probeErr:   errors.New("dial unix /tmp/x/domains.sock: connect: no such file or directory"),
			wantFrags:  []string{"control probe failed", "domains.sock", pool.WidgetAppPath(), "re-run `ccp migrate`"},
			wantProbes: 1,
		},
		"enabled but not serving refuses with the System Settings hint": {
			available:  true,
			capable:    false,
			wantFrags:  []string{"fileprovider unavailable", "enabled but not serving", "System Settings", "ccp fp onboard"},
			wantProbes: 1,
		},
		"enabled and serving passes": {
			available:   true,
			capable:     true,
			wantBackend: fkoverlay.BackendFileProvider,
			wantProbes:  1,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			probes := swapFPGateSeams(t, tc.available, tc.capable, tc.probeErr)
			s := &Server{cl: newClaims(), m: &pool.Manager{}}

			backend, reason := s.fpGate(t.Context())
			if tc.wantBackend != "" {
				if backend != tc.wantBackend || reason != "" {
					t.Fatalf("fpGate = (%q, %q), want a %s pass", backend, reason, tc.wantBackend)
				}
			} else if backend != "" || reason == "" {
				t.Fatalf("fpGate = (%q, %q), want a refusal", backend, reason)
			}
			for _, frag := range tc.wantFrags {
				if !strings.Contains(reason, frag) {
					t.Errorf("refusal %q missing %q", reason, frag)
				}
			}
			if *probes != tc.wantProbes {
				t.Fatalf("capability probe dials = %d, want %d", *probes, tc.wantProbes)
			}
		})
	}
}

// TestHandleMigrateFileProviderGateBlocked: a failed fpGate refuses the whole
// op before any account row, conversion, or the recorded default is touched.
func TestHandleMigrateFileProviderGateBlocked(t *testing.T) {
	s, _, fake := newMigrateServer(t)
	swapFPGateSeams(t, false, false, nil)

	resp := s.handleMigrate(t.Context(), migrateReq(nil, "fileprovider"))
	if resp.OK {
		t.Fatal("migrate passed with the extension absent")
	}
	if !strings.Contains(resp.Error, pool.FPExtensionBundleID) {
		t.Fatalf("refusal %q does not name the extension", resp.Error)
	}
	if len(resp.Migrations) != 0 {
		t.Fatalf("migrations attempted despite a failed gate: %v", resp.Migrations)
	}
	if kindOf(t, s, 1) != "symlink" || kindOf(t, s, 2) != "symlink" {
		t.Fatal("rows changed despite a failed gate")
	}
	if fake.reconcileCount() != 0 {
		t.Fatalf("overlay reconciles = %d despite a failed gate", fake.reconcileCount())
	}
	if _, ok, err := s.m.Store.GetMeta("overlay_kind"); err != nil || ok {
		t.Fatalf("default recorded despite a failed gate (ok=%v err=%v)", ok, err)
	}
}

// TestHandleMigrateFileProviderZeroConversionsFlipsDefault: rows already on
// fileprovider convert nothing, but a passed gate still records the
// new-account default — the fuse zero-conversion precedent.
func TestHandleMigrateFileProviderZeroConversionsFlipsDefault(t *testing.T) {
	s, _, _ := newMigrateServer(t)
	swapFPGateSeams(t, true, true, nil)
	for _, id := range []int{1, 2} {
		if err := s.m.Store.SetAccountOverlayKind(id, "fileprovider"); err != nil {
			t.Fatal(err)
		}
	}

	resp := s.handleMigrate(t.Context(), migrateReq(nil, "fileprovider"))
	if !resp.OK {
		t.Fatalf("migrate failed: %s", resp.Error)
	}
	got := outcomes(resp)
	if got[1] != MigrationAlready || got[2] != MigrationAlready {
		t.Fatalf("outcomes = %v, want both already", got)
	}
	if v, ok, err := s.m.Store.GetMeta("overlay_kind"); err != nil || !ok || v != "fileprovider" {
		t.Fatalf("meta overlay_kind = %q ok=%v err=%v, want fileprovider", v, ok, err)
	}
}

// countLeaseSeizes wraps the server's lease-root seam with a counter so a test can
// prove whether a conversion took a consumer-held exclusive seize.
func countLeaseSeizes(t *testing.T, s *Server) *int {
	t.Helper()
	root, err := s.m.LeaseRoot()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	s.m.LeaseRoot = func() (string, error) {
		n++
		return root, nil
	}
	return &n
}

// TestConvertAccountLeaseFenceSplit pins the F7 who-performs-the-destructive-step
// split, including the self-bounce-trap regression: a FUSE source's teardown is
// delegated to the holder (the holder is the seizer), so cc-pool must NOT hold a
// consumer exclusive fence WHILE that teardown runs — its own EX would bounce the
// holder's Seize ClassBusy forever. The sequenced-op rule then fences the local
// restore-and-relink that follows the CONFIRMED teardown (never around it). A
// symlink source's whole plain-dir mutation cc-pool performs itself IS fenced.
func TestConvertAccountLeaseFenceSplit(t *testing.T) {
	t.Run("fuse-source teardown is not wrapped in a consumer seize", func(t *testing.T) {
		s, _, fake := newMigrateServer(t)
		if err := os.MkdirAll(pool.ClaudeDir(), 0o700); err != nil {
			t.Fatal(err)
		}
		setRowKind(t, s, 1, fkoverlay.BackendNFS)

		// Capture the real lease root BEFORE the seize counter wraps LeaseRoot, so
		// the in-teardown probe below does not itself register as a consumer seize.
		root, err := s.m.LeaseRoot()
		if err != nil {
			t.Fatal(err)
		}
		// The fake holder-delegated teardown stands in for the holder's OWN Seize: it
		// must be able to take the row's exclusive lease DURING teardown. If cc-pool
		// wrapped the teardown in a consumer EX, this bounces ErrBusy — the self-bounce
		// trap. A legacy/plain fuse row leases its ConfigDir, which is dir here.
		var teardownRan bool
		var teardownSeizeErr error
		fake.teardownFn = func(_, dir string) error {
			teardownRan = true
			fence, ferr := lease.Seize(root, dir)
			teardownSeizeErr = ferr
			if ferr == nil {
				_ = fence.Release() // release at once so the post-teardown local fence can take it
			}
			return nil
		}

		seizes := countLeaseSeizes(t, s)
		one := 1
		resp := s.handleMigrate(t.Context(), migrateReq(&one, "symlink"))
		if !resp.OK {
			t.Fatalf("migrate: %s", resp.Error)
		}
		if !teardownRan {
			t.Fatal("the holder-delegated fuse teardown never ran")
		}
		if errors.Is(teardownSeizeErr, lease.ErrBusy) {
			t.Fatal("the holder could not seize the lease during teardown: cc-pool wrapped a holder-delegated teardown in a consumer EX (the self-bounce trap)")
		}
		if teardownSeizeErr != nil {
			t.Fatalf("in-teardown seize failed unexpectedly: %v", teardownSeizeErr)
		}
		// Sequenced-op rule: exactly ONE consumer seize, fencing the local
		// restore-and-relink AFTER the confirmed teardown (never around it).
		if *seizes != 1 {
			t.Fatalf("fuse-source conversion took %d consumer seizes; want exactly 1 (the post-teardown local-drain fence)", *seizes)
		}
	})

	t.Run("symlink source is fenced by a consumer seize", func(t *testing.T) {
		s, _, _ := newMigrateServer(t)
		seizes := countLeaseSeizes(t, s)

		one := 1
		resp := s.handleMigrate(t.Context(), migrateReq(&one, "fuse"))
		if !resp.OK {
			t.Fatalf("migrate: %s", resp.Error)
		}
		if *seizes == 0 {
			t.Fatal("symlink-source conversion took no consumer seize; the local-mutation lease fence is missing")
		}
	})
}

// TestHandleMigrateUnknownTargetNamesAllThree pins the refusal for a junk
// target naming every valid arm.
func TestHandleMigrateUnknownTargetNamesAllThree(t *testing.T) {
	s, _, _ := newMigrateServer(t)
	resp := s.handleMigrate(t.Context(), migrateReq(nil, "granite"))
	if resp.OK || !strings.Contains(resp.Error, "want fuse, symlink, or fileprovider") {
		t.Fatalf("resp = %+v, want an unknown-target refusal naming all three arms", resp)
	}
}
