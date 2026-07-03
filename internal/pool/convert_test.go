package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

func testSpec() fkoverlay.Spec { return overlaySpec() }

func newSymlinkProvider() *fkoverlay.SymlinkProvider {
	return &fkoverlay.SymlinkProvider{Spec: testSpec()}
}

const (
	identityJSON      = `{"oauthAccount":{"accountUuid":"u-1","emailAddress":"a@example.com"}}`
	wrongIdentityJSON = `{"oauthAccount":{"accountUuid":"u-IMPOSTOR","emailAddress":"x@example.com"}}`
)

type fakeFuse struct {
	ops           *[]string
	setupErr      error
	teardownErr   error
	wrongIdentity bool
	noMountView   bool             // Setup materializes nothing at dir/.claude.json: through-mount reads are unavailable
	onSetup       func(dir string) // runs inside Setup, before setupErr
	created       string
}

func (f *fakeFuse) Backend() fkoverlay.Backend    { return fkoverlay.BackendNFS }
func (f *fakeFuse) Sync(_, _ string) error        { return nil }
func (f *fakeFuse) Health(_, _ string) error      { return nil }
func (f *fakeFuse) PrivateRoot(dir string) string { return fkoverlay.FusePrivateRoot(dir) }
func (f *fakeFuse) Setup(_, dir string) error {
	priv := fkoverlay.FusePrivateRoot(dir)
	privIdentity := false
	if _, err := os.Stat(filepath.Join(priv, ".claude.json")); err == nil {
		privIdentity = true
	}
	*f.ops = append(*f.ops, fmt.Sprintf("fuse.setup(priv-identity=%v)", privIdentity))
	if f.onSetup != nil {
		f.onSetup(dir)
	}
	if f.setupErr != nil {
		return f.setupErr
	}
	if f.wrongIdentity {
		// The private root ends up holding an identity that is not the one the
		// conversion moved — the state the post-mount verify must catch.
		return os.WriteFile(filepath.Join(priv, ".claude.json"), []byte(wrongIdentityJSON), 0o600)
	}
	if f.noMountView || !privIdentity {
		return nil
	}
	mounted := filepath.Join(dir, ".claude.json")
	if err := os.Symlink(filepath.Join(priv, ".claude.json"), mounted); err != nil {
		return err
	}
	f.created = mounted
	return nil
}

func (f *fakeFuse) Teardown(_, _ string) error {
	*f.ops = append(*f.ops, "fuse.teardown")
	if f.teardownErr != nil {
		return f.teardownErr
	}
	if f.created != "" {
		_ = os.Remove(f.created)
		f.created = ""
	}
	return nil
}

func newConvertFixture(t *testing.T, fake *fakeFuse) (*Manager, store.Account, string) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	base := filepath.Join(home, ".claude")
	if err := os.MkdirAll(filepath.Join(base, "projects"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "settings.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	dir := filepath.Join(home, "acct-01")
	if err := newSymlinkProvider().Setup(base, dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".claude.json"), []byte(identityJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "backups"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "backups", "b.bak"), []byte("bak"), 0o600); err != nil {
		t.Fatal(err)
	}

	st := openTestStore(t)
	a := store.Account{ID: 1, ConfigDir: dir, KeychainService: "svc", KeychainAccount: "user", OverlayKind: "symlink"}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	m := &Manager{Store: st}
	if fake != nil {
		m.OverlayFor = func(backend fkoverlay.Backend) (fkoverlay.Provider, error) {
			if backend.IsFuse() {
				return fake, nil
			}
			return newSymlinkProvider(), nil
		}
	}
	return m, a, dir
}

func storedKind(t *testing.T, m *Manager, id int) string {
	t.Helper()
	a, err := m.Store.GetAccount(id)
	if err != nil {
		t.Fatal(err)
	}
	return a.OverlayKind
}

func TestConvertOverlayNoopWhenAlreadyTarget(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops}
	m, a, dir := newConvertFixture(t, fake)
	got, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendSymlink)
	if err != nil {
		t.Fatalf("ConvertOverlay: %v", err)
	}
	if got.OverlayKind != "symlink" || len(ops) != 0 {
		t.Fatalf("no-op convert: kind=%s ops=%v", got.OverlayKind, ops)
	}
	if _, err := os.Stat(filepath.Join(dir, ".claude.json")); err != nil {
		t.Fatalf("no-op convert disturbed the dir: %v", err)
	}
}

// TestConvertOverlayRejectsWrongKindFake pins the Backend() equality fences: a wrong-backend provider running the wrong code path destroys account state.
func TestConvertOverlayRejectsWrongKindFake(t *testing.T) {
	wrongKind := func(fkoverlay.Backend) (fkoverlay.Provider, error) { return newSymlinkProvider(), nil }

	t.Run("target fence", func(t *testing.T) {
		m, a, dir := newConvertFixture(t, nil)
		m.OverlayFor = wrongKind
		_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendNFS)
		if !errors.Is(err, ErrConvertUnsupported) {
			t.Fatalf("ConvertOverlay error = %v, want ErrConvertUnsupported", err)
		}
		if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
			t.Fatalf("identity disturbed by refused convert: %q", got)
		}
		if storedKind(t, m, a.ID) != "symlink" {
			t.Fatal("row changed by refused convert")
		}
	})

	t.Run("source fence", func(t *testing.T) {
		m, a, dir := newConvertFixture(t, nil)
		m.OverlayFor = wrongKind
		a.OverlayKind = "nfs"
		if err := m.Store.UpsertAccount(a); err != nil {
			t.Fatal(err)
		}
		_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendSymlink)
		if !errors.Is(err, ErrConvertUnsupported) {
			t.Fatalf("ConvertOverlay error = %v, want ErrConvertUnsupported", err)
		}
		if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
			t.Fatalf("identity disturbed by refused convert: %q", got)
		}
		if storedKind(t, m, a.ID) != "nfs" {
			t.Fatal("row changed by refused convert")
		}
	})
}

// TestConvertOverlayRetreatWithoutLiveMount pins the escape hatch when fuse rows outlive their mounts: with nothing mounted Teardown is a no-op, so the fuse→symlink retreat is pure file moves and works in every build.
func TestConvertOverlayRetreatWithoutLiveMount(t *testing.T) {
	m, a, dir := newConvertFixture(t, nil)
	// Fuse rest state.
	priv := fkoverlay.FusePrivateRoot(dir)
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, ".claude.json"), filepath.Join(priv, ".claude.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "backups"), filepath.Join(priv, "backups")); err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	if err := m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	back, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendSymlink)
	if err != nil {
		t.Fatalf("retreat without a live mount: %v", err)
	}
	if back.OverlayKind != "symlink" || storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row not flipped back to symlink")
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity not restored: %q", got)
	}
	if got := readFileT(t, filepath.Join(dir, "backups", "b.bak")); got != "bak" {
		t.Fatalf("backups not restored: %q", got)
	}
	if _, err := os.Readlink(filepath.Join(dir, "projects")); err != nil {
		t.Fatalf("symlink overlay not re-asserted: %v", err)
	}
}

func TestConvertToFuseHappyPath(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops}
	m, a, dir := newConvertFixture(t, fake)
	priv := fkoverlay.FusePrivateRoot(dir)

	got, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendNFS)
	if err != nil {
		t.Fatalf("ConvertOverlay: %v", err)
	}
	if got.OverlayKind != "nfs" || storedKind(t, m, a.ID) != "nfs" {
		t.Fatalf("row not flipped: returned=%s stored=%s", got.OverlayKind, storedKind(t, m, a.ID))
	}
	// Move precedes the mount: Setup saw the identity in the private dir.
	if len(ops) != 1 || ops[0] != "fuse.setup(priv-identity=true)" {
		t.Fatalf("ops = %v, want one setup with private identity in place", ops)
	}
	if gotJSON := readFileT(t, filepath.Join(priv, ".claude.json")); gotJSON != identityJSON {
		t.Fatalf("identity in private root = %q", gotJSON)
	}
	if gotBak := readFileT(t, filepath.Join(priv, "backups", "b.bak")); gotBak != "bak" {
		t.Fatalf("backups content lost: %q", gotBak)
	}
	if _, err := os.Lstat(filepath.Join(dir, "projects")); !os.IsNotExist(err) {
		t.Fatal("shared symlink survived conversion")
	}
}

// TestConvertToFuseVerifiesIdentityInPrivateRootNotThroughMount pins the
// post-mount identity verify to the private backing file. It reproduces the
// live incident this guards against: `ccp migrate --to fuse --force` on a dir
// a live claude session still held would stall in the old through-mount
// readIdentity(dir/.claude.json) — an unbounded os.ReadFile at the
// macOS-NFS/fuse-t transport layer — until the client timeout fired, and the
// stalled read's eventual error rolled back onto the busy mount, leaving the
// account mounted with a symlink row and its private files stranded. The
// fake's noMountView mode materializes nothing at dir/.claude.json, exactly a
// mirror whose through-mount reads are unavailable at verify time, so the
// conversion must succeed purely off priv/.claude.json — the file ReadSynth
// serves as the mount's content anyway. Reverted to read dir/.claude.json,
// the verify misses, the conversion rolls back, and this test fails.
func TestConvertToFuseVerifiesIdentityInPrivateRootNotThroughMount(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops, noMountView: true}
	m, a, dir := newConvertFixture(t, fake)
	priv := fkoverlay.FusePrivateRoot(dir)

	got, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendNFS)
	if err != nil {
		t.Fatalf("ConvertOverlay with no through-mount view: %v", err)
	}
	if got.OverlayKind != "nfs" || storedKind(t, m, a.ID) != "nfs" {
		t.Fatalf("row not flipped: returned=%s stored=%s", got.OverlayKind, storedKind(t, m, a.ID))
	}
	// Exactly one setup and no teardown: the verify never tripped a rollback.
	if len(ops) != 1 || ops[0] != "fuse.setup(priv-identity=true)" {
		t.Fatalf("ops = %v, want exactly one setup with the private identity in place", ops)
	}
	if gotJSON := readFileT(t, filepath.Join(priv, ".claude.json")); gotJSON != identityJSON {
		t.Fatalf("identity in private root = %q, want %q", gotJSON, identityJSON)
	}
	if gotBak := readFileT(t, filepath.Join(priv, "backups", "b.bak")); gotBak != "bak" {
		t.Fatalf("backups content lost: %q", gotBak)
	}
	// Private files moved, shared links torn down, nothing minted at the
	// mountpoint: the dir is a bare mountpoint with zero stranded state.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Fatalf("account dir not left as a bare mountpoint: %v", names)
	}
}

// TestConvertToFuseMovesMcpNeedsAuthCacheToPrivate pins the anti-silent-data-loss
// arm of the mcp-needs-auth-cache.json private classification. Unclassified, the
// name defaulted to shared, and claude's atomic rewrite (temp+rename) turned the
// overlay symlink into a real per-account file; on symlink→fuse conversion
// MovePrivateEntries relocates only private entries, so that real file was
// neither moved nor merged — it got shadowed under the mount and the account
// silently reverted to ~/.claude's shared copy. Classified private, the file
// must land in the private backing root byte-identical and leave the account
// dir (moved, not copied).
func TestConvertToFuseMovesMcpNeedsAuthCacheToPrivate(t *testing.T) {
	const authCacheJSON = `{"duckbill":{"timestamp":123}}`
	ops := []string{}
	fake := &fakeFuse{ops: &ops}
	m, a, dir := newConvertFixture(t, fake)
	priv := fkoverlay.FusePrivateRoot(dir)
	if err := os.WriteFile(filepath.Join(dir, "mcp-needs-auth-cache.json"), []byte(authCacheJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendNFS)
	if err != nil {
		t.Fatalf("ConvertOverlay: %v", err)
	}
	if got.OverlayKind != "nfs" || storedKind(t, m, a.ID) != "nfs" {
		t.Fatalf("row not flipped: returned=%s stored=%s", got.OverlayKind, storedKind(t, m, a.ID))
	}
	if gotCache := readFileT(t, filepath.Join(priv, "mcp-needs-auth-cache.json")); gotCache != authCacheJSON {
		t.Fatalf("mcp-needs-auth-cache.json in private root = %q, want %q", gotCache, authCacheJSON)
	}
	if _, err := os.Lstat(filepath.Join(dir, "mcp-needs-auth-cache.json")); !os.IsNotExist(err) {
		t.Fatal("mcp-needs-auth-cache.json left in the account dir: copied (or shadowed under the mount), not moved")
	}
}

// TestConvertToFuseRemovesStaleMcpNeedsAuthCacheSymlink pins the not-yet-drifted
// common case: an account overlaid before mcp-needs-auth-cache.json was
// classified private holds a shared symlink at the name (the base owns the real
// file). The symlink→fuse conversion must succeed, remove the stale link
// outright — never leave it to dangle or shadow under the mountpoint — and
// leave the shared base copy in place, never pulled into the account's private
// root.
func TestConvertToFuseRemovesStaleMcpNeedsAuthCacheSymlink(t *testing.T) {
	const baseCacheJSON = `{"shared-server":{"timestamp":456}}`
	ops := []string{}
	fake := &fakeFuse{ops: &ops}
	m, a, dir := newConvertFixture(t, fake)
	base := ClaudeDir()
	priv := fkoverlay.FusePrivateRoot(dir)
	if err := os.WriteFile(filepath.Join(base, "mcp-needs-auth-cache.json"), []byte(baseCacheJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	// Pre-classification accounts linked the name like any shared entry; today's
	// Sync refuses to lay this link, so seed the legacy state by hand.
	if err := os.Symlink(filepath.Join(base, "mcp-needs-auth-cache.json"), filepath.Join(dir, "mcp-needs-auth-cache.json")); err != nil {
		t.Fatal(err)
	}

	got, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendNFS)
	if err != nil {
		t.Fatalf("ConvertOverlay with a stale shared link: %v", err)
	}
	if got.OverlayKind != "nfs" || storedKind(t, m, a.ID) != "nfs" {
		t.Fatalf("row not flipped: returned=%s stored=%s", got.OverlayKind, storedKind(t, m, a.ID))
	}
	if _, err := os.Lstat(filepath.Join(dir, "mcp-needs-auth-cache.json")); !os.IsNotExist(err) {
		t.Fatal("stale shared link survived the conversion")
	}
	if _, err := os.Lstat(filepath.Join(priv, "mcp-needs-auth-cache.json")); !os.IsNotExist(err) {
		t.Fatal("the base's shared copy was pulled into the private root")
	}
	if gotBase := readFileT(t, filepath.Join(base, "mcp-needs-auth-cache.json")); gotBase != baseCacheJSON {
		t.Fatalf("shared base copy disturbed: %q", gotBase)
	}
	if _, err := os.Lstat(filepath.Join(dir, "projects")); !os.IsNotExist(err) {
		t.Fatal("shared symlink survived conversion")
	}
}

func TestConvertToFuseSetupFailureRollsBack(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops, setupErr: errors.New("grant Network Volumes access")}
	m, a, dir := newConvertFixture(t, fake)
	priv := fkoverlay.FusePrivateRoot(dir)

	_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendNFS)
	if err == nil || !strings.Contains(err.Error(), "rolled back to symlink") {
		t.Fatalf("error = %v, want rollback report", err)
	}
	if !strings.Contains(err.Error(), "grant Network Volumes access") {
		t.Fatalf("error %v does not carry the mount cause", err)
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity not restored: %q", got)
	}
	if got := readFileT(t, filepath.Join(dir, "backups", "b.bak")); got != "bak" {
		t.Fatalf("backups not restored: %q", got)
	}
	if _, err := os.Readlink(filepath.Join(dir, "projects")); err != nil {
		t.Fatalf("symlink overlay not re-asserted: %v", err)
	}
	if storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row flipped despite failed mount")
	}
	if has, _ := fkoverlay.HasPrivateEntries(priv, testSpec()); has {
		t.Fatal("private files stranded in backing dir after rollback")
	}
}

func TestConvertToFuseUnmountFailureAbortsRollback(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops, setupErr: errors.New("mount timed out"), teardownErr: errors.New("still mounted")}
	m, a, dir := newConvertFixture(t, fake)
	priv := fkoverlay.FusePrivateRoot(dir)

	_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendNFS)
	if err == nil || !strings.Contains(err.Error(), "mount timed out") || !strings.Contains(err.Error(), "still mounted") {
		t.Fatalf("error = %v, want both faults reported", err)
	}
	// Rollback aborted: no symlink re-setup over a possibly-live mount; identity stays in the private dir.
	if _, err := os.Lstat(filepath.Join(dir, "projects")); !os.IsNotExist(err) {
		t.Fatal("symlinks were laid despite a failed unmount")
	}
	if got := readFileT(t, filepath.Join(priv, ".claude.json")); got != identityJSON {
		t.Fatalf("identity not preserved in private root: %q", got)
	}
	if storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row flipped despite failed mount")
	}
}

// TestConvertToFuseIdentityMismatchRollsBack pins the verify's mismatch arm:
// a private root holding an identity other than the one the conversion moved
// rolls back to symlink, keeps the row on symlink, and preserves the divergent
// bytes for inspection instead of destroying them.
func TestConvertToFuseIdentityMismatchRollsBack(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops, wrongIdentity: true}
	m, a, dir := newConvertFixture(t, fake)

	_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendNFS)
	if err == nil || !strings.Contains(err.Error(), "identity in private root is u-IMPOSTOR, expected u-1") {
		t.Fatalf("error = %v, want the private-root identity mismatch", err)
	}
	if !strings.Contains(err.Error(), "rolled back to symlink") {
		t.Fatalf("error = %v, want rollback report", err)
	}
	// The rollback moves the divergent file back rather than deleting data.
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != wrongIdentityJSON {
		t.Fatalf("divergent identity not preserved through rollback: %q", got)
	}
	if _, err := os.Readlink(filepath.Join(dir, "projects")); err != nil {
		t.Fatalf("symlink overlay not re-asserted: %v", err)
	}
	if storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row flipped despite identity mismatch")
	}
}

func TestConvertToSymlink(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops}
	m, a, dir := newConvertFixture(t, fake)
	priv := fkoverlay.FusePrivateRoot(dir)

	fwd, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendNFS)
	if err != nil {
		t.Fatalf("forward convert: %v", err)
	}
	back, err := m.ConvertOverlay(t.Context(), fwd, fkoverlay.BackendSymlink)
	if err != nil {
		t.Fatalf("reverse convert: %v", err)
	}
	if back.OverlayKind != "symlink" || storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row not flipped back")
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity not restored: %q", got)
	}
	if got := readFileT(t, filepath.Join(dir, "backups", "b.bak")); got != "bak" {
		t.Fatalf("backups not restored: %q", got)
	}
	if _, err := os.Readlink(filepath.Join(dir, "projects")); err != nil {
		t.Fatalf("symlink overlay not re-asserted: %v", err)
	}
	if _, err := os.Lstat(priv); !os.IsNotExist(err) {
		t.Fatal("emptied private root not removed")
	}
}

func TestConvertToSymlinkAbortsOnFailedUnmount(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops}
	m, a, _ := newConvertFixture(t, fake)
	fwd, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendNFS)
	if err != nil {
		t.Fatalf("forward convert: %v", err)
	}
	fake.teardownErr = errors.New("still mounted")
	_, err = m.ConvertOverlay(t.Context(), fwd, fkoverlay.BackendSymlink)
	if err == nil || !strings.Contains(err.Error(), "still mounted") {
		t.Fatalf("error = %v, want unmount failure", err)
	}
	if storedKind(t, m, a.ID) != "nfs" {
		t.Fatal("row flipped despite failed unmount")
	}
}

// TestConvertToSymlinkSweepsSharedOrphans: a force-unmounted fuse mirror leaves real orphans at shared names (projects/, history.jsonl); the retreat must relocate them into base (merging), re-link, and never leak the account's private identity into base.
func TestConvertToSymlinkSweepsSharedOrphans(t *testing.T) {
	m, a, dir := newConvertFixture(t, nil)
	base := ClaudeDir()
	priv := fkoverlay.FusePrivateRoot(dir)

	// Fuse rest state.
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, ".claude.json"), filepath.Join(priv, ".claude.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "backups"), filepath.Join(priv, "backups")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "projects")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "projects", "existing.json"), []byte("base-side"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "projects"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "projects", "p.json"), []byte("orphan-session"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "history.jsonl"), []byte("orphan-history"), 0o600); err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	if err := m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	back, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendSymlink)
	if err != nil {
		t.Fatalf("retreat with shared orphans: %v", err)
	}
	if back.OverlayKind != "symlink" || storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row not flipped to symlink")
	}
	if got := readFileT(t, filepath.Join(base, "projects", "p.json")); got != "orphan-session" {
		t.Fatalf("orphan project not swept into base: %q", got)
	}
	if got := readFileT(t, filepath.Join(base, "projects", "existing.json")); got != "base-side" {
		t.Fatalf("pre-existing base project disturbed: %q", got)
	}
	if got := readFileT(t, filepath.Join(base, "history.jsonl")); got != "orphan-history" {
		t.Fatalf("orphan history not swept into base: %q", got)
	}
	if target, err := os.Readlink(filepath.Join(dir, "projects")); err != nil || target != filepath.Join(base, "projects") {
		t.Fatalf("projects not re-linked into base: target=%q err=%v", target, err)
	}
	if _, err := os.Readlink(filepath.Join(dir, "history.jsonl")); err != nil {
		t.Fatalf("history.jsonl not re-linked into base: %v", err)
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity not restored: %q", got)
	}
	if _, err := os.Lstat(filepath.Join(base, ".claude.json")); !os.IsNotExist(err) {
		t.Fatal("account identity leaked into the shared base")
	}
	if got := readFileT(t, filepath.Join(dir, "backups", "b.bak")); got != "bak" {
		t.Fatalf("backups not restored: %q", got)
	}
	if _, err := os.Lstat(priv); !os.IsNotExist(err) {
		t.Fatal("emptied private root not removed")
	}
}

// TestRollbackToSymlinkSweepsSharedOrphans pins the rollback call site: a failed fuse Setup that left an orphan at a shared name must still roll back cleanly (orphan swept into base, links re-laid).
func TestRollbackToSymlinkSweepsSharedOrphans(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{
		ops:      &ops,
		setupErr: errors.New("mount timed out"),
		onSetup: func(dir string) {
			// claude touched the bare mountpoint before the mount came up.
			_ = os.WriteFile(filepath.Join(dir, "history.jsonl"), []byte("orphan"), 0o600)
		},
	}
	m, a, dir := newConvertFixture(t, fake)
	base := ClaudeDir()

	_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendNFS)
	if err == nil || !strings.Contains(err.Error(), "rolled back to symlink") {
		t.Fatalf("error = %v, want rollback report", err)
	}
	if storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row flipped despite failed mount")
	}
	if got := readFileT(t, filepath.Join(base, "history.jsonl")); got != "orphan" {
		t.Fatalf("orphan not swept into base during rollback: %q", got)
	}
	if _, err := os.Readlink(filepath.Join(dir, "history.jsonl")); err != nil {
		t.Fatalf("history.jsonl not re-linked after rollback: %v", err)
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity not restored after rollback: %q", got)
	}
}

func TestHealStrandedPrivate(t *testing.T) {
	m, a, dir := newConvertFixture(t, nil)
	priv := fkoverlay.FusePrivateRoot(dir)

	healed, err := m.HealStrandedPrivate(a)
	if err != nil || healed {
		t.Fatalf("clean account: healed=%v err=%v, want false,nil", healed, err)
	}

	// Strand the identity: a conversion that died before rollback finished.
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, ".claude.json"), filepath.Join(priv, ".claude.json")); err != nil {
		t.Fatal(err)
	}
	healed, err = m.HealStrandedPrivate(a)
	if err != nil || !healed {
		t.Fatalf("healed=%v err=%v, want true,nil", healed, err)
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity not restored: %q", got)
	}
	if _, err := os.Readlink(filepath.Join(dir, "projects")); err != nil {
		t.Fatalf("symlink overlay not re-asserted: %v", err)
	}
	if _, err := os.Lstat(priv); !os.IsNotExist(err) {
		t.Fatal("emptied private root not removed")
	}

	healed, err = m.HealStrandedPrivate(a)
	if err != nil || healed {
		t.Fatalf("re-heal: healed=%v err=%v, want false,nil", healed, err)
	}

	// Misuse: healing a fuse-kind account is a programmer error.
	a.OverlayKind = "nfs"
	if _, err := m.HealStrandedPrivate(a); err == nil {
		t.Fatal("healing a fuse account did not error")
	}
}

// TestConvertRetreatThenLaunchMergePropagatesBase pins the migrate↔merge interplay across a fuse→symlink retreat: the launch merge stays out while the row says fuse, then propagates a fresh base key into the moved-back file with the identity byte-identical.
func TestConvertRetreatThenLaunchMergePropagatesBase(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops}
	m, a, dir := newConvertFixture(t, fake)
	if err := os.WriteFile(ClaudeJSONPath(),
		[]byte(`{"theme":"light","freshKey":true,"oauthAccount":{"accountUuid":"base-own"}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	fwd, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendNFS)
	if err != nil {
		t.Fatalf("forward convert: %v", err)
	}
	out, err := m.MergeBaseClaudeJSON(fwd)
	if err != nil || out != MergeSkippedOverlay {
		t.Fatalf("merge against the fuse row: outcome=%q err=%v, want %q", out, err, MergeSkippedOverlay)
	}

	back, err := m.ConvertOverlay(t.Context(), fwd, fkoverlay.BackendSymlink)
	if err != nil {
		t.Fatalf("retreat: %v", err)
	}
	out, err = m.MergeBaseClaudeJSON(back)
	if err != nil || out != MergeApplied {
		t.Fatalf("launch merge after retreat: outcome=%q err=%v, want %q", out, err, MergeApplied)
	}
	got := rawTop(t, readFile(t, filepath.Join(dir, ".claude.json")))
	if string(got["freshKey"]) != `true` || string(got["theme"]) != `"light"` {
		t.Fatalf("fresh base keys did not reach the moved-back file: freshKey=%s theme=%s", got["freshKey"], got["theme"])
	}
	if string(got["oauthAccount"]) != `{"accountUuid":"u-1","emailAddress":"a@example.com"}` {
		t.Fatalf("identity disturbed by the launch merge: %s", got["oauthAccount"])
	}
}

// TestStrandedPrivateMergeRefusalKeepsHealable: with .claude.json stranded in the private backing dir, the launch merge errors without minting a file, so the later heal meets no collision and the next merge converges.
func TestStrandedPrivateMergeRefusalKeepsHealable(t *testing.T) {
	m, a, dir := newConvertFixture(t, nil)
	priv := fkoverlay.FusePrivateRoot(dir)
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, ".claude.json"), filepath.Join(priv, ".claude.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ClaudeJSONPath(), []byte(`{"theme":"light"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := m.MergeBaseClaudeJSON(a); err == nil || !strings.Contains(err.Error(), "ccp doctor") {
		t.Fatalf("merge with a stranded copy = %v, want the ccp doctor refusal", err)
	}
	if _, err := os.Lstat(filepath.Join(dir, ".claude.json")); !os.IsNotExist(err) {
		t.Fatal("the refused merge minted a file over the heal's restore target")
	}

	healed, err := m.HealStrandedPrivate(a)
	if err != nil || !healed {
		t.Fatalf("heal after refused merge: healed=%v err=%v, want true,nil", healed, err)
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("healed file = %q, want the stranded identity %q", got, identityJSON)
	}

	out, err := m.MergeBaseClaudeJSON(a)
	if err != nil || out != MergeApplied {
		t.Fatalf("launch merge after heal: outcome=%q err=%v, want %q", out, err, MergeApplied)
	}
	got := rawTop(t, readFile(t, filepath.Join(dir, ".claude.json")))
	if string(got["theme"]) != `"light"` {
		t.Fatalf("base key did not reach the healed file: %s", got["theme"])
	}
	if string(got["oauthAccount"]) != `{"accountUuid":"u-1","emailAddress":"a@example.com"}` {
		t.Fatalf("identity disturbed by the post-heal merge: %s", got["oauthAccount"])
	}
}

// TestHealResolvesDuplicatePrivateFile: the same private file in both the account dir and the private backing dir resolves last-write-wins (newer copy survives), the heal converges, and the resolution is reported through the overlay seam.
func TestHealResolvesDuplicatePrivateFile(t *testing.T) {
	m, a, dir := newConvertFixture(t, nil)
	priv := fkoverlay.FusePrivateRoot(dir)
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	// The stranded backing copy is newer.
	base := time.Now()
	if err := os.WriteFile(filepath.Join(dir, ".last-update-result.json"), []byte("stale-in-dir"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(dir, ".last-update-result.json"), base.Add(-time.Hour), base.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(priv, ".last-update-result.json"), []byte("fresh-from-priv"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(priv, ".last-update-result.json"), base, base); err != nil {
		t.Fatal(err)
	}

	var resolved []string
	prev := fkoverlay.ResolvedConflictLogf
	fkoverlay.ResolvedConflictLogf = func(format string, args ...any) {
		resolved = append(resolved, fmt.Sprintf(format, args...))
	}
	defer func() { fkoverlay.ResolvedConflictLogf = prev }()

	healed, err := m.HealStrandedPrivate(a)
	if err != nil || !healed {
		t.Fatalf("heal with a duplicate private file: healed=%v err=%v, want true,nil", healed, err)
	}
	got, err := os.ReadFile(filepath.Join(dir, ".last-update-result.json")) //nolint:gosec // G304: dir is under the test's own t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "fresh-from-priv" {
		t.Fatalf("healed file = %q, want the newer backing copy", got)
	}
	if _, err := os.Stat(priv); !os.IsNotExist(err) {
		t.Errorf("emptied private root not removed: %v", err)
	}
	if len(resolved) != 1 || !strings.Contains(resolved[0], "kept newer copy") {
		t.Errorf("resolution log = %v, want one 'kept newer copy'", resolved)
	}
}

func TestSetDefaultOverlayKind(t *testing.T) {
	st := openTestStore(t)
	m := &Manager{Store: st}

	if err := m.SetDefaultOverlayKind(fkoverlay.BackendSymlink); err != nil {
		t.Fatalf("set symlink default: %v", err)
	}
	v, ok, err := st.GetMeta("overlay_kind")
	if err != nil || !ok || v != "symlink" {
		t.Fatalf("meta = %q ok=%v err=%v", v, ok, err)
	}

	if err := m.SetDefaultOverlayKind("zfs"); err == nil {
		t.Fatal("unknown kind accepted")
	}

	// The fuse fence keys on hosting capability, not provider kind: RemoteProvider always reports KindFuse.
	m.CanHostFuse = func() bool { return false }
	if err := m.SetDefaultOverlayKind(fkoverlay.BackendNFS); !errors.Is(err, ErrConvertUnsupported) {
		t.Fatalf("fuse default without fuse hosting = %v, want ErrConvertUnsupported", err)
	}
	if v, _, _ := st.GetMeta("overlay_kind"); v != "symlink" {
		t.Fatalf("refused default rewrote meta to %q", v)
	}

	m.CanHostFuse = func() bool { return true }
	if err := m.SetDefaultOverlayKind(fkoverlay.BackendNFS); err != nil {
		t.Fatalf("fuse default with fuse hosting: %v", err)
	}
	if v, _, _ := st.GetMeta("overlay_kind"); v != "nfs" {
		t.Fatalf("meta = %q, want fuse", v)
	}

	// Unseamed, the fence is this build's real capability.
	m.CanHostFuse = nil
	err = m.SetDefaultOverlayKind(fkoverlay.BackendNFS)
	if CanHostFuse() {
		if err != nil {
			t.Fatalf("fuse default refused in a fuse build: %v", err)
		}
	} else if !errors.Is(err, ErrConvertUnsupported) {
		t.Fatalf("fuse default in pure build = %v, want ErrConvertUnsupported", err)
	}
}

// hookedSymlink wraps the real symlink provider, running preTeardown before
// Teardown; a non-nil hook error replaces the real teardown. It injects faults
// (or a mid-window cancellation) into the strand window between
// MovePrivateEntries and SetAccountOverlayKind.
type hookedSymlink struct {
	*fkoverlay.SymlinkProvider
	preTeardown func() error
}

func (h *hookedSymlink) Teardown(base, dir string) error {
	if h.preTeardown != nil {
		if err := h.preTeardown(); err != nil {
			return err
		}
	}
	return h.SymlinkProvider.Teardown(base, dir)
}

// TestConvertToFuseCancelledBeforeMoveAbortsCleanly: a spent budget observed
// before anything moved is a clean abort — no rollback machinery, no provider
// calls, account untouched.
func TestConvertToFuseCancelledBeforeMoveAbortsCleanly(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops}
	m, a, dir := newConvertFixture(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := m.ConvertOverlay(ctx, a, fkoverlay.BackendNFS)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if strings.Contains(err.Error(), "rolled back") {
		t.Fatalf("clean pre-move abort took the rollback path: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("providers touched on a pre-move abort: ops = %v", ops)
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity disturbed by a pre-move abort: %q", got)
	}
	if _, err := os.Readlink(filepath.Join(dir, "projects")); err != nil {
		t.Fatalf("symlink overlay disturbed: %v", err)
	}
	if _, err := os.Lstat(fkoverlay.FusePrivateRoot(dir)); !os.IsNotExist(err) {
		t.Fatal("a pre-move abort minted a private root")
	}
	if storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row changed by a pre-move abort")
	}
}

// TestConvertToFuseCancelledMidWindowRollsBackWithoutMounting: a cancellation
// landing inside the strand window (private files already in priv) must roll
// back to symlink and must NOT start a mount it has no time to verify.
func TestConvertToFuseCancelledMidWindowRollsBackWithoutMounting(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops}
	m, a, dir := newConvertFixture(t, fake)
	priv := fkoverlay.FusePrivateRoot(dir)
	ctx, cancel := context.WithCancel(context.Background())
	m.OverlayFor = func(b fkoverlay.Backend) (fkoverlay.Provider, error) {
		if b.IsFuse() {
			return fake, nil
		}
		// The daemon shuts down while the symlink teardown runs.
		return &hookedSymlink{SymlinkProvider: newSymlinkProvider(), preTeardown: func() error { cancel(); return nil }}, nil
	}

	_, err := m.ConvertOverlay(ctx, a, fkoverlay.BackendNFS)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled as the cause", err)
	}
	if !strings.Contains(err.Error(), "rolled back to symlink") {
		t.Fatalf("error = %v, want rollback report", err)
	}
	for _, op := range ops {
		if strings.HasPrefix(op, "fuse.setup") {
			t.Fatalf("a spent budget started a mount anyway: ops = %v", ops)
		}
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity not restored: %q", got)
	}
	if _, err := os.Readlink(filepath.Join(dir, "projects")); err != nil {
		t.Fatalf("symlink overlay not re-asserted: %v", err)
	}
	if _, err := os.Lstat(priv); !os.IsNotExist(err) {
		t.Fatal("private root stranded after the rollback")
	}
	if storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row flipped despite the cancelled conversion")
	}
}

// TestConvertToFuseTeardownFailureRollsBack pins the strand window's teardown
// arm: a failed symlink teardown used to return with private files stranded in
// priv (row still symlink, recovery only via HealStrandedPrivate); it must now
// roll back in place.
func TestConvertToFuseTeardownFailureRollsBack(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops}
	m, a, dir := newConvertFixture(t, fake)
	priv := fkoverlay.FusePrivateRoot(dir)
	m.OverlayFor = func(b fkoverlay.Backend) (fkoverlay.Provider, error) {
		if b.IsFuse() {
			return fake, nil
		}
		return &hookedSymlink{SymlinkProvider: newSymlinkProvider(), preTeardown: func() error { return errors.New("unlink exploded") }}, nil
	}

	_, err := m.ConvertOverlay(context.Background(), a, fkoverlay.BackendNFS)
	if err == nil || !strings.Contains(err.Error(), "tear down symlinks") || !strings.Contains(err.Error(), "unlink exploded") {
		t.Fatalf("error = %v, want the teardown cause", err)
	}
	if !strings.Contains(err.Error(), "rolled back to symlink") {
		t.Fatalf("error = %v, want rollback report — a teardown failure must not strand", err)
	}
	for _, op := range ops {
		if strings.HasPrefix(op, "fuse.setup") {
			t.Fatalf("mounted despite a failed symlink teardown: ops = %v", ops)
		}
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity not restored: %q", got)
	}
	if has, herr := fkoverlay.HasPrivateEntries(priv, testSpec()); herr != nil || has {
		t.Fatalf("private files stranded in %s after rollback (has=%v err=%v)", priv, has, herr)
	}
	if _, err := os.Readlink(filepath.Join(dir, "projects")); err != nil {
		t.Fatalf("symlink overlay not re-asserted: %v", err)
	}
	if storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row flipped despite the failed conversion")
	}
}

// TestConvertToSymlinkIgnoresAndCleansAppleDoubleLitter: "._*" AppleDouble
// sidecars (pre-mitigation fuse litter) are never linked, moved, or swept into
// the shared base, and litter in the private root no longer blocks its
// removal; an unclassified ".foo" and a real shared orphan behave exactly as
// before.
func TestConvertToSymlinkIgnoresAndCleansAppleDoubleLitter(t *testing.T) {
	m, a, dir := newConvertFixture(t, nil)
	base := ClaudeDir()
	priv := fkoverlay.FusePrivateRoot(dir)

	// Fuse rest state with litter everywhere a pre-mitigation mount left it.
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, ".claude.json"), filepath.Join(priv, ".claude.json")); err != nil {
		t.Fatal(err)
	}
	for path, content := range map[string]string{
		filepath.Join(priv, "._.claude.json"): "sidecar-priv",
		filepath.Join(dir, "._history.jsonl"): "sidecar-dir",
		filepath.Join(dir, "history.jsonl"):   "orphan-history", // real shared orphan (no link at this name)
		filepath.Join(dir, ".foo"):            "unclassified",
	} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	a.OverlayKind = "nfs"
	if err := m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	back, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendSymlink)
	if err != nil {
		t.Fatalf("retreat with AppleDouble litter: %v", err)
	}
	if back.OverlayKind != "symlink" || storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row not flipped to symlink")
	}
	// Litter never reaches the shared base.
	for _, name := range []string{"._history.jsonl", "._.claude.json"} {
		if _, err := os.Lstat(filepath.Join(base, name)); !os.IsNotExist(err) {
			t.Fatalf("AppleDouble litter %q swept into the shared base", name)
		}
	}
	// The '._'-littered private root no longer survives conversion.
	if _, err := os.Lstat(priv); !os.IsNotExist(err) {
		t.Fatal("littered private root survived the conversion")
	}
	// Litter in the account dir is ignored: still the plain file, never linked.
	if fi, err := os.Lstat(filepath.Join(dir, "._history.jsonl")); err != nil || fi.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("dir litter disturbed: fi=%v err=%v, want the untouched plain file", fi, err)
	}
	// Real orphans and unclassified dotfiles keep their old behavior.
	if got := readFileT(t, filepath.Join(base, "history.jsonl")); got != "orphan-history" {
		t.Fatalf("real orphan not swept into base: %q", got)
	}
	if got := readFileT(t, filepath.Join(base, ".foo")); got != "unclassified" {
		t.Fatalf(".foo (non-matching prefix) no longer sweeps as a shared orphan: %q", got)
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity not restored: %q", got)
	}
}

// TestConvertToSymlinkLeavesUnclassifiedPrivateRootEntries: only skip litter
// is cleared from the private root; anything unclassified (".foo") keeps the
// dir alive — its contents are data deletion could destroy.
func TestConvertToSymlinkLeavesUnclassifiedPrivateRootEntries(t *testing.T) {
	m, a, dir := newConvertFixture(t, nil)
	priv := fkoverlay.FusePrivateRoot(dir)
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, ".claude.json"), filepath.Join(priv, ".claude.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(priv, ".foo"), []byte("keep-me"), 0o600); err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	if err := m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	if _, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendSymlink); err != nil {
		t.Fatalf("retreat: %v", err)
	}
	if got := readFileT(t, filepath.Join(priv, ".foo")); got != "keep-me" {
		t.Fatalf("unclassified private-root entry destroyed: %q", got)
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity not restored: %q", got)
	}
}

// TestRollbackToSymlinkClearsAppleDoubleLitter: a failed mount whose fuse
// setup littered the backing dir with skip litter still rolls back to a clean
// symlink account — litter cleared with the private root, never moved into the
// shared base or the account dir. The .DS_Store is load-bearing: macOS
// rmdir(2) silently deletes orphaned AppleDouble "._*" entries, so a lone
// sidecar would vanish with the dir even if the spec-driven clearing in
// removePrivateRootIfEmpty were deleted; .DS_Store gets no such kernel help,
// so priv only goes away when the clearing runs. (The SkipPrefixes
// classification itself is pinned by TestOverlaySpecSkipsAppleDoubleLitter
// and TestConvertToSymlinkIgnoresAndCleansAppleDoubleLitter.)
func TestRollbackToSymlinkClearsAppleDoubleLitter(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{
		ops:      &ops,
		setupErr: errors.New("mount timed out"),
		onSetup: func(dir string) {
			// A pre-mitigation holder wrote an AppleDouble sidecar and Finder's
			// .DS_Store into the backing dir.
			priv := fkoverlay.FusePrivateRoot(dir)
			_ = os.WriteFile(filepath.Join(priv, "._.claude.json"), []byte("sidecar"), 0o600)
			_ = os.WriteFile(filepath.Join(priv, ".DS_Store"), []byte("finder"), 0o600)
		},
	}
	m, a, dir := newConvertFixture(t, fake)
	base := ClaudeDir()
	priv := fkoverlay.FusePrivateRoot(dir)

	_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendNFS)
	if err == nil || !strings.Contains(err.Error(), "rolled back to symlink") {
		t.Fatalf("error = %v, want rollback report", err)
	}
	if _, err := os.Lstat(priv); !os.IsNotExist(err) {
		t.Fatal("littered private root survived the rollback")
	}
	for _, name := range []string{"._.claude.json", ".DS_Store"} {
		if _, err := os.Lstat(filepath.Join(base, name)); !os.IsNotExist(err) {
			t.Fatalf("skip litter %q swept into the shared base during rollback", name)
		}
		if _, err := os.Lstat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("skip litter %q moved back into the account dir during rollback", name)
		}
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity not restored: %q", got)
	}
	if storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row flipped despite failed mount")
	}
}

func readFileT(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is under the test's own t.TempDir()
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}
