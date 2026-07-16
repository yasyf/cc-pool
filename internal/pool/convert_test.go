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

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/fileproviderd"
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

func (f *fakeFuse) Backend() fkoverlay.Backend                  { return fkoverlay.BackendNFS }
func (f *fakeFuse) Check(context.Context, string, string) error { return nil }
func (f *fakeFuse) PrivateRoot(dir string) string               { return fkoverlay.FusePrivateRoot(dir) }
func (f *fakeFuse) Reconcile(_ context.Context, _, dir string) error {
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

func (f *fakeFuse) Teardown(context.Context, string, string) (string, error) {
	*f.ops = append(*f.ops, "fuse.teardown")
	if f.teardownErr != nil {
		return "", f.teardownErr
	}
	if f.created != "" {
		_ = os.Remove(f.created)
		f.created = ""
	}
	return "", nil
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
	if err := newSymlinkProvider().Reconcile(t.Context(), base, dir); err != nil {
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
	// A fake fuse provider: the real mux provider's Teardown would refuse the
	// real-dir rest state this test models (the mux world's rest state is a bridge
	// symlink; TestConvertRoundTripWithBridgeSymlinks covers that). Here the fuse
	// teardown is a no-op, exactly the "nothing mounted" case, so the retreat is
	// pure file moves — what this test pins.
	ops := []string{}
	m, a, dir := newConvertFixture(t, &fakeFuse{ops: &ops})
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
	ops := []string{}
	m, a, dir := newConvertFixture(t, &fakeFuse{ops: &ops})
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

	// A fileprovider row's private root is in active use exactly like a fuse
	// row's — never fold FP into the "not fuse" arm.
	a.OverlayKind = "fileprovider"
	if _, err := m.HealStrandedPrivate(a); err == nil {
		t.Fatal("healing a fileprovider account did not error")
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

	// fileprovider is recorded as-is: availability is gated at the migrate
	// entry points (daemon fpGate, CLI precheck), not here.
	if err := m.SetDefaultOverlayKind(fkoverlay.BackendFileProvider); err != nil {
		t.Fatalf("set fileprovider default: %v", err)
	}
	if v, _, _ := st.GetMeta("overlay_kind"); v != "fileprovider" {
		t.Fatalf("meta = %q, want fileprovider", v)
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

func (h *hookedSymlink) Teardown(ctx context.Context, base, dir string) (string, error) {
	if h.preTeardown != nil {
		if err := h.preTeardown(); err != nil {
			return "", err
		}
	}
	return h.SymlinkProvider.Teardown(ctx, base, dir)
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
	ops := []string{}
	m, a, dir := newConvertFixture(t, &fakeFuse{ops: &ops})
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
	ops := []string{}
	m, a, dir := newConvertFixture(t, &fakeFuse{ops: &ops})
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

// replaceWithBridge swaps an account dir for a mux bridge symlink into the shared
// mux root — the on-disk shape of a fuse-mux account (IsBridgeSymlink reads true).
func replaceWithBridge(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(MuxRootDir(), filepath.Base(dir)), dir); err != nil {
		t.Fatal(err)
	}
}

// replaceWithFPBridge swaps an account dir for a File Provider domain bridge
// symlink — a symlink into an OS-surfaced domain root, NOT the mux root, so
// IsBridgeSymlink reads FALSE. This is the exact wreckage a crashed
// symlink→fileprovider convert left behind (row=symlink, dir=FP bridge) that
// IsBridgeSymlink missed and the broader requireRealDir guard must catch. Returns
// the link target.
func replaceWithFPBridge(t *testing.T, dir string) string {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "CCPoolStatus-"+filepath.Base(dir))
	if err := os.Symlink(target, dir); err != nil {
		t.Fatal(err)
	}
	return target
}

// muxSetupSim mirrors the fuse provider's setupMux: an empty account dir is
// replaced by the bridge symlink, a non-empty one is refused (ErrAccountDirOccupied),
// and an existing symlink is left in place.
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
	return os.Symlink(filepath.Join(MuxRootDir(), filepath.Base(dir)), dir)
}

// bridgeFuse is a fuse provider fake that models the mux bridge on disk: Setup
// lays the bridge symlink (muxSetupSim), Teardown removes it. It lets the
// convert round-trip run through the real file-move orchestration without a holder.
type bridgeFuse struct {
	ops      *[]string
	setupErr error
	onSetup  func(dir string) // fault injection inside Setup, before setupErr
}

func (b *bridgeFuse) Backend() fkoverlay.Backend                  { return fkoverlay.BackendNFS }
func (b *bridgeFuse) Check(context.Context, string, string) error { return nil }
func (b *bridgeFuse) PrivateRoot(dir string) string               { return fkoverlay.FusePrivateRoot(dir) }

func (b *bridgeFuse) Reconcile(_ context.Context, base, dir string) error {
	*b.ops = append(*b.ops, "setup")
	if b.onSetup != nil {
		b.onSetup(dir)
	}
	if b.setupErr != nil {
		return b.setupErr
	}
	return muxSetupSim(base, dir)
}

func (b *bridgeFuse) Teardown(_ context.Context, _, dir string) (string, error) {
	*b.ops = append(*b.ops, "teardown")
	fi, err := os.Lstat(dir)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if fi.Mode()&os.ModeSymlink == 0 {
		return "", fmt.Errorf("refusing to remove %s: not a symlink", dir)
	}
	return "", os.Remove(dir)
}

// TestConvertToFuseRefusesOverlaySymlink is the R2 regression pin for convertToFuse:
// a symlink row whose dir is unexpectedly a symlink — a mux bridge OR the
// FP-bridge shape IsBridgeSymlink missed — must refuse with ErrDirIsOverlaySymlink,
// name the link target, and touch no provider; moving files through it
// (MovePrivateEntries) would write into the live mirror/domain.
func TestConvertToFuseRefusesOverlaySymlink(t *testing.T) {
	for _, shape := range []string{"mux-bridge", "fp-bridge"} {
		t.Run(shape, func(t *testing.T) {
			ops := []string{}
			m, a, dir := newConvertFixture(t, &fakeFuse{ops: &ops})
			var target string
			if shape == "mux-bridge" {
				replaceWithBridge(t, dir)
				target = filepath.Join(MuxRootDir(), filepath.Base(dir))
			} else {
				target = replaceWithFPBridge(t, dir)
			}

			_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendNFS)
			if !errors.Is(err, ErrDirIsOverlaySymlink) {
				t.Fatalf("convertToFuse over a %s = %v, want errors.Is ErrDirIsOverlaySymlink", shape, err)
			}
			if !strings.Contains(err.Error(), target) {
				t.Fatalf("refusal %q does not name the link target %q", err, target)
			}
			if len(ops) != 0 {
				t.Fatalf("refused convert still touched the fuse provider: %v", ops)
			}
			if storedKind(t, m, a.ID) != "symlink" {
				t.Fatal("row flipped despite a refused convert")
			}
		})
	}
}

// TestHealStrandedPrivateRefusesOverlaySymlink is the R2 regression pin for the
// heal path: a symlink row with stranded private files whose dir is a symlink — a
// mux bridge OR the FP-bridge shape IsBridgeSymlink missed — must refuse with
// ErrDirIsOverlaySymlink and leave the stranded copy intact for `ccp doctor`;
// moving the files back through it would corrupt the mirror/domain.
func TestHealStrandedPrivateRefusesOverlaySymlink(t *testing.T) {
	for _, shape := range []string{"mux-bridge", "fp-bridge"} {
		t.Run(shape, func(t *testing.T) {
			m, a, dir := newConvertFixture(t, nil)
			priv := fkoverlay.FusePrivateRoot(dir)
			if err := os.MkdirAll(priv, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(filepath.Join(dir, ".claude.json"), filepath.Join(priv, ".claude.json")); err != nil {
				t.Fatal(err)
			}
			var target string
			if shape == "mux-bridge" {
				replaceWithBridge(t, dir)
				target = filepath.Join(MuxRootDir(), filepath.Base(dir))
			} else {
				target = replaceWithFPBridge(t, dir)
			}

			healed, err := m.HealStrandedPrivate(a)
			if healed || !errors.Is(err, ErrDirIsOverlaySymlink) {
				t.Fatalf("HealStrandedPrivate over a %s = (%v, %v), want errors.Is ErrDirIsOverlaySymlink", shape, healed, err)
			}
			if !strings.Contains(err.Error(), target) || !strings.Contains(err.Error(), "ccp doctor") {
				t.Fatalf("refusal %q must name the target %q and `ccp doctor`", err, target)
			}
			if got := readFileT(t, filepath.Join(priv, ".claude.json")); got != identityJSON {
				t.Fatalf("stranded identity moved through the mirror: %q", got)
			}
		})
	}
}

// TestConvertRoundTripBridgeSymlink pins the mux round-trip: symlink→fuse lays the
// bridge symlink and moves private files to the backing root; fuse→symlink removes
// the bridge and restores a clean symlink account with its identity intact.
func TestConvertRoundTripBridgeSymlink(t *testing.T) {
	ops := []string{}
	fake := &bridgeFuse{ops: &ops}
	m, a, dir := newConvertFixture(t, nil)
	m.OverlayFor = func(backend fkoverlay.Backend) (fkoverlay.Provider, error) {
		if backend.IsFuse() {
			return fake, nil
		}
		return newSymlinkProvider(), nil
	}
	priv := fkoverlay.FusePrivateRoot(dir)

	fwd, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendNFS)
	if err != nil {
		t.Fatalf("symlink→fuse: %v", err)
	}
	if fwd.OverlayKind != "nfs" || storedKind(t, m, a.ID) != "nfs" {
		t.Fatalf("row not flipped to fuse: returned=%s stored=%s", fwd.OverlayKind, storedKind(t, m, a.ID))
	}
	if !IsBridgeSymlink(dir) {
		t.Fatal("symlink→fuse did not lay the bridge symlink")
	}
	if got := readFileT(t, filepath.Join(priv, ".claude.json")); got != identityJSON {
		t.Fatalf("identity not moved to the backing root: %q", got)
	}

	back, err := m.ConvertOverlay(t.Context(), fwd, fkoverlay.BackendSymlink)
	if err != nil {
		t.Fatalf("fuse→symlink: %v", err)
	}
	if back.OverlayKind != "symlink" || storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row not flipped back to symlink")
	}
	if IsBridgeSymlink(dir) {
		t.Fatal("fuse→symlink left the bridge symlink in place")
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity not restored after retreat: %q", got)
	}
	if _, err := os.Readlink(filepath.Join(dir, "projects")); err != nil {
		t.Fatalf("symlink overlay not re-asserted after retreat: %v", err)
	}
}

// fakeFP is a File Provider provider fake that models the FP overlay on disk:
// Setup "registers" the domain by minting a root dir under domainsRoot and lays
// the account-dir symlink with the REAL fail-closed fileproviderd.AtomicSymlink
// — the exact clobber guard the conversion relies on — and Teardown retracts it
// with the real fail-closed RemoveSymlink and forgets the domain (removing a
// never-registered domain is a no-op, mirroring RemoteDomainHost.Remove).
type fakeFP struct {
	ops         *[]string
	domainsRoot string
	registered  map[string]bool
	setupErr    error
	teardownErr error
	probeErr    error            // scripted ProbeDomain verdict; nil reads the domain root
	onSetup     func(dir string) // runs after the domain is minted, before the symlink
}

func newFakeFP(t *testing.T, ops *[]string) *fakeFP {
	t.Helper()
	return &fakeFP{ops: ops, domainsRoot: t.TempDir(), registered: map[string]bool{}}
}

func (f *fakeFP) Backend() fkoverlay.Backend                  { return fkoverlay.BackendFileProvider }
func (f *fakeFP) Check(context.Context, string, string) error { return nil }
func (f *fakeFP) PrivateRoot(dir string) string               { return fkoverlay.FusePrivateRoot(dir) }

// ProbeDomain models the companion app's control-op verdict: it reads the
// backing .claude.json the bridge serves at the domain root (never a
// through-domain filesystem read) and reports its byte count — nil (absent), a
// pointer to 0 (empty), or a pointer to the size. It is what the derive-path
// convert gate calls when Manager.FPProbe is unset.
func (f *fakeFP) ProbeDomain(_ context.Context, dir string) (*int64, error) {
	if f.probeErr != nil {
		return nil, f.probeErr
	}
	b, err := os.ReadFile(filepath.Join(f.domainRoot(dir), ".claude.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	n := int64(len(b))
	return &n, nil
}

// ProbeDomainShallow models the shallow control op: whether .claude.json is listed
// in the domain root (no byte read). scripted probeErr overrides the listing.
func (f *fakeFP) ProbeDomainShallow(_ context.Context, dir string) (bool, error) {
	if f.probeErr != nil {
		return false, f.probeErr
	}
	_, err := os.Stat(filepath.Join(f.domainRoot(dir), ".claude.json"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (f *fakeFP) domainRoot(dir string) string {
	return filepath.Join(f.domainsRoot, "CCPoolStatus-"+filepath.Base(dir))
}

func (f *fakeFP) Reconcile(_ context.Context, _, dir string) error {
	*f.ops = append(*f.ops, "fp.setup")
	if f.setupErr != nil {
		return f.setupErr
	}
	root := f.domainRoot(dir)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	f.registered[filepath.Base(dir)] = true
	if f.onSetup != nil {
		f.onSetup(dir)
	}
	if err := fileproviderd.AtomicSymlink(dir, root); err != nil {
		return err
	}
	priv := fkoverlay.FusePrivateRoot(dir)
	if err := os.MkdirAll(priv, 0o700); err != nil {
		return err
	}
	// Model a materialized domain: the bridge serves the backing .claude.json at
	// the domain root, so ProbeDomain (the control-op verdict) reports real
	// content instead of a false miss.
	if b, err := os.ReadFile(filepath.Join(priv, ".claude.json")); err == nil { //nolint:gosec // G304: priv is a test-owned temp dir, not user input.
		if err := os.WriteFile(filepath.Join(root, ".claude.json"), b, 0o600); err != nil { //nolint:gosec // G703: root is a test-owned temp dir, not user input.
			return err
		}
	}
	return nil
}

func (f *fakeFP) Teardown(_ context.Context, _, dir string) (string, error) {
	*f.ops = append(*f.ops, "fp.teardown")
	if f.teardownErr != nil {
		return "", f.teardownErr
	}
	if err := fileproviderd.RemoveSymlink(dir); err != nil {
		return "", err
	}
	delete(f.registered, filepath.Base(dir))
	return "", nil
}

// RemoveDomain deregisters WITHOUT retracting the bridge symlink (unlike Teardown),
// mirroring fusekit's RemoveDomain: removing a never-registered domain is a no-op.
func (f *fakeFP) RemoveDomain(_ context.Context, dir string) error {
	*f.ops = append(*f.ops, "fp.removedomain")
	delete(f.registered, filepath.Base(dir))
	return nil
}

// DomainRoot models the host's zero-spawn State query: a registered domain returns
// its root, an unregistered one is fileproviderd.ErrNoDomain.
func (f *fakeFP) DomainRoot(_ context.Context, dir string) (string, error) {
	if !f.registered[filepath.Base(dir)] {
		return "", fmt.Errorf("state domain %s: %w", filepath.Base(dir), fileproviderd.ErrNoDomain)
	}
	return f.domainRoot(dir), nil
}

// fpOverlayFor dispatches provider resolution to the fake FP provider, the
// given fake fuse provider, and the real symlink provider.
func fpOverlayFor(fp *fakeFP, fuse fkoverlay.Provider) func(fkoverlay.Backend) (fkoverlay.Provider, error) {
	return func(b fkoverlay.Backend) (fkoverlay.Provider, error) {
		switch {
		case b == fkoverlay.BackendFileProvider:
			return fp, nil
		case b.IsFuse():
			return fuse, nil
		default:
			return newSymlinkProvider(), nil
		}
	}
}

// assertSymlinkRestored asserts the full symlink rest state after a rolled-back
// conversion: identity and backups back in a REAL account dir, links re-laid,
// row untouched, nothing stranded in the private backing root.
func assertSymlinkRestored(t *testing.T, m *Manager, id int, dir, wantIdentity string) {
	t.Helper()
	if fi, err := os.Lstat(dir); err != nil || fi.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("account dir is not a real dir after rollback: fi=%v err=%v", fi, err)
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != wantIdentity {
		t.Fatalf("identity after rollback = %q, want %q", got, wantIdentity)
	}
	if got := readFileT(t, filepath.Join(dir, "backups", "b.bak")); got != "bak" {
		t.Fatalf("backups not restored: %q", got)
	}
	if _, err := os.Readlink(filepath.Join(dir, "projects")); err != nil {
		t.Fatalf("symlink overlay not re-asserted: %v", err)
	}
	if storedKind(t, m, id) != "symlink" {
		t.Fatal("row flipped despite failed conversion")
	}
	if has, err := fkoverlay.HasPrivateEntries(fkoverlay.FusePrivateRoot(dir), testSpec()); err != nil || has {
		t.Fatalf("private files stranded after rollback (has=%v err=%v)", has, err)
	}
}

func TestConvertSymlinkToFileProvider(t *testing.T) {
	ops := []string{}
	m, a, dir := newConvertFixture(t, nil)
	fp := newFakeFP(t, &ops)
	m.OverlayFor = fpOverlayFor(fp, &fakeFuse{ops: &ops})
	priv := fkoverlay.FusePrivateRoot(dir)

	got, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendFileProvider)
	if err != nil {
		t.Fatalf("ConvertOverlay: %v", err)
	}
	if got.OverlayKind != "fileprovider" || storedKind(t, m, a.ID) != "fileprovider" {
		t.Fatalf("row not flipped: returned=%s stored=%s", got.OverlayKind, storedKind(t, m, a.ID))
	}
	// The drain preceded Setup: exactly one registration, no teardown.
	if len(ops) != 1 || ops[0] != "fp.setup" {
		t.Fatalf("ops = %v, want exactly one fp.setup", ops)
	}
	if target, err := os.Readlink(dir); err != nil || target != fp.domainRoot(dir) {
		t.Fatalf("account dir is not the domain symlink: target=%q err=%v", target, err)
	}
	if !fp.registered[filepath.Base(dir)] {
		t.Fatal("domain not registered")
	}
	if gotJSON := readFileT(t, filepath.Join(priv, ".claude.json")); gotJSON != identityJSON {
		t.Fatalf("identity in private root = %q", gotJSON)
	}
	if gotBak := readFileT(t, filepath.Join(priv, "backups", "b.bak")); gotBak != "bak" {
		t.Fatalf("backups content lost: %q", gotBak)
	}
}

func TestConvertFuseToFileProviderRetargetsBridge(t *testing.T) {
	ops := []string{}
	m, a, dir := newConvertFixture(t, nil)
	fp := newFakeFP(t, &ops)
	m.OverlayFor = fpOverlayFor(fp, &bridgeFuse{ops: &ops})
	priv := fkoverlay.FusePrivateRoot(dir)

	// Post-mux fuse rest state: privates in the backing root, dir a bridge symlink.
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, ".claude.json"), filepath.Join(priv, ".claude.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "backups"), filepath.Join(priv, "backups")); err != nil {
		t.Fatal(err)
	}
	replaceWithBridge(t, dir)
	a.OverlayKind = "nfs"
	if err := m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	got, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendFileProvider)
	if err != nil {
		t.Fatalf("fuse→fileprovider: %v", err)
	}
	if got.OverlayKind != "fileprovider" || storedKind(t, m, a.ID) != "fileprovider" {
		t.Fatalf("row not flipped: returned=%s stored=%s", got.OverlayKind, storedKind(t, m, a.ID))
	}
	// Subtree detach precedes the retarget; no file ever moved.
	if len(ops) != 2 || ops[0] != "teardown" || ops[1] != "fp.setup" {
		t.Fatalf("ops = %v, want [teardown fp.setup]", ops)
	}
	if IsBridgeSymlink(dir) {
		t.Fatal("bridge symlink survived the retarget")
	}
	if target, err := os.Readlink(dir); err != nil || target != fp.domainRoot(dir) {
		t.Fatalf("account dir is not the domain symlink: target=%q err=%v", target, err)
	}
	if gotJSON := readFileT(t, filepath.Join(priv, ".claude.json")); gotJSON != identityJSON {
		t.Fatalf("identity in private root disturbed: %q", gotJSON)
	}
}

// TestConvertFuseToFileProviderFailClosedOnRealDir pins the fail-closed
// AtomicSymlink guard on the fuse→FP retarget: a fuse row whose account dir is
// unexpectedly a REAL dir (the legacy pre-mux shape) must abort and roll back,
// never clobber the dir with the domain symlink.
func TestConvertFuseToFileProviderFailClosedOnRealDir(t *testing.T) {
	ops := []string{}
	m, a, dir := newConvertFixture(t, nil)
	fp := newFakeFP(t, &ops)
	// A lenient fuse teardown (no-op on a real dir) models "nothing mounted", so
	// the flow reaches FP Setup and its AtomicSymlink guard.
	m.OverlayFor = fpOverlayFor(fp, &fakeFuse{ops: &ops, noMountView: true})
	a.OverlayKind = "nfs"
	if err := m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendFileProvider)
	if err == nil || !strings.Contains(err.Error(), "refusing to replace") {
		t.Fatalf("error = %v, want the AtomicSymlink non-symlink refusal", err)
	}
	if !strings.Contains(err.Error(), "rolled back to nfs") {
		t.Fatalf("error = %v, want rollback report", err)
	}
	if fi, err := os.Lstat(dir); err != nil || fi.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("account dir clobbered: fi=%v err=%v, want the untouched real dir", fi, err)
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity disturbed by refused retarget: %q", got)
	}
	if got := readFileT(t, filepath.Join(dir, "backups", "b.bak")); got != "bak" {
		t.Fatalf("backups disturbed by refused retarget: %q", got)
	}
	if storedKind(t, m, a.ID) != "nfs" {
		t.Fatal("row flipped despite refused conversion")
	}
	// LEAK FIX: fusekit registers the domain BEFORE it lays the bridge symlink, so
	// the AtomicSymlink refusal left a registered domain. The rollback's
	// retractFileProviderIfLaid real-dir arm must deregister it (RemoveDomain) rather
	// than return nil and leak the registration forever.
	if len(fp.registered) != 0 {
		t.Fatalf("leaked domain not deregistered by the rollback's real-dir arm: %v", fp.registered)
	}
	sawRemove := false
	for _, op := range ops {
		if op == "fp.removedomain" {
			sawRemove = true
		}
	}
	if !sawRemove {
		t.Fatalf("rollback did not call RemoveDomain on the real-dir arm: ops = %v", ops)
	}
}

func TestConvertFuseToFileProviderRollsBackOnSetupFailure(t *testing.T) {
	ops := []string{}
	m, a, dir := newConvertFixture(t, nil)
	fp := newFakeFP(t, &ops)
	fp.setupErr = errors.New("no entitlement")
	m.OverlayFor = fpOverlayFor(fp, &bridgeFuse{ops: &ops})
	priv := fkoverlay.FusePrivateRoot(dir)

	// Post-mux fuse rest state.
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, ".claude.json"), filepath.Join(priv, ".claude.json")); err != nil {
		t.Fatal(err)
	}
	replaceWithBridge(t, dir)
	a.OverlayKind = "nfs"
	if err := m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendFileProvider)
	if err == nil || !strings.Contains(err.Error(), "register domain") || !strings.Contains(err.Error(), "no entitlement") {
		t.Fatalf("error = %v, want the setup cause", err)
	}
	if !strings.Contains(err.Error(), "rolled back to nfs") {
		t.Fatalf("error = %v, want rollback report", err)
	}
	// The rollback re-laid the bridge symlink over the vacated path.
	want := []string{"teardown", "fp.setup", "fp.teardown", "setup"}
	if fmt.Sprintf("%v", ops) != fmt.Sprintf("%v", want) {
		t.Fatalf("ops = %v, want %v", ops, want)
	}
	if !IsBridgeSymlink(dir) {
		t.Fatal("bridge symlink not restored by rollback")
	}
	if gotJSON := readFileT(t, filepath.Join(priv, ".claude.json")); gotJSON != identityJSON {
		t.Fatalf("identity in private root disturbed: %q", gotJSON)
	}
	if storedKind(t, m, a.ID) != "nfs" {
		t.Fatal("row flipped despite failed conversion")
	}
}

func TestConvertToFileProviderRollsBackOnFailure(t *testing.T) {
	cases := []struct {
		name         string
		prep         func(t *testing.T, m *Manager, fp *fakeFP, dir string)
		wantErr      []string
		wantIdentity string // dir/.claude.json after rollback; identityJSON when empty
		check        func(t *testing.T, dir string, ops []string)
	}{
		{
			name: "symlink teardown failure",
			prep: func(t *testing.T, m *Manager, _ *fakeFP, _ string) {
				t.Helper()
				prev := m.OverlayFor
				m.OverlayFor = func(b fkoverlay.Backend) (fkoverlay.Provider, error) {
					if b == fkoverlay.BackendSymlink {
						return &hookedSymlink{SymlinkProvider: newSymlinkProvider(), preTeardown: func() error { return errors.New("unlink exploded") }}, nil
					}
					return prev(b)
				}
			},
			wantErr: []string{"tear down symlinks", "unlink exploded", "rolled back to symlink"},
			check: func(t *testing.T, _ string, ops []string) {
				t.Helper()
				if len(ops) != 0 {
					t.Fatalf("FP provider touched before the drain finished: ops = %v", ops)
				}
			},
		},
		{
			name: "unclassified leftover blocks the drain",
			prep: func(t *testing.T, _ *Manager, _ *fakeFP, dir string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, "strange.bin"), []byte("keep"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			wantErr: []string{"remove drained account dir", "rolled back to symlink"},
			check: func(t *testing.T, dir string, ops []string) {
				t.Helper()
				// Fail-closed: Setup never ran, and the unclassified bytes survive
				// (the rollback sweeps them into base and re-links, like every
				// shared-orphan sweep).
				if len(ops) != 0 {
					t.Fatalf("FP setup ran despite an occupied account dir: ops = %v", ops)
				}
				if got := readFileT(t, filepath.Join(dir, "strange.bin")); got != "keep" {
					t.Fatalf("unclassified leftover lost: %q", got)
				}
			},
		},
		{
			name: "domain registration failure",
			prep: func(t *testing.T, _ *Manager, fp *fakeFP, _ string) {
				t.Helper()
				fp.setupErr = errors.New("no entitlement")
			},
			wantErr: []string{"register domain", "no entitlement", "rolled back to symlink"},
		},
		{
			name: "identity mismatch",
			prep: func(t *testing.T, _ *Manager, fp *fakeFP, _ string) {
				t.Helper()
				fp.onSetup = func(dir string) {
					priv := fkoverlay.FusePrivateRoot(dir)
					if err := os.WriteFile(filepath.Join(priv, ".claude.json"), []byte(wrongIdentityJSON), 0o600); err != nil {
						t.Error(err)
					}
				}
			},
			wantErr: []string{"identity in private root is u-IMPOSTOR, expected u-1", "rolled back to symlink"},
			// The rollback preserves the divergent bytes rather than destroying them.
			wantIdentity: wrongIdentityJSON,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ops := []string{}
			m, a, dir := newConvertFixture(t, nil)
			fp := newFakeFP(t, &ops)
			m.OverlayFor = fpOverlayFor(fp, &fakeFuse{ops: &ops})
			if tc.prep != nil {
				tc.prep(t, m, fp, dir)
			}

			_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendFileProvider)
			if err == nil {
				t.Fatal("conversion succeeded despite the injected failure")
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %v does not carry %q", err, want)
				}
			}
			wantIdentity := tc.wantIdentity
			if wantIdentity == "" {
				wantIdentity = identityJSON
			}
			assertSymlinkRestored(t, m, a.ID, dir, wantIdentity)
			if tc.check != nil {
				tc.check(t, dir, ops)
			}
		})
	}
}

// TestConvertToFileProviderMoveFailureRoutesRollback pins the earliest failure
// step: a private-file move that dies before anything relocated still routes
// through the rollback and leaves the account untouched.
func TestConvertToFileProviderMoveFailureRoutesRollback(t *testing.T) {
	ops := []string{}
	m, a, dir := newConvertFixture(t, nil)
	fp := newFakeFP(t, &ops)
	m.OverlayFor = fpOverlayFor(fp, &fakeFuse{ops: &ops})
	// The private root's path is occupied by a FILE: the drain's MkdirAll fails
	// before any entry moves.
	if err := os.WriteFile(fkoverlay.FusePrivateRoot(dir), []byte("squatter"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendFileProvider)
	if err == nil || !strings.Contains(err.Error(), "move private files") {
		t.Fatalf("error = %v, want the move cause", err)
	}
	// The drain died before Setup, so no domain was ever registered: the rollback's
	// real-dir retract sees no registration (zero-spawn DomainRoot) and deregisters
	// nothing, touching no provider mutation.
	if len(ops) != 0 {
		t.Fatalf("providers touched despite a dead-on-arrival move: ops = %v", ops)
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity disturbed: %q", got)
	}
	if storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row flipped despite failed conversion")
	}
}

// TestConvertToFileProviderRefusesOverlaySymlink is the R2 regression pin for the
// symlink→fileprovider arm — and the exact traced-loss shape: a crashed convert
// leaves the dir an FP-bridge symlink (row still symlink) that IsBridgeSymlink
// missed, so the retry convert drained MovePrivateEntries THROUGH the live domain
// and the fresher-wins resolver destroyed the identity. The broader requireRealDir
// guard must refuse both the mux-bridge and FP-bridge shapes with
// ErrDirIsOverlaySymlink and touch no provider.
func TestConvertToFileProviderRefusesOverlaySymlink(t *testing.T) {
	for _, shape := range []string{"mux-bridge", "fp-bridge"} {
		t.Run(shape, func(t *testing.T) {
			ops := []string{}
			m, a, dir := newConvertFixture(t, nil)
			fp := newFakeFP(t, &ops)
			m.OverlayFor = fpOverlayFor(fp, &fakeFuse{ops: &ops})
			var target string
			if shape == "mux-bridge" {
				replaceWithBridge(t, dir)
				target = filepath.Join(MuxRootDir(), filepath.Base(dir))
			} else {
				target = replaceWithFPBridge(t, dir)
			}

			_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendFileProvider)
			if !errors.Is(err, ErrDirIsOverlaySymlink) {
				t.Fatalf("convert over a %s = %v, want errors.Is ErrDirIsOverlaySymlink", shape, err)
			}
			if !strings.Contains(err.Error(), target) {
				t.Fatalf("refusal %q does not name the link target %q", err, target)
			}
			if len(ops) != 0 {
				t.Fatalf("refused convert still touched a provider: %v", ops)
			}
			if storedKind(t, m, a.ID) != "symlink" {
				t.Fatal("row flipped despite a refused convert")
			}
		})
	}
}

// TestConvertToFileProviderCancelledBeforeMoveAbortsCleanly: a spent budget
// observed before anything moved is a clean abort — no rollback machinery, no
// provider calls, account untouched.
func TestConvertToFileProviderCancelledBeforeMoveAbortsCleanly(t *testing.T) {
	ops := []string{}
	m, a, dir := newConvertFixture(t, nil)
	fp := newFakeFP(t, &ops)
	m.OverlayFor = fpOverlayFor(fp, &fakeFuse{ops: &ops})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := m.ConvertOverlay(ctx, a, fkoverlay.BackendFileProvider)
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
	if _, err := os.Lstat(fkoverlay.FusePrivateRoot(dir)); !os.IsNotExist(err) {
		t.Fatal("a pre-move abort minted a private root")
	}
	if storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row changed by a pre-move abort")
	}
}

// TestConvertToFileProviderProbeGate pins the post-Setup readiness gate (the
// defect the FP-migrate-storm exposed): a domain that registers but does not
// serve reads rolls back before the row flips, a serving domain flips it, and an
// identity-less account skips the probe entirely — FPFS serves 0 bytes for a
// no-identity domain, so a through-domain read there proves nothing.
func TestConvertToFileProviderProbeGate(t *testing.T) {
	t.Run("wedged domain rolls back before the row flips", func(t *testing.T) {
		ops := []string{}
		m, a, dir := newConvertFixture(t, nil)
		fp := newFakeFP(t, &ops)
		m.OverlayFor = fpOverlayFor(fp, &fakeFuse{ops: &ops})

		m.FPProbe = func(context.Context, string) error { return overlay.ErrFPProbeWedged }

		_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendFileProvider)
		if err == nil || !strings.Contains(err.Error(), "domain registered but does not serve reads") {
			t.Fatalf("error = %v, want the does-not-serve rollback", err)
		}
		if !strings.Contains(err.Error(), "rolled back to symlink") {
			t.Fatalf("error = %v, want rollback report", err)
		}
		// Setup laid the domain; the failed probe drove a teardown before the flip.
		want := []string{"fp.setup", "fp.teardown"}
		if fmt.Sprintf("%v", ops) != fmt.Sprintf("%v", want) {
			t.Fatalf("ops = %v, want %v", ops, want)
		}
		assertSymlinkRestored(t, m, a.ID, dir, identityJSON)
		if len(fp.registered) != 0 {
			t.Fatalf("domain still registered after the wedged-probe rollback: %v", fp.registered)
		}
	})

	t.Run("serving domain flips the row after probing the flipped dir", func(t *testing.T) {
		ops := []string{}
		m, a, dir := newConvertFixture(t, nil)
		fp := newFakeFP(t, &ops)
		m.OverlayFor = fpOverlayFor(fp, &fakeFuse{ops: &ops})

		probed := ""
		m.FPProbe = func(_ context.Context, configDir string) error { probed = configDir; return nil }

		got, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendFileProvider)
		if err != nil {
			t.Fatalf("ConvertOverlay: %v", err)
		}
		if got.OverlayKind != "fileprovider" || storedKind(t, m, a.ID) != "fileprovider" {
			t.Fatalf("row not flipped: returned=%s stored=%s", got.OverlayKind, storedKind(t, m, a.ID))
		}
		if probed != dir {
			t.Fatalf("probe read %q, want the flipped account dir %q", probed, dir)
		}
	})

	t.Run("identity-less account never probes", func(t *testing.T) {
		ops := []string{}
		m, a, dir := newConvertFixture(t, nil)
		fp := newFakeFP(t, &ops)
		m.OverlayFor = fpOverlayFor(fp, &fakeFuse{ops: &ops})
		// An account that never completed a login has no identity to serve.
		if err := os.Remove(filepath.Join(dir, ".claude.json")); err != nil {
			t.Fatal(err)
		}

		calls := 0
		m.FPProbe = func(context.Context, string) error {
			calls++
			return errors.New("probe must not run for an identity-less account")
		}

		got, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendFileProvider)
		if err != nil {
			t.Fatalf("ConvertOverlay: %v", err)
		}
		if got.OverlayKind != "fileprovider" || storedKind(t, m, a.ID) != "fileprovider" {
			t.Fatalf("row not flipped: returned=%s stored=%s", got.OverlayKind, storedKind(t, m, a.ID))
		}
		if calls != 0 {
			t.Fatalf("probe ran %d time(s) for an identity-less account, want 0", calls)
		}
	})
}

func TestConvertFileProviderToSymlink(t *testing.T) {
	ops := []string{}
	m, a, dir := newConvertFixture(t, nil)
	fp := newFakeFP(t, &ops)
	m.OverlayFor = fpOverlayFor(fp, &bridgeFuse{ops: &ops})
	priv := fkoverlay.FusePrivateRoot(dir)

	fwd, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendFileProvider)
	if err != nil {
		t.Fatalf("forward convert: %v", err)
	}
	back, err := m.ConvertOverlay(t.Context(), fwd, fkoverlay.BackendSymlink)
	if err != nil {
		t.Fatalf("fileprovider→symlink: %v", err)
	}
	if back.OverlayKind != "symlink" || storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row not flipped back to symlink")
	}
	// The account dir was re-created as a REAL dir with everything restored.
	if fi, err := os.Lstat(dir); err != nil || fi.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("account dir not a real dir after retreat: fi=%v err=%v", fi, err)
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
	if len(fp.registered) != 0 {
		t.Fatalf("domain still registered after retreat: %v", fp.registered)
	}
}

func TestConvertFileProviderToFuse(t *testing.T) {
	ops := []string{}
	m, a, dir := newConvertFixture(t, nil)
	fp := newFakeFP(t, &ops)
	m.OverlayFor = fpOverlayFor(fp, &bridgeFuse{ops: &ops})
	priv := fkoverlay.FusePrivateRoot(dir)

	fwd, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendFileProvider)
	if err != nil {
		t.Fatalf("forward convert: %v", err)
	}
	got, err := m.ConvertOverlay(t.Context(), fwd, fkoverlay.BackendNFS)
	if err != nil {
		t.Fatalf("fileprovider→fuse: %v", err)
	}
	if got.OverlayKind != "nfs" || storedKind(t, m, a.ID) != "nfs" {
		t.Fatalf("row not flipped: returned=%s stored=%s", got.OverlayKind, storedKind(t, m, a.ID))
	}
	// Retract, then the fuse Setup lays its own bridge; nothing moved.
	want := []string{"fp.setup", "fp.teardown", "setup"}
	if fmt.Sprintf("%v", ops) != fmt.Sprintf("%v", want) {
		t.Fatalf("ops = %v, want %v", ops, want)
	}
	if !IsBridgeSymlink(dir) {
		t.Fatal("fuse bridge symlink not laid")
	}
	if gotJSON := readFileT(t, filepath.Join(priv, ".claude.json")); gotJSON != identityJSON {
		t.Fatalf("identity in private root disturbed: %q", gotJSON)
	}
	if len(fp.registered) != 0 {
		t.Fatalf("domain still registered after conversion: %v", fp.registered)
	}
}

func TestConvertFileProviderToFuseRollsBack(t *testing.T) {
	cases := []struct {
		name     string
		prep     func(t *testing.T, bf *bridgeFuse)
		wantErr  []string
		wantPriv string // priv/.claude.json after rollback
	}{
		{
			name: "fuse setup failure",
			prep: func(t *testing.T, bf *bridgeFuse) {
				t.Helper()
				bf.setupErr = errors.New("holder unreachable")
			},
			wantErr:  []string{"mount", "holder unreachable", "rolled back to fileprovider"},
			wantPriv: identityJSON,
		},
		{
			name: "identity mismatch",
			prep: func(t *testing.T, bf *bridgeFuse) {
				t.Helper()
				bf.onSetup = func(dir string) {
					priv := fkoverlay.FusePrivateRoot(dir)
					if err := os.WriteFile(filepath.Join(priv, ".claude.json"), []byte(wrongIdentityJSON), 0o600); err != nil {
						t.Error(err)
					}
				}
			},
			wantErr: []string{"identity in private root is u-IMPOSTOR, expected u-1", "rolled back to fileprovider"},
			// Divergent bytes preserved for inspection, never destroyed.
			wantPriv: wrongIdentityJSON,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ops := []string{}
			m, a, dir := newConvertFixture(t, nil)
			fp := newFakeFP(t, &ops)
			bf := &bridgeFuse{ops: &ops}
			m.OverlayFor = fpOverlayFor(fp, bf)
			priv := fkoverlay.FusePrivateRoot(dir)

			fwd, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendFileProvider)
			if err != nil {
				t.Fatalf("forward convert: %v", err)
			}
			tc.prep(t, bf)

			_, err = m.ConvertOverlay(t.Context(), fwd, fkoverlay.BackendNFS)
			if err == nil {
				t.Fatal("conversion succeeded despite the injected failure")
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %v does not carry %q", err, want)
				}
			}
			// The FP shape is fully restored: domain symlink back, row untouched.
			if target, rerr := os.Readlink(dir); rerr != nil || target != fp.domainRoot(dir) {
				t.Fatalf("domain symlink not restored: target=%q err=%v", target, rerr)
			}
			if storedKind(t, m, a.ID) != "fileprovider" {
				t.Fatal("row flipped despite failed conversion")
			}
			if gotJSON := readFileT(t, filepath.Join(priv, ".claude.json")); gotJSON != tc.wantPriv {
				t.Fatalf("private identity after rollback = %q, want %q", gotJSON, tc.wantPriv)
			}
		})
	}
}

// TestRollbackIdentityVerifySurfacesRecoverySources pins the identity invariant:
// when a rollback's restore move leaves the identity unreadable at dir/.claude.json
// (the fresher-wins EXDEV loss — modeled here by the backing identity vanishing
// with no replacement), the returned error is ErrIdentityLost and names the
// recovery sources in order (the account-dir and private-root conflict siblings,
// the private-root backups, then a fresh login), and the row never flips.
func TestRollbackIdentityVerifySurfacesRecoverySources(t *testing.T) {
	ops := []string{}
	m, a, dir := newConvertFixture(t, nil)
	fp := newFakeFP(t, &ops)
	m.OverlayFor = fpOverlayFor(fp, &fakeFuse{ops: &ops})
	priv := fkoverlay.FusePrivateRoot(dir)
	// The convert drains dir→priv; Setup's hook then deletes priv/.claude.json with
	// no surviving replacement, so the failed probe's rollback finds nothing to move
	// back and dir/.claude.json is unreadable afterward.
	fp.onSetup = func(dir string) {
		if err := os.Remove(filepath.Join(fkoverlay.FusePrivateRoot(dir), ".claude.json")); err != nil {
			t.Error(err)
		}
	}

	_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendFileProvider)
	if !errors.Is(err, ErrIdentityLost) {
		t.Fatalf("error = %v, want errors.Is ErrIdentityLost", err)
	}
	for _, frag := range []string{
		filepath.Join(dir, ".claude.json.conflict-"),
		filepath.Join(priv, ".claude.json.conflict-"),
		filepath.Join(priv, "backups", ".claude.json.backup."),
		"claude /login",
	} {
		if !strings.Contains(err.Error(), frag) {
			t.Errorf("ErrIdentityLost %q missing recovery source %q", err, frag)
		}
	}
	if storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row flipped despite the lost identity")
	}
}

// TestConvertCrashInjectionPreservesIdentity is the transactionality pin: for a
// failure injected at each step of a symlink→fileprovider conversion — a
// dead-on-arrival move, a teardown fault mid-window, a registration failure, a
// post-Setup probe failure, an interrupted rollback, and the FP-bridge wreckage a
// prior crash left — the account's identity is never lost. It is readable at its
// recoverable location after every outcome (dir once the rollback completes, the
// private backing root while a rollback is interrupted mid-restore), or the flow
// refused to move anything with ErrDirIsOverlaySymlink. The row never flips.
func TestConvertCrashInjectionPreservesIdentity(t *testing.T) {
	type where int
	const (
		atDir where = iota
		atPriv
		refused
	)
	cases := []struct {
		name string
		prep func(t *testing.T, m *Manager, fp *fakeFP, dir string)
		want where
	}{
		{
			name: "move dead on arrival moves nothing",
			prep: func(t *testing.T, _ *Manager, _ *fakeFP, dir string) {
				t.Helper()
				// The private-root path is a FILE: the drain's MkdirAll fails before
				// any entry moves.
				if err := os.WriteFile(fkoverlay.FusePrivateRoot(dir), []byte("squat"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: atDir,
		},
		{
			name: "symlink teardown fault mid-window rolls back",
			prep: func(t *testing.T, m *Manager, _ *fakeFP, _ string) {
				t.Helper()
				prev := m.OverlayFor
				m.OverlayFor = func(b fkoverlay.Backend) (fkoverlay.Provider, error) {
					if b == fkoverlay.BackendSymlink {
						return &hookedSymlink{SymlinkProvider: newSymlinkProvider(), preTeardown: func() error { return errors.New("unlink exploded") }}, nil
					}
					return prev(b)
				}
			},
			want: atDir,
		},
		{
			name: "registration failure rolls back",
			prep: func(t *testing.T, _ *Manager, fp *fakeFP, _ string) {
				t.Helper()
				fp.setupErr = errors.New("no entitlement")
			},
			want: atDir,
		},
		{
			name: "post-Setup probe failure rolls back",
			prep: func(t *testing.T, m *Manager, _ *fakeFP, _ string) {
				t.Helper()
				m.FPProbe = func(context.Context, string) error { return overlay.ErrFPProbeWedged }
			},
			want: atDir,
		},
		{
			name: "interrupted rollback strands the identity recoverably in the private root",
			prep: func(t *testing.T, _ *Manager, fp *fakeFP, _ string) {
				t.Helper()
				// Setup registers + lays the symlink; the probe fails, so a rollback
				// starts — but the FP retract's Teardown errors, so the rollback stops
				// with the private files (identity included) intact in the backing
				// root for the daemon's HealStrandedPrivate.
				fp.probeErr = errors.New("does not serve")
				fp.teardownErr = errors.New("domain wedged")
			},
			want: atPriv,
		},
		{
			name: "FP-bridge wreckage refuses, moving nothing",
			prep: func(t *testing.T, _ *Manager, _ *fakeFP, dir string) {
				t.Helper()
				// The traced-loss shape: a crash drained dir→priv then laid the domain
				// bridge, leaving the row on symlink. The identity lives in priv; the
				// retry convert must refuse rather than drain THROUGH the live domain.
				priv := fkoverlay.FusePrivateRoot(dir)
				if err := os.MkdirAll(priv, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(filepath.Join(dir, ".claude.json"), filepath.Join(priv, ".claude.json")); err != nil {
					t.Fatal(err)
				}
				replaceWithFPBridge(t, dir)
			},
			want: refused,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ops := []string{}
			m, a, dir := newConvertFixture(t, nil)
			fp := newFakeFP(t, &ops)
			m.OverlayFor = fpOverlayFor(fp, &fakeFuse{ops: &ops})
			priv := fkoverlay.FusePrivateRoot(dir)
			tc.prep(t, m, fp, dir)

			_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.BackendFileProvider)
			if err == nil {
				t.Fatal("conversion succeeded despite the injected failure")
			}
			switch tc.want {
			case atDir:
				if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
					t.Fatalf("identity not restored to the account dir: %q", got)
				}
			case atPriv:
				if got := readFileT(t, filepath.Join(priv, ".claude.json")); got != identityJSON {
					t.Fatalf("identity not stranded recoverably in the private root: %q", got)
				}
			case refused:
				if !errors.Is(err, ErrDirIsOverlaySymlink) {
					t.Fatalf("error = %v, want errors.Is ErrDirIsOverlaySymlink (moved nothing)", err)
				}
				if len(ops) != 0 {
					t.Fatalf("refused convert still touched a provider: %v", ops)
				}
			}
			// Never lost: a valid identity must survive at dir OR priv.
			_, dirErr := readIdentity(filepath.Join(dir, ".claude.json"))
			_, privErr := readIdentity(filepath.Join(priv, ".claude.json"))
			if dirErr != nil && privErr != nil {
				t.Fatalf("identity lost from every read path: dir=%v priv=%v", dirErr, privErr)
			}
			if storedKind(t, m, a.ID) != "symlink" {
				t.Fatalf("row flipped despite the injected failure: %q", storedKind(t, m, a.ID))
			}
		})
	}
}

// stubProvider satisfies Provider while reporting an arbitrary backend, so the
// dispatch's default arm is reachable behind the Backend() equality fences.
type stubProvider struct{ backend fkoverlay.Backend }

func (s stubProvider) Backend() fkoverlay.Backend                      { return s.backend }
func (s stubProvider) Reconcile(context.Context, string, string) error { return nil }
func (s stubProvider) Check(context.Context, string, string) error     { return nil }
func (s stubProvider) Teardown(context.Context, string, string) (string, error) {
	return "", nil
}
func (s stubProvider) PrivateRoot(dir string) string { return dir }

// TestConvertOverlayRefusesUnknownTargetArm pins the dispatch's loud default: a
// target backend with no conversion arm errors without touching the account.
func TestConvertOverlayRefusesUnknownTargetArm(t *testing.T) {
	m, a, dir := newConvertFixture(t, nil)
	m.OverlayFor = func(b fkoverlay.Backend) (fkoverlay.Provider, error) {
		if b == fkoverlay.Backend("zfs") {
			return stubProvider{backend: "zfs"}, nil
		}
		return newSymlinkProvider(), nil
	}

	_, err := m.ConvertOverlay(t.Context(), a, fkoverlay.Backend("zfs"))
	if err == nil || !strings.Contains(err.Error(), "no conversion arm") {
		t.Fatalf("ConvertOverlay to zfs = %v, want the no-conversion-arm refusal", err)
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity disturbed by refused convert: %q", got)
	}
	if storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row changed by refused convert")
	}
}
