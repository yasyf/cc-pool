package pool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/store"
)

const (
	identityJSON      = `{"oauthAccount":{"accountUuid":"u-1","emailAddress":"a@example.com"}}`
	wrongIdentityJSON = `{"oauthAccount":{"accountUuid":"u-IMPOSTOR","emailAddress":"x@example.com"}}`
)

// fakeFuse stands in for the fuse provider so conversion logic runs without a
// live mount. Setup simulates the mirror by linking the account dir's
// .claude.json to the private backing copy (or, when wrongIdentity is set, by
// serving a different identity); Teardown removes whatever Setup created,
// matching a real unmount making the mirrored view vanish.
type fakeFuse struct {
	ops           *[]string
	setupErr      error
	teardownErr   error
	wrongIdentity bool
	created       string
}

func (f *fakeFuse) Kind() overlay.Kind            { return overlay.KindFuse }
func (f *fakeFuse) Sync(base, dir string) error   { return nil }
func (f *fakeFuse) Health(base, dir string) error { return nil }
func (f *fakeFuse) PrivateRoot(dir string) string { return overlay.FusePrivateRoot(dir) }
func (f *fakeFuse) Setup(base, dir string) error {
	privIdentity := false
	if _, err := os.Stat(filepath.Join(overlay.FusePrivateRoot(dir), ".claude.json")); err == nil {
		privIdentity = true
	}
	*f.ops = append(*f.ops, fmt.Sprintf("fuse.setup(priv-identity=%v)", privIdentity))
	if f.setupErr != nil {
		return f.setupErr
	}
	mounted := filepath.Join(dir, ".claude.json")
	if f.wrongIdentity {
		if err := os.WriteFile(mounted, []byte(wrongIdentityJSON), 0o600); err != nil {
			return err
		}
	} else if privIdentity {
		if err := os.Symlink(filepath.Join(overlay.FusePrivateRoot(dir), ".claude.json"), mounted); err != nil {
			return err
		}
	} else {
		return nil
	}
	f.created = mounted
	return nil
}
func (f *fakeFuse) Teardown(base, dir string) error {
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

// newConvertFixture builds a real symlink account over a seeded base and a
// manager whose fuse provider is the fake.
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
	if err := (&overlay.SymlinkProvider{}).Setup(base, dir); err != nil {
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
		m.OverlayFor = func(kind overlay.Kind) overlay.Provider {
			if kind == overlay.KindFuse {
				return fake
			}
			return &overlay.SymlinkProvider{}
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
	got, err := m.ConvertOverlay(a, overlay.KindSymlink)
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

// TestConvertOverlayRejectsWrongKindFake pins the Kind() equality fences. The
// real resolver can no longer hand back a wrong-kind provider (KindFuse maps
// to the holder-backed RemoteProvider, which always reports KindFuse), so the
// fences guard against wrong-kind INJECTED fakes — a conversion that thinks
// it is operating fuse-side while running symlink code paths is exactly how
// account state gets destroyed.
func TestConvertOverlayRejectsWrongKindFake(t *testing.T) {
	wrongKind := func(overlay.Kind) overlay.Provider { return &overlay.SymlinkProvider{} }

	t.Run("target fence", func(t *testing.T) {
		m, a, dir := newConvertFixture(t, nil)
		m.OverlayFor = wrongKind
		_, err := m.ConvertOverlay(a, overlay.KindFuse)
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
		a.OverlayKind = "fuse"
		if err := m.Store.UpsertAccount(a); err != nil {
			t.Fatal(err)
		}
		_, err := m.ConvertOverlay(a, overlay.KindSymlink)
		if !errors.Is(err, ErrConvertUnsupported) {
			t.Fatalf("ConvertOverlay error = %v, want ErrConvertUnsupported", err)
		}
		if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
			t.Fatalf("identity disturbed by refused convert: %q", got)
		}
		if storedKind(t, m, a.ID) != "fuse" {
			t.Fatal("row changed by refused convert")
		}
	})
}

// TestConvertOverlayRetreatWithoutLiveMount pins the escape hatch for a
// machine whose fuse rows outlived their mounts (holder gone, or fuse-t
// uninstalled entirely): the fuse→symlink retreat resolves the real
// holder-backed RemoteProvider, whose Teardown is an immediate no-op with
// zero holder contact when nothing is mounted — so the retreat is pure file
// moves and works identically in every build.
func TestConvertOverlayRetreatWithoutLiveMount(t *testing.T) {
	m, a, dir := newConvertFixture(t, nil) // no seam: the real resolver
	// Stage the fuse rest state: row says fuse, identity and backups live in
	// the private backing dir, no links in the (unmounted) account dir.
	priv := overlay.FusePrivateRoot(dir)
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, ".claude.json"), filepath.Join(priv, ".claude.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "backups"), filepath.Join(priv, "backups")); err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "fuse"
	if err := m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	back, err := m.ConvertOverlay(a, overlay.KindSymlink)
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
	priv := overlay.FusePrivateRoot(dir)

	got, err := m.ConvertOverlay(a, overlay.KindFuse)
	if err != nil {
		t.Fatalf("ConvertOverlay: %v", err)
	}
	if got.OverlayKind != "fuse" || storedKind(t, m, a.ID) != "fuse" {
		t.Fatalf("row not flipped: returned=%s stored=%s", got.OverlayKind, storedKind(t, m, a.ID))
	}
	// Move happened BEFORE the mount: Setup must have seen the identity
	// already in the private backing dir.
	if len(ops) != 1 || ops[0] != "fuse.setup(priv-identity=true)" {
		t.Fatalf("ops = %v, want one setup with private identity in place", ops)
	}
	if gotJSON := readFileT(t, filepath.Join(priv, ".claude.json")); gotJSON != identityJSON {
		t.Fatalf("identity in private root = %q", gotJSON)
	}
	if gotBak := readFileT(t, filepath.Join(priv, "backups", "b.bak")); gotBak != "bak" {
		t.Fatalf("backups content lost: %q", gotBak)
	}
	// The old overlay's shared links are gone (teardown ran).
	if _, err := os.Lstat(filepath.Join(dir, "projects")); !os.IsNotExist(err) {
		t.Fatal("shared symlink survived conversion")
	}
}

func TestConvertToFuseSetupFailureRollsBack(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops, setupErr: errors.New("grant Network Volumes access")}
	m, a, dir := newConvertFixture(t, fake)
	priv := overlay.FusePrivateRoot(dir)

	_, err := m.ConvertOverlay(a, overlay.KindFuse)
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
	if has, _ := overlay.HasPrivateEntries(priv); has {
		t.Fatal("private files stranded in backing dir after rollback")
	}
}

func TestConvertToFuseUnmountFailureAbortsRollback(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops, setupErr: errors.New("mount timed out"), teardownErr: errors.New("still mounted")}
	m, a, dir := newConvertFixture(t, fake)
	priv := overlay.FusePrivateRoot(dir)

	_, err := m.ConvertOverlay(a, overlay.KindFuse)
	if err == nil || !strings.Contains(err.Error(), "mount timed out") || !strings.Contains(err.Error(), "still mounted") {
		t.Fatalf("error = %v, want both faults reported", err)
	}
	// Rollback aborted: NO symlink re-setup over what may be a live mount, and
	// the identity stays safe in the private backing dir.
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

func TestConvertToFuseIdentityMismatchRollsBack(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops, wrongIdentity: true}
	m, a, dir := newConvertFixture(t, fake)

	_, err := m.ConvertOverlay(a, overlay.KindFuse)
	if err == nil || !strings.Contains(err.Error(), "identity through mount") {
		t.Fatalf("error = %v, want identity mismatch", err)
	}
	if got := readFileT(t, filepath.Join(dir, ".claude.json")); got != identityJSON {
		t.Fatalf("identity not restored after mismatch: %q", got)
	}
	if storedKind(t, m, a.ID) != "symlink" {
		t.Fatal("row flipped despite identity mismatch")
	}
}

func TestConvertToSymlink(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops}
	m, a, dir := newConvertFixture(t, fake)
	priv := overlay.FusePrivateRoot(dir)

	// Forward first, then reverse — the round trip the rollout rehearses.
	fwd, err := m.ConvertOverlay(a, overlay.KindFuse)
	if err != nil {
		t.Fatalf("forward convert: %v", err)
	}
	back, err := m.ConvertOverlay(fwd, overlay.KindSymlink)
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
	fwd, err := m.ConvertOverlay(a, overlay.KindFuse)
	if err != nil {
		t.Fatalf("forward convert: %v", err)
	}
	fake.teardownErr = errors.New("still mounted")
	_, err = m.ConvertOverlay(fwd, overlay.KindSymlink)
	if err == nil || !strings.Contains(err.Error(), "still mounted") {
		t.Fatalf("error = %v, want unmount failure", err)
	}
	if storedKind(t, m, a.ID) != "fuse" {
		t.Fatal("row flipped despite failed unmount")
	}
}

func TestHealStrandedPrivate(t *testing.T) {
	m, a, dir := newConvertFixture(t, nil)
	priv := overlay.FusePrivateRoot(dir)

	// Nothing stranded: no-op.
	healed, err := m.HealStrandedPrivate(a)
	if err != nil || healed {
		t.Fatalf("clean account: healed=%v err=%v, want false,nil", healed, err)
	}

	// Strand the identity (a conversion that died before rollback finished):
	// the file is in the backing dir, not the account dir.
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

	// Second run: nothing left to heal.
	healed, err = m.HealStrandedPrivate(a)
	if err != nil || healed {
		t.Fatalf("re-heal: healed=%v err=%v, want false,nil", healed, err)
	}

	// Misuse: healing a fuse-kind account is a programmer error.
	a.OverlayKind = "fuse"
	if _, err := m.HealStrandedPrivate(a); err == nil {
		t.Fatal("healing a fuse account did not error")
	}
}

// TestConvertRetreatThenLaunchMergePropagatesBase pins the migrate↔merge
// interplay across a fuse→symlink retreat: while the row says fuse the launch
// merge stays out, and once the retreat moves the private file back and flips
// the row, the launch merge propagates a fresh base key into the moved-back
// file while the account's identity survives byte-identical.
func TestConvertRetreatThenLaunchMergePropagatesBase(t *testing.T) {
	ops := []string{}
	fake := &fakeFuse{ops: &ops}
	m, a, dir := newConvertFixture(t, fake)
	if err := os.WriteFile(ClaudeJSONPath(),
		[]byte(`{"theme":"light","freshKey":true,"oauthAccount":{"accountUuid":"base-own"}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	fwd, err := m.ConvertOverlay(a, overlay.KindFuse)
	if err != nil {
		t.Fatalf("forward convert: %v", err)
	}
	out, err := m.MergeBaseClaudeJSON(fwd)
	if err != nil || out != MergeSkippedOverlay {
		t.Fatalf("merge against the fuse row: outcome=%q err=%v, want %q", out, err, MergeSkippedOverlay)
	}

	back, err := m.ConvertOverlay(fwd, overlay.KindSymlink)
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

// TestStrandedPrivateMergeRefusalKeepsHealable pins the no-collision interplay
// between the launch merge's stranded-copy guard and HealStrandedPrivate: with
// the account's .claude.json stranded in the fuse private backing dir
// (interrupted conversion), the launch merge errors without minting a file, so
// the subsequent heal's moveEntry meets no collision, the healed file carries
// the stranded identity, and the next launch merge converges.
func TestStrandedPrivateMergeRefusalKeepsHealable(t *testing.T) {
	m, a, dir := newConvertFixture(t, nil)
	priv := overlay.FusePrivateRoot(dir)
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

// TestHealResolvesDuplicatePrivateFile pins the crash-repair fix: when an
// abnormal shutdown leaves the same private file in BOTH the account dir and
// the fuse private backing dir, the heal's MovePrivateEntries used to hit a
// file-file collision and refuse forever — dead-locking the account. It now
// resolves last-write-wins (newer copy survives), the heal converges, and the
// resolution is reported through the overlay seam.
func TestHealResolvesDuplicatePrivateFile(t *testing.T) {
	m, a, dir := newConvertFixture(t, nil)
	priv := overlay.FusePrivateRoot(dir)
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	// Same private name in both roots: the stranded backing copy is newer.
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
	prev := overlay.ResolvedConflictLogf
	overlay.ResolvedConflictLogf = func(format string, args ...any) {
		resolved = append(resolved, fmt.Sprintf(format, args...))
	}
	defer func() { overlay.ResolvedConflictLogf = prev }()

	healed, err := m.HealStrandedPrivate(a)
	if err != nil || !healed {
		t.Fatalf("heal with a duplicate private file: healed=%v err=%v, want true,nil", healed, err)
	}
	got, err := os.ReadFile(filepath.Join(dir, ".last-update-result.json"))
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

	if err := m.SetDefaultOverlayKind(overlay.KindSymlink); err != nil {
		t.Fatalf("set symlink default: %v", err)
	}
	v, ok, err := st.GetMeta("overlay_kind")
	if err != nil || !ok || v != "symlink" {
		t.Fatalf("meta = %q ok=%v err=%v", v, ok, err)
	}

	if err := m.SetDefaultOverlayKind("zfs"); err == nil {
		t.Fatal("unknown kind accepted")
	}

	// The fuse fence keys on hosting capability, not on the resolved
	// provider's kind — the RemoteProvider always reports KindFuse, so a
	// provider-kind fence would always pass.
	m.CanHostFuse = func() bool { return false }
	if err := m.SetDefaultOverlayKind(overlay.KindFuse); !errors.Is(err, ErrConvertUnsupported) {
		t.Fatalf("fuse default without fuse hosting = %v, want ErrConvertUnsupported", err)
	}
	if v, _, _ := st.GetMeta("overlay_kind"); v != "symlink" {
		t.Fatalf("refused default rewrote meta to %q", v)
	}

	m.CanHostFuse = func() bool { return true }
	if err := m.SetDefaultOverlayKind(overlay.KindFuse); err != nil {
		t.Fatalf("fuse default with fuse hosting: %v", err)
	}
	if v, _, _ := st.GetMeta("overlay_kind"); v != "fuse" {
		t.Fatalf("meta = %q, want fuse", v)
	}

	// Unseamed, the fence is this build's real capability.
	m.CanHostFuse = nil
	err = m.SetDefaultOverlayKind(overlay.KindFuse)
	if CanHostFuse() {
		if err != nil {
			t.Fatalf("fuse default refused in a fuse build: %v", err)
		}
	} else if !errors.Is(err, ErrConvertUnsupported) {
		t.Fatalf("fuse default in pure build = %v, want ErrConvertUnsupported", err)
	}
}

func readFileT(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}
