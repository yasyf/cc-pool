//go:build fuse && cgo && darwin

package pool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// inProcessFuseSeam routes a fuse backend to THIS process's in-process
// provider: these tests host their mounts themselves. The real resolver would
// hand back the holder-backed RemoteFuseProvider, whose plumbing is pinned in
// fusekit/mountd.
func inProcessFuseSeam(backend fkoverlay.Backend) (fkoverlay.Provider, error) {
	if backend.IsFuse() {
		return &overlay.FuseProvider{}, nil
	}
	return newSymlinkProvider(), nil
}

// TestFuseConvertRoundTrip runs the production symlink→fuse→symlink conversion
// against a real fuse-t mount in temp dirs — the CI form of the live rollout
// (and its rollback rehearsal). Requires fuse-t and the one-time "Network
// Volumes" grant; skips like TestFuseMirrorRoundTrip when a mount cannot come
// up.
func TestFuseConvertRoundTrip(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	base := filepath.Join(home, ".claude")
	if err := os.MkdirAll(filepath.Join(base, "projects"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "settings.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Probe first so a grant-less machine skips instead of failing.
	probeSrc, probeMnt := t.TempDir(), t.TempDir()
	if err := (&overlay.FuseProvider{}).Setup(probeSrc, probeMnt); err != nil {
		t.Skipf("fuse-t mount unavailable (acceptable; symlink is the default): %v", err)
	}
	if err := (&overlay.FuseProvider{}).Teardown(probeSrc, probeMnt); err != nil {
		t.Fatalf("probe teardown: %v", err)
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
	m := &Manager{Store: st, OverlayFor: inProcessFuseSeam}

	fused, err := m.ConvertOverlay(a, fkoverlay.BackendNFS)
	if err != nil {
		t.Fatalf("convert to fuse: %v", err)
	}
	t.Cleanup(func() { _ = (&overlay.FuseProvider{}).Teardown(base, dir) })
	if fused.OverlayKind != "nfs" {
		t.Fatalf("row = %s, want fuse", fused.OverlayKind)
	}

	priv := fkoverlay.FusePrivateRoot(dir)
	// Identity readable THROUGH the live mount, physically homed in .private.
	if got, err := os.ReadFile(filepath.Join(dir, ".claude.json")); err != nil || string(got) != identityJSON { //nolint:gosec // G304: path is under the test's own t.TempDir(), not external input
		t.Fatalf("identity through mount = %q err=%v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(priv, ".claude.json")); err != nil || string(got) != identityJSON { //nolint:gosec // G304: path is under the test's own t.TempDir(), not external input
		t.Fatalf("identity in private root = %q err=%v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(dir, "backups", "b.bak")); err != nil || string(got) != "bak" { //nolint:gosec // G304: path is under the test's own t.TempDir(), not external input
		t.Fatalf("backups through mount = %q err=%v", got, err)
	}
	// Shared entries serve from base through the mirror.
	if _, err := os.Stat(filepath.Join(dir, "settings.json")); err != nil {
		t.Fatalf("shared entry not visible through mount: %v", err)
	}
	// Hazard canary: nothing leaked into the real base — no identity, and not
	// a single symlink (a symlink written through the mirror would land here).
	if _, err := os.Lstat(filepath.Join(base, ".claude.json")); !os.IsNotExist(err) {
		t.Fatal("identity leaked into base")
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Type()&os.ModeSymlink != 0 {
			t.Fatalf("symlink %q leaked into base", e.Name())
		}
	}

	// The mirror is live, so Mounted must see dir as a mountpoint. The kernel
	// records this $TMPDIR mount under its firmlinked f_mntonname
	// (/private/var/...), which dir's raw /var/... spelling would miss — this
	// proves mountCandidates' parent-resolved second spelling against a REAL
	// kernel mount table.
	if !overlay.Mounted(dir) {
		t.Fatal("dir not seen as mounted while fuse mirror is live")
	}

	// Reverse — the rollout's rollback command.
	back, err := m.ConvertOverlay(fused, fkoverlay.BackendSymlink)
	if err != nil {
		t.Fatalf("convert back to symlink: %v", err)
	}
	if back.OverlayKind != "symlink" {
		t.Fatalf("row = %s, want symlink", back.OverlayKind)
	}
	if overlay.Mounted(dir) {
		t.Fatal("dir still mounted after reverse conversion")
	}
	if got, err := os.ReadFile(filepath.Join(dir, ".claude.json")); err != nil || string(got) != identityJSON { //nolint:gosec // G304: path is under the test's own t.TempDir(), not external input
		t.Fatalf("identity after reverse = %q err=%v", got, err)
	}
	if target, err := os.Readlink(filepath.Join(dir, "projects")); err != nil || target != filepath.Join(base, "projects") {
		t.Fatalf("projects link after reverse = %q err=%v", target, err)
	}
	if got, err := os.ReadFile(filepath.Join(dir, "backups", "b.bak")); err != nil || string(got) != "bak" { //nolint:gosec // G304: path is under the test's own t.TempDir(), not external input
		t.Fatalf("backups after reverse = %q err=%v", got, err)
	}
}

// TestFuseConvertIdentityVerifiedAgainstForeignBase pins the migrate↔merge
// interplay at convertToFuse's post-mount identity verification: that re-read
// traverses the live merged view, and with a base ~/.claude.json carrying a
// DIFFERENT oauthAccount the verification must still see the account's own
// identity — oauthAccount is blacklisted, so the merged read sources it from
// the private file. A leak would surface the base UUID, mismatch, and roll the
// conversion back. Requires fuse-t like TestFuseConvertRoundTrip; skips when a
// mount cannot come up.
func TestFuseConvertIdentityVerifiedAgainstForeignBase(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	base := filepath.Join(home, ".claude")
	if err := os.MkdirAll(filepath.Join(base, "projects"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "settings.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	// The base SIBLING ~/.claude.json: a foreign identity plus a shareable
	// marker proving the merged view (not a raw passthrough) serves the reads.
	const foreignBase = `{"theme":"light","mergedViewMarker":true,"oauthAccount":{"accountUuid":"u-IMPOSTOR","emailAddress":"x@example.com"}}`
	if err := os.WriteFile(filepath.Join(home, ".claude.json"), []byte(foreignBase), 0o600); err != nil {
		t.Fatal(err)
	}

	probeSrc, probeMnt := t.TempDir(), t.TempDir()
	if err := (&overlay.FuseProvider{}).Setup(probeSrc, probeMnt); err != nil {
		t.Skipf("fuse-t mount unavailable (acceptable; symlink is the default): %v", err)
	}
	if err := (&overlay.FuseProvider{}).Teardown(probeSrc, probeMnt); err != nil {
		t.Fatalf("probe teardown: %v", err)
	}

	dir := filepath.Join(home, "acct-01")
	if err := newSymlinkProvider().Setup(base, dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".claude.json"), []byte(identityJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	st := openTestStore(t)
	a := store.Account{ID: 1, ConfigDir: dir, KeychainService: "svc", KeychainAccount: "user", OverlayKind: "symlink"}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	m := &Manager{Store: st, OverlayFor: inProcessFuseSeam}

	fused, err := m.ConvertOverlay(a, fkoverlay.BackendNFS)
	if err != nil {
		t.Fatalf("convert to fuse with a foreign base identity: %v", err)
	}
	t.Cleanup(func() { _ = (&overlay.FuseProvider{}).Teardown(base, dir) })
	if fused.OverlayKind != "nfs" {
		t.Fatalf("row = %s, want fuse", fused.OverlayKind)
	}

	// The same read convertToFuse just verified: identity through the live
	// merged view is the account's own, never the base's.
	id, err := readIdentity(filepath.Join(dir, ".claude.json"))
	if err != nil {
		t.Fatalf("identity through merged mount: %v", err)
	}
	if id.AccountUUID != "u-1" || id.EmailAddress != "a@example.com" {
		t.Fatalf("identity through merged mount = %+v, want the account's u-1/a@example.com", id)
	}
	// The merged view is provably live (base's shareable marker shows), so the
	// identity above came through the merge, not a passthrough.
	got := rawTop(t, readFile(t, filepath.Join(dir, ".claude.json")))
	if string(got["mergedViewMarker"]) != `true` || string(got["theme"]) != `"light"` {
		t.Fatalf("merged view not live through the mount: marker=%s theme=%s", got["mergedViewMarker"], got["theme"])
	}
	// Neither side was rewritten: a conversion is not a commit, so the private
	// file keeps the identity verbatim and the base sibling keeps its own.
	if gotPriv := readFileT(t, filepath.Join(fkoverlay.FusePrivateRoot(dir), ".claude.json")); gotPriv != identityJSON {
		t.Fatalf("private file = %q, want the untouched identity", gotPriv)
	}
	if gotBase := readFileT(t, filepath.Join(home, ".claude.json")); gotBase != foreignBase {
		t.Fatalf("base sibling rewritten by conversion: %q", gotBase)
	}
}
