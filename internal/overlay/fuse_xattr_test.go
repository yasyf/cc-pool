//go:build fuse && cgo && darwin

package overlay

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/winfsp/cgofuse/fuse"
	"golang.org/x/sys/unix"
)

// listXattrNames collects every name the mirror's Listxattr yields for path.
func listXattrNames(t *testing.T, fs *mirrorFS, path string) []string {
	t.Helper()
	var names []string
	if st := fs.Listxattr(path, func(name string) bool {
		names = append(names, name)
		return true
	}); st != 0 {
		t.Fatalf("Listxattr(%q) = %d, want 0", path, st)
	}
	return names
}

// realXattrNames is the kernel's authoritative xattr list for a backing file —
// the oracle the mirror's Listxattr must reproduce exactly. The environment
// taxes freshly created files with kernel-attached attrs (com.apple.provenance
// here), so "exactly what the test set" is not a stable expectation; "exactly
// what the kernel reports" is.
func realXattrNames(t *testing.T, backing string) []string {
	t.Helper()
	sz, err := unix.Llistxattr(backing, nil)
	if err != nil {
		t.Fatalf("llistxattr %s: %v", backing, err)
	}
	if sz == 0 {
		return nil
	}
	buf := make([]byte, sz)
	n, err := unix.Llistxattr(backing, buf)
	if err != nil {
		t.Fatalf("llistxattr %s: %v", backing, err)
	}
	var names []string
	for _, name := range strings.Split(string(buf[:n]), "\x00") {
		if name != "" {
			names = append(names, name)
		}
	}
	return names
}

// TestMirrorXattrRoundTrip pins the full Set/Get/List/Remove cycle through the
// mirror's methods for both backing dirs: bytes must land on (and read back
// from) exactly the file fs.real routes to, byte-identical — including
// embedded NULs and high bytes, which xattr values may carry.
func TestMirrorXattrRoundTrip(t *testing.T) {
	const attr = "com.example.ccp-test"
	value := []byte("v\x00\xffbytes")
	cases := map[string]struct {
		fusePath string
		backing  func(home string) string
	}{
		"shared file backs onto root": {
			fusePath: "/settings.json",
			backing:  func(home string) string { return filepath.Join(home, ".claude", "settings.json") },
		},
		"private file backs onto privateRoot": {
			fusePath: "/.claude.json",
			backing:  func(home string) string { return filepath.Join(home, "acct.private", ".claude.json") },
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fs, home := newClaudeJSONMirror(t, "", "")
			backing := tc.backing(home)
			if err := os.WriteFile(backing, []byte("content"), 0o600); err != nil {
				t.Fatal(err)
			}

			if st := fs.Setxattr(tc.fusePath, attr, value, 0); st != 0 {
				t.Fatalf("Setxattr = %d, want 0", st)
			}
			truth := make([]byte, 64)
			n, err := unix.Lgetxattr(backing, attr, truth)
			if err != nil {
				t.Fatalf("xattr did not land on %s: %v", backing, err)
			}
			if !bytes.Equal(truth[:n], value) {
				t.Fatalf("backing xattr = %q, want %q", truth[:n], value)
			}

			st, got := fs.Getxattr(tc.fusePath, attr)
			if st != 0 {
				t.Fatalf("Getxattr = %d, want 0", st)
			}
			if !bytes.Equal(got, value) {
				t.Fatalf("Getxattr value = %q, want %q", got, value)
			}

			names := listXattrNames(t, fs, tc.fusePath)
			if !slices.Equal(names, realXattrNames(t, backing)) {
				t.Fatalf("Listxattr names = %v, want exactly the kernel's %v", names, realXattrNames(t, backing))
			}
			if !slices.Contains(names, attr) {
				t.Fatalf("Listxattr names = %v, want them to contain %s", names, attr)
			}

			if st := fs.Removexattr(tc.fusePath, attr); st != 0 {
				t.Fatalf("Removexattr = %d, want 0", st)
			}
			if st, _ := fs.Getxattr(tc.fusePath, attr); st != -int(unix.ENOATTR) {
				t.Fatalf("Getxattr after remove = %d, want -ENOATTR (%d)", st, -int(unix.ENOATTR))
			}
			if names := listXattrNames(t, fs, tc.fusePath); slices.Contains(names, attr) {
				t.Fatalf("Listxattr after remove = %v, want %s gone", names, attr)
			}
		})
	}
}

// TestMirrorXattrPrivateEntryRouting proves /.claude.json xattrs land on the
// private backing file even when a decoy of the same name exists under root —
// the merged read view covers content only, never xattr routing.
func TestMirrorXattrPrivateEntryRouting(t *testing.T) {
	const attr = "com.example.ccp-routing"
	fs, home := newClaudeJSONMirror(t, `{"a":1}`, "")
	decoy := filepath.Join(home, ".claude", ".claude.json")
	if err := os.WriteFile(decoy, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	if st := fs.Setxattr("/.claude.json", attr, []byte("priv"), 0); st != 0 {
		t.Fatalf("Setxattr = %d, want 0", st)
	}
	buf := make([]byte, 16)
	n, err := unix.Lgetxattr(filepath.Join(home, "acct.private", ".claude.json"), attr, buf)
	if err != nil || string(buf[:n]) != "priv" {
		t.Fatalf("private file xattr = %q, %v; want %q on privateRoot's file", buf[:n], err, "priv")
	}
	if _, err := unix.Lgetxattr(decoy, attr, nil); err != unix.ENOATTR {
		t.Fatalf("root decoy Lgetxattr err = %v, want ENOATTR (xattr leaked into root)", err)
	}
}

// TestMirrorXattrSidecarRouting pins B4: a top-level AppleDouble sidecar
// "._<name>" routes wherever "<name>" routes, so a sidecar of a private file
// can never land orphaned in the shared base.
func TestMirrorXattrSidecarRouting(t *testing.T) {
	fs := newMirrorFS("/base", "/priv", "/.claude.json")
	cases := map[string]string{ //nolint:gosec // G101: a test fixture map of names, not real credentials
		"/._.claude.json":              "/priv/._.claude.json",
		"/._.claude.json.tmp.ab12cd34": "/priv/._.claude.json.tmp.ab12cd34",
		"/._.credentials.json":         "/priv/._.credentials.json",
		"/._daemon":                    "/priv/._daemon",
		"/._backups":                   "/priv/._backups",
		"/._settings.json":             "/base/._settings.json",
		"/._projects":                  "/base/._projects",
		"/projects/._p.json":           "/base/projects/._p.json", // only TOP-level sidecars reroute
	}
	for in, want := range cases {
		if got := fs.real(in); got != want {
			t.Errorf("real(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestMirrorXattrSetxattrErrnoTranslation pins the one errno rewrite in the
// xattr layer: ENOTSUP becomes EPERM (only ENOTSUP from setxattr trips xnu's
// AppleDouble ._ fallback), everything else passes through. No input may ever
// map to -ENOTSUP or -ENOSYS.
func TestMirrorXattrSetxattrErrnoTranslation(t *testing.T) {
	cases := map[string]struct {
		err  error
		want int
	}{
		"nil is success":                      {nil, 0},
		"ENOTSUP translated to EPERM":         {unix.ENOTSUP, -int(syscall.EPERM)},
		"EPERM passes (com.apple.provenance)": {unix.EPERM, -int(syscall.EPERM)},
		"ENOENT passes":                       {unix.ENOENT, -int(syscall.ENOENT)},
		"ENOATTR passes":                      {unix.ENOATTR, -int(unix.ENOATTR)},
		"EACCES passes":                       {unix.EACCES, -int(syscall.EACCES)},
		"non-errno error degrades to EIO":     {os.ErrClosed, -int(syscall.EIO)},
	}
	for name, tc := range cases {
		got := setxattrErrno(tc.err)
		if got != tc.want {
			t.Errorf("%s: setxattrErrno = %d, want %d", name, got, tc.want)
		}
		if got == -int(syscall.ENOTSUP) || got == -int(syscall.ENOSYS) {
			t.Errorf("%s: setxattrErrno = %d (ENOTSUP/ENOSYS) — re-triggers the AppleDouble fallback", name, got)
		}
	}
}

// TestMirrorXattrErrnos pins the failure-path statuses of every op: real
// errnos from the backing fs, never the -ENOSYS a missing implementation
// would yield (fuse-t requires Listxattr implemented or Getxattr fails,
// fuse-t issue #62).
func TestMirrorXattrErrnos(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, "", "")
	if err := os.WriteFile(filepath.Join(home, ".claude", "settings.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	if st, _ := fs.Getxattr("/settings.json", "com.example.absent"); st != -int(unix.ENOATTR) {
		t.Errorf("Getxattr missing attr = %d, want -ENOATTR (%d)", st, -int(unix.ENOATTR))
	}
	if st := fs.Removexattr("/settings.json", "com.example.absent"); st != -int(unix.ENOATTR) {
		t.Errorf("Removexattr missing attr = %d, want -ENOATTR (%d)", st, -int(unix.ENOATTR))
	}
	if st := fs.Setxattr("/nope.txt", "com.example.x", []byte("v"), 0); st != -int(syscall.ENOENT) {
		t.Errorf("Setxattr missing file = %d, want -ENOENT", st)
	}
	if st := fs.Listxattr("/nope.txt", func(string) bool { return true }); st != -int(syscall.ENOENT) {
		t.Errorf("Listxattr missing file = %d, want -ENOENT", st)
	}
}

// TestMirrorXattrZeroLengthAndOverwrite pins the fuse-t #57 (zero-length
// value) and #74 (consecutive setxattr) shapes at the method level — OUR
// layer must round-trip an empty value and let the last of several writes to
// one name win byte-exactly, whatever fuse-t does above it.
func TestMirrorXattrZeroLengthAndOverwrite(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, "", "")
	if err := os.WriteFile(filepath.Join(home, ".claude", "settings.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	if st := fs.Setxattr("/settings.json", "com.example.empty", []byte{}, 0); st != 0 {
		t.Fatalf("Setxattr zero-length = %d, want 0", st)
	}
	st, got := fs.Getxattr("/settings.json", "com.example.empty")
	if st != 0 || len(got) != 0 {
		t.Fatalf("Getxattr zero-length = (%d, %q), want (0, empty)", st, got)
	}
	if names := listXattrNames(t, fs, "/settings.json"); !slices.Contains(names, "com.example.empty") {
		t.Fatalf("Listxattr = %v, want it to contain the zero-length attr", names)
	}

	// Consecutive overwrites of one name: longer→shorter→longer, the last
	// write wins byte-exactly with no residue from earlier values.
	for _, v := range []string{"first-long-value", "v2", "third-even-longer-value"} {
		if st := fs.Setxattr("/settings.json", "com.example.multi", []byte(v), 0); st != 0 {
			t.Fatalf("Setxattr %q = %d, want 0", v, st)
		}
	}
	st, got = fs.Getxattr("/settings.json", "com.example.multi")
	if st != 0 || string(got) != "third-even-longer-value" {
		t.Fatalf("Getxattr after overwrites = (%d, %q), want the final value", st, got)
	}
}

// TestMirrorXattrListEarlyStop pins the fill contract: a false return stops
// enumeration immediately.
func TestMirrorXattrListEarlyStop(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, "", "")
	if err := os.WriteFile(filepath.Join(home, ".claude", "settings.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, attr := range []string{"com.example.a", "com.example.b"} {
		if st := fs.Setxattr("/settings.json", attr, []byte("v"), 0); st != 0 {
			t.Fatalf("Setxattr %s = %d, want 0", attr, st)
		}
	}
	calls := 0
	if st := fs.Listxattr("/settings.json", func(string) bool {
		calls++
		return false
	}); st != 0 {
		t.Fatalf("Listxattr = %d, want 0", st)
	}
	if calls != 1 {
		t.Fatalf("fill called %d times after returning false, want exactly 1", calls)
	}
}

// TestMirrorXattrSidecarVisibility pins that the shared privateName predicate
// did not change visibility for normal names: a private-entry sidecar in
// privateRoot merges into the root listing, a non-private sidecar there stays
// invisible (and never shadows root's own), and Getattr of a non-private
// sidecar resolves root's file.
func TestMirrorXattrSidecarVisibility(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, "", "")
	root := filepath.Join(home, ".claude")
	priv := filepath.Join(home, "acct.private")
	files := map[string]string{
		filepath.Join(root, "settings.json"):    "0123456789",
		filepath.Join(root, "._settings.json"):  "rootside",         // size 8: root's own sidecar
		filepath.Join(priv, "._daemon"):         "x",                // private sidecar: must merge
		filepath.Join(priv, "._settings.json"):  "wrong-side-decoy", // size 16: must stay invisible
		filepath.Join(priv, "stray-shared.txt"): "x",                // non-private private-root stray: must stay invisible
	}
	for path, content := range files {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	var names []string
	if st := fs.Readdir("/", func(name string, _ *fuse.Stat_t, _ int64) bool {
		names = append(names, name)
		return true
	}, 0, ^uint64(0)); st != 0 {
		t.Fatalf("Readdir = %d, want 0", st)
	}
	slices.Sort(names)
	want := []string{".", "..", "._daemon", "._settings.json", "settings.json"}
	if !slices.Equal(names, want) {
		t.Fatalf("Readdir names = %v, want %v", names, want)
	}

	var st fuse.Stat_t
	if rc := fs.Getattr("/._settings.json", &st, ^uint64(0)); rc != 0 {
		t.Fatalf("Getattr(/._settings.json) = %d, want 0", rc)
	}
	if st.Size != int64(len("rootside")) {
		t.Fatalf("Getattr(/._settings.json).Size = %d, want root's %d (resolved the private decoy)", st.Size, len("rootside"))
	}
}

// TestSweepAppleDoubleLitter pins the one-time Setup sweep: only top-level
// "._" sidecars whose parent name is a PrivateEntry are unlinked; everything
// else in base — including a sidecar of a shared file — is untouched.
func TestSweepAppleDoubleLitter(t *testing.T) {
	base := t.TempDir()
	for _, name := range []string{
		"._.claude.json", "._.claude.json.tmp.123.abc", "._daemon", // litter: removed
		"._settings.json", "settings.json", // not mount-origin litter: kept
	} {
		if err := os.WriteFile(filepath.Join(base, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	sweepAppleDoubleLitter(base)

	entries, err := os.ReadDir(base)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(entries))
	for _, e := range entries {
		got = append(got, e.Name())
	}
	slices.Sort(got)
	want := []string{"._settings.json", "settings.json"}
	if !slices.Equal(got, want) {
		t.Fatalf("base after sweep = %v, want %v", got, want)
	}

	// Best-effort contract: an unreadable base is a no-op, never a failure
	// that could block a mount.
	sweepAppleDoubleLitter(filepath.Join(base, "missing"))
}

// TestSpikeNamedattrMount is the env-gated mounted acceptance test for the
// namedattr xattr path: through a real fuse-t mount, xattrs round-trip and NO
// AppleDouble "._" sidecar ever appears in the backing dir — the litter the
// nonamedattr default produced. Gated because it needs fuse-t installed plus
// the one-time Network Volumes TCC grant.
func TestSpikeNamedattrMount(t *testing.T) {
	if os.Getenv("CCP_FUSE_MOUNT_TESTS") != "1" {
		t.Skip("set CCP_FUSE_MOUNT_TESTS=1 to run the mounted xattr spike")
	}
	src := t.TempDir()
	mnt := t.TempDir()
	p := &FuseProvider{}
	if err := p.Setup(src, mnt); err != nil {
		t.Fatalf("mount: %v", err)
	}
	defer func() { _ = p.Teardown(src, mnt) }()

	assertNoSidecars := func(stage string) {
		t.Helper()
		entries, err := os.ReadDir(src)
		if err != nil {
			t.Fatalf("%s: read backing dir: %v", stage, err)
		}
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "._") {
				t.Fatalf("%s: AppleDouble sidecar %q littered the backing dir", stage, e.Name())
			}
		}
	}
	getxattr := func(path, attr string) []byte {
		t.Helper()
		buf := make([]byte, 256)
		n, err := unix.Getxattr(path, attr, buf)
		if err != nil {
			t.Fatalf("getxattr %s on %s: %v", attr, path, err)
		}
		return buf[:n]
	}

	const attr = "com.example.ccp-spike"
	file := filepath.Join(mnt, "spike.txt")
	if err := os.WriteFile(file, []byte("payload"), 0o644); err != nil { //nolint:gosec // G306: perms are intentional for this test fixture file
		t.Fatalf("write through mount: %v", err)
	}
	if err := unix.Setxattr(file, attr, []byte("v1"), 0); err != nil {
		t.Fatalf("setxattr through mount: %v", err)
	}
	if got := getxattr(file, attr); string(got) != "v1" {
		t.Fatalf("xattr through mount = %q, want v1", got)
	}
	lbuf := make([]byte, 1024)
	n, err := unix.Listxattr(file, lbuf)
	if err != nil {
		t.Fatalf("listxattr through mount: %v", err)
	}
	if !slices.Contains(strings.Split(string(lbuf[:n]), "\x00"), attr) {
		t.Fatalf("listxattr through mount = %q, want it to contain %s", lbuf[:n], attr)
	}
	assertNoSidecars("after set/get/list")

	renamed := filepath.Join(mnt, "spike-renamed.txt")
	if err := os.Rename(file, renamed); err != nil {
		t.Fatalf("rename through mount: %v", err)
	}
	if got := getxattr(renamed, attr); string(got) != "v1" {
		t.Fatalf("xattr after rename = %q, want v1 (xattr must survive rename)", got)
	}
	assertNoSidecars("after rename")

	for _, v := range []string{"second-value", "3"} {
		if err := unix.Setxattr(renamed, attr, []byte(v), 0); err != nil {
			t.Fatalf("consecutive setxattr %q: %v", v, err)
		}
	}
	if got := getxattr(renamed, attr); string(got) != "3" {
		t.Fatalf("xattr after consecutive sets = %q, want final value 3", got)
	}

	if err := unix.Setxattr(renamed, "com.example.ccp-empty", []byte{}, 0); err != nil {
		t.Fatalf("zero-length setxattr through mount: %v", err)
	}
	if got := getxattr(renamed, "com.example.ccp-empty"); len(got) != 0 {
		t.Fatalf("zero-length xattr = %q, want empty", got)
	}
	assertNoSidecars("after overwrite and zero-length")
}
