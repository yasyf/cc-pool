//go:build fuse && cgo && darwin

package overlay

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"golang.org/x/sys/unix"
)

// mountProbeMirror mounts a passthrough mirror of base at mnt, skipping the
// test when fuse-t is unavailable and tearing the mount down on cleanup. base
// is seeded with one real entry so MountAlive's liveness compare is
// deterministic (an empty base is vacuously live before the mount lands).
func mountProbeMirror(t *testing.T, base, mnt string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(base, "hello.txt"), []byte("hi"), 0o644); err != nil { //nolint:gosec // G306: perms are intentional for this test fixture file
		t.Fatal(err)
	}
	p := &FuseProvider{}
	if err := p.Setup(base, mnt); err != nil {
		t.Skipf("fuse-t mount unavailable (acceptable; symlink is the default): %v", err)
	}
	t.Cleanup(func() { _ = p.Teardown(base, mnt) })
}

// readProbeThroughMount reads the full probe file through the kernel mount
// and verifies it is exactly FillProbe's pattern for the nonce in its header,
// returning that nonce.
func readProbeThroughMount(t *testing.T, mnt string) uint64 {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(mnt, ProbeFileName)) //nolint:gosec // G304: path is under the test's own t.TempDir(), not external input
	if err != nil {
		t.Fatalf("read probe through mount: %v", err)
	}
	if len(got) != ProbeFileSize {
		t.Fatalf("probe read = %d bytes, want %d", len(got), ProbeFileSize)
	}
	nonce := binary.BigEndian.Uint64(got[:8])
	want := make([]byte, ProbeFileSize)
	FillProbe(nonce, 0, want)
	if !bytes.Equal(got, want) {
		t.Fatalf("probe bytes diverge from FillProbe(%#x) at offset %d", nonce, firstDiff(got, want))
	}
	return nonce
}

// firstDiff returns the first index where a and b differ, or -1.
func firstDiff(a, b []byte) int {
	for i := range a {
		if a[i] != b[i] {
			return i
		}
	}
	return -1
}

// TestMirrorServesSyntheticProbeFile reads the full 2 MiB virtual probe file
// through a real fuse-t mount twice. Each read must match FillProbe for the
// nonce in its own header, and the two opens must observe DIFFERENT nonces —
// the anti-page-cache assertion: identical nonces mean the NFS client served
// the second probe from cached pages and a deep probe would prove nothing.
func TestMirrorServesSyntheticProbeFile(t *testing.T) {
	base, mnt := t.TempDir(), t.TempDir()
	mountProbeMirror(t, base, mnt)
	probePath := filepath.Join(mnt, ProbeFileName)

	fi, err := os.Stat(probePath)
	if err != nil {
		t.Fatalf("stat probe through mount: %v", err)
	}
	if fi.Size() != ProbeFileSize {
		t.Errorf("probe size = %d, want %d", fi.Size(), ProbeFileSize)
	}
	if !fi.Mode().IsRegular() {
		t.Errorf("probe mode = %v, want a regular file", fi.Mode())
	}
	if fi.Mode().Perm()&0o222 != 0 {
		t.Errorf("probe mode = %v, want read-only", fi.Mode())
	}

	nonce1 := readProbeThroughMount(t, mnt)
	nonce2 := readProbeThroughMount(t, mnt)
	if nonce1 == nonce2 {
		t.Fatalf("two consecutive opens observed the same nonce %#x — the page cache served a stale probe", nonce1)
	}
}

// TestMirrorProbeFileHiddenFromReaddir: Readdir fills only real entries — the
// virtual probe file is stat-able and readable but never listed.
func TestMirrorProbeFileHiddenFromReaddir(t *testing.T) {
	base, mnt := t.TempDir(), t.TempDir()
	mountProbeMirror(t, base, mnt)

	entries, err := os.ReadDir(mnt)
	if err != nil {
		t.Fatalf("readdir through mount: %v", err)
	}
	var sawReal bool
	for _, e := range entries {
		if e.Name() == ProbeFileName {
			t.Errorf("readdir listed %s — the probe file must stay hidden", ProbeFileName)
		}
		if e.Name() == "hello.txt" {
			sawReal = true
		}
	}
	if !sawReal {
		t.Error("readdir did not list the real base entry hello.txt")
	}
	// Hidden, not absent: the probe must still answer a direct stat.
	if _, err := os.Stat(filepath.Join(mnt, ProbeFileName)); err != nil {
		t.Errorf("stat of the unlisted probe file failed: %v", err)
	}
}

// TestMirrorProbeFileRejectsWrites: every write-shaped op against the probe
// path is refused, and none of them leaks a real .ccp-probe into base.
func TestMirrorProbeFileRejectsWrites(t *testing.T) {
	base, mnt := t.TempDir(), t.TempDir()
	mountProbeMirror(t, base, mnt)
	probePath := filepath.Join(mnt, ProbeFileName)
	victim := filepath.Join(mnt, "victim.txt")
	if err := os.WriteFile(victim, []byte("keep"), 0o644); err != nil { //nolint:gosec // G306: perms are intentional for this test fixture file
		t.Fatal(err)
	}

	ops := []struct {
		name string
		op   func() error
	}{
		{"open for write", func() error {
			f, err := os.OpenFile(probePath, os.O_WRONLY, 0) //nolint:gosec // G304: path is under the test's own t.TempDir(), not external input
			if err == nil {
				_ = f.Close()
			}
			return err
		}},
		{"open read-write", func() error {
			f, err := os.OpenFile(probePath, os.O_RDWR, 0) //nolint:gosec // G304: path is under the test's own t.TempDir(), not external input
			if err == nil {
				_ = f.Close()
			}
			return err
		}},
		{"create over", func() error {
			f, err := os.OpenFile(probePath, os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // G304: path is under the test's own t.TempDir(), not external input
			if err == nil {
				_ = f.Close()
			}
			return err
		}},
		{"create exclusive", func() error {
			f, err := os.OpenFile(probePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // G304: path is under the test's own t.TempDir(), not external input
			if err == nil {
				_ = f.Close()
			}
			return err
		}},
		{"unlink", func() error { return os.Remove(probePath) }},
		{"rmdir", func() error { return syscall.Rmdir(probePath) }},
		{"truncate", func() error { return os.Truncate(probePath, 0) }},
		{"chmod", func() error { return os.Chmod(probePath, 0o600) }},
		// chown to the current owner is a kernel/NFS no-op that issues no SETATTR
		// and never reaches the fs, so the Chown guard is pinned directly in
		// TestMirrorProbeOpsGuardedWithoutMount instead of through the mount.
		{"chtimes", func() error { return os.Chtimes(probePath, time.Now(), time.Now()) }},
		{"setxattr", func() error { return unix.Setxattr(probePath, "user.ccp-test", []byte("x"), 0) }},
		{"removexattr", func() error { return unix.Removexattr(probePath, "user.ccp-test") }},
		{"mkdir", func() error { return os.Mkdir(probePath, 0o755) }}, //nolint:gosec // G301: dir perms are intentional for this test's fixture layout
		{"symlink onto", func() error { return os.Symlink(victim, probePath) }},
		{"link onto", func() error { return os.Link(victim, probePath) }},
		{"link away", func() error { return os.Link(probePath, filepath.Join(mnt, "hardlinked")) }},
		{"rename away", func() error { return os.Rename(probePath, filepath.Join(mnt, "stolen")) }},
		{"rename onto", func() error { return os.Rename(victim, probePath) }},
	}
	for _, tc := range ops {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.op(); err == nil {
				t.Fatalf("%s succeeded, want refusal", tc.name)
			}
		})
	}

	// The refusals must leave everything intact: the virtual probe still
	// serves full-size, the victim survived, and base gained no real probe.
	fi, err := os.Stat(probePath)
	if err != nil || fi.Size() != ProbeFileSize {
		t.Errorf("probe after refused writes: size=%v err=%v, want %d bytes", fi, err, ProbeFileSize)
	}
	if _, err := os.Stat(filepath.Join(base, "victim.txt")); err != nil {
		t.Errorf("victim file lost to a refused rename: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(base, ProbeFileName)); !os.IsNotExist(err) {
		t.Errorf("a write op leaked a real %s into base (err %v) — base must never be touched for probing", ProbeFileName, err)
	}
}

// TestMirrorProbeFileShadowsRealBaseEntry: a real file named .ccp-probe in
// base is shadowed by the virtual one — intercept-before-real() guarantees
// neither reads nor metadata mutations through the mount ever reach it, and
// it stays byte- and attribute-identical.
func TestMirrorProbeFileShadowsRealBaseEntry(t *testing.T) {
	base, mnt := t.TempDir(), t.TempDir()
	basePath := filepath.Join(base, ProbeFileName)
	junk := []byte("junk left by something that is not us")
	if err := os.WriteFile(basePath, junk, 0o644); err != nil { //nolint:gosec // G306: perms are intentional for this test fixture file
		t.Fatal(err)
	}
	before, err := os.Lstat(basePath)
	if err != nil {
		t.Fatal(err)
	}
	mountProbeMirror(t, base, mnt)
	probePath := filepath.Join(mnt, ProbeFileName)

	fi, err := os.Stat(probePath)
	if err != nil {
		t.Fatalf("stat probe through mount: %v", err)
	}
	if fi.Size() != ProbeFileSize {
		t.Fatalf("probe size through mount = %d, want the virtual %d — the real base entry leaked through", fi.Size(), ProbeFileSize)
	}
	readProbeThroughMount(t, mnt) // full virtual content, never the junk

	// The shadow entry's NAME must not leak into the listing either: the probe
	// is never listed by Readdir, even when base holds a real .ccp-probe.
	entries, err := os.ReadDir(mnt)
	if err != nil {
		t.Fatalf("readdir through mount: %v", err)
	}
	for _, e := range entries {
		if e.Name() == ProbeFileName {
			t.Errorf("readdir listed %s — the shadowed base entry's name must stay hidden", ProbeFileName)
		}
	}

	// Metadata mutations through the probe name must be refused at the fs, not
	// forwarded to the shadowed base entry — touch/xattr/chown/link against
	// mnt/.ccp-probe must never land on base/.ccp-probe.
	mutations := []struct {
		name string
		op   func() error
	}{
		{"chtimes", func() error { return os.Chtimes(probePath, time.Unix(1, 0), time.Unix(1, 0)) }},
		// chown-to-self is a kernel/NFS no-op (no SETATTR), pinned at the fs level
		// in TestMirrorProbeOpsGuardedWithoutMount; the base-unchanged check below
		// already proves no chown lands on the shadowed base entry.
		{"setxattr", func() error { return unix.Setxattr(probePath, "user.ccp-shadow-test", []byte("x"), 0) }},
		{"removexattr", func() error { return unix.Removexattr(probePath, "user.ccp-shadow-test") }},
		{"truncate", func() error { return os.Truncate(probePath, 1) }},
		{"link away", func() error { return os.Link(probePath, filepath.Join(mnt, "hardlinked")) }},
	}
	for _, tc := range mutations {
		if err := tc.op(); err == nil {
			t.Errorf("%s through the probe path succeeded, want refusal", tc.name)
		}
	}

	back, err := os.ReadFile(basePath) //nolint:gosec // G304: path is under the test's own t.TempDir(), not external input
	if err != nil {
		t.Fatalf("read real base entry: %v", err)
	}
	if !bytes.Equal(back, junk) {
		t.Fatalf("real base %s changed under the shadow:\n%q", ProbeFileName, back)
	}
	after, err := os.Lstat(basePath)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Errorf("base %s mtime moved %v -> %v — a refused chtimes leaked through the probe name", ProbeFileName, before.ModTime(), after.ModTime())
	}
	if _, err := unix.Lgetxattr(basePath, "user.ccp-shadow-test", nil); err == nil {
		t.Errorf("a refused setxattr landed user.ccp-shadow-test on the shadowed base entry")
	}
	if _, err := os.Lstat(filepath.Join(base, "hardlinked")); !os.IsNotExist(err) {
		t.Errorf("a refused link hardlinked the shadowed base entry into base (err %v)", err)
	}
}

// TestDeepProbeRealMountRoundTrip: DeepProbeWithin against a real healthy
// mount succeeds, twice — the second probe must not be satisfied by the first
// one's cached pages (F_NOCACHE + advancing mtime + per-open nonce).
func TestDeepProbeRealMountRoundTrip(t *testing.T) {
	base, mnt := t.TempDir(), t.TempDir()
	mountProbeMirror(t, base, mnt)
	for i := 1; i <= 2; i++ {
		if err := DeepProbeWithin(mnt); err != nil {
			t.Fatalf("DeepProbeWithin pass %d = %v, want nil against a healthy mount", i, err)
		}
	}
}

// TestProbeFhRangeDisjoint pins the handle-space partition without a mount:
// probe handles live in [1<<61, 1<<62), merged-view synthetic handles in
// [1<<62, ^uint64(0)), and the two predicates can never both hold.
func TestProbeFhRangeDisjoint(t *testing.T) {
	cases := []struct {
		name          string
		fh            uint64
		wantProbe     bool
		wantSynthetic bool
	}{
		{"real kernel fd", 7, false, false},
		{"first probe handle", probeFhBase, true, false},
		{"last probe handle", syntheticFhBase - 1, true, false},
		{"first synthetic merged handle", syntheticFhBase, false, true},
		{"no handle", ^uint64(0), false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := probeFh(tc.fh); got != tc.wantProbe {
				t.Errorf("probeFh(%#x) = %v, want %v", tc.fh, got, tc.wantProbe)
			}
			if got := syntheticFh(tc.fh); got != tc.wantSynthetic {
				t.Errorf("syntheticFh(%#x) = %v, want %v", tc.fh, got, tc.wantSynthetic)
			}
		})
	}

	// A handle minted by the view itself lands in the probe range, and a
	// write-access open is refused before any handle is minted.
	v := newProbeView()
	st, fh := v.open(syscall.O_RDONLY)
	if st != 0 || !probeFh(fh) || syntheticFh(fh) {
		t.Fatalf("open(O_RDONLY) = (%d, %#x), want status 0 and a probe-range handle", st, fh)
	}
	v.release(fh)
	if st, _ := v.open(syscall.O_WRONLY); st != -int(syscall.EACCES) {
		t.Fatalf("open(O_WRONLY) = %d, want -EACCES", st)
	}
	if st, _ := v.open(syscall.O_RDWR); st != -int(syscall.EACCES) {
		t.Fatalf("open(O_RDWR) = %d, want -EACCES", st)
	}
}

// TestMirrorProbeOpsGuardedWithoutMount pins the fs-level probe guards by
// calling the mirror's methods directly: the kernel-mount tests above can
// never reach some of them (the client VFS fails rmdir/opendir of a node
// Getattr reports as S_IFREG before any FUSE op is issued), so only a direct
// call proves the guard holds if that reachability ever changes.
func TestMirrorProbeOpsGuardedWithoutMount(t *testing.T) {
	base, priv := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(base, "hello.txt"), []byte("hi"), 0o644); err != nil { //nolint:gosec // G306: perms are intentional for this test fixture file
		t.Fatal(err)
	}
	fs := newMirrorFS(base, priv, filepath.Join(t.TempDir(), ".claude.json"))

	// Statfs before any shadow entry exists: a passthrough would ENOENT.
	var sfs fuse.Statfs_t
	if st := fs.Statfs(probeFusePath, &sfs); st != 0 || sfs.Bsize == 0 {
		t.Errorf("Statfs(%s) = %d (Bsize %d), want 0 with root's stats", probeFusePath, st, sfs.Bsize)
	}

	// A shadowed base DIRECTORY named .ccp-probe is the one input where an
	// unguarded Rmdir would delete from base.
	shadowDir := filepath.Join(base, ProbeFileName)
	if err := os.Mkdir(shadowDir, 0o755); err != nil { //nolint:gosec // G301: dir perms are intentional for this test's fixture layout
		t.Fatal(err)
	}
	if st := fs.Rmdir(probeFusePath); st != -int(syscall.EPERM) {
		t.Errorf("Rmdir(%s) = %d, want -EPERM", probeFusePath, st)
	}
	if _, err := os.Stat(shadowDir); err != nil {
		t.Errorf("shadowed base directory gone after refused Rmdir: %v", err)
	}
	if st, fh := fs.Opendir(probeFusePath); st != -int(syscall.ENOTDIR) || fh != ^uint64(0) {
		t.Errorf("Opendir(%s) = (%d, %#x), want (-ENOTDIR, no handle)", probeFusePath, st, fh)
	}

	// Metadata-mutation guards. A through-mount chown to the current owner is a
	// kernel/NFS no-op that issues no SETATTR, so only a direct call proves the
	// Chown guard; chmod/utimens are pinned here too for symmetry.
	if st := fs.Chmod(probeFusePath, 0o600); st != -int(syscall.EPERM) {
		t.Errorf("Chmod(%s) = %d, want -EPERM", probeFusePath, st)
	}
	if st := fs.Chown(probeFusePath, uint32(os.Getuid()), uint32(os.Getgid())); st != -int(syscall.EPERM) { //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
		t.Errorf("Chown(%s) = %d, want -EPERM", probeFusePath, st)
	}
	if st := fs.Utimens(probeFusePath, []fuse.Timespec{{Sec: 1}, {Sec: 1}}); st != -int(syscall.EPERM) {
		t.Errorf("Utimens(%s) = %d, want -EPERM", probeFusePath, st)
	}

	// Readdir of root hides the shadow entry's name but lists real entries.
	listed := map[string]bool{}
	if st := fs.Readdir("/", func(name string, _ *fuse.Stat_t, _ int64) bool {
		listed[name] = true
		return true
	}, 0, ^uint64(0)); st != 0 {
		t.Fatalf("Readdir(/) = %d, want 0", st)
	}
	if listed[ProbeFileName] {
		t.Errorf("Readdir listed %s despite the shadow filter", ProbeFileName)
	}
	if !listed["hello.txt"] {
		t.Errorf("Readdir hid the real entry hello.txt; listed = %v", listed)
	}

	// The filter is root-only: a nested .ccp-probe is an ordinary real file.
	sub := filepath.Join(base, "projects")
	if err := os.Mkdir(sub, 0o755); err != nil { //nolint:gosec // G301: dir perms are intentional for this test's fixture layout
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, ProbeFileName), nil, 0o644); err != nil { //nolint:gosec // G306: perms are intentional for this test fixture file
		t.Fatal(err)
	}
	nested := map[string]bool{}
	if st := fs.Readdir("/projects", func(name string, _ *fuse.Stat_t, _ int64) bool {
		nested[name] = true
		return true
	}, 0, ^uint64(0)); st != 0 {
		t.Fatalf("Readdir(/projects) = %d, want 0", st)
	}
	if !nested[ProbeFileName] {
		t.Errorf("nested %s missing from /projects listing — the root-only filter over-applied", ProbeFileName)
	}
}
