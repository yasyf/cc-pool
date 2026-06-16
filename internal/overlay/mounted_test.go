package overlay

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"golang.org/x/sys/unix"
)

// statfsAt builds one mount-table entry with mntonname as its mountpoint
// (f_mntonname) and mntfromname as its source (f_mntfromname), copied into the
// fixed [1024]byte fields exactly as the kernel reports them. Short names leave
// the trailing bytes NUL — the condition unix.ByteSliceToString must stop at.
func statfsAt(mntonname, mntfromname string) unix.Statfs_t {
	var s unix.Statfs_t
	copy(s.Mntonname[:], mntonname)
	copy(s.Mntfromname[:], mntfromname)
	return s
}

// TestMountpointIn pins the byte-exact mountpoint membership test over
// hand-built mount tables: only an entry whose f_mntonname equals a candidate
// matches, prefixes and sub-paths do not, the parent-resolved spelling is
// honored, f_mntfromname is never consulted, and the [1024]byte name is read
// as a NUL-terminated C string.
func TestMountpointIn(t *testing.T) {
	// A small fixed table reused across the membership cases.
	table := []unix.Statfs_t{
		statfsAt("/", "/dev/disk1s1"),
		statfsAt("/dev", "devfs"),
		statfsAt("/System/Volumes/Data", "/dev/disk1s2"),
	}
	cases := []struct {
		name       string
		table      []unix.Statfs_t
		candidates []string
		want       bool
	}{
		{
			name:       "exact match is a mountpoint",
			table:      table,
			candidates: []string{"/dev"},
			want:       true,
		},
		{
			name:       "absent path is not a mountpoint",
			table:      table,
			candidates: []string{"/no/such/mount"},
			want:       false,
		},
		{
			name:       "a later candidate matches when the first does not",
			table:      table,
			candidates: []string{"/raw/spelling", "/dev"},
			want:       true,
		},
		{
			name:       "a candidate that is a prefix of a mountpoint does not match",
			table:      []unix.Statfs_t{statfsAt("/var/folders/x/T/acct-01", "fuse")},
			candidates: []string{"/var"},
			want:       false,
		},
		{
			name:       "a longer prefix of a mountpoint still does not match",
			table:      []unix.Statfs_t{statfsAt("/var/folders/x/T/acct-01", "fuse")},
			candidates: []string{"/var/folders/x"},
			want:       false,
		},
		{
			name:       "a dir under a non-root mountpoint is not itself a mountpoint",
			table:      []unix.Statfs_t{statfsAt("/mnt/data", "/dev/disk2")},
			candidates: []string{"/mnt/data/sub"},
			want:       false,
		},
		{
			name:       "the parent-resolved candidate matches the kernel's firmlinked spelling",
			table:      []unix.Statfs_t{statfsAt("/private/var/folders/x/T/acct-01", "fuse")},
			candidates: []string{"/var/folders/x/T/acct-01", "/private/var/folders/x/T/acct-01"},
			want:       true,
		},
		{
			name:       "f_mntfromname is never consulted",
			table:      []unix.Statfs_t{statfsAt("/somewhere/else", "/p/acct-01")},
			candidates: []string{"/p/acct-01"},
			want:       false,
		},
		{
			name:       "the name is NUL-terminated and bytes after the first NUL are ignored",
			table:      []unix.Statfs_t{statfsAt("/p/acct-01\x00stale-leftover", "fuse")},
			candidates: []string{"/p/acct-01"},
			want:       true,
		},
		{
			name:       "an empty table never matches",
			table:      nil,
			candidates: []string{"/dev"},
			want:       false,
		},
		{
			name:       "no candidates never match",
			table:      table,
			candidates: nil,
			want:       false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mountpointIn(tc.table, tc.candidates); got != tc.want {
				t.Errorf("mountpointIn = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestMountCandidates pins the spellings Mounted matches against the kernel:
// the cleaned raw dir is always first, a symlinked parent contributes its
// resolved spelling second, an already-resolved or unresolvable parent yields
// the raw spelling alone, and EvalSymlinks resolves only the parent — never
// dir — per AGENTS.md invariant #3.
func TestMountCandidates(t *testing.T) {
	t.Run("clean is applied and an unresolvable parent degrades to raw-only", func(t *testing.T) {
		// /p does not exist, so the parent cannot be resolved; the sole
		// candidate is the cleaned raw dir, with the ".." collapsed.
		got := mountCandidates("/p/x/../acct-01")
		want := []string{"/p/acct-01"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("mountCandidates = %v, want %v", got, want)
		}
	})

	t.Run("an already-resolved parent yields a single candidate", func(t *testing.T) {
		// EvalSymlinks is idempotent, so when dir's parent is already in its
		// real spelling the resolved candidate equals the raw one and is not
		// duplicated.
		realParent, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		dir := filepath.Join(realParent, "child")
		got := mountCandidates(dir)
		want := []string{dir}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("mountCandidates = %v, want %v", got, want)
		}
	})

	t.Run("a symlinked parent adds the resolved spelling, raw first", func(t *testing.T) {
		realParent, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(t.TempDir(), "link")
		if err := os.Symlink(realParent, link); err != nil {
			t.Fatal(err)
		}
		dir := filepath.Join(link, "child")
		got := mountCandidates(dir)
		want := []string{dir, filepath.Join(realParent, "child")}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("mountCandidates = %v, want %v", got, want)
		}
		// Raw-first: the unresolved spelling the caller passed is always
		// candidates[0]; the firmlink-resolved spelling is the fallback. dir's
		// own basename ("child") is never resolved, only the parent.
		if got[0] != dir {
			t.Errorf("candidates[0] = %q, want the raw dir %q", got[0], dir)
		}
	})
}

// TestMountedRealTable exercises Mounted against this machine's real kernel
// mount table: the root volume and devfs are always mountpoints, while a fresh
// temp dir and a missing path are not — the latter proving Mounted answers
// false without erroring or blocking on a path that does not exist.
func TestMountedRealTable(t *testing.T) {
	if !Mounted("/") {
		t.Error("Mounted(/) = false; the root volume is always a mountpoint")
	}
	if !Mounted("/dev") {
		t.Error("Mounted(/dev) = false; devfs is always a mountpoint")
	}
	if Mounted(t.TempDir()) {
		t.Error("Mounted(tempdir) = true; a fresh temp dir is not a mountpoint")
	}
	missing := filepath.Join(t.TempDir(), "definitely-not-a-mountpoint")
	if Mounted(missing) {
		t.Errorf("Mounted(%q) = true; a missing path is not a mountpoint", missing)
	}
}
