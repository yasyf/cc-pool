package overlay

import (
	"path/filepath"

	"golang.org/x/sys/unix"
)

// Mounted reports whether dir is currently a mountpoint. It reads the kernel's
// cached mount table with Getfsstat(MNT_NOWAIT) and checks dir for membership.
// MNT_NOWAIT returns the in-kernel snapshot without refreshing any filesystem,
// so the call cannot block — unlike an lstat of the mountpoint, which on a
// fuse-t mirror resolves INTO the NFS-backed fs (a GETATTR) and hangs forever
// on a wedged mount. Either Getfsstat call failing reads false, matching the
// old lstat-based predicate's error-as-not-mounted contract.
//
// Membership in the mount table is identical to the dev-id compare it replaces:
// on macOS a dir's device differs from its parent's iff it is a mountpoint, and
// a mountpoint is exactly what the kernel records as an f_mntonname. Only
// f_mntonname is consulted; f_mntfromname (the mount source) is never matched.
//
// Invariant #3 (AGENTS.md): Mounted is a read-only predicate and never
// realpaths, normalizes, stores, or hashes the account dir itself. The single
// EvalSymlinks in mountCandidates resolves only dir's PARENT — never dir — so
// the mount is never touched, and the resolved spelling is used for comparison
// only (the kernel records a $TMPDIR mount under /private/var/..., a spelling a
// byte-exact match would otherwise miss).
func Mounted(dir string) bool {
	n, err := unix.Getfsstat(nil, unix.MNT_NOWAIT)
	if err != nil {
		return false
	}
	// +4 headroom for a mount racing in between the sizing call and the fill.
	table := make([]unix.Statfs_t, n+4)
	n, err = unix.Getfsstat(table, unix.MNT_NOWAIT)
	if err != nil {
		return false
	}
	return mountpointIn(table[:n], mountCandidates(dir))
}

// mountpointIn reports whether any mount-table entry's mountpoint (f_mntonname)
// byte-exactly equals one of candidates. It never consults f_mntfromname: the
// question is "is this path a mountpoint", not "what is mounted there".
func mountpointIn(table []unix.Statfs_t, candidates []string) bool {
	for i := range table {
		name := unix.ByteSliceToString(table[i].Mntonname[:])
		for _, c := range candidates {
			if name == c {
				return true
			}
		}
	}
	return false
}

// mountCandidates returns the spellings of dir to match against the kernel's
// f_mntonname values: the byte-exact cleaned dir, plus — when dir's PARENT
// resolves through symlinks to a different spelling — the resolved-parent +
// base. The kernel records a mount under $TMPDIR as /private/var/... (the
// firmlinked real path), which a byte-exact match would miss. Resolving only
// the parent (never dir) keeps the probe off the mount itself; the result is
// compare-only, per invariant #3.
func mountCandidates(dir string) []string {
	dir = filepath.Clean(dir)
	candidates := []string{dir}
	if parent, err := filepath.EvalSymlinks(filepath.Dir(dir)); err == nil {
		if alt := filepath.Join(parent, filepath.Base(dir)); alt != dir {
			candidates = append(candidates, alt)
		}
	}
	return candidates
}
