package overlay

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// ResolvedConflictLogf surfaces every private-file collision that moveEntry
// reconciles by last-write-wins, so the recovery is observable and never silent
// data loss. Each process that drives a move wires it: the daemon points it at
// its logger at startup, and a daemon-less `doctor --fix` points it at its
// output for the duration of the heal it runs itself. The default no-op keeps
// the overlay package free of a logging dependency and silent in tests.
//
// It is a package-level seam rather than a return value because the conflict is
// resolved deep inside an idempotent move that several callers invoke as a bare
// expression — two of them inside errors.Join — and the pool.Manager methods
// that drive conversion and healing carry no logger of their own. Threading a
// resolved-list return up through those signatures (and their daemon callers)
// would not let those log-less sites report anything; this seam makes every
// path observable with no ripple. No lock guards it: it is assigned once at
// daemon startup, before any sweep or conversion runs.
var ResolvedConflictLogf = func(format string, args ...any) {}

// This file holds the untagged primitives that overlay conversion (and crash
// repair) is built from. They compile in every build variant: even a non-fuse
// binary must be able to recognize a fuse account's private backing dir and
// move stranded private files back, and must be able to refuse symlink
// operations on a live mountpoint.

// FusePrivateRoot is the fuse provider's per-account private backing dir: a
// sibling of the mountpoint (accountDir + ".private"). Private entries
// (PrivateEntry names) physically live there while the account uses the fuse
// overlay; the mirror redirects their paths so they remain visible through the
// mount. The path is never exported as CLAUDE_CONFIG_DIR and never hashed for
// Keychain service names.
func FusePrivateRoot(accountDir string) string {
	return accountDir + ".private"
}

// MovePrivateEntries relocates every top-level private entry (PrivateEntry
// names, which include the ExcludedEntries dirs) from one private root to the
// other via same-volume rename. Shared symlinks and unclassified entries are
// left untouched. It is idempotent and resumable: already-moved entries are
// skipped, so re-running after a crash converges. A directory that exists on
// both sides is merged child-by-child (fuse Setup pre-creates empty excluded
// dirs in the backing root); a regular file that exists on both sides is
// reconciled (see moveEntry) so an abnormal shutdown can never strand the
// account; a file-vs-directory clash at one name is genuine corruption and
// fails loudly with both copies intact. Per-entry failures are collected with
// errors.Join, like Sync.
func MovePrivateEntries(from, to string) error {
	if from == "" || to == "" {
		return fmt.Errorf("move private entries: empty root (from %q, to %q)", from, to)
	}
	if from == to {
		return fmt.Errorf("move private entries: from and to are both %q", from)
	}
	entries, err := os.ReadDir(from)
	if err != nil {
		return fmt.Errorf("read private root %q: %w", from, err)
	}
	if err := os.MkdirAll(to, 0o700); err != nil {
		return fmt.Errorf("mkdir private root %q: %w", to, err)
	}
	var errs []error
	for _, e := range entries {
		name := e.Name()
		if !PrivateEntry(name) {
			continue
		}
		src := filepath.Join(from, name)
		// A symlink at a private name is our own stale artifact from before the
		// name was classified private (same invariant as assertNoSymlink):
		// remove it, never move it.
		if fi, err := os.Lstat(src); err == nil && fi.Mode()&os.ModeSymlink != 0 {
			if err := os.Remove(src); err != nil {
				errs = append(errs, fmt.Errorf("remove stale private link %q: %w", src, err))
			}
			continue
		}
		if err := moveEntry(src, filepath.Join(to, name)); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// moveEntry renames src to dst, reconciling an existing destination by kind:
//   - dst missing: plain rename.
//   - both directories: merged child-by-child (mergeDir).
//   - both regular files: a collision left by an abnormal shutdown that wrote
//     the same private file into both roots. Refusing forever dead-locks the
//     account (the mount sweep and the symlink retreat each hit it from opposite
//     directions), so it is RESOLVED, not refused: identical content drops src
//     and keeps dst; differing content is last-write-wins, the newer mtime
//     surviving at dst (ties keep dst). Every resolution is reported through
//     ResolvedConflictLogf — a deliberate recovery, never silent data loss.
//   - one a file and the other a directory: genuine corruption; fail loudly
//     with both copies intact (never clobber a directory with a file, or
//     vice-versa).
func moveEntry(src, dst string) error {
	dfi, err := os.Lstat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("stat %q: %w", dst, err)
		}
		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("move %q: %w", src, err)
		}
		return nil
	}
	sfi, err := os.Lstat(src)
	if err != nil {
		return fmt.Errorf("stat %q: %w", src, err)
	}
	switch {
	case sfi.IsDir() && dfi.IsDir():
		return mergeDir(src, dst)
	case sfi.Mode().IsRegular() && dfi.Mode().IsRegular():
		return resolveFileConflict(src, dst, sfi, dfi)
	default:
		return fmt.Errorf("private entry type mismatch: %q and %q are not both regular files or both directories; refusing to clobber across types", src, dst)
	}
}

// resolveFileConflict reconciles a regular-file-vs-regular-file collision so an
// abnormal shutdown that left a private file in both roots can never strand the
// account. Identical bytes: drop src, keep dst. Differing bytes: last-write-
// wins — the newer mtime survives at dst (src renamed over dst when src is
// newer; src removed when dst is newer or the mtimes tie). The resolution is
// reported through ResolvedConflictLogf so it is observable.
func resolveFileConflict(src, dst string, sfi, dfi os.FileInfo) error {
	same, err := sameContent(src, dst)
	if err != nil {
		return err
	}
	if same {
		if err := os.Remove(src); err != nil {
			return fmt.Errorf("drop identical duplicate %q: %w", src, err)
		}
		ResolvedConflictLogf("resolved private-file conflict on %s (identical duplicate discarded)", dst)
		return nil
	}
	if sfi.ModTime().After(dfi.ModTime()) {
		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("replace stale %q with newer %q: %w", dst, src, err)
		}
		ResolvedConflictLogf("resolved private-file conflict on %s (kept newer copy, discarded stale duplicate)", dst)
		return nil
	}
	if err := os.Remove(src); err != nil {
		return fmt.Errorf("discard stale duplicate %q: %w", src, err)
	}
	ResolvedConflictLogf("resolved private-file conflict on %s (kept newer copy, discarded stale duplicate)", dst)
	return nil
}

// sameContent reports whether two files hold identical bytes. Private files are
// small (identity, credentials, update markers), so a full read-and-compare is
// cheap and exact — no size or hash shortcut needed.
func sameContent(a, b string) (bool, error) {
	ab, err := os.ReadFile(a)
	if err != nil {
		return false, fmt.Errorf("read %q: %w", a, err)
	}
	bb, err := os.ReadFile(b)
	if err != nil {
		return false, fmt.Errorf("read %q: %w", b, err)
	}
	return bytes.Equal(ab, bb), nil
}

// mergeDir moves src's children into dst (recursing into shared subdirs) and
// removes the then-empty src. OS cruft (.DS_Store) is dropped, not merged.
func mergeDir(src, dst string) error {
	children, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("read %q: %w", src, err)
	}
	var errs []error
	for _, c := range children {
		if skipEntries[c.Name()] {
			if err := os.Remove(filepath.Join(src, c.Name())); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		if err := moveEntry(filepath.Join(src, c.Name()), filepath.Join(dst, c.Name())); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	if err := os.Remove(src); err != nil {
		return fmt.Errorf("remove merged dir %q: %w", src, err)
	}
	return nil
}

// HasPrivateEntries reports whether dir holds meaningful per-account private
// state: a private file, or a private dir with at least one entry. The empty
// excluded dirs that fuse Setup pre-creates do not count — they are shape, not
// state. Used to detect files stranded in a fuse private root by an
// interrupted conversion. A missing dir trivially has none.
func HasPrivateEntries(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("read private root %q: %w", dir, err)
	}
	for _, e := range entries {
		if !PrivateEntry(e.Name()) {
			continue
		}
		if !e.IsDir() {
			return true, nil
		}
		children, err := os.ReadDir(filepath.Join(dir, e.Name()))
		if err != nil {
			return false, fmt.Errorf("read private dir %q: %w", filepath.Join(dir, e.Name()), err)
		}
		if len(children) > 0 {
			return true, nil
		}
	}
	return false, nil
}

// Mounted reports whether dir is currently a mountpoint (its device id differs
// from its parent's). Symlink-provider operations consult it to refuse writing
// symlinks "into" a live fuse mirror — those writes would pass through to the
// real ~/.claude.
func Mounted(dir string) bool {
	var ds, ps syscall.Stat_t
	if syscall.Lstat(dir, &ds) != nil {
		return false
	}
	if syscall.Lstat(filepath.Dir(dir), &ps) != nil {
		return false
	}
	return ds.Dev != ps.Dev
}
