package pool

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// execve is the syscall.Exec seam; tests replace it so the real self-exec never runs.
var execve = syscall.Exec

// ReexecStable materializes the running executable at dir/name and re-execs it
// there so macOS TCC grants, keyed by resolved executable path, survive the
// per-version install path of a Homebrew keg. It returns nil when the running
// executable already resolves to dir/name (the post-exec pass, which stops a
// re-exec loop), so callers invoke it unconditionally at startup; on success the
// re-exec replaces the process and never returns. os.Args[1:] carries over
// verbatim while argv[0] is rewritten to the stable path.
func ReexecStable(dir, name string) error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(exe)
	if err != nil {
		return fmt.Errorf("resolve executable symlinks: %w", err)
	}
	return reexecStable(resolved, dir, name)
}

func reexecStable(resolved, dir, name string) error {
	// The guard compares real paths, not inodes: resolved is symlink-free, so
	// equality proves the running executable IS the regular file at dir/name — the
	// no-op post-exec pass. A hardlink or symlink leaf at target must NOT be blessed.
	realDir, err := filepath.EvalSymlinks(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return fmt.Errorf("resolve stable dir %s: %w", dir, err)
	case filepath.Join(realDir, name) == resolved:
		return nil
	}

	target := filepath.Join(dir, name)
	// A symlink leaf would survive materialize's byte-identical skip and the kernel
	// would exec through it, keying TCC to its destination.
	if fi, err := os.Lstat(target); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		if err := os.Remove(target); err != nil {
			return fmt.Errorf("remove symlink at stable path %s: %w", target, err)
		}
	}
	if err := materializeStableExe(resolved, dir, name); err != nil {
		return fmt.Errorf("materialize stable executable: %w", err)
	}
	if err := execve(target, append([]string{target}, os.Args[1:]...), os.Environ()); err != nil {
		return fmt.Errorf("re-exec at %s: %w", target, err)
	}
	return nil
}

// materializeStableExe copies srcPath to dir/name atomically, skipping the copy
// when a byte-identical executable already sits there.
func materializeStableExe(srcPath, dir, name string) error {
	target := filepath.Join(dir, name)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create stable exec dir %s: %w", dir, err)
	}
	switch matched, err := stableExeMatches(srcPath, target); {
	case err != nil:
		return err
	case matched:
		return nil
	}
	// #nosec G304 -- srcPath is the resolved path of the currently running executable.
	in, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open source %s: %w", srcPath, err)
	}
	defer func() { _ = in.Close() }()
	tmp, err := os.CreateTemp(dir, name+".tmp-*")
	if err != nil {
		return fmt.Errorf("create stable temp in %s: %w", dir, err)
	}
	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(tmp.Name())
		}
	}()
	if _, err := io.Copy(tmp, in); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("copy to %s: %w", tmp.Name(), err)
	}
	if err := tmp.Chmod(0o755); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod %s: %w", tmp.Name(), err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmp.Name(), err)
	}
	if err := os.Rename(tmp.Name(), target); err != nil {
		return fmt.Errorf("rename into %s: %w", target, err)
	}
	renamed = true
	return nil
}

// stableExeMatches reports whether target is a byte-identical executable copy of
// srcPath (size then SHA-256); an absent or non-executable target is a mismatch.
func stableExeMatches(srcPath, target string) (bool, error) {
	si, err := os.Stat(srcPath)
	if err != nil {
		return false, fmt.Errorf("stat source %s: %w", srcPath, err)
	}
	ti, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat stable exe %s: %w", target, err)
	}
	if !ti.Mode().IsRegular() || ti.Mode().Perm()&0o111 == 0 {
		return false, nil
	}
	if si.Size() != ti.Size() {
		return false, nil
	}
	sh, err := fileSHA256(srcPath)
	if err != nil {
		return false, err
	}
	th, err := fileSHA256(target)
	if err != nil {
		return false, err
	}
	return sh == th, nil
}

func fileSHA256(path string) ([sha256.Size]byte, error) {
	// #nosec G304 -- path is one of the self-controlled executable paths compared above.
	f, err := os.Open(path)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("hash %s: %w", path, err)
	}
	return [sha256.Size]byte(h.Sum(nil)), nil
}
