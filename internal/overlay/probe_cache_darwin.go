//go:build darwin

package overlay

import (
	"os"

	"golang.org/x/sys/unix"
)

// disableReadCache turns off the NFS client page cache for f via F_NOCACHE so a
// deep probe's read reaches the fuse-t mirror instead of being served from
// cache. macOS-only: F_NOCACHE is a Darwin fcntl, and the mount holder that
// runs this probe only exists on macOS.
func disableReadCache(f *os.File) error {
	_, err := unix.FcntlInt(f.Fd(), unix.F_NOCACHE, 1)
	return err
}
