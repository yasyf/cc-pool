//go:build darwin

package overlay

import (
	"os"

	"golang.org/x/sys/unix"
)

// disableReadCache turns off the NFS client page cache for f via F_NOCACHE so a
// deep probe's read reaches the fuse-t mirror instead of cache (Darwin-only fcntl).
func disableReadCache(f *os.File) error {
	_, err := unix.FcntlInt(f.Fd(), unix.F_NOCACHE, 1)
	return err
}
