//go:build darwin

package pool

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func credentialLockFingerprintForPath(path string) (credentialLockFingerprint, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return credentialLockFingerprint{}, err
	}
	stat := info.Sys().(*syscall.Stat_t)
	return credentialLockFingerprint{
		Device: uint64(stat.Dev), Inode: stat.Ino,
		BirthSecond: stat.Birthtimespec.Sec, BirthNanos: stat.Birthtimespec.Nsec,
	}, nil
}

func publishCredentialLockDirectory(stage, target string) error {
	return unix.RenameatxNp(
		unix.AT_FDCWD, stage, unix.AT_FDCWD, target, unix.RENAME_EXCL,
	)
}
