//go:build darwin

package pool

import (
	"errors"
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
	if stat.Dev < 0 {
		return credentialLockFingerprint{}, errors.New("credential lock device is negative")
	}
	// #nosec G115 -- stat.Dev is proven non-negative immediately above.
	device := uint64(stat.Dev)
	return credentialLockFingerprint{
		Device: device, Inode: stat.Ino,
		BirthSecond: stat.Birthtimespec.Sec, BirthNanos: stat.Birthtimespec.Nsec,
	}, nil
}

func publishCredentialLockDirectory(stage, target string) error {
	return unix.RenameatxNp(
		unix.AT_FDCWD, stage, unix.AT_FDCWD, target, unix.RENAME_EXCL,
	)
}
