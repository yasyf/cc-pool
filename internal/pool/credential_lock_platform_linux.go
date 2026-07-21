//go:build linux

package pool

import (
	"errors"

	"golang.org/x/sys/unix"
)

func credentialLockFingerprintForPath(path string) (credentialLockFingerprint, error) {
	var stat unix.Statx_t
	if err := unix.Statx(
		unix.AT_FDCWD, path, unix.AT_SYMLINK_NOFOLLOW,
		unix.STATX_INO|unix.STATX_BTIME, &stat,
	); err != nil {
		return credentialLockFingerprint{}, err
	}
	if stat.Mask&unix.STATX_BTIME == 0 {
		return credentialLockFingerprint{}, errors.New(
			"credential lock filesystem does not expose birth-time identity",
		)
	}
	return credentialLockFingerprint{
		Device: uint64(stat.Dev_major)<<32 | uint64(stat.Dev_minor), Inode: stat.Ino,
		BirthSecond: stat.Btime.Sec, BirthNanos: int64(stat.Btime.Nsec),
	}, nil
}

func publishCredentialLockDirectory(stage, target string) error {
	return unix.Renameat2(
		unix.AT_FDCWD, stage, unix.AT_FDCWD, target, unix.RENAME_NOREPLACE,
	)
}
