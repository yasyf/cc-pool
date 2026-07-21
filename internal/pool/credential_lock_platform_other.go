//go:build !darwin && !linux

package pool

import "errors"

func credentialLockFingerprintForPath(string) (credentialLockFingerprint, error) {
	return credentialLockFingerprint{}, errors.New("credential lock identity is unsupported on this platform")
}

func publishCredentialLockDirectory(string, string) error {
	return errors.New("exclusive credential lock publication is unsupported on this platform")
}
