package pool

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/yasyf/cc-pool/internal/creds"
)

var ErrAccountConfigLinkConflict = errors.New("account config link conflicts with verified presentation")

// AccountConfigDir returns the stable execution path for one immutable account instance.
func AccountConfigDir(instanceID string) (string, error) {
	if err := validateAccountInstanceID(instanceID); err != nil {
		return "", err
	}
	return filepath.Join(StateDir(), "config", instanceID), nil
}

// AccountKeychainService returns Claude's path-derived service for one immutable account instance.
func AccountKeychainService(instanceID string) (string, error) {
	configDir, err := AccountConfigDir(instanceID)
	if err != nil {
		return "", err
	}
	return creds.ServiceName(configDir), nil
}

// EnsureAccountConfigDir atomically creates one stable execution link to a verified public path.
func EnsureAccountConfigDir(instanceID, verifiedPublicPath string) error {
	return replaceAccountConfigDir(instanceID, "", verifiedPublicPath, false)
}

// RetargetAccountConfigDir atomically replaces one exact previously verified target.
func RetargetAccountConfigDir(instanceID, previousVerifiedPublicPath, verifiedPublicPath string) error {
	return replaceAccountConfigDir(instanceID, previousVerifiedPublicPath, verifiedPublicPath, true)
}

// RepairAccountConfigDir converges a missing, previous, or already-updated link after restart.
func RepairAccountConfigDir(instanceID, previousVerifiedPublicPath, verifiedPublicPath string) error {
	linkPath, err := AccountConfigDir(instanceID)
	if err != nil {
		return err
	}
	info, err := os.Lstat(linkPath)
	if errors.Is(err, os.ErrNotExist) {
		return EnsureAccountConfigDir(instanceID, verifiedPublicPath)
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return fmt.Errorf("%w: %s is not a symlink", ErrAccountConfigLinkConflict, linkPath)
	}
	current, err := readExactAccountConfigTarget(linkPath)
	if err != nil {
		return err
	}
	switch current {
	case verifiedPublicPath:
		return nil
	case previousVerifiedPublicPath:
		return RetargetAccountConfigDir(instanceID, previousVerifiedPublicPath, verifiedPublicPath)
	default:
		return fmt.Errorf("%w: %s targets %q", ErrAccountConfigLinkConflict, linkPath, current)
	}
}

// RemoveAccountConfigDir removes one exact stable execution link after presentation retirement.
func RemoveAccountConfigDir(instanceID, verifiedPublicPath string) error {
	linkPath, err := AccountConfigDir(instanceID)
	if err != nil {
		return err
	}
	if err := validateAccountConfigTarget(linkPath, verifiedPublicPath); err != nil {
		return err
	}
	info, err := os.Lstat(linkPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return fmt.Errorf("%w: %s is not a symlink", ErrAccountConfigLinkConflict, linkPath)
	}
	current, err := readExactAccountConfigTarget(linkPath)
	if err != nil {
		return err
	}
	if current != verifiedPublicPath {
		return fmt.Errorf("%w: %s targets %q, expected %q", ErrAccountConfigLinkConflict, linkPath, current, verifiedPublicPath)
	}
	if err := os.Remove(linkPath); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(linkPath))
}

// ValidateAccountConfigDir verifies one exact stable execution link and its public target.
func ValidateAccountConfigDir(instanceID, expectedPublicPath string) error {
	linkPath, err := AccountConfigDir(instanceID)
	if err != nil {
		return err
	}
	if err := validateAccountConfigTarget(linkPath, expectedPublicPath); err != nil {
		return err
	}
	parent := filepath.Dir(linkPath)
	stateDir := StateDir()
	if filepath.Dir(parent) != stateDir {
		return fmt.Errorf("%w: account config parent is outside the state directory", ErrAccountConfigLinkConflict)
	}
	if err := validateRealPrivateDirectory(stateDir); err != nil {
		return fmt.Errorf("account state directory: %w", err)
	}
	if err := validateRealPrivateDirectory(parent); err != nil {
		return fmt.Errorf("account config parent: %w", err)
	}
	info, err := os.Lstat(linkPath)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink == 0 || info.IsDir() {
		return fmt.Errorf("%w: %s is not the stable execution symlink", ErrAccountConfigLinkConflict, linkPath)
	}
	current, err := readExactAccountConfigTarget(linkPath)
	if err != nil {
		return err
	}
	if current != expectedPublicPath {
		return fmt.Errorf(
			"%w: %s targets %q, expected %q",
			ErrAccountConfigLinkConflict, linkPath, current, expectedPublicPath,
		)
	}
	if err := validatePublicPathUnderHome(expectedPublicPath); err != nil {
		return err
	}
	if err := validateRealPrivateDirectory(expectedPublicPath); err != nil {
		return fmt.Errorf("account public target: %w", err)
	}
	return nil
}

func replaceAccountConfigDir(
	instanceID string,
	previousVerifiedPublicPath string,
	verifiedPublicPath string,
	retarget bool,
) error {
	linkPath, err := AccountConfigDir(instanceID)
	if err != nil {
		return err
	}
	if err := validateAccountConfigTarget(linkPath, verifiedPublicPath); err != nil {
		return err
	}
	if retarget {
		if err := validateAccountConfigTarget(linkPath, previousVerifiedPublicPath); err != nil {
			return err
		}
	}
	parent := filepath.Dir(linkPath)
	if err := ensureAccountConfigParent(parent); err != nil {
		return err
	}
	info, statErr := os.Lstat(linkPath)
	switch {
	case statErr == nil:
		if info.Mode()&os.ModeSymlink == 0 {
			return fmt.Errorf("%w: %s is not a symlink", ErrAccountConfigLinkConflict, linkPath)
		}
		current, err := readExactAccountConfigTarget(linkPath)
		if err != nil {
			return err
		}
		if current == verifiedPublicPath {
			return nil
		}
		if !retarget || current != previousVerifiedPublicPath {
			return fmt.Errorf("%w: %s targets %q", ErrAccountConfigLinkConflict, linkPath, current)
		}
	case errors.Is(statErr, os.ErrNotExist):
		if retarget {
			return fmt.Errorf("%w: %s is missing", ErrAccountConfigLinkConflict, linkPath)
		}
	case statErr != nil:
		return statErr
	}
	temporary, err := temporaryAccountConfigLink(parent, verifiedPublicPath)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(temporary) }()
	if err := os.Rename(temporary, linkPath); err != nil {
		return err
	}
	return syncDirectory(parent)
}

func validateAccountInstanceID(instanceID string) error {
	if len(instanceID) != 32 {
		return errors.New("account instance id must be exactly 32 lowercase hexadecimal characters")
	}
	for _, value := range instanceID {
		if (value < '0' || value > '9') && (value < 'a' || value > 'f') {
			return errors.New("account instance id must be exactly 32 lowercase hexadecimal characters")
		}
	}
	return nil
}

func validateAccountConfigTarget(linkPath, target string) error {
	if target == "" || !filepath.IsAbs(target) || filepath.Clean(target) != target ||
		strings.ContainsRune(target, 0) || target == linkPath {
		return errors.New("account config target must be one exact absolute presentation path")
	}
	return nil
}

func ensureAccountConfigParent(parent string) error {
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(parent)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return fmt.Errorf("account config parent %s must be a real 0700 directory", parent)
	}
	return nil
}

func validateRealPrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return errors.New("path must be a real private directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return errors.New("path has the wrong owner")
	}
	return nil
}

func validatePublicPathUnderHome(path string) error {
	home, err := Home()
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(home, path)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("%w: account public target is outside the user home", ErrAccountConfigLinkConflict)
	}
	current := home
	for _, component := range strings.Split(relative, string(os.PathSeparator)) {
		if component == "" || component == "." || component == ".." {
			return fmt.Errorf("%w: account public target is not exact", ErrAccountConfigLinkConflict)
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: account public target crosses a non-directory or symlink", ErrAccountConfigLinkConflict)
		}
	}
	return nil
}

func readExactAccountConfigTarget(linkPath string) (string, error) {
	target, err := os.Readlink(linkPath)
	if err != nil {
		return "", err
	}
	if err := validateAccountConfigTarget(linkPath, target); err != nil {
		return "", fmt.Errorf("%w: %s: %v", ErrAccountConfigLinkConflict, linkPath, err)
	}
	return target, nil
}

func temporaryAccountConfigLink(parent, target string) (string, error) {
	for range 8 {
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return "", err
		}
		path := filepath.Join(parent, ".link-"+hex.EncodeToString(nonce[:]))
		if err := os.Symlink(target, path); err == nil {
			return path, nil
		} else if !errors.Is(err, os.ErrExist) {
			return "", err
		}
	}
	return "", errors.New("allocate account config link: exhausted names")
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
