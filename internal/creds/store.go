package creds

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

// ErrUnavailable means a Keychain item's state could not be determined: in a
// headless (e.g. SSH) session security(1)'s user search list lacks the login
// keychain, so item state is unknowable — absence proves nothing.
var ErrUnavailable = errors.New("login keychain not in the security search list")

// keychainPathRE captures each quoted keychain path in `security list-keychains` output.
var keychainPathRE = regexp.MustCompile(`"([^"]+)"`)

// Store is one concrete credential location — a single Keychain item or a
// single plaintext credential file — with uniform read/write/delete semantics.
type Store interface {
	// Source identifies the backend kind.
	Source() Source
	// Read returns the stored credential, or ErrNotFound when absent. The
	// Keychain backend returns ErrUnavailable instead when absence cannot be
	// proven (login keychain unsearchable).
	Read() (*Credential, error)
	// Write upserts the credential.
	Write(*Credential) error
	// Delete removes the credential; missing is not an error.
	Delete() error
	// String is the human-readable location for error messages.
	String() string
}

// KeychainItem is the Store for one macOS Keychain generic-password item,
// addressed by service and account labels and accessed via security(1). An
// empty Account means AccountLabel(), matching the package-level helpers.
type KeychainItem struct {
	Service string
	Account string
}

// Source returns SourceKeychain.
func (k KeychainItem) Source() Source { return SourceKeychain }

// Read fetches and parses the item. On a miss it distinguishes true absence
// (ErrNotFound) from an unsearchable login keychain (ErrUnavailable) with one
// extra list-keychains exec, run only on the miss path.
func (k KeychainItem) Read() (*Credential, error) {
	cred, err := Read(k.Service, k.Account)
	if !errors.Is(err, ErrNotFound) {
		return cred, err
	}
	searchable, err := loginKeychainSearchable()
	if err != nil {
		return nil, err
	}
	if !searchable {
		return nil, ErrUnavailable
	}
	return nil, ErrNotFound
}

// Write upserts cred under the item.
func (k KeychainItem) Write(cred *Credential) error { return Write(k.Service, k.Account, cred) }

// Delete removes the item; missing is not an error.
func (k KeychainItem) Delete() error { return Delete(k.Service, k.Account) }

// Reassert reads then rewrites the item through our security(1) so our later
// access is prompt-free (ACL ownership) whatever process created it.
func (k KeychainItem) Reassert() (*Credential, error) {
	cred, err := k.Read()
	if err != nil {
		return nil, err
	}
	if err := k.Write(cred); err != nil {
		return nil, err
	}
	return cred, nil
}

// String names the Keychain item for error messages.
func (k KeychainItem) String() string {
	return fmt.Sprintf("keychain item %q", k.Service)
}

// loginKeychainSearchable reports whether security(1)'s user search list
// contains the login keychain; without it a find miss proves nothing.
func loginKeychainSearchable() (bool, error) {
	//nolint:gosec // G204: securityBin is the fixed /usr/bin/security path
	cmd := exec.Command(securityBin, "list-keychains", "-d", "user")
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return false, fmt.Errorf("security list-keychains: %w: %s", err, strings.TrimSpace(errb.String()))
	}
	for _, m := range keychainPathRE.FindAllStringSubmatch(out.String(), -1) {
		if strings.HasPrefix(filepath.Base(m[1]), "login.keychain") {
			return true, nil
		}
	}
	return false, nil
}

// FileStore is the Store for claude's plaintext .credentials.json fallback
// inside one config dir.
type FileStore struct {
	ConfigDir string
}

// Source returns SourceFile.
func (f FileStore) Source() Source { return SourceFile }

// Read parses the credential file, returning ErrNotFound when absent.
func (f FileStore) Read() (*Credential, error) { return ReadFileCredential(f.ConfigDir) }

// Write writes the credential file at 0600, atomically.
func (f FileStore) Write(cred *Credential) error { return WriteFileCredential(f.ConfigDir, cred) }

// Delete removes the credential file; missing is not an error.
func (f FileStore) Delete() error {
	if err := os.Remove(FileCredentialPath(f.ConfigDir)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// String returns the credential file path.
func (f FileStore) String() string { return FileCredentialPath(f.ConfigDir) }

var (
	_ Store = KeychainItem{}
	_ Store = FileStore{}
)
