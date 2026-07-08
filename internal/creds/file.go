package creds

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// credentialFile is claude's plaintext credential fallback, written when the
// Keychain is unavailable. Same {"claudeAiOauth":…} JSON as the Keychain secret.
const credentialFile = ".credentials.json"

// Source identifies which backend currently holds an account's credential.
type Source int

const (
	// SourceKeychain is the macOS Keychain item named ServiceName(configDir).
	SourceKeychain Source = iota
	// SourceFile is claude's plaintext $CONFIG_DIR/.credentials.json fallback.
	SourceFile
)

// String names the backend for display and the daemon wire.
func (s Source) String() string {
	switch s {
	case SourceKeychain:
		return "keychain"
	case SourceFile:
		return "file"
	}
	return fmt.Sprintf("source(%d)", int(s))
}

// FileCredentialPath returns the plaintext credential path for a config dir.
func FileCredentialPath(configDir string) string {
	return filepath.Join(configDir, credentialFile)
}

// FileCredentialExists reports whether configDir holds a plaintext credential.
func FileCredentialExists(configDir string) bool {
	_, err := os.Stat(FileCredentialPath(configDir))
	return err == nil
}

// ReadFileCredential reads and parses the plaintext credential in configDir,
// returning ErrNotFound when the file is absent.
func ReadFileCredential(configDir string) (*Credential, error) {
	path := FileCredentialPath(configDir)
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is the account's own .credentials.json under the cc-pool-managed config dir
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return parseCredential(b)
}

// WriteFileCredential writes cred to configDir's plaintext credential file at
// 0600, atomically so a concurrent reader never sees a partial file.
func WriteFileCredential(configDir string, cred *Credential) error {
	if err := cred.validateForWrite(); err != nil {
		return err
	}
	blob, err := cred.Marshal()
	if err != nil {
		return err
	}
	return writeCredentialFile(FileCredentialPath(configDir), blob)
}

// writeCredentialFile writes via temp+rename at 0600. The temp keeps the
// .credentials.json. prefix so overlay.PrivateEntry holds it back too.
func writeCredentialFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, credentialFile+".tmp.*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }() // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
