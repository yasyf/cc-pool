package creds

import (
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
