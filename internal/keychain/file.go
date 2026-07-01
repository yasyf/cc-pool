package keychain

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
	blob, err := cred.Marshal()
	if err != nil {
		return err
	}
	return writeCredentialFile(FileCredentialPath(configDir), blob)
}

// LocateCredential resolves an account's live credential, Keychain first (as
// claude prefers) then the plaintext file. Returns the account label (computed
// for the file, which has no -a), the source, or ErrNotFound if neither holds it.
func LocateCredential(configDir, service string) (account string, src Source, err error) {
	acct, err := DiscoverAccount(service)
	if err == nil {
		return acct, SourceKeychain, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return "", SourceKeychain, err
	}
	if FileCredentialExists(configDir) {
		return AccountLabel(), SourceFile, nil
	}
	return "", SourceKeychain, ErrNotFound
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
