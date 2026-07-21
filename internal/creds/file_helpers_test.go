package creds

import (
	"errors"
	"fmt"
	"os"
)

func fileCredentialExistsForTest(configDir string) bool {
	_, err := os.Stat(FileCredentialPath(configDir))
	return err == nil
}

func readFileCredentialForTest(configDir string) (*Credential, error) {
	path := FileCredentialPath(configDir)
	data, err := os.ReadFile(path) //nolint:gosec // test-owned path
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return parseCredential(data)
}

func writeFileCredentialForTest(configDir string, credential *Credential) error {
	if err := credential.validateForWrite(); err != nil {
		return err
	}
	data, err := credential.Marshal()
	if err != nil {
		return err
	}
	return writeCredentialFile(FileCredentialPath(configDir), data)
}
