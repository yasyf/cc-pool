package pool

import (
	"context"
	"os"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
)

func fileCredentialExistsForTest(configDir string) bool {
	_, err := os.Stat(creds.FileCredentialPath(configDir))
	return err == nil
}

func readFileCredentialForTest(configDir string) (*creds.Credential, error) {
	return credstest.FileStore(configDir).Read(context.Background())
}

func writeFileCredentialForTest(configDir string, credential *creds.Credential) error {
	return credstest.FileStore(configDir).Write(context.Background(), credential)
}
