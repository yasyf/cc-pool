package pool

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func testAccountConfigDir(id int) string {
	configDir, err := AccountConfigDir(fmt.Sprintf("%032x", id))
	if err != nil {
		panic(err)
	}
	return configDir
}

func testFileProviderPublicPath(id int) string {
	return filepath.Join(mustHome(), "Library", "CloudStorage", "CCPoolStatus-"+AccountDirName(id))
}

func prepareTestAccountConfigDir(t *testing.T, id int) string {
	t.Helper()
	publicPath := testFileProviderPublicPath(id)
	if err := os.MkdirAll(publicPath, 0o700); err != nil {
		t.Fatal(err)
	}
	instanceID := fmt.Sprintf("%032x", id)
	if err := EnsureAccountConfigDir(instanceID, publicPath); err != nil {
		t.Fatal(err)
	}
	return testAccountConfigDir(id)
}
