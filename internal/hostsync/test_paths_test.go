package hostsync

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/pool"
)

func testAccountConfigDir(id int) string {
	configDir, err := pool.AccountConfigDir(fmt.Sprintf("%032x", id))
	if err != nil {
		panic(err)
	}
	return configDir
}

func testFileProviderPublicPath(id int) string {
	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	return filepath.Join(home, "Library", "CloudStorage", "CCPoolStatus-"+pool.AccountDirName(id))
}
