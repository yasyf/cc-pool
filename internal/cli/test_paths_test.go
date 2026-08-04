package cli

import (
	"os"
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/pool"
)

func testFileProviderPublicPath(id int) string {
	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	return filepath.Join(home, "Library", "CloudStorage", "CCPoolStatus-"+pool.AccountDirName(id))
}
