package cli

import (
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/pool"
)

func testFileProviderConfigDir(id int) string {
	return filepath.Join("/Users/test/Library/CloudStorage", "CCPoolStatus-"+pool.AccountDirName(id))
}
