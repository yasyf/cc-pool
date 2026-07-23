package pool

import "path/filepath"

func testFileProviderConfigDir(id int) string {
	return filepath.Join(mustHome(), "Library", "CloudStorage", "CCPoolStatus-"+AccountDirName(id))
}
