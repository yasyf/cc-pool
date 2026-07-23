package daemon

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProductionOrdinaryCallersContainNoProtectedLifecycleOperations(t *testing.T) {
	for _, root := range []string{".", "../cli", "../../cmd"} {
		rootFS, err := os.OpenRoot(root)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = rootFS.Close() })
		err = fs.WalkDir(rootFS.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			payload, err := rootFS.ReadFile(path)
			if err != nil {
				return err
			}
			source := string(payload)
			for _, forbidden := range []string{
				"wire.LifecyclePeer", "lifeproto.", "NewHealthRequest(",
				"NewShutdownRequest(", "NewHandoffRequest(", "RegisterLifecycle(", `"daemon-health"`,
			} {
				if strings.Contains(source, forbidden) {
					t.Errorf("production ordinary caller %s contains protected lifecycle operation %q", path, forbidden)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
