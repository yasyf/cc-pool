package cli

import (
	"os"
	"testing"

	"github.com/yasyf/cc-pool/internal/testhome"
)

func tempHome(t *testing.T) string {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "ccp-home")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	testhome.Sandbox(t, home)
	return home
}
