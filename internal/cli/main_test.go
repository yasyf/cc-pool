package cli

import (
	"fmt"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/trust"
)

func TestMain(m *testing.M) {
	if handled, err := trust.RunVerifierChild(os.Args[1:], os.Stdout); handled {
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}
