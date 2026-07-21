package cli

import (
	"testing"

	"github.com/yasyf/cc-pool/internal/store"
)

func TestBuildDaemonLoginCommandRoutesThroughCLI(t *testing.T) {
	command, err := buildDaemonLoginCommand(store.Account{ID: 18})
	if err != nil {
		t.Fatal(err)
	}
	if len(command.Args) != 3 || command.Args[1] != "login" || command.Args[2] != "18" {
		t.Fatalf("args = %v, want self login 18", command.Args)
	}
}
