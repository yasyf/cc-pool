// Command cc-pool is the single binary behind both `cc-pool` and its
// `ccp` symlink.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/yasyf/cc-pool/internal/cli"
	"github.com/yasyf/fusekit/proc"
)

func main() {
	// Close any inherited non-CLOEXEC descriptor a caller leaked into this process
	// (e.g. a claude session holding another account's session lease that runs
	// `ccp env`) BEFORE any lease Acquire, so a detached lease agent we later spawn
	// never inherits — and pins — an unrelated account's lease for the terminal's
	// lifetime. The lease-agent subcommand keeps its fd-3 readiness pipe but must still
	// drop any OTHER inherited non-CLOEXEC fd (a manually- or claude-spawned agent can
	// inherit an unrelated lease), so it runs the fd-3-preserving sweep.
	sweep := proc.CloseInheritedFDs
	if cli.LeaseAgentInvocation(os.Args[1:]) {
		sweep = cli.SweepInheritedFDsExceptReadyPipe
	}
	if err := sweep(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	root := cli.NewRootCmd()
	if base := filepath.Base(os.Args[0]); base == "ccp" {
		root.Use = "ccp"
	}
	root.SetArgs(cli.InjectRun(os.Args[1:]))

	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
