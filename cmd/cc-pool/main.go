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
	"github.com/yasyf/daemonkit/proc"
)

func main() {
	// A nested ccp must not inherit the actual launched session's lease fd.
	if err := proc.CloseInheritedFDs(); err != nil {
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
