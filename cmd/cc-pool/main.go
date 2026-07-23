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
	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if recognized, err := daemon.RunStopControlChild(ctx, os.Args[1:]); recognized {
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}
	if hostsync.IsWorkerInvocation(os.Args[1:]) {
		owner, err := supervise.ReceiveTrackedOwner(ctx, proc.RecoverySourceOwner)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		if err := proc.CloseInheritedFDs(); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		if err := daemon.RunHostSyncWorker(ctx, owner, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}

	// A nested ccp must not inherit the actual launched session's lease fd.
	if err := proc.CloseInheritedFDs(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	if pool.IsBackingWorkerInvocation(os.Args[1:]) {
		if err := pool.RunBackingWorker(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}
	if pool.IsCredentialCASWorkerInvocation(os.Args[1:]) {
		if err := pool.RunCredentialCASWorker(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}
	if procscan.IsWorkerInvocation(os.Args[1:]) {
		if err := procscan.RunWorker(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}
	if daemon.IsCredentialWriteWorkerInvocation(os.Args[1:]) {
		if err := daemon.RunCredentialWriteWorker(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}

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
