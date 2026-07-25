package daemon

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
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
	ctx := context.Background()
	var err error
	switch {
	case hostsync.IsWorkerInvocation(os.Args[1:]):
		err = RunHostSyncWorker(ctx, os.Stdin, os.Stdout)
	case IsCredentialWriteWorkerInvocation(os.Args[1:]):
		err = RunCredentialWriteWorker(ctx, os.Stdin, os.Stdout)
	case pool.IsBackingWorkerInvocation(os.Args[1:]):
		err = pool.RunBackingWorker(ctx, os.Stdin, os.Stdout)
	case pool.IsCredentialCASWorkerInvocation(os.Args[1:]):
		err = pool.RunCredentialCASWorker(ctx, os.Stdin, os.Stdout)
	case procscan.IsWorkerInvocation(os.Args[1:]):
		err = procscan.RunWorker(ctx, os.Stdin, os.Stdout)
	default:
		os.Exit(m.Run())
	}
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
