package hostsync

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	switch {
	case os.Getenv(testRPCStateEnv) != "":
		err = runTestRPCServer(ctx)
	case pool.IsBackingWorkerInvocation(os.Args[1:]):
		err = pool.RunBackingWorker(ctx, os.Stdin, os.Stdout)
	case procscan.IsWorkerInvocation(os.Args[1:]):
		err = procscan.RunWorker(ctx, os.Stdin, os.Stdout)
	default:
		os.Exit(m.Run())
	}
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	os.Exit(0)
}
