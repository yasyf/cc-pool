package hostsync

import (
	"context"
	"fmt"
	"os"

	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

const testRPCStateEnv = "CCP_HOSTSYNC_TEST_RPC_STATE"

type testRPCConsumer struct {
	state syncservice.RawRegistry
}

func (testRPCConsumer) Capabilities(context.Context) (syncservice.Capabilities, error) {
	return syncservice.DefaultCapabilities("cc-pool-test"), nil
}

func (testRPCConsumer) List(context.Context) ([]syncservice.WatchItem, error) {
	return nil, nil
}

func (testRPCConsumer) Reconcile(context.Context, string) (syncservice.ReconcileResult, error) {
	return syncservice.ReconcileResult{}, nil
}

func (testRPCConsumer) Sync(context.Context, string) (syncservice.SyncResult, error) {
	return syncservice.SyncResult{}, nil
}

func (c testRPCConsumer) GetState(context.Context) (syncservice.RawRegistry, error) {
	return c.state, nil
}

func runTestRPCServer(ctx context.Context) error {
	state, err := os.ReadFile(os.Getenv(testRPCStateEnv))
	if err != nil {
		return fmt.Errorf("read test RPC registry: %w", err)
	}
	dispatcher := rpc.NewDispatcher()
	syncservice.RegisterConsumer(dispatcher, testRPCConsumer{state: state})
	return rpc.NewServer(dispatcher).ServeSession(ctx, os.Stdin, os.Stdout)
}
