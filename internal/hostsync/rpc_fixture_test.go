package hostsync

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

const testRPCStateEnv = "CCP_HOSTSYNC_TEST_RPC_STATE"

type testRPCConsumer struct {
	state []byte
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

func (c testRPCConsumer) Export(_ context.Context, request syncservice.ExportRequest) (syncservice.ChangeEnvelope, error) {
	return syncservice.NewExportedChange(
		request.ServiceID, request.SchemaFingerprint, syncservice.ChangeSnapshot,
		syncservice.NewRevision(0), syncservice.NewRevision(1), c.state,
	)
}

func (testRPCConsumer) Apply(_ context.Context, change syncservice.ChangeEnvelope) (syncservice.ApplyResult, error) {
	return syncservice.ApplyResult{AckedRevision: change.SourceRevision}, nil
}

func runTestRPCServer(ctx context.Context) error {
	state, err := base64.RawStdEncoding.DecodeString(os.Getenv(testRPCStateEnv))
	if err != nil {
		return fmt.Errorf("decode test RPC registry: %w", err)
	}
	if len(state) == 0 {
		state = []byte(`{}`)
	}
	dispatcher := rpc.NewDispatcher()
	syncservice.RegisterConsumer(dispatcher, testRPCConsumer{state: state})
	return rpc.NewServer(dispatcher).ServeSession(ctx, os.Stdin, os.Stdout)
}
