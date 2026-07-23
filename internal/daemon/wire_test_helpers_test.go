package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/wire"
)

func serveHandlerOnSocket(t *testing.T, serverState *Server) string {
	t.Helper()
	socketDir, err := os.MkdirTemp("/tmp", "ccp-wire-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socket := filepath.Join(socketDir, "d.sock")
	ladder, err := operationLadder()
	if err != nil {
		t.Fatal(err)
	}
	server := &wire.Server{WireBuild: WireBuild, Ladder: ladder, MaxSessions: 2}
	for _, operation := range []Op{OpSelect, OpSelectCommit, OpSelectAbort, OpStatus} {
		operation := operation
		server.RegisterConcurrent(wire.Op(operation), func(ctx context.Context, request wire.Request) (any, error) {
			var payload Request
			if err := decodeStrict(request.Payload, &payload); err != nil {
				return nil, err
			}
			payload.Op = operation
			return serverState.dispatch(ctx, payload), nil
		})
	}
	startTestWireRuntime(t, socket, "test-runtime", server, buildTestProtectedClassifier{}, []wire.ObservationRoute{
		testHealthObservation("test-runtime", nil),
	})
	return socket
}
