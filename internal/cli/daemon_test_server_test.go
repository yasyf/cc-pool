package cli

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	ccdaemon "github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/version"
	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/wire"
)

type daemonTestHandler func(context.Context, ccdaemon.Op, ccdaemon.Request) ccdaemon.Response

type daemonTestLifecycle struct{ build string }

func (l daemonTestLifecycle) Health(context.Context) (dkdaemon.Health, error) {
	return dkdaemon.Health{
		Build: l.build, Protocol: int(wire.ProtocolVersion), PID: os.Getpid(), State: dkdaemon.StateHealthy,
	}, nil
}

func (daemonTestLifecycle) Shutdown(context.Context) error { return nil }
func (daemonTestLifecycle) Handoff(context.Context) error  { return nil }

// startDaemonTestServer exposes the current persistent daemon protocol at the
// default test HOME socket. Tests vary only business behavior and build ID.
func startDaemonTestServer(t *testing.T, build string, handler daemonTestHandler) {
	t.Helper()
	if build == "" {
		build = version.String()
	}
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", pool.SocketPath())
	if err != nil {
		t.Fatal(err)
	}
	serverDurations := make(map[wire.Op]time.Duration)
	clientDurations := make(map[wire.Op]time.Duration)
	for _, op := range []ccdaemon.Op{
		ccdaemon.OpSelect,
		ccdaemon.OpSelectCommit,
		ccdaemon.OpSelectAbort,
		ccdaemon.OpStatus,
		ccdaemon.OpMigrate,
		ccdaemon.OpCredMove,
		ccdaemon.OpFPRepair,
		ccdaemon.OpFPBridgeCheck,
	} {
		serverDurations[wire.Op(op)] = 2 * time.Minute
		clientDurations[wire.Op(op)] = 3 * time.Minute
	}
	ladder, err := wire.NewLadder(serverDurations, clientDurations)
	if err != nil {
		t.Fatal(err)
	}
	server := &wire.Server{Build: build, Ladder: ladder}
	for _, op := range []ccdaemon.Op{
		ccdaemon.OpSelect,
		ccdaemon.OpSelectCommit,
		ccdaemon.OpSelectAbort,
		ccdaemon.OpStatus,
		ccdaemon.OpMigrate,
		ccdaemon.OpCredMove,
		ccdaemon.OpFPRepair,
		ccdaemon.OpFPBridgeCheck,
	} {
		op := op
		server.RegisterConcurrent(wire.Op(op), func(ctx context.Context, request wire.Request) (any, error) {
			var payload ccdaemon.Request
			if err := json.Unmarshal(request.Payload, &payload); err != nil {
				return nil, err
			}
			payload.Op = op
			if handler == nil {
				return ccdaemon.Response{OK: true, Version: build}, nil
			}
			return handler(ctx, op, payload), nil
		})
	}
	server.RegisterLifecycle(daemonTestLifecycle{build: build})
	intake := &drain.Intake{}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener, intake.Admit, intake.AdmitLifecycle) }()
	t.Cleanup(func() {
		intake.Close()
		_ = server.CloseIntake()
		_ = intake.Settle(context.Background())
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("serve daemon test socket: %v", err)
		}
	})
}
