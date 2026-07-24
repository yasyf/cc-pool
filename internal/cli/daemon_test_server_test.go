package cli

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	ccdaemon "github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/version"
	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/worker"
)

type daemonTestHandler func(context.Context, ccdaemon.Op, ccdaemon.Request) ccdaemon.Response

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
	serverDurations := make(map[wire.Op]time.Duration)
	clientDurations := make(map[wire.Op]time.Duration)
	for _, op := range []ccdaemon.Op{
		ccdaemon.OpHealth,
		ccdaemon.OpSelect,
		ccdaemon.OpSelectCommit,
		ccdaemon.OpSelectAbort,
		ccdaemon.OpStatus,
		ccdaemon.OpAccountIdentity,
	} {
		serverDurations[wire.Op(op)] = 2 * time.Minute
		clientDurations[wire.Op(op)] = 3 * time.Minute
	}
	ladder, err := wire.NewLadder(serverDurations, clientDurations)
	if err != nil {
		t.Fatal(err)
	}
	server := &wire.Server{
		WireBuild: ccdaemon.WireBuild, Ladder: ladder, MaxSessions: 8,
	}
	for _, op := range []ccdaemon.Op{
		ccdaemon.OpSelect,
		ccdaemon.OpSelectCommit,
		ccdaemon.OpSelectAbort,
		ccdaemon.OpStatus,
		ccdaemon.OpAccountIdentity,
	} {
		op := op
		server.Register(wire.HandlerSpec{
			Op: wire.Op(op), Concurrent: true,
			Handler: func(ctx context.Context, request wire.Request) (any, error) {
				var payload ccdaemon.Request
				if err := json.Unmarshal(request.Payload, &payload); err != nil {
					return nil, err
				}
				payload.Op = op
				if handler == nil {
					return ccdaemon.Response{OK: true, Version: build}, nil
				}
				return handler(ctx, op, payload), nil
			},
		})
	}
	stateDir := t.TempDir()
	generation, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	workers, err := worker.NewPool(worker.Config{
		Capacity: 3, QueueCapacity: 3, MaxTotalRun: time.Minute,
		MaxStdinBytes: 1 << 20, MaxStdoutBytes: 1 << 20, MaxStderrBytes: 1 << 20,
	}, &proc.Reaper{
		Store: &proc.FileStore{Path: stateDir + "/workers-v1.db"}, Generation: generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	children, err := proc.NewManager(2, &proc.Reaper{
		Store: &proc.FileStore{Path: stateDir + "/children-v1.db"}, Generation: generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	stopRole := trust.PeerRole("com.yasyf.cc-pool.cli-test.stop")
	receiptRole := trust.PeerRole("com.yasyf.cc-pool.cli-test.receipt")
	readinessRole := trust.PeerRole("com.yasyf.cc-pool.cli-test.readiness")
	requirement := trust.Requirement{TeamID: "ABCDE12345", SigningIdentifier: "com.yasyf.cc-pool.cli-test"}
	policy, err := trust.NewTrustPolicy(trust.TrustPolicyConfig{
		ExpectedUID: os.Geteuid(), AllowUnprotected: true,
		Roles: map[trust.PeerRole]trust.Requirement{
			stopRole: requirement, receiptRole: requirement, readinessRole: requirement,
		},
		StopRoles: []trust.PeerRole{stopRole}, ReceiptRoles: []trust.PeerRole{receiptRole},
		ReadinessRoles: []trust.PeerRole{readinessRole},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := wire.NewRuntime(wire.RuntimeConfig{
		Socket: pool.SocketPath(), RuntimeBuild: build, RuntimeProtocol: int(wire.ProtocolVersion),
		Wire: server, TrustPolicy: policy,
		StopControlStore: &proc.FileStore{Path: stateDir + "/stop-control-v1.db"},
		Observations: []wire.ObservationRoute{{
			Op: wire.Op(ccdaemon.OpHealth), MaxResponseBytes: 16 << 10,
			Handler: func(_ context.Context, request wire.ObservationRequest) (wire.ObservationResponse, error) {
				if request.Op != wire.Op(ccdaemon.OpHealth) || request.Tenant != "" {
					return wire.ObservationResponse{}, errors.New("daemon test health route is not exact")
				}
				var healthRequest ccdaemon.HealthRequest
				if decodeErr := json.Unmarshal(request.Payload, &healthRequest); decodeErr != nil ||
					healthRequest.Schema != ccdaemon.DaemonHealthSchema {
					return wire.ObservationResponse{}, errors.New("daemon test health schema is not exact")
				}
				payload, encodeErr := json.Marshal(ccdaemon.HealthResponse{
					Schema: ccdaemon.DaemonHealthSchema, RuntimeBuild: build, RuntimeProtocol: int(wire.ProtocolVersion),
					PID: os.Getpid(), ProcessGeneration: generation.String(), State: ccdaemon.RuntimeStateHealthy,
					Ready: true,
				})
				return wire.ObservationResponse{Payload: payload}, encodeErr
			},
		}},
		ListenerWait: time.Second, ShutdownTimeout: time.Second,
		Workers: workers, Children: children,
	})
	if err != nil {
		t.Fatal(err)
	}
	slot := dkdaemon.NewPublicationSlot[struct{}](runtime)
	activation, err := runtime.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := activation.ClaimProductSettlement()
	if err != nil {
		t.Fatal(err)
	}
	publication, err := slot.Stage(activation, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if err := activation.CommitReady(publication); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- runtime.Wait(context.Background()) }()
	go func() {
		<-activation.Context().Done()
		_ = settlement.Complete()
	}()
	t.Cleanup(func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancelClose()
		if err := runtime.Close(closeCtx); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("close daemon test runtime: %v", err)
		}
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("serve daemon test socket: %v", err)
		}
	})
}
