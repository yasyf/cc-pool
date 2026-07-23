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
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
)

type daemonTestHandler func(context.Context, ccdaemon.Op, ccdaemon.Request) ccdaemon.Response

type daemonTestCloser struct{}

func (daemonTestCloser) Close() error { return nil }

type daemonTestWorkers struct{}

func (daemonTestWorkers) Close()                     {}
func (daemonTestWorkers) Cancel()                    {}
func (daemonTestWorkers) Wait(context.Context) error { return nil }

type daemonTestStopVerifier struct{}

func (daemonTestStopVerifier) Validate() error { return nil }
func (daemonTestStopVerifier) VerifyStopControl(context.Context, wire.Peer, string) (proc.Record, error) {
	return proc.Record{}, errors.New("daemon test stop control is unavailable")
}

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
		ccdaemon.OpCredMove,
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
		WireBuild: ccdaemon.WireBuild, Ladder: ladder, MaxSessions: 2,
	}
	for _, op := range []ccdaemon.Op{
		ccdaemon.OpSelect,
		ccdaemon.OpSelectCommit,
		ccdaemon.OpSelectAbort,
		ccdaemon.OpStatus,
		ccdaemon.OpCredMove,
		ccdaemon.OpAccountIdentity,
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
	intake := &drain.Intake{}
	runtime, err := wire.NewRuntime(wire.RuntimeConfig{
		Socket: pool.SocketPath(), RuntimeBuild: build, RuntimeProtocol: int(wire.ProtocolVersion),
		Wire:       server,
		Classifier: daemonTestProtectedClassifier{}, ReservedProtectedSessions: 1,
		StopVerifier: daemonTestStopVerifier{},
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
					PID: os.Getpid(), ProcessGeneration: "test-generation", State: ccdaemon.RuntimeStateHealthy,
					Ready: true,
				})
				return wire.ObservationResponse{Payload: payload}, encodeErr
			},
		}},
		ListenerWait: time.Second, ShutdownTimeout: time.Second,
		Admission: intake, Workers: daemonTestWorkers{}, State: daemonTestCloser{},
		Resources: daemonTestCloser{}, Activate: func(dkdaemon.Activation) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- runtime.Run(t.Context())
	}()
	readyCtx, cancelReady := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancelReady()
	if err := runtime.WaitReady(readyCtx); err != nil {
		t.Fatal(err)
	}
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

type daemonTestProtectedClassifier struct{}

func (daemonTestProtectedClassifier) Validate() error { return nil }
func (daemonTestProtectedClassifier) Classify(context.Context, wire.Peer) (bool, error) {
	return true, nil
}
func (daemonTestProtectedClassifier) AuthorizeLifecycleBuild(string, string) bool { return true }
