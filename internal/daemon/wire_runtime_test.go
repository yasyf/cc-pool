package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
)

type testRuntimeCloser struct{}

func (testRuntimeCloser) Close() error { return nil }

type testRuntimeWorkers struct{}

func (testRuntimeWorkers) Close()                     {}
func (testRuntimeWorkers) Cancel()                    {}
func (testRuntimeWorkers) Wait(context.Context) error { return nil }

type testRuntimeStopVerifier struct{}

func (testRuntimeStopVerifier) Validate() error { return nil }
func (testRuntimeStopVerifier) VerifyStopControl(context.Context, wire.Peer, string) (proc.Record, error) {
	return proc.Record{}, errors.New("test stop control is unavailable")
}

func testHealthObservation(build string, called func()) wire.ObservationRoute {
	return wire.ObservationRoute{
		Op: wire.Op(OpHealth), MaxResponseBytes: daemonHealthMaxResponse,
		Handler: func(ctx context.Context, request wire.ObservationRequest) (wire.ObservationResponse, error) {
			if err := ctx.Err(); err != nil {
				return wire.ObservationResponse{}, err
			}
			if request.Op != wire.Op(OpHealth) || request.Tenant != "" {
				return wire.ObservationResponse{}, errors.New("test health observation route is not exact")
			}
			var input DaemonHealthRequest
			if err := decodeStrict(request.Payload, &input); err != nil || input.Schema != DaemonHealthSchema {
				return wire.ObservationResponse{}, errors.New("test health observation schema is not exact")
			}
			if called != nil {
				called()
			}
			payload, err := json.Marshal(healthyDaemonHealth(build))
			return wire.ObservationResponse{Payload: payload}, err
		},
	}
}

func healthyDaemonHealth(build string) DaemonHealthResponse {
	return DaemonHealthResponse{
		Schema: DaemonHealthSchema, RuntimeBuild: build, RuntimeProtocol: int(wire.ProtocolVersion),
		PID: os.Getpid(), ProcessGeneration: "test-generation", State: DaemonRuntimeStateHealthy,
		Ready: true,
	}
}

func startTestWireRuntime(
	t *testing.T,
	socket string,
	build string,
	server *wire.Server,
	classifier wire.ProtectedSessionClassifier,
	observations []wire.ObservationRoute,
) {
	t.Helper()
	intake := &drain.Intake{}
	runtime, err := wire.NewRuntime(wire.RuntimeConfig{
		Socket: socket, RuntimeBuild: build, RuntimeProtocol: int(wire.ProtocolVersion),
		Wire:       server,
		Classifier: classifier, ReservedProtectedSessions: 1, Observations: observations,
		StopVerifier: testRuntimeStopVerifier{},
		ListenerWait: time.Second, ShutdownTimeout: time.Second,
		Admission: intake, Workers: testRuntimeWorkers{}, State: testRuntimeCloser{},
		Resources: testRuntimeCloser{}, Activate: func(dkdaemon.Activation) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- runtime.Run(t.Context()) }()
	readyCtx, cancelReady := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancelReady()
	if err := runtime.WaitReady(readyCtx); err != nil {
		t.Fatalf("start test daemon runtime: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancelClose()
		if err := runtime.Close(closeCtx); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("close test daemon runtime: %v", err)
		}
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("serve test daemon runtime: %v", err)
		}
	})
}
