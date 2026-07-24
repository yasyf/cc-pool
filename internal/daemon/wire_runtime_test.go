package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/worker"
)

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
			var input HealthRequest
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

func healthyDaemonHealth(build string) HealthResponse {
	return HealthResponse{
		Schema: DaemonHealthSchema, RuntimeBuild: build, RuntimeProtocol: int(wire.ProtocolVersion),
		PID: os.Getpid(), ProcessGeneration: "test-generation", State: RuntimeStateHealthy,
		Ready: true,
	}
}

func startTestWireRuntime(
	t *testing.T,
	socket string,
	build string,
	server *wire.Server,
	_ any,
	observations []wire.ObservationRoute,
) {
	t.Helper()
	stateDir := t.TempDir()
	generation, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	workers, err := worker.NewPool(worker.Config{
		Capacity: 2, QueueCapacity: 2, MaxTotalRun: time.Minute,
		MaxStdinBytes: 1 << 20, MaxStdoutBytes: 1 << 20, MaxStderrBytes: 1 << 20,
	}, &proc.Reaper{
		Store: &proc.FileStore{Path: filepath.Join(stateDir, "workers-v1.db")}, Generation: generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	children, err := proc.NewManager(2, &proc.Reaper{
		Store: &proc.FileStore{Path: filepath.Join(stateDir, "children-v1.db")}, Generation: generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := daemonTrustPolicy()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := wire.NewRuntime(wire.RuntimeConfig{
		Socket: socket, RuntimeBuild: build, RuntimeProtocol: int(wire.ProtocolVersion),
		Wire: server, TrustPolicy: policy,
		StopControlStore: &proc.FileStore{Path: filepath.Join(stateDir, "stop-control-v1.db")},
		Observations:     observations,
		ListenerWait:     time.Second, ShutdownTimeout: time.Second,
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
			t.Errorf("close test daemon runtime: %v", err)
		}
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("serve test daemon runtime: %v", err)
		}
	})
}
