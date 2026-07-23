package daemon

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/yasyf/cc-pool/internal/version"
	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/wire"
)

func TestRuntimeHealthReflectsHolderSessionState(t *testing.T) {
	server := &Server{}
	if state := server.runtimeHealthState(); state != dkdaemon.StateDegraded || !server.runtimeBusy() {
		t.Fatalf("unactivated health = %q busy=%t", state, server.runtimeBusy())
	}
	server.holderActive.Store(true)
	if state := server.runtimeHealthState(); state != dkdaemon.StateHealthy || server.runtimeBusy() {
		t.Fatalf("active health = %q busy=%t", state, server.runtimeBusy())
	}
	server.holderActive.Store(false)
	server.holderLost.Store(true)
	if state := server.runtimeHealthState(); state != dkdaemon.StateFailed || !server.runtimeBusy() {
		t.Fatalf("lost health = %q busy=%t", state, server.runtimeBusy())
	}
}

func TestDaemonHealthObservationRequiresPublishedHealthyRuntime(t *testing.T) {
	health := dkdaemon.Health{
		RuntimeBuild: version.String(), RuntimeProtocol: int(wire.ProtocolVersion),
		ProcessGeneration: "generation-1", PID: 42, State: dkdaemon.StateHealthy, Busy: true,
	}
	server := &Server{
		wireIntake:    &drain.Intake{},
		runtimeHealth: func(context.Context) (dkdaemon.Health, error) { return health, nil },
	}
	route := server.daemonHealthRoute()
	if route.Op != wire.Op(OpHealth) || route.MaxResponseBytes != daemonHealthMaxResponse || !route.AvailableBeforeReady {
		t.Fatalf("daemon health route = %+v", route)
	}
	request := wire.ObservationRequest{Op: wire.Op(OpHealth), Payload: []byte(`{"schema":1}`)}
	result, err := server.daemonHealthObservation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	var response DaemonHealthResponse
	if err := json.Unmarshal(result.Payload, &response); err != nil {
		t.Fatal(err)
	}
	if response.Ready || response.State != DaemonRuntimeStateHealthy || !response.Busy {
		t.Fatalf("unpublished health = %+v", response)
	}
	health.Busy = false
	health.Ready = true
	result, err = server.daemonHealthObservation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	response = DaemonHealthResponse{}
	if err := json.Unmarshal(result.Payload, &response); err != nil {
		t.Fatal(err)
	}
	if !response.Ready || response.RuntimeBuild != version.String() || response.ProcessGeneration != "generation-1" {
		t.Fatalf("health observation = %+v", response)
	}
	health.Draining = true
	health.Ready = false
	result, err = server.daemonHealthObservation(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	response = DaemonHealthResponse{}
	if err := json.Unmarshal(result.Payload, &response); err != nil {
		t.Fatal(err)
	}
	if response.Ready || !response.Draining {
		t.Fatalf("draining health = %+v", response)
	}
	health.State = dkdaemon.State("future")
	if _, err := server.daemonHealthObservation(context.Background(), request); err == nil {
		t.Fatal("health observation accepted an unknown daemonkit state")
	}
}
