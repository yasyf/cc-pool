package daemon

import (
	"os"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/wire"
)

func TestValidateDaemonHealthRequiresExactReadyRuntime(t *testing.T) {
	const build = "cc-pool-test"
	healthy := DaemonHealthResponse{
		Schema: DaemonHealthSchema, RuntimeBuild: build, RuntimeProtocol: int(wire.ProtocolVersion), PID: os.Getpid(),
		ProcessGeneration: "generation-1", State: DaemonRuntimeStateHealthy, Ready: true,
	}
	if err := validateDaemonHealth(healthy, build); err != nil {
		t.Fatalf("healthy runtime response: %v", err)
	}
	for _, test := range []struct {
		name string
		edit func(*DaemonHealthResponse)
		want string
	}{
		{name: "schema", edit: func(h *DaemonHealthResponse) { h.Schema++ }, want: "identity is not exact"},
		{name: "runtime build", edit: func(h *DaemonHealthResponse) { h.RuntimeBuild = "other" }, want: "build is not exact"},
		{name: "runtime protocol", edit: func(h *DaemonHealthResponse) { h.RuntimeProtocol++ }, want: "identity is not exact"},
		{name: "pid", edit: func(h *DaemonHealthResponse) { h.PID = 0 }, want: "identity is not exact"},
		{name: "process generation", edit: func(h *DaemonHealthResponse) { h.ProcessGeneration = "" }, want: "identity is not exact"},
		{name: "unknown state", edit: func(h *DaemonHealthResponse) { h.State = "future" }, want: "identity is not exact"},
		{name: "state", edit: func(h *DaemonHealthResponse) { h.State = DaemonRuntimeStateDegraded }, want: "is not ready"},
		{name: "draining", edit: func(h *DaemonHealthResponse) { h.Draining = true }, want: "is not ready"},
		{name: "busy", edit: func(h *DaemonHealthResponse) { h.Busy = true }, want: "is not ready"},
		{name: "ready", edit: func(h *DaemonHealthResponse) { h.Ready = false }, want: "is not ready"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := healthy
			test.edit(&got)
			if err := validateDaemonHealth(got, build); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateDaemonHealth(%#v) = %v, want %q", got, err, test.want)
			}
		})
	}
}
