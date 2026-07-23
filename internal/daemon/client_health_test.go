package daemon

import (
	"strings"
	"testing"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/wire/lifeproto"
)

func TestValidateDaemonHealthRequiresExactReadyLifecycle(t *testing.T) {
	const build = "cc-pool-test"
	healthy := lifeproto.NewHealthResponse(
		build,
		int(wire.ProtocolVersion),
		42,
		string(dkdaemon.StateHealthy),
		false,
		false,
	)
	if err := validateDaemonHealth(healthy, build); err != nil {
		t.Fatalf("healthy lifecycle: %v", err)
	}
	for _, test := range []struct {
		name string
		edit func(*lifeproto.HealthResponse)
		want string
	}{
		{name: "envelope version", edit: func(h *lifeproto.HealthResponse) { h.V++ }, want: "identity is not exact"},
		{name: "operation", edit: func(h *lifeproto.HealthResponse) { h.Op = lifeproto.OpShutdown }, want: "identity is not exact"},
		{name: "build", edit: func(h *lifeproto.HealthResponse) { h.Build = "other" }, want: "identity is not exact"},
		{name: "protocol", edit: func(h *lifeproto.HealthResponse) { h.Protocol++ }, want: "identity is not exact"},
		{name: "pid", edit: func(h *lifeproto.HealthResponse) { h.PID = 0 }, want: "identity is not exact"},
		{name: "state", edit: func(h *lifeproto.HealthResponse) { h.State = string(dkdaemon.StateDegraded) }, want: "is not ready"},
		{name: "draining", edit: func(h *lifeproto.HealthResponse) { h.Draining = true }, want: "is not ready"},
		{name: "busy", edit: func(h *lifeproto.HealthResponse) { h.Busy = true }, want: "is not ready"},
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
