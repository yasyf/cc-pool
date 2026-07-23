package daemon

import (
	"context"
	"errors"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/wire"
)

const stopControlChildArgument = "--cc-pool-daemon-stop-control-v1"

var runDaemonStopControlChild = service.RunStopControlChild

// StopControlChildArguments returns the exact hidden role arguments.
func StopControlChildArguments() []string { return []string{stopControlChildArgument} }

// RunStopControlChild recognizes and executes one controller-authorized daemon settlement.
func RunStopControlChild(ctx context.Context, args []string) (bool, error) {
	if len(args) == 0 || args[0] != stopControlChildArgument {
		return false, nil
	}
	if len(args) != 1 {
		return true, errors.New("daemon: malformed stop-control child arguments")
	}
	_, err := runDaemonStopControlChild(ctx, service.StopControlClientConfig{
		Dial: wire.UnixDialer(pool.SocketPath()), WireBuild: WireBuild,
		RuntimeProtocol: int(wire.ProtocolVersion),
	})
	return true, err
}
