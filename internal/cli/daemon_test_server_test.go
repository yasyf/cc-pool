package cli

import (
	"context"
	"testing"

	ccdaemon "github.com/yasyf/cc-pool/internal/daemon"
)

type daemonTestHandler func(context.Context, ccdaemon.Op, ccdaemon.Request) ccdaemon.Response

// startDaemonTestServer stood up the retired wire transport on the real
// socket. daemonkit derives its socket and state from the passwd home, so an
// in-test daemon needs the fleet's serve sandbox first — cc-notes 6ef1e56.
func startDaemonTestServer(t *testing.T, build string, handler daemonTestHandler) {
	t.Helper()
	_ = build
	_ = handler
	t.Skip("daemon-socket tests await the daemonkit serve sandbox (cc-notes 6ef1e56)")
}
