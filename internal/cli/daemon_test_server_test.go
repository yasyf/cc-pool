package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	ccdaemon "github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/testhome"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit"
)

type daemonTestHandler func(context.Context, ccdaemon.Op, ccdaemon.Request) ccdaemon.Response

// daemonTestReadyOp is the fixture-owned readiness probe: intercepted before
// the consumer handler, retried until the daemon stops refusing, because
// WaitReady routes through the control lane and daemonkit refuses an
// in-process self-attach there (control.go:136). The retired transport had no
// such guard — the old fixture's in-process Control coverage exercised a
// shape daemonkit now forbids by design.
const daemonTestReadyOp = "__fixture-ready"

type daemonTestProduct struct {
	handler daemonTestHandler
}

func (p daemonTestProduct) Handle(ctx context.Context, req daemonkit.Request) (daemonkit.Reply, error) {
	if req.Op == daemonTestReadyOp {
		return daemonkit.Reply{Body: []byte("{}")}, nil
	}
	var request ccdaemon.Request
	if err := json.Unmarshal(req.Body, &request); err != nil {
		return daemonkit.Reply{}, err
	}
	request.Op = ccdaemon.Op(req.Op)
	body, err := json.Marshal(p.handler(ctx, request.Op, request))
	if err != nil {
		return daemonkit.Reply{}, err
	}
	return daemonkit.Reply{Body: body}, nil
}

func (daemonTestProduct) Drain(daemonkit.Budget) error { return nil }
func (daemonTestProduct) Close(daemonkit.Budget) error { return nil }

// startDaemonTestServer serves handler in-process on the real label-derived
// socket, so the consumer RPC path is exercised end to end over the business
// lane. The control lane is stubbed at the cli seam with the same exact-ready
// validation the client applies: build mismatch surfaces as the client's own
// ErrDaemonBuildMismatch. Callers that sandboxed already keep their home; the
// root must be short — macOS caps sun_path at 104 bytes and daemonkit refuses
// a longer socket path typed.
func startDaemonTestServer(t *testing.T, build string, handler daemonTestHandler) {
	t.Helper()
	if build == "" {
		build = version.String()
	}
	if os.Getenv(testhome.EnvOverride) == "" {
		tempHome(t)
	}
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	swapVar(t, &daemonHealthContext, func(context.Context, *ccdaemon.Client) (*ccdaemon.HealthResponse, error) {
		if build != version.String() {
			return nil, fmt.Errorf(
				"%w: build=%q want=%q", ccdaemon.ErrDaemonBuildMismatch, build, version.String(),
			)
		}
		return &ccdaemon.HealthResponse{
			Schema: ccdaemon.DaemonHealthSchema, RuntimeBuild: build,
			State: ccdaemon.RuntimeStateHealthy, Ready: true,
		}, nil
	})
	spec := ccdaemon.Spec(daemonkit.Program{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := daemonkit.Serve(ctx, spec, func(daemonkit.Ctx) (daemonkit.Product, error) {
			return daemonTestProduct{handler: handler}, nil
		})
		done <- err
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("serve test daemon: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("test daemon did not drain")
		}
	})
	client, err := daemonkit.Open(spec)
	if err != nil {
		t.Fatal(err)
	}
	lane := client.Business()
	t.Cleanup(func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), time.Second)
		defer cancelClose()
		_ = lane.Close(closeCtx)
	})
	readyCtx, cancelReady := context.WithTimeout(ctx, 10*time.Second)
	defer cancelReady()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		attempt, cancelAttempt := context.WithTimeout(readyCtx, time.Second)
		_, lastErr = lane.Call(attempt, daemonTestReadyOp, []byte("{}"))
		cancelAttempt()
		if lastErr == nil {
			return
		}
		select {
		case err := <-done:
			t.Fatalf("test daemon exited before readiness: %v", errors.Join(err, lastErr))
		case <-readyCtx.Done():
			t.Fatalf("await test daemon readiness: %v", errors.Join(readyCtx.Err(), lastErr))
		case <-ticker.C:
		}
	}
}
