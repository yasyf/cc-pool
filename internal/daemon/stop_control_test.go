package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/wire"
)

func TestRunStopControlChildRecognizesExactPrivateMode(t *testing.T) {
	for _, args := range [][]string{nil, {"status"}} {
		recognized, err := RunStopControlChild(t.Context(), args)
		if recognized || err != nil {
			t.Fatalf("RunStopControlChild(%q) = %t, %v", args, recognized, err)
		}
	}
	recognized, err := RunStopControlChild(t.Context(), []string{stopControlChildArgument, "extra"})
	if !recognized || err == nil {
		t.Fatalf("malformed child = %t, %v", recognized, err)
	}
}

func TestRunStopControlChildDelegatesStableWireIdentity(t *testing.T) {
	want := errors.New("delegated")
	original := runDaemonStopControlChild
	t.Cleanup(func() { runDaemonStopControlChild = original })
	runDaemonStopControlChild = func(_ context.Context, config service.StopControlClientConfig) (wire.StopResult, error) {
		if config.WireBuild != WireBuild || config.RuntimeProtocol != int(wire.ProtocolVersion) || config.Dial == nil {
			t.Fatalf("stop child config = %+v", config)
		}
		return wire.StopResult{}, want
	}
	recognized, err := RunStopControlChild(t.Context(), StopControlChildArguments())
	if !recognized || !errors.Is(err, want) {
		t.Fatalf("RunStopControlChild() = %t, %v", recognized, err)
	}
}

func TestMainDispatchesStopControlBeforeInheritedFDCleanup(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("..", "..", "cmd", "cc-pool", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	stop := strings.Index(source, "daemon.RunStopControlChild")
	worker := strings.Index(source, "hostsync.IsWorkerInvocation")
	closeFDs := strings.Index(source, "proc.CloseInheritedFDs")
	cli := strings.Index(source, "cli.NewRootCmd")
	if stop < 0 || worker < 0 || closeFDs < 0 || cli < 0 || stop >= worker || stop >= closeFDs || stop >= cli {
		t.Fatalf("main dispatch order stop=%d worker=%d close=%d cli=%d", stop, worker, closeFDs, cli)
	}
}
