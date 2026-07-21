package holderbridge

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

func TestRuntimePlanSpecPinsProductIdentityAndProtectedPolicy(t *testing.T) {
	const appPath = "/Applications/CCPoolStatus.app"
	const runtimeDirectory = "/Users/test/.cc-pool/fusekit"
	const presentationRoot = "/Users/test/.cc-pool/accounts"
	const buildID = "v0.60.0"
	spec := RuntimePlanSpec(appPath, runtimeDirectory, presentationRoot, buildID, nil)
	application := spec.Application
	if application != Application(appPath) || application.BundleID != BundleID ||
		application.TeamID != TeamID || application.Broker != application.Runtime ||
		application.Runtime.ExecutableName != ExecutableName ||
		application.Runtime.SigningIdentifier != BundleID {
		t.Fatalf("application = %#v", application)
	}
	if spec.RuntimeDirectory != runtimeDirectory || spec.PresentationRoot != presentationRoot ||
		spec.BuildID != buildID ||
		!spec.SourceCapable || spec.BrokerPolicy.RequiredAppGroup != AppGroup ||
		spec.RuntimePolicy.RequiredAppGroup != AppGroup {
		t.Fatalf("runtime plan spec = %#v", spec)
	}
}

func TestToolRunnerExecutesAndSettlesOneDisposableTask(t *testing.T) {
	runner, err := NewToolRunner(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	directory := runner.directory
	if err := runner.Run(t.Context(), supervise.Task{
		RecoveryClass: proc.RecoveryTask,
		Path:          "/usr/bin/true",
	}); err != nil {
		t.Fatal(err)
	}
	if err := runner.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("runner directory still exists: %v", err)
	}
	if err := runner.Close(t.Context()); err != nil {
		t.Fatalf("idempotent close = %v", err)
	}
}

func TestToolRunnerRejectsTaskAfterClose(t *testing.T) {
	runner, err := NewToolRunner(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	err = runner.Run(context.Background(), supervise.Task{
		RecoveryClass: proc.RecoveryTask,
		Path:          "/usr/bin/true",
	})
	if !errors.Is(err, supervise.ErrClosed) {
		t.Fatalf("Run after Close = %v, want ErrClosed", err)
	}
}
