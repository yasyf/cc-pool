package pool

import (
	"context"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/workerexec"
	"github.com/yasyf/daemonkit/worker"
)

type hostSyncCommandRunner struct{ request worker.CommandRequest }

func (r *hostSyncCommandRunner) Run(_ context.Context, request worker.CommandRequest) (worker.CommandResult, error) {
	r.request = request
	return worker.CommandResult{}, nil
}

func TestHostSyncWorkersUseDistinctV1ProcessLedger(t *testing.T) {
	if HostSyncWorkerStorePath() == DisposableWorkerStorePath() {
		t.Fatal("host-sync and daemon workers share a process ledger")
	}
	if got := HostSyncWorkerStorePath(); !strings.HasSuffix(got, "hostsync-workers-v1.db") {
		t.Fatalf("host-sync worker ledger = %q, want v1 path", got)
	}
	for _, path := range []string{HostSyncHelperWorkerStorePath(), HostSyncHelperChildStorePath(), HostSyncHelperStopStorePath()} {
		if path == HostSyncWorkerStorePath() || path == HostSyncChildStorePath() {
			t.Fatalf("resident helper shares disposable operation ledger %q", path)
		}
		if !strings.HasSuffix(path, "-v1.db") {
			t.Fatalf("resident helper ledger = %q, want v1 path", path)
		}
	}
}

func TestHostSyncCommandUsesExplicitExecutableWithSealedPath(t *testing.T) {
	t.Setenv("PATH", "")
	runner := &hostSyncCommandRunner{}
	manager := &Manager{workers: &workerRuntime{}, taskRunner: runner}
	if err := manager.RunHostSyncCommand(t.Context(), "/opt/homebrew/bin/synckitd", "register", "/cfg/cc-pool.json"); err != nil {
		t.Fatal(err)
	}
	if runner.request.Path != "/opt/homebrew/bin/synckitd" {
		t.Fatalf("command path = %q", runner.request.Path)
	}
	if got := runner.request.Args; len(got) != 2 || got[0] != "register" || got[1] != "/cfg/cc-pool.json" {
		t.Fatalf("command args = %v", got)
	}
	if runner.request.Env != nil {
		t.Fatalf("command inherited environment: %v", runner.request.Env)
	}

	for _, path := range []string{"", "synckitd", "/opt/homebrew/bin/../bin/synckitd"} {
		if err := manager.RunHostSyncCommand(t.Context(), path, "register"); err == nil {
			t.Fatalf("inexact command path %q accepted", path)
		}
	}
}

var _ workerexec.Runner = (*hostSyncCommandRunner)(nil)
