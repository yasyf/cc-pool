package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/drain"
)

func TestRuntimeAcquiresListenerBeforeGenerationActivation(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "ccp-activation-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	role := filepath.Join(root, "ccp")
	if err := os.WriteFile(role, []byte("fixture"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root)
	socket := filepath.Join(root, "daemon.sock")
	want := errors.New("activation stopped")
	originalEnsure := ensureHolderRuntime
	t.Cleanup(func() { ensureHolderRuntime = originalEnsure })
	ensureHolderRuntime = func(context.Context) error {
		info, err := os.Lstat(socket)
		if err != nil {
			t.Fatalf("listener was not acquired before activation: %v", err)
		}
		if info.Mode()&os.ModeSocket == 0 {
			t.Fatalf("activation observed non-socket listener mode %v", info.Mode())
		}
		return want
	}
	s := &Server{
		socket:       socket,
		wireIntake:   &drain.Intake{},
		syncIntake:   &drain.Intake{},
		evictTimeout: defaultEvictTimeout,
	}
	wireServer, runtime, err := s.runtime()
	if err != nil {
		t.Fatal(err)
	}
	wireServer.RegisterLifecycle(runtime)
	if err := runtime.Run(t.Context()); !errors.Is(err, want) {
		t.Fatalf("runtime error = %v, want activation failure", err)
	}
	if s.m != nil || s.tenantClient != nil || s.disposableWorkers != nil {
		t.Fatal("failed activation published generation-owned resources")
	}
}

func TestServiceRolePathPreservesStableAlias(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "ccp-v1")
	if err := os.WriteFile(target, []byte("fixture"), 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "ccp")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root)
	path, err := ServiceRolePath()
	if err != nil {
		t.Fatal(err)
	}
	if path != alias {
		t.Fatalf("ServiceRolePath() = %q, want stable alias %q", path, alias)
	}
}
