package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/drain"
)

func writeExecutableFixture(t *testing.T, dir, name string) string {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o700)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("fixture"); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, name)
}

func TestRuntimeAcquiresListenerBeforeGenerationActivation(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "ccp-activation-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	_ = writeExecutableFixture(t, root, "ccp")
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

func TestServiceRolePathUsesExactCurrentExecutableWithoutPATH(t *testing.T) {
	root := t.TempDir()
	target := writeExecutableFixture(t, root, "ccp-v1")
	alias := filepath.Join(root, "ccp")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	original := serviceRoleExecutable
	serviceRoleExecutable = func() (string, error) { return alias, nil }
	t.Cleanup(func() { serviceRoleExecutable = original })
	t.Setenv("PATH", "/usr/bin:/bin")
	path, err := ServiceRolePath()
	if err != nil {
		t.Fatal(err)
	}
	if path != alias {
		t.Fatalf("ServiceRolePath() = %q, want current executable %q", path, alias)
	}
}

func TestServiceRolePathRejectsNonAbsoluteCurrentExecutable(t *testing.T) {
	original := serviceRoleExecutable
	serviceRoleExecutable = func() (string, error) { return "ccp", nil }
	t.Cleanup(func() { serviceRoleExecutable = original })
	t.Setenv("PATH", "/opt/homebrew/bin")
	if _, err := ServiceRolePath(); err == nil {
		t.Fatal("ServiceRolePath accepted a relative current executable")
	}
}
