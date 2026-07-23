package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/daemonrole"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/wire"
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
	resolvedDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(resolvedDir, name)
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
	_, runtime, err := s.runtime()
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Run(t.Context()); !errors.Is(err, want) {
		t.Fatalf("runtime error = %v, want activation failure", err)
	}
	if s.m != nil || s.tenantClient != nil || s.disposableWorkers != nil {
		t.Fatal("failed activation published generation-owned resources")
	}
}

func TestServiceRolePathIsStableHomebrewAliasWithoutPATH(t *testing.T) {
	t.Setenv("PATH", "/usr/bin:/bin")
	if path := ServiceRolePath(); path != "/opt/homebrew/bin/cc-pool" {
		t.Fatalf("ServiceRolePath() = %q", path)
	}
}

func TestCurrentServiceExecutableRequiresExactAbsolutePath(t *testing.T) {
	root := t.TempDir()
	target := writeExecutableFixture(t, root, "ccp-v1")
	original := currentServiceExecutable
	currentServiceExecutable = func() (string, error) { return target, nil }
	t.Cleanup(func() { currentServiceExecutable = original })
	path, err := CurrentServiceExecutable()
	if err != nil {
		t.Fatal(err)
	}
	if path != target {
		t.Fatalf("CurrentServiceExecutable() = %q, want %q", path, target)
	}
	currentServiceExecutable = func() (string, error) { return "ccp", nil }
	if _, err := CurrentServiceExecutable(); err == nil {
		t.Fatal("CurrentServiceExecutable accepted a relative executable")
	}
}

func TestStableServiceRoleReauthorizesOnlyRetargetedSuccessor(t *testing.T) {
	dir := t.TempDir()
	oldExecutable := writeExecutableFixture(t, dir, "cc-pool-old")
	newExecutable := writeExecutableFixture(t, dir, "cc-pool-new")
	unrelated := writeExecutableFixture(t, dir, "unrelated")
	alias := filepath.Join(dir, "cc-pool")
	if err := os.Symlink(oldExecutable, alias); err != nil {
		t.Fatal(err)
	}
	classifier := daemonrole.Classifier{RoleID: ServiceRoleID, RolePath: alias}
	peer := func(path string) wire.Peer {
		return wire.Peer{PID: os.Getpid(), UID: os.Geteuid(), StartTime: "start", Boot: "boot", Executable: path}
	}
	if accepted, err := classifier.Classify(t.Context(), peer(oldExecutable)); err != nil || !accepted {
		t.Fatalf("old role accepted=%t err=%v", accepted, err)
	}
	replacement := alias + ".new"
	if err := os.Symlink(newExecutable, replacement); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, alias); err != nil {
		t.Fatal(err)
	}
	if accepted, err := classifier.Classify(t.Context(), peer(newExecutable)); err != nil || !accepted {
		t.Fatalf("new role accepted=%t err=%v", accepted, err)
	}
	for _, path := range []string{oldExecutable, unrelated} {
		if accepted, err := classifier.Classify(t.Context(), peer(path)); err != nil || accepted {
			t.Fatalf("role %q accepted=%t err=%v", path, accepted, err)
		}
	}
}
