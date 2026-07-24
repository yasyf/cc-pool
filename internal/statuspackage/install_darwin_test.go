//go:build darwin

package statuspackage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestInstallDelegatesExactSourceToDaemonkitOwnedApply(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "Cellar", "libexec", "CCPoolStatus.app")
	target := filepath.Join(root, "Applications", "CCPoolStatus.app")
	var applied string
	ops := testOperations(source, target)
	ops.apply = func(_ context.Context, candidate string) error {
		applied = candidate
		if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("product package layer wrote canonical target before daemonkit: %v", err)
		}
		return nil
	}
	if err := install(t.Context(), ops); err != nil {
		t.Fatal(err)
	}
	if applied != source {
		t.Fatalf("candidate = %q, want %q", applied, source)
	}
	if info, err := os.Lstat(filepath.Dir(target)); err != nil || !info.IsDir() {
		t.Fatalf("canonical parent = %#v, %v", info, err)
	}
}

func TestInstallReturnsApplyFailureWithoutProductRollback(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "Cellar", "libexec", "CCPoolStatus.app")
	target := filepath.Join(root, "Applications", "CCPoolStatus.app")
	want := errors.New("sealed apply failed")
	ops := testOperations(source, target)
	ops.apply = func(context.Context, string) error { return want }
	if err := install(t.Context(), ops); !errors.Is(err, want) {
		t.Fatalf("install error = %v, want %v", err, want)
	}
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("product package layer changed canonical target: %v", err)
	}
}

func TestInstallRejectsIdenticalPackagedAndInstalledPaths(t *testing.T) {
	target := filepath.Join(t.TempDir(), "Applications", "CCPoolStatus.app")
	ops := testOperations(target, target)
	if err := install(t.Context(), ops); err == nil {
		t.Fatal("install accepted identical source and target paths")
	}
}

func TestInstallRejectsSymlinkedApplicationDirectory(t *testing.T) {
	root := t.TempDir()
	realParent := filepath.Join(root, "real")
	if err := os.Mkdir(realParent, 0o700); err != nil {
		t.Fatal(err)
	}
	linkedParent := filepath.Join(root, "Applications")
	if err := os.Symlink(realParent, linkedParent); err != nil {
		t.Fatal(err)
	}
	called := false
	ops := testOperations(
		filepath.Join(root, "libexec", "CCPoolStatus.app"),
		filepath.Join(linkedParent, "CCPoolStatus.app"),
	)
	ops.apply = func(context.Context, string) error { called = true; return nil }
	if err := install(t.Context(), ops); err == nil {
		t.Fatal("install accepted a symlinked application directory")
	}
	if called {
		t.Fatal("daemonkit apply was called after directory validation failed")
	}
}

func TestUninstallDelegatesSealedRemoval(t *testing.T) {
	want := errors.New("sealed uninstall failed")
	calls := 0
	ops := operations{uninstall: func(context.Context) error {
		calls++
		return want
	}}
	if err := uninstall(t.Context(), ops); !errors.Is(err, want) {
		t.Fatalf("uninstall error = %v, want %v", err, want)
	}
	if calls != 1 {
		t.Fatalf("uninstall calls = %d, want 1", calls)
	}
}

func testOperations(source, target string) operations {
	return operations{
		packagedPath:  func() (string, error) { return source, nil },
		installedPath: func() string { return target },
		apply:         func(context.Context, string) error { return nil },
		uninstall:     func(context.Context) error { return nil },
	}
}
