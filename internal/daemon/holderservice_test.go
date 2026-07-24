package daemon

import (
	"context"
	"errors"
	"testing"
)

func TestInstallHolderServiceRequiresAlreadyActivePackagedRuntime(t *testing.T) {
	original := statusAppRequire
	t.Cleanup(func() { statusAppRequire = original })
	calls := 0
	statusAppRequire = func(context.Context) error {
		calls++
		return nil
	}
	install, err := InstallHolderService(t.Context())
	if err != nil || install == nil || calls != 1 {
		t.Fatalf("install/calls/error = %#v/%d/%v", install, calls, err)
	}
	install.Commit()
	if err := install.Rollback(t.Context()); err != nil || calls != 1 {
		t.Fatalf("nonmutating rollback calls/error = %d/%v", calls, err)
	}

	want := errors.New("active service unavailable")
	statusAppRequire = func(context.Context) error { return want }
	if _, err := InstallHolderService(t.Context()); !errors.Is(err, want) {
		t.Fatalf("require error = %v, want %v", err, want)
	}
}

func TestEnsureHolderServiceDelegatesExactActiveRequirement(t *testing.T) {
	original := statusAppRequire
	t.Cleanup(func() { statusAppRequire = original })
	calls := 0
	statusAppRequire = func(context.Context) error {
		calls++
		return nil
	}
	if err := EnsureHolderService(t.Context()); err != nil || calls != 1 {
		t.Fatalf("calls/error = %d/%v", calls, err)
	}
}

func TestStopAndUninstallHolderServiceDelegatesSealedRemoval(t *testing.T) {
	original := statusAppUninstall
	t.Cleanup(func() { statusAppUninstall = original })
	calls := 0
	statusAppUninstall = func(context.Context) error {
		calls++
		return nil
	}
	if err := StopAndUninstallHolderService(t.Context()); err != nil || calls != 1 {
		t.Fatalf("calls/error = %d/%v", calls, err)
	}
	want := errors.New("uninstall failed")
	statusAppUninstall = func(context.Context) error { return want }
	if err := StopAndUninstallHolderService(t.Context()); !errors.Is(err, want) {
		t.Fatalf("uninstall error = %v, want %v", err, want)
	}
}
