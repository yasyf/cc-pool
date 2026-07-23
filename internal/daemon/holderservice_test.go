package daemon

import (
	"context"
	"errors"
	"testing"
)

func TestInstallHolderServiceDelegatesAtomicDeployment(t *testing.T) {
	original := statusAppInstall
	t.Cleanup(func() { statusAppInstall = original })
	calls := 0
	statusAppInstall = func(context.Context) error {
		calls++
		return nil
	}
	err := InstallHolderService(t.Context())
	if err != nil || calls != 1 {
		t.Fatalf("calls/error = %d/%v", calls, err)
	}

	want := errors.New("deployment failed")
	statusAppInstall = func(context.Context) error { return want }
	if err := InstallHolderService(t.Context()); !errors.Is(err, want) {
		t.Fatalf("install error = %v, want %v", err, want)
	}
}

func TestStopAndUninstallHolderServiceDelegatesDurableDeactivation(t *testing.T) {
	original := statusAppDeactivate
	t.Cleanup(func() { statusAppDeactivate = original })
	calls := 0
	statusAppDeactivate = func(context.Context) error {
		calls++
		return nil
	}
	if err := StopAndUninstallHolderService(t.Context()); err != nil || calls != 1 {
		t.Fatalf("calls/error = %d/%v", calls, err)
	}
	want := errors.New("deactivation failed")
	statusAppDeactivate = func(context.Context) error { return want }
	if err := StopAndUninstallHolderService(t.Context()); !errors.Is(err, want) {
		t.Fatalf("deactivation error = %v, want %v", err, want)
	}
}
