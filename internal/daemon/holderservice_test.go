package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/yasyf/cc-pool/internal/statusapp"
)

func TestInstallHolderServiceDelegatesAtomicDeployment(t *testing.T) {
	original := statusAppInstall
	t.Cleanup(func() { statusAppInstall = original })
	calls := 0
	wantReceipt := statusapp.ServiceInstallReceipt{Changed: true}
	statusAppInstall = func(context.Context) (statusapp.ServiceInstallReceipt, error) {
		calls++
		return wantReceipt, nil
	}
	install, err := InstallHolderService(t.Context())
	if err != nil || calls != 1 || install.Receipt != wantReceipt {
		t.Fatalf("calls/error = %d/%v", calls, err)
	}

	want := errors.New("deployment failed")
	statusAppInstall = func(context.Context) (statusapp.ServiceInstallReceipt, error) {
		return statusapp.ServiceInstallReceipt{}, want
	}
	if _, err := InstallHolderService(t.Context()); !errors.Is(err, want) {
		t.Fatalf("install error = %v, want %v", err, want)
	}
}

func TestHolderServiceInstallRollbackStopsAtCommitBoundary(t *testing.T) {
	original := statusAppRollback
	t.Cleanup(func() { statusAppRollback = original })
	rollbacks := 0
	statusAppRollback = func(context.Context, statusapp.ServiceInstallReceipt) error {
		rollbacks++
		return nil
	}
	install := &HolderServiceInstall{Receipt: statusapp.ServiceInstallReceipt{Changed: true}}
	if err := install.Rollback(t.Context()); err != nil || rollbacks != 1 {
		t.Fatalf("pre-commit rollback = %d/%v", rollbacks, err)
	}
	install.Commit()
	if err := install.Rollback(t.Context()); err != nil || rollbacks != 1 {
		t.Fatalf("post-commit rollback = %d/%v", rollbacks, err)
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
