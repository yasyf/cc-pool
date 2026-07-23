package daemon

import (
	"context"
	"fmt"

	"github.com/yasyf/cc-pool/internal/statusapp"
)

var (
	statusAppInstall    = statusapp.InstallService
	statusAppDeactivate = statusapp.DeactivateService
	statusAppRollback   = func(ctx context.Context, receipt statusapp.ServiceInstallReceipt) error {
		return receipt.Rollback(ctx)
	}
)

// HolderServiceInstall owns one exact pre-bootstrap signed service deployment.
type HolderServiceInstall struct {
	Receipt   statusapp.ServiceInstallReceipt
	committed bool
}

// Commit crosses the daemon-bootstrap boundary and permanently disarms rollback.
func (i *HolderServiceInstall) Commit() { i.committed = true }

// Rollback deactivates only this receipt's uncommitted newly activated holder.
func (i *HolderServiceInstall) Rollback(ctx context.Context) error {
	if i == nil || i.committed {
		return nil
	}
	if err := statusAppRollback(ctx, i.Receipt); err != nil {
		return fmt.Errorf("rollback FuseKit runtime service: %w", err)
	}
	return nil
}

// StopAndUninstallHolderService removes the holder from the complete desired service set.
func StopAndUninstallHolderService(ctx context.Context) error {
	if err := statusAppDeactivate(ctx); err != nil {
		return fmt.Errorf("remove FuseKit runtime service: %w", err)
	}
	return nil
}

// EnsureHolderService installs the fixed app service and proves its FuseKit session ready.
func EnsureHolderService(ctx context.Context) error {
	install, err := InstallHolderService(ctx)
	if err != nil {
		return err
	}
	install.Commit()
	return nil
}

// InstallHolderService atomically deploys the signed app and its complete service plan.
func InstallHolderService(ctx context.Context) (*HolderServiceInstall, error) {
	receipt, err := statusAppInstall(ctx)
	if err != nil {
		return nil, err
	}
	return &HolderServiceInstall{Receipt: receipt}, nil
}
