package daemon

import (
	"context"
	"fmt"

	"github.com/yasyf/cc-pool/internal/statusapp"
)

var (
	statusAppInstall    = statusapp.InstallService
	statusAppDeactivate = statusapp.DeactivateService
)

// StopAndUninstallHolderService removes the holder from the complete desired service set.
func StopAndUninstallHolderService(ctx context.Context) error {
	if err := statusAppDeactivate(ctx); err != nil {
		return fmt.Errorf("remove FuseKit runtime service: %w", err)
	}
	return nil
}

// EnsureHolderService installs the fixed app service and proves its FuseKit session ready.
func EnsureHolderService(ctx context.Context) error {
	return InstallHolderService(ctx)
}

// InstallHolderService atomically deploys the signed app and its complete service plan.
func InstallHolderService(ctx context.Context) error {
	if err := statusAppInstall(ctx); err != nil {
		return err
	}
	return nil
}
