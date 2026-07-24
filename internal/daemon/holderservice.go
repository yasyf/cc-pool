package daemon

import (
	"context"
	"fmt"

	"github.com/yasyf/cc-pool/internal/statusapp"
)

var (
	statusAppRequire   = statusapp.RequireActiveService
	statusAppUninstall = statusapp.UninstallPackagedApp
)

// HolderServiceInstall records one successful exact active-service requirement.
type HolderServiceInstall struct{}

// Commit records no product rollback because daemonkit terminally owns candidate application.
func (*HolderServiceInstall) Commit() {}

// Rollback is a no-op because requiring the active service performs no mutation.
func (*HolderServiceInstall) Rollback(ctx context.Context) error {
	_ = ctx
	return nil
}

// StopAndUninstallHolderService removes the holder from the complete desired service set.
func StopAndUninstallHolderService(ctx context.Context) error {
	if err := statusAppUninstall(ctx); err != nil {
		return fmt.Errorf("remove FuseKit runtime service: %w", err)
	}
	return nil
}

// EnsureHolderService requires the packaged fixed app service and proves its FuseKit session ready.
func EnsureHolderService(ctx context.Context) error {
	return statusAppRequire(ctx)
}

// InstallHolderService requires the separately packaged signed app and its complete service plan.
func InstallHolderService(ctx context.Context) (*HolderServiceInstall, error) {
	if err := statusAppRequire(ctx); err != nil {
		return nil, err
	}
	return &HolderServiceInstall{}, nil
}
