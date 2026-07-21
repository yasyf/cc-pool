package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/fusekit/holder"
)

const (
	holderReadinessWindow  = 15 * time.Second
	holderServiceWorkers   = 1
	holderServiceCloseWait = 30 * time.Second
)

var (
	holderEntitlementPolicyDigest = [32]byte{
		0x48, 0xe0, 0x64, 0xe5, 0xc6, 0xcd, 0x4f, 0xa0,
		0x85, 0xd9, 0x91, 0x31, 0x57, 0x59, 0x35, 0x1d,
		0x8a, 0xb7, 0x6f, 0xb2, 0x23, 0xaf, 0x6b, 0x9f,
		0x8c, 0x6f, 0x25, 0xa6, 0xcf, 0x7f, 0xbd, 0x9b,
	}
	holderApplication = func() holder.SignedApplication {
		return holderbridge.Application(pool.WidgetAppPath())
	}
	holderAppStat        = os.Lstat
	holderControllerOpen = func(
		ctx context.Context,
		config service.ControllerConfig,
	) (holderServiceController, error) {
		return service.NewController(ctx, config)
	}
	holderReady = func(ctx context.Context, socket string) error {
		client, err := tenantfs.NewClient(ctx, socket)
		if err != nil {
			return err
		}
		return client.Close()
	}
	holderServicePresent = func(agent service.Agent) (bool, error) {
		path, err := agent.PlistPath()
		if err != nil {
			return false, err
		}
		_, err = os.Lstat(path)
		if err == nil {
			return true, nil
		}
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
)

type holderServiceController interface {
	Converge(context.Context, []service.Agent) error
	Close(context.Context) error
}

// HolderDeploymentPlan returns the daemon-facing fixed application contract.
func HolderDeploymentPlan() (holder.DeploymentPlan, error) {
	return holder.NewDeploymentPlan(holder.DeploymentPlanSpec{
		Application:         holderApplication(),
		RuntimeDirectory:    pool.FuseKitRuntimeDir(),
		BuildID:             version.String(),
		SourceCapable:       true,
		BrokerPolicyDigest:  holderEntitlementPolicyDigest,
		RuntimePolicyDigest: holderEntitlementPolicyDigest,
	})
}

// StopAndUninstallHolderService removes the holder from the complete desired service set.
func StopAndUninstallHolderService(ctx context.Context) error {
	if err := convergeHolderServices(ctx, nil); err != nil {
		return fmt.Errorf("remove FuseKit holder service: %w", err)
	}
	return nil
}

// EnsureHolderService installs the fixed app service and proves its FuseKit session ready.
func EnsureHolderService(ctx context.Context) error {
	plan, err := HolderDeploymentPlan()
	if err != nil {
		return fmt.Errorf("derive FuseKit holder plan: %w", err)
	}
	appPath := plan.Application().AppPath
	info, err := holderAppStat(appPath)
	if err != nil {
		return fmt.Errorf("required FuseKit holder app %s: %w", appPath, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("required FuseKit holder app %s is not a direct app bundle", appPath)
	}
	agent := plan.Agent()
	return withHolderServiceController(ctx, func(controller holderServiceController) error {
		preexisting, err := holderServicePresent(agent)
		if err != nil {
			return fmt.Errorf("inspect FuseKit holder service: %w", err)
		}
		if err := controller.Converge(ctx, []service.Agent{agent}); err != nil {
			return rollbackNewHolderService(ctx, controller, preexisting,
				fmt.Errorf("converge FuseKit holder service: %w", err))
		}
		readyCtx, cancel := context.WithTimeout(ctx, holderReadinessWindow)
		defer cancel()
		for {
			err := holderReady(readyCtx, plan.Paths().Socket)
			if err == nil {
				return nil
			}
			select {
			case <-readyCtx.Done():
				readinessErr := fmt.Errorf(
					"wait for FuseKit holder readiness: %w", errors.Join(readyCtx.Err(), err),
				)
				return rollbackNewHolderService(ctx, controller, preexisting, readinessErr)
			case <-time.After(100 * time.Millisecond):
			}
		}
	})
}

func rollbackNewHolderService(
	ctx context.Context,
	controller holderServiceController,
	preexisting bool,
	cause error,
) error {
	if preexisting {
		return cause
	}
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), holderServiceCloseWait)
	defer cancel()
	if err := controller.Converge(rollbackCtx, nil); err != nil {
		return errors.Join(cause, fmt.Errorf("roll back FuseKit holder service: %w", err))
	}
	return cause
}

func convergeHolderServices(ctx context.Context, agents []service.Agent) error {
	return withHolderServiceController(ctx, func(controller holderServiceController) error {
		return controller.Converge(ctx, agents)
	})
}

func withHolderServiceController(
	ctx context.Context,
	run func(holderServiceController) error,
) (err error) {
	controller, err := holderControllerOpen(ctx, service.ControllerConfig{
		StatePath:   filepath.Join(pool.FuseKitRuntimeDir(), "service-state.db"),
		ProcessPath: filepath.Join(pool.FuseKitRuntimeDir(), "service-processes.db"),
		WorkerLimit: holderServiceWorkers,
	})
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), holderServiceCloseWait)
		defer cancel()
		err = errors.Join(err, controller.Close(closeCtx))
	}()
	return run(controller)
}
