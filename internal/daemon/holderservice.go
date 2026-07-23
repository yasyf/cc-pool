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
	"github.com/yasyf/fusekit/mountproto"
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
	holderRuntimeReady = func(ctx context.Context, socket string) error {
		client, err := tenantfs.NewClient(ctx, socket)
		if err != nil {
			return err
		}
		runtimeHealth, err := client.RuntimeHealth(ctx)
		closeErr := client.Close()
		if err != nil {
			return errors.Join(err, closeErr)
		}
		return errors.Join(validateHolderRuntimeHealth(runtimeHealth), closeErr)
	}
	holderReady = func(ctx context.Context, socket string) error {
		return holderRuntimeReady(ctx, socket)
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

func validateHolderRuntimeHealth(health mountproto.RuntimeHealthResponse) error {
	if health.ActivationGeneration == "" {
		return errors.New("holder runtime activation generation is empty")
	}
	if health.NativePhase != mountproto.NativePhaseLive || health.NativeMount == nil {
		return fmt.Errorf(
			"holder native presentation is not ready: phase=%q proof=%t",
			health.NativePhase, health.NativeMount != nil,
		)
	}
	proof := health.NativeMount
	expectedSource, err := mountproto.NativeMountSource(pool.FuseKitPresentationRoot())
	if err != nil {
		return fmt.Errorf("derive holder native mount source: %w", err)
	}
	if proof.PresentationRoot != pool.FuseKitPresentationRoot() ||
		proof.Filesystem != mountproto.NativeMountFilesystem ||
		proof.Source != expectedSource ||
		proof.RootReadEpoch == 0 {
		return fmt.Errorf(
			"holder native mount proof is not exact: root=%q filesystem=%q source=%q root_read_epoch=%d",
			proof.PresentationRoot, proof.Filesystem, proof.Source, proof.RootReadEpoch,
		)
	}
	return nil
}

type holderServiceController interface {
	Converge(context.Context, []service.Agent) error
	Close(context.Context) error
}

// HolderServiceInstall records whether one install transaction created the holder service.
type HolderServiceInstall struct {
	created bool
}

// Rollback removes only a holder service created by this install transaction.
func (install HolderServiceInstall) Rollback(ctx context.Context) error {
	if !install.created {
		return nil
	}
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), holderServiceCloseWait)
	defer cancel()
	if err := StopAndUninstallHolderService(rollbackCtx); err != nil {
		return fmt.Errorf("roll back FuseKit holder install: %w", err)
	}
	return nil
}

// HolderDeploymentPlan returns the daemon-facing fixed application contract.
func HolderDeploymentPlan() (holder.DeploymentPlan, error) {
	return holder.NewDeploymentPlan(holder.DeploymentPlanSpec{
		Application:         holderApplication(),
		RuntimeDirectory:    pool.FuseKitRuntimeDir(),
		PresentationRoot:    pool.FuseKitPresentationRoot(),
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
	_, err := InstallHolderService(ctx)
	return err
}

// InstallHolderService installs the holder and returns its transaction-scoped rollback receipt.
func InstallHolderService(ctx context.Context) (HolderServiceInstall, error) {
	plan, err := HolderDeploymentPlan()
	if err != nil {
		return HolderServiceInstall{}, fmt.Errorf("derive FuseKit holder plan: %w", err)
	}
	appPath := plan.Application().AppPath
	info, err := holderAppStat(appPath)
	if err != nil {
		return HolderServiceInstall{}, fmt.Errorf("required FuseKit holder app %s: %w", appPath, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return HolderServiceInstall{}, fmt.Errorf("required FuseKit holder app %s is not a direct app bundle", appPath)
	}
	agent := plan.Agent()
	install := HolderServiceInstall{}
	err = withHolderServiceController(ctx, func(controller holderServiceController) error {
		preexisting, err := holderServicePresent(agent)
		if err != nil {
			return fmt.Errorf("inspect FuseKit holder service: %w", err)
		}
		install.created = !preexisting
		if err := controller.Converge(ctx, []service.Agent{agent}); err != nil {
			return fmt.Errorf("converge FuseKit holder service: %w", err)
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
				return fmt.Errorf(
					"wait for FuseKit holder readiness: %w", errors.Join(readyCtx.Err(), err),
				)
			case <-time.After(100 * time.Millisecond):
			}
		}
	})
	if err == nil {
		return install, nil
	}
	return HolderServiceInstall{}, errors.Join(err, install.Rollback(ctx))
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
