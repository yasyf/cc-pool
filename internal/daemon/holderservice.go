package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/holder"
	"github.com/yasyf/fusekit/mountproto"
)

const (
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
	holderRuntimeHealth = func(ctx context.Context, socket string) (mountproto.RuntimeHealthResponse, error) {
		client, err := tenantfs.NewClient(ctx, socket)
		if err != nil {
			return mountproto.RuntimeHealthResponse{}, err
		}
		runtimeHealth, err := client.RuntimeHealth(ctx)
		closeErr := client.Close()
		if err != nil {
			return mountproto.RuntimeHealthResponse{}, errors.Join(err, closeErr)
		}
		return runtimeHealth, closeErr
	}
	holderRuntimeReady = func(ctx context.Context, socket string) error {
		runtimeHealth, err := holderRuntimeHealth(ctx, socket)
		if err != nil {
			return err
		}
		return validateHolderRuntimeHealth(runtimeHealth)
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
	if health.Protocol != mountproto.Version || health.Code != mountproto.ErrorCodeOk || health.Message != "" {
		return fmt.Errorf(
			"holder runtime health response is not exact: protocol=%d code=%q message=%q",
			health.Protocol, health.Code, health.Message,
		)
	}
	if health.RuntimeBuild != version.String() {
		return fmt.Errorf(
			"holder runtime build is not exact: build=%q want=%q",
			health.RuntimeBuild, version.String(),
		)
	}
	if health.RuntimeProtocol != mountproto.RuntimeProtocolVersion {
		return fmt.Errorf(
			"holder runtime protocol is not exact: protocol=%d want=%d",
			health.RuntimeProtocol, mountproto.RuntimeProtocolVersion,
		)
	}
	if health.RuntimePID <= 0 {
		return fmt.Errorf("holder runtime pid is invalid: pid=%d", health.RuntimePID)
	}
	if health.ProcessGeneration == "" {
		return errors.New("holder runtime process generation is empty")
	}
	if health.ActivationGeneration == "" {
		return errors.New("holder runtime activation generation is empty")
	}
	if health.State != mountproto.RuntimeStateHealthy || health.Draining || health.Busy || !health.Ready {
		return fmt.Errorf(
			"holder runtime lifecycle is not ready: state=%q draining=%t busy=%t",
			health.State, health.Draining, health.Busy,
		)
	}
	if health.ReadinessPhase != mountproto.ReadinessPhaseReady ||
		health.ReadinessStep != mountproto.ReadinessStepPublished {
		return fmt.Errorf(
			"holder runtime readiness is not published: phase=%q step=%q",
			health.ReadinessPhase, health.ReadinessStep,
		)
	}
	if health.NativePhase != mountproto.NativePhaseDisabled || health.NativeMount != nil {
		return fmt.Errorf(
			"holder native presentation is not disabled: phase=%q proof=%t",
			health.NativePhase, health.NativeMount != nil,
		)
	}
	if health.BrokerPhase != mountproto.BrokerPhaseLive {
		return fmt.Errorf("holder broker is not ready: phase=%q", health.BrokerPhase)
	}
	return nil
}

type holderServiceController interface {
	Converge(context.Context, []service.Agent) error
	StopRuntime(context.Context, service.StopControlSpec) (wire.StopResult, error)
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
		BuildID:             version.String(),
		Readiness:           holderbridge.ReadinessContract(),
		SourceCapable:       true,
		BrokerPolicyDigest:  holderEntitlementPolicyDigest,
		RuntimePolicyDigest: holderEntitlementPolicyDigest,
	})
}

// StopAndUninstallHolderService removes the holder from the complete desired service set.
func StopAndUninstallHolderService(ctx context.Context) error {
	plan, err := HolderDeploymentPlan()
	if err != nil {
		return fmt.Errorf("derive FuseKit holder plan: %w", err)
	}
	if err := withHolderServiceController(ctx, func(controller holderServiceController) error {
		if err := stopObservedHolderRuntime(ctx, controller, plan, wire.StopIntentUninstall); err != nil {
			return err
		}
		return controller.Converge(ctx, nil)
	}); err != nil {
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
		if err := stopObservedHolderRuntime(ctx, controller, plan, wire.StopIntentUpgrade); err != nil {
			return err
		}
		if err := controller.Converge(ctx, []service.Agent{agent}); err != nil {
			return fmt.Errorf("converge FuseKit holder service: %w", err)
		}
		readyCtx, cancel := context.WithTimeout(ctx, plan.Readiness().ObservationTimeout())
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

func validateHolderStopTarget(health mountproto.RuntimeHealthResponse) error {
	if health.Protocol != mountproto.Version || health.Code != mountproto.ErrorCodeOk || health.Message != "" {
		return fmt.Errorf(
			"holder stop target response is not exact: protocol=%d code=%q message=%q",
			health.Protocol, health.Code, health.Message,
		)
	}
	if health.RuntimeBuild == "" || health.RuntimeProtocol != mountproto.RuntimeProtocolVersion ||
		health.RuntimePID <= 1 || health.ProcessGeneration == "" {
		return fmt.Errorf(
			"holder stop target identity is incomplete: build=%q protocol=%d pid=%d generation=%q",
			health.RuntimeBuild, health.RuntimeProtocol, health.RuntimePID, health.ProcessGeneration,
		)
	}
	return nil
}

func stopObservedHolderRuntime(
	ctx context.Context,
	controller holderServiceController,
	plan holder.DeploymentPlan,
	intent wire.StopIntent,
) error {
	stopCtx, cancel := context.WithTimeout(ctx, plan.Readiness().ObservationTimeout())
	defer cancel()
	health, err := holderRuntimeHealth(stopCtx, plan.Paths().Socket)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("observe FuseKit holder stop target: %w", err)
	}
	if err := validateHolderStopTarget(health); err != nil {
		return err
	}
	if intent == wire.StopIntentUpgrade && health.RuntimeBuild == version.String() &&
		validateHolderRuntimeHealth(health) == nil {
		return nil
	}
	if intent == wire.StopIntentUpgrade && health.RuntimeBuild == version.String() {
		intent = wire.StopIntentRestart
	}
	_, err = controller.StopRuntime(stopCtx, service.StopControlSpec{
		Executable: plan.RuntimeExecutable(),
		Args:       holder.StopControlChildArguments(), Role: holderbridge.StopRoleID,
		RuntimeBuild: version.String(), RuntimeProtocol: int(mountproto.RuntimeProtocolVersion),
		TargetProcessGeneration: health.ProcessGeneration, Intent: intent,
	})
	if err != nil {
		return fmt.Errorf("stop exact FuseKit holder generation: %w", err)
	}
	return nil
}

func withHolderServiceController(
	ctx context.Context,
	run func(holderServiceController) error,
) (err error) {
	controller, err := holderControllerOpen(ctx, service.ControllerConfig{
		StatePath:   pool.FuseKitServiceStatePath(),
		ProcessPath: pool.FuseKitServiceProcessStorePath(),
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
