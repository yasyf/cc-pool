package statusapp

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/deploy"
	"github.com/yasyf/daemonkit/launchd"
	"github.com/yasyf/fusekit/holder"
)

type deploymentController interface {
	Install(context.Context, deploy.Candidate) (deploy.Generation, error)
	Supersede(context.Context, deploy.Candidate) (deploy.Generation, error)
	Activate(context.Context) (deploy.Activation, error)
	Uninstall(context.Context) (deploy.Removal, error)
}

type candidatePlan struct {
	candidate deploy.Candidate
	agents    []launchd.Agent
}

var (
	newDeployer         = func(config deploy.Config) (deploymentController, error) { return deploy.Open(config) }
	makeCandidatePlan   = packagedCandidatePlan
	makeInstalledAgents = installedServiceAgents
	installedAppPath    = pool.WidgetAppPath
)

// ServiceInstallReceipt binds one landed generation to the updater build that
// applied it and to the activation that proved its runtime live.
type ServiceInstallReceipt struct {
	ConsumerBuild string
	Generation    deploy.Generation
	Activation    deploy.Activation
}

// Rollback is idempotent because deploy settles every outstanding swap before
// ApplyPackagedApp returns.
func (ServiceInstallReceipt) Rollback(context.Context) error { return nil }

// ApplyPackagedApp installs or upgrades one exact packaged signed application candidate.
func ApplyPackagedApp(ctx context.Context, candidateSourcePath string) (ServiceInstallReceipt, error) {
	appVersion, err := statusAppVersion()
	if err != nil {
		return ServiceInstallReceipt{}, err
	}
	consumerBuild, err := holderbridge.DeploymentIdentity()
	if err != nil {
		return ServiceInstallReceipt{}, err
	}
	appPath := installedAppPath()
	plan, err := makeCandidatePlan(appPath, version.String(), candidateSourcePath)
	if err != nil {
		return ServiceInstallReceipt{}, err
	}
	if plan.candidate.Version != appVersion {
		return ServiceInstallReceipt{}, fmt.Errorf(
			"CCPoolStatus: packaged candidate declares version %q, not the exact release %q",
			plan.candidate.Version, appVersion,
		)
	}
	controller, err := openDeployment(appPath, plan.agents)
	if err != nil {
		return ServiceInstallReceipt{}, err
	}
	generation, err := landCandidate(ctx, controller, appPath, plan.candidate)
	if err != nil {
		return ServiceInstallReceipt{}, fmt.Errorf("CCPoolStatus: land packaged candidate: %w", err)
	}
	activation, err := activate(ctx, controller)
	if err != nil {
		return ServiceInstallReceipt{}, err
	}
	if !sameApplicationBytes(activation.Generation, generation) {
		return ServiceInstallReceipt{}, errors.New("CCPoolStatus: activation does not prove the landed generation")
	}
	return ServiceInstallReceipt{
		ConsumerBuild: consumerBuild, Generation: generation, Activation: activation,
	}, nil
}

// RequireActiveService converges the exact installed application and proves its live FuseKit runtime ready.
func RequireActiveService(ctx context.Context) error {
	controller, err := openInstalledDeployment(installedAppPath())
	if err != nil {
		return err
	}
	_, err = activate(ctx, controller)
	return err
}

// UninstallPackagedApp quiesces and removes the exact deploy-sealed installed application.
func UninstallPackagedApp(ctx context.Context) error {
	controller, err := openInstalledDeployment(installedAppPath())
	if err != nil {
		return err
	}
	removeCtx, cancel := context.WithTimeout(ctx, holderbridge.RuntimeShutdownTimeout)
	defer cancel()
	if _, err := controller.Uninstall(removeCtx); err != nil {
		return fmt.Errorf("CCPoolStatus: uninstall packaged application: %w", err)
	}
	return nil
}

func openInstalledDeployment(appPath string) (deploymentController, error) {
	agents, err := makeInstalledAgents(appPath, version.String())
	if err != nil {
		return nil, err
	}
	return openDeployment(appPath, agents)
}

func openDeployment(appPath string, agents []launchd.Agent) (deploymentController, error) {
	requirement := statusAppRequirement()
	controller, err := newDeployer(deploy.Config{
		App:         appPath,
		Requirement: requirement,
		Daemon: daemonkit.Daemon{
			Label: daemonkit.Label(holderbridge.DeploymentServiceLabel),
			Trust: daemonkit.Trust{Serving: daemonkit.ServingSigned(requirement)},
		},
		Agents: agents,
	})
	if err != nil {
		return nil, fmt.Errorf("CCPoolStatus: open sealed deployment: %w", err)
	}
	return controller, nil
}

func statusAppRequirement() daemonkit.Requirement {
	return daemonkit.Requirement{TeamID: holderbridge.TeamID, SigningIdentifier: holderbridge.BundleID}
}

func packagedCandidatePlan(appPath, buildID, sourceAppPath string) (candidatePlan, error) {
	plan, err := holder.NewCandidatePlan(
		holderbridge.DeploymentPlanSpec(appPath, pool.FuseKitRuntimeDir(), buildID), sourceAppPath,
	)
	if err != nil {
		return candidatePlan{}, fmt.Errorf("CCPoolStatus: bind packaged candidate plan: %w", err)
	}
	return candidatePlan{candidate: plan.Candidate(), agents: plan.Agents()}, nil
}

func installedServiceAgents(appPath, buildID string) ([]launchd.Agent, error) {
	plan, err := holderbridge.NewDeploymentPlan(appPath, pool.FuseKitRuntimeDir(), buildID)
	if err != nil {
		return nil, fmt.Errorf("CCPoolStatus: bind installed service plan: %w", err)
	}
	return []launchd.Agent{plan.Agent()}, nil
}

func landCandidate(
	ctx context.Context,
	controller deploymentController,
	appPath string,
	candidate deploy.Candidate,
) (deploy.Generation, error) {
	landCtx, cancel := context.WithTimeout(ctx, holderbridge.RuntimeShutdownTimeout)
	defer cancel()
	_, err := os.Lstat(appPath)
	if errors.Is(err, os.ErrNotExist) {
		return controller.Install(landCtx, candidate)
	}
	if err != nil {
		return deploy.Generation{}, fmt.Errorf("CCPoolStatus: inspect installed application: %w", err)
	}
	return controller.Supersede(landCtx, candidate)
}

func activate(ctx context.Context, controller deploymentController) (deploy.Activation, error) {
	activateCtx, cancel := context.WithTimeout(ctx, holderbridge.ReadinessContract().ObservationTimeout())
	defer cancel()
	activation, err := controller.Activate(activateCtx)
	if err != nil {
		return deploy.Activation{}, fmt.Errorf("CCPoolStatus: activate installed application: %w", err)
	}
	return activation, nil
}

func sameApplicationBytes(left, right deploy.Generation) bool {
	left.Path, right.Path = "", ""
	left.FileID, right.FileID = deploy.FileID{}, deploy.FileID{}
	return left == right
}
