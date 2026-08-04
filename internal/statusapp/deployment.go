package statusapp

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/bundle"
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
	deployInventory     = deploy.Inventory
	tenantLaneReady     = func(ctx context.Context) (daemonkit.Health, error) {
		client, err := daemonkit.Open(tenantfs.Daemon())
		if err != nil {
			return daemonkit.Health{}, fmt.Errorf("CCPoolStatus: open the cc-pool tenant lane: %w", err)
		}
		readyCtx, cancel := context.WithTimeout(ctx, holderbridge.ReadinessContract().ObservationTimeout())
		defer cancel()
		return client.WaitReady(readyCtx)
	}
)

// ServiceInstallReceipt binds one landed generation to the activation that
// proved its runtime live.
type ServiceInstallReceipt struct {
	Generation deploy.Generation
	Activation deploy.Activation
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
	return ServiceInstallReceipt{Generation: generation, Activation: activation}, nil
}

// RequireActiveService proves the installed application's FuseKit runtime is
// serving. It only observes: landing a generation is ApplyPackagedApp's to
// perform and launchd's to sustain, so requiring the service never activates
// one. WaitReady alone proves live-and-ready under the tenant lane's
// same-user floor, which any same-UID listener clears — the executable-scoped
// inventory is what ties the answering PID to the installed application's own
// runtime binary. The health observation is bracketed between two inventories
// that must both pin the answering PID to one process instance
// ({Start, Boot}), so a reused PID cannot correlate; an exec within one
// instance keeps its triple and is not excluded. A holder that first appears
// during the observation — a restart launchd was still completing — re-runs
// the complete bracket within ctx instead of being refused.
func RequireActiveService(ctx context.Context) error {
	executable := bundle.ExePath(installedAppPath(), holderbridge.ExecutableName)
	before, err := deployInventory(executable)
	if err != nil {
		return fmt.Errorf("CCPoolStatus: inventory the installed runtime: %w", err)
	}
	for {
		health, err := tenantLaneReady(ctx)
		if err != nil {
			return fmt.Errorf("CCPoolStatus: require the live FuseKit runtime: %w", err)
		}
		after, err := deployInventory(executable)
		if err != nil {
			return fmt.Errorf("CCPoolStatus: inventory the installed runtime: %w", err)
		}
		first, held := inventoryPin(before, health.PID)
		second, serving := inventoryPin(after, health.PID)
		if held && serving && first.Start == second.Start && first.Boot == second.Boot {
			return nil
		}
		if held || !serving || ctx.Err() != nil {
			return fmt.Errorf(
				"CCPoolStatus: the process serving the tenant lane (pid %d) does not run the installed application",
				health.PID,
			)
		}
		before = after
	}
}

func inventoryPin(survivors deploy.Survivors, pid int) (deploy.LiveProcess, bool) {
	for _, live := range survivors.Live {
		if live.PID == pid {
			return live, true
		}
	}
	return deploy.LiveProcess{}, false
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
