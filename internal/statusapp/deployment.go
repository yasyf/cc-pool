package statusapp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

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
	Reset(context.Context) error
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
	// readinessBudget is this package's default deadline for the whole
	// bracket-retry observation when the caller stated none: two complete
	// observation windows under the fleet contract, the cadence that paces
	// them, and one cadence of headroom — so one legitimate slow start and
	// one mid-bracket restart both converge inside it, while a copy that can
	// never pin is refused instead of retried forever. A caller that states a
	// longer deadline buys more observations, never a longer one.
	readinessBudget = func() time.Duration {
		return 2*holderbridge.ReadinessContract().ObservationTimeout() + 2*bracketRetryCadence
	}
	tenantLaneReady = func(ctx context.Context) (daemonkit.Health, error) {
		client, err := daemonkit.Open(observerTenantDaemon())
		if err != nil {
			return daemonkit.Health{}, fmt.Errorf("CCPoolStatus: open the cc-pool tenant lane: %w", err)
		}
		readyCtx, cancel := context.WithTimeout(ctx, holderbridge.ReadinessContract().ObservationTimeout())
		defer cancel()
		return client.WaitReady(readyCtx)
	}
)

// observerTenantDaemon is the tenant lane as CCPoolStatus attaches to it:
// the shared identity, with the serving process additionally required to
// prove the installed application's own signing identity on the very
// connection the health observation rides. The same-user floor admits any
// same-UID listener and a process-table snapshot cannot bind an answer to
// the answerer; the authenticated session does. Trust.Serving is read only
// by the attaching client, so the shared serving side is untouched.
func observerTenantDaemon() daemonkit.Daemon {
	d := tenantfs.Daemon()
	d.Trust.Serving = daemonkit.ServingSigned(statusAppRequirement())
	return d
}

// ServiceInstallReceipt binds one landed generation to the activation that
// proved its runtime live.
type ServiceInstallReceipt struct {
	Generation deploy.Generation
	Activation deploy.Activation
}

// Rollback is idempotent because deploy settles every outstanding swap before
// ApplyPackagedApp returns.
func (ServiceInstallReceipt) Rollback(context.Context) error { return nil }

// ApplyPackagedApp installs or upgrades one exact packaged signed application
// candidate. Activation observes the primary label, which the app publishes
// before it publishes the source fleet and starts the tenant lane beside it, so
// an activation that succeeds says nothing about a startup that then unwinds.
// The receipt is therefore cut only once the tenant lane — downstream of both —
// answers from the installed runtime.
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
	if err := RequireActiveService(ctx); err != nil {
		return ServiceInstallReceipt{}, err
	}
	return ServiceInstallReceipt{Generation: generation, Activation: activation}, nil
}

// bracketRetryCadence paces bracket re-observation while a launchd restart
// transient converges, so an unconverged check waits rather than spinning.
const bracketRetryCadence = 250 * time.Millisecond

// RequireActiveService proves the installed application's FuseKit runtime is
// serving. It only observes: landing a generation is ApplyPackagedApp's to
// perform and launchd's to sustain, so requiring the service never activates
// one. The connection itself requires the installed application's signing
// identity, and the health observation is bracketed between two inventories
// that must both pin the answering PID to one process instance
// ({Start, Boot}): only a stable signed, inventory-pinned instance succeeds.
// Everything short of that retries the complete bracket within ctx — a
// holder appearing mid-observation, disappearing and being replaced,
// re-pinning under a reused PID, and failing to publish readiness inside one
// observation window are one family of legitimate launchd restart transients,
// refused only when ctx runs out, never admitted early. The contract's
// observation timeout caps one window, never the whole wait: a caller stating
// a longer deadline spends it on further complete brackets. A caller that
// states no deadline gets the package's readiness budget, so a persistent
// mismatch refuses instead of retrying forever.
func RequireActiveService(ctx context.Context) error {
	ctx, cancel := budgeted(ctx, readinessBudget())
	defer cancel()
	executable := bundle.ExePath(installedAppPath(), holderbridge.ExecutableName)
	before, err := deployInventory(executable)
	if err != nil {
		return fmt.Errorf("CCPoolStatus: inventory the installed runtime: %w", err)
	}
	for {
		health, err := tenantLaneReady(ctx)
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) || !awaitBracketRetry(ctx) {
				return fmt.Errorf("CCPoolStatus: require the live FuseKit runtime: %w", err)
			}
			continue
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
		before = after
		if !awaitBracketRetry(ctx) {
			return fmt.Errorf(
				"CCPoolStatus: the process serving the tenant lane (pid %d) does not run the installed application",
				health.PID,
			)
		}
	}
}

// awaitBracketRetry paces the next complete bracket and reports whether ctx
// still affords one. A ctx already done takes the refusal immediately: the
// cadence is never ready before Done on the very first select.
func awaitBracketRetry(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(bracketRetryCadence):
		return true
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

// budgeted states budget as ctx's deadline when ctx carries none. A caller
// that stated its own keeps it: the budget is this package's default, never
// an override of a deadline the caller chose.
func budgeted(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	if _, stated := ctx.Deadline(); stated {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, budget)
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

// ResetPackagedApp retires the installed application's agents and leaves its
// installed bytes in place, recovering a deployment no other verb accepts.
func ResetPackagedApp(ctx context.Context) error {
	controller, err := openInstalledDeployment(installedAppPath())
	if err != nil {
		return err
	}
	resetCtx, cancel := context.WithTimeout(ctx, holderbridge.RuntimeShutdownTimeout)
	defer cancel()
	if err := controller.Reset(resetCtx); err != nil {
		return fmt.Errorf("CCPoolStatus: reset packaged application: %w", err)
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
