package statusapp

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/daemonkit/deployment"
	"github.com/yasyf/daemonkit/service"
)

type deploymentController interface {
	Deploy(context.Context, deployment.Config) (deploymentReceipt, error)
	Deactivate(context.Context, deployment.DeactivateConfig) (deactivationResult, error)
	Status(context.Context, deployment.Config) (deploymentStatus, error)
}

type deploymentReceipt struct {
	operationID    string
	state          deployment.DeploymentState
	current        deployment.CanonicalGeneration
	hasCurrent     bool
	plan           service.Plan
	activationPlan service.Plan
}

type deactivationResult struct {
	state      deployment.DeactivationState
	receipt    deploymentReceipt
	hasReceipt bool
}

type deploymentStatus struct {
	receipt          deploymentReceipt
	hasReceipt       bool
	configMatches    bool
	recoveryRequired bool
}

// ServiceDeploymentState is the exact durable holder deployment state.
type ServiceDeploymentState string

// Service deployment states distinguish managed absence, retained inactivity, and active runtime.
const (
	ServiceDeploymentAbsent   ServiceDeploymentState = "absent"
	ServiceDeploymentInactive ServiceDeploymentState = "inactive"
	ServiceDeploymentActive   ServiceDeploymentState = "active"
)

// ServiceDeployment identifies one exact durable holder generation.
type ServiceDeployment struct {
	State       ServiceDeploymentState
	OperationID string
	Holder      deployment.CanonicalGeneration
}

// ServiceInstallReceipt binds one install attempt to its exact prior and active holder states.
type ServiceInstallReceipt struct {
	Prior          ServiceDeployment
	Current        ServiceDeployment
	Changed        bool
	NewlyActivated bool
}

// Rollback deactivates only the exact newly activated holder generation.
func (r ServiceInstallReceipt) Rollback(ctx context.Context) error {
	return rollbackService(ctx, newDeployer(), r)
}

type daemonkitDeploymentController struct {
	inner *deployment.Controller
}

func (c daemonkitDeploymentController) Deploy(
	ctx context.Context,
	config deployment.Config,
) (deploymentReceipt, error) {
	receipt, err := c.inner.Deploy(ctx, config)
	if err != nil {
		return deploymentReceipt{}, err
	}
	return observeDeploymentReceipt(receipt), nil
}

func (c daemonkitDeploymentController) Deactivate(
	ctx context.Context,
	config deployment.DeactivateConfig,
) (deactivationResult, error) {
	result, err := c.inner.Deactivate(ctx, config)
	if err != nil {
		return deactivationResult{}, err
	}
	receipt, ok := result.Receipt()
	return deactivationResult{
		state: result.State(), receipt: observeDeploymentReceipt(receipt), hasReceipt: ok,
	}, nil
}

func (c daemonkitDeploymentController) Status(
	ctx context.Context,
	config deployment.Config,
) (deploymentStatus, error) {
	status, err := c.inner.Status(ctx, config)
	if err != nil {
		return deploymentStatus{}, err
	}
	receipt, ok := status.Receipt()
	return deploymentStatus{
		receipt: observeDeploymentReceipt(receipt), hasReceipt: ok, configMatches: status.ConfigMatches(),
		recoveryRequired: status.RecoveryRequired(),
	}, nil
}

func observeDeploymentReceipt(receipt deployment.DeploymentReceipt) deploymentReceipt {
	current, hasCurrent := receipt.Current()
	return deploymentReceipt{
		operationID: receipt.OperationID(), state: receipt.State(), current: current, hasCurrent: hasCurrent,
		plan: receipt.Plan(), activationPlan: receipt.ActivationPlan(),
	}
}

var (
	newDeployer      = func() deploymentController { return daemonkitDeploymentController{inner: deployment.New()} }
	makeProductHooks = newProductHooks
)

func statusAppCodeIdentity() codeidentity.CodeIdentity {
	return codeidentity.CodeIdentity{
		TeamID: holderbridge.TeamID, SigningIdentifier: holderbridge.BundleID,
	}
}

// InstallService deploys the exact signed status application and returns its rollback receipt.
func InstallService(ctx context.Context) (ServiceInstallReceipt, error) {
	return installService(ctx, newDeployer())
}

// DeactivateService durably retires the status app runtime and service plan.
func DeactivateService(ctx context.Context) error {
	_, err := deactivateService(ctx, newDeployer())
	return err
}

type serviceDeploymentSpec struct {
	config deployment.Config
	hooks  productHooks
}

func makeServiceDeploymentSpec() (serviceDeploymentSpec, error) {
	bundle, err := release()
	if err != nil {
		return serviceDeploymentSpec{}, err
	}
	consumerBuild, policyDigest, err := holderbridge.DeploymentIdentity()
	if err != nil {
		return serviceDeploymentSpec{}, err
	}
	hooks := makeProductHooks(version.String(), policyDigest)
	return serviceDeploymentSpec{
		config: deployment.Config{
			Dir: pool.WidgetAppDir(), AppName: appName, Release: bundle,
			Identity:      statusAppCodeIdentity(),
			ConsumerBuild: consumerBuild, PolicyDigest: policyDigest,
			RuntimeQuiesce: hooks.runtimeQuiesce, PostInstallProof: hooks.postInstallProof,
			PriorAppRestoreProof: hooks.priorAppRestoreProof, BuildPlan: hooks.buildPlan,
			Readiness: hooks.readiness,
		},
		hooks: hooks,
	}, nil
}

func installService(ctx context.Context, controller deploymentController) (ServiceInstallReceipt, error) {
	spec, err := makeServiceDeploymentSpec()
	if err != nil {
		return ServiceInstallReceipt{}, err
	}
	prior, _, _, err := observeServiceDeployment(ctx, controller, spec)
	if err != nil {
		return ServiceInstallReceipt{}, fmt.Errorf("CCPoolStatus: observe prior signed app deployment: %w", err)
	}
	if err := ensureInstallDirectory(spec.config.Dir); err != nil {
		return ServiceInstallReceipt{}, err
	}
	receipt, deployErr := controller.Deploy(ctx, spec.config)
	if deployErr != nil {
		_, observed, matches, observeErr := observeServiceDeployment(ctx, controller, spec)
		if observeErr != nil || !matches {
			return ServiceInstallReceipt{}, fmt.Errorf(
				"CCPoolStatus: deploy signed app %s: %w",
				version.Version, errors.Join(deployErr, observeErr),
			)
		}
		receipt = observed
	}
	if err := validateActiveDeployment(ctx, spec, receipt); err != nil {
		return ServiceInstallReceipt{}, err
	}
	current := publicServiceDeployment(receipt)
	return ServiceInstallReceipt{
		Prior: prior, Current: current, Changed: !reflect.DeepEqual(prior, current),
		NewlyActivated: prior.State != ServiceDeploymentActive,
	}, nil
}

func observeServiceDeployment(
	ctx context.Context,
	controller deploymentController,
	spec serviceDeploymentSpec,
) (ServiceDeployment, deploymentReceipt, bool, error) {
	status, err := controller.Status(ctx, spec.config)
	if err != nil {
		return ServiceDeployment{}, deploymentReceipt{}, false, err
	}
	if status.recoveryRequired {
		return ServiceDeployment{}, deploymentReceipt{}, false, errors.New("CCPoolStatus: deployment requires explicit recovery")
	}
	if !status.hasReceipt {
		return ServiceDeployment{State: ServiceDeploymentAbsent}, deploymentReceipt{}, status.configMatches, nil
	}
	if err := validateRetainedDeployment(spec.hooks, status.receipt); err != nil {
		return ServiceDeployment{}, deploymentReceipt{}, false, err
	}
	return publicServiceDeployment(status.receipt), status.receipt, status.configMatches, nil
}

func publicServiceDeployment(receipt deploymentReceipt) ServiceDeployment {
	state := ServiceDeploymentInactive
	if receipt.state == deployment.DeploymentActive {
		state = ServiceDeploymentActive
	}
	return ServiceDeployment{State: state, OperationID: receipt.operationID, Holder: receipt.current}
}

func validateActiveDeployment(ctx context.Context, spec serviceDeploymentSpec, receipt deploymentReceipt) error {
	if receipt.state != deployment.DeploymentActive || !receipt.hasCurrent {
		return errors.New("CCPoolStatus: deployment did not return one complete current generation")
	}
	if err := validateRetainedDeployment(spec.hooks, receipt); err != nil {
		return err
	}
	if receipt.current.Release != spec.config.Release {
		return errors.New("CCPoolStatus: deployment returned the wrong current release")
	}
	wantPlan, err := spec.hooks.buildPlan(ctx, deployment.Operation{
		ID: receipt.operationID, Generation: receipt.current,
	})
	if err != nil {
		return fmt.Errorf("CCPoolStatus: derive deployed service plan: %w", err)
	}
	if !samePlan(receipt.plan, wantPlan) || !samePlan(receipt.activationPlan, wantPlan) {
		return errors.New("CCPoolStatus: deployment returned the wrong active service plan")
	}
	return nil
}

func rollbackService(ctx context.Context, controller deploymentController, receipt ServiceInstallReceipt) error {
	if !receipt.Changed || !receipt.NewlyActivated || receipt.Prior.State == ServiceDeploymentActive {
		return nil
	}
	if receipt.Current.State != ServiceDeploymentActive || receipt.Current.OperationID == "" {
		return errors.New("CCPoolStatus: rollback receipt has no exact active holder")
	}
	spec, err := makeServiceDeploymentSpec()
	if err != nil {
		return err
	}
	observed, _, _, err := observeServiceDeployment(ctx, controller, spec)
	if err != nil {
		return fmt.Errorf("CCPoolStatus: observe holder rollback target: %w", err)
	}
	if observed.State != ServiceDeploymentActive {
		return nil
	}
	if !reflect.DeepEqual(observed, receipt.Current) {
		return errors.New("CCPoolStatus: holder changed after install receipt; refusing rollback")
	}
	result, err := deactivateService(ctx, controller)
	if err != nil {
		return err
	}
	if result.state == deployment.DeactivationInactive && result.receipt.current != receipt.Current.Holder {
		return errors.New("CCPoolStatus: rollback deactivated the wrong holder generation")
	}
	return nil
}

func deactivateService(ctx context.Context, controller deploymentController) (deactivationResult, error) {
	consumerBuild, policyDigest, err := holderbridge.DeploymentIdentity()
	if err != nil {
		return deactivationResult{}, err
	}
	hooks := makeProductHooks(version.String(), policyDigest)
	result, err := controller.Deactivate(ctx, deployment.DeactivateConfig{
		Dir: pool.WidgetAppDir(), AppName: appName,
		Identity:      statusAppCodeIdentity(),
		ConsumerBuild: consumerBuild, PolicyDigest: policyDigest,
		RuntimeQuiesce: hooks.runtimeQuiesce,
		Readiness:      hooks.readiness,
	})
	if err != nil {
		return deactivationResult{}, fmt.Errorf("CCPoolStatus: deactivate signed app runtime: %w", err)
	}
	switch result.state {
	case deployment.DeactivationAbsent:
		if result.hasReceipt {
			return deactivationResult{}, errors.New("CCPoolStatus: absent deactivation returned a receipt")
		}
	case deployment.DeactivationInactive:
		if !result.hasReceipt {
			return deactivationResult{}, errors.New("CCPoolStatus: inactive deactivation returned no receipt")
		}
		if err := validateInactiveResult(hooks, result.receipt); err != nil {
			return deactivationResult{}, err
		}
	default:
		return deactivationResult{}, errors.New("CCPoolStatus: deactivation returned an unknown state")
	}
	return result, nil
}

func validateInactiveResult(hooks productHooks, receipt deploymentReceipt) error {
	if receipt.state != deployment.DeploymentInactive {
		return errors.New("CCPoolStatus: deactivation did not return one exact inactive generation")
	}
	return validateRetainedDeployment(hooks, receipt)
}

func validateRetainedDeployment(hooks productHooks, receipt deploymentReceipt) error {
	emptyPlan, err := service.NewPlan(nil)
	if err != nil {
		return fmt.Errorf("CCPoolStatus: derive empty service plan: %w", err)
	}
	if !receipt.hasCurrent || receipt.operationID == "" {
		return errors.New("CCPoolStatus: deployment did not retain one exact generation")
	}
	switch receipt.state {
	case deployment.DeploymentActive:
		if !samePlan(receipt.plan, receipt.activationPlan) {
			return errors.New("CCPoolStatus: active deployment plan does not match its retained activation plan")
		}
	case deployment.DeploymentInactive:
		if !samePlan(receipt.plan, emptyPlan) {
			return errors.New("CCPoolStatus: inactive deployment retained a non-empty service plan")
		}
	default:
		return errors.New("CCPoolStatus: deployment receipt has no exact retained state")
	}
	wantRequirement, err := statusAppCodeIdentity().DRString()
	if err != nil {
		return fmt.Errorf("CCPoolStatus: derive designated requirement: %w", err)
	}
	if receipt.current.Path != pool.WidgetAppPath() ||
		receipt.current.DesignatedRequirement != wantRequirement ||
		receipt.current.Release.Version == "" || receipt.current.Release.URL == "" ||
		receipt.current.Release.SHA256 == (deployment.SHA256{}) || receipt.current.CDHash == "" ||
		receipt.current.BundleDigest == (deployment.SHA256{}) ||
		receipt.current.Device == "" || receipt.current.Inode == "" {
		return errors.New("CCPoolStatus: deployment did not retain the exact signed app generation")
	}
	operation := deployment.Operation{ID: receipt.operationID, Generation: receipt.current}
	buildID, err := exactPlanBuildID(operation, receipt.activationPlan)
	if err != nil {
		return err
	}
	wantPlan, err := hooks.servicePlanBuild(operation, buildID)
	if err != nil {
		return fmt.Errorf("CCPoolStatus: derive retained activation plan: %w", err)
	}
	if !samePlan(receipt.activationPlan, wantPlan) {
		return errors.New("CCPoolStatus: deployment returned the wrong retained activation plan")
	}
	return nil
}

func samePlan(left, right service.Plan) bool {
	return left.Digest() == right.Digest() && reflect.DeepEqual(left.Agents(), right.Agents())
}
