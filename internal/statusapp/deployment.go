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

// InstallService deploys the exact signed status application and reconciles its service plan.
func InstallService(ctx context.Context) error {
	_, err := installService(ctx, newDeployer())
	return err
}

// DeactivateService durably retires the status app runtime and service plan.
func DeactivateService(ctx context.Context) error {
	_, err := deactivateService(ctx, newDeployer())
	return err
}

func installService(ctx context.Context, controller deploymentController) (deploymentReceipt, error) {
	directory := pool.WidgetAppDir()
	bundle, err := release()
	if err != nil {
		return deploymentReceipt{}, err
	}
	consumerBuild, policyDigest, err := holderbridge.DeploymentIdentity()
	if err != nil {
		return deploymentReceipt{}, err
	}
	if err := ensureInstallDirectory(directory); err != nil {
		return deploymentReceipt{}, err
	}
	hooks := makeProductHooks(version.String(), policyDigest)
	receipt, err := controller.Deploy(ctx, deployment.Config{
		Dir: directory, AppName: appName, Release: bundle,
		Identity:      statusAppCodeIdentity(),
		ConsumerBuild: consumerBuild, PolicyDigest: policyDigest,
		RuntimeQuiesce: hooks.runtimeQuiesce, PostInstallProof: hooks.postInstallProof,
		PriorAppRestoreProof: hooks.priorAppRestoreProof, BuildPlan: hooks.buildPlan,
		Readiness: hooks.readiness,
	})
	if err != nil {
		return deploymentReceipt{}, fmt.Errorf("CCPoolStatus: deploy signed app %s: %w", version.Version, err)
	}
	if receipt.state != deployment.DeploymentActive || !receipt.hasCurrent {
		return deploymentReceipt{}, errors.New("CCPoolStatus: deployment did not return one complete current generation")
	}
	wantRequirement, err := statusAppCodeIdentity().DRString()
	if err != nil {
		return deploymentReceipt{}, fmt.Errorf("CCPoolStatus: derive designated requirement: %w", err)
	}
	if receipt.current.Path != pool.WidgetAppPath() || receipt.current.Release != bundle ||
		receipt.current.DesignatedRequirement != wantRequirement ||
		receipt.current.CDHash == "" || receipt.current.BundleDigest == (deployment.SHA256{}) ||
		receipt.current.Device == "" || receipt.current.Inode == "" {
		return deploymentReceipt{}, errors.New("CCPoolStatus: deployment returned the wrong current generation")
	}
	if receipt.operationID == "" {
		return deploymentReceipt{}, errors.New("CCPoolStatus: deployment returned no operation identity")
	}
	wantPlan, err := hooks.buildPlan(ctx, deployment.Operation{
		ID: receipt.operationID, Generation: receipt.current,
	})
	if err != nil {
		return deploymentReceipt{}, fmt.Errorf("CCPoolStatus: derive deployed service plan: %w", err)
	}
	if !samePlan(receipt.plan, wantPlan) || !samePlan(receipt.activationPlan, wantPlan) {
		return deploymentReceipt{}, errors.New("CCPoolStatus: deployment returned the wrong active service plan")
	}
	return receipt, nil
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
	emptyPlan, err := service.NewPlan(nil)
	if err != nil {
		return fmt.Errorf("CCPoolStatus: derive empty service plan: %w", err)
	}
	if receipt.state != deployment.DeploymentInactive || !receipt.hasCurrent ||
		receipt.operationID == "" || !samePlan(receipt.plan, emptyPlan) {
		return errors.New("CCPoolStatus: deactivation did not return one exact inactive generation")
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
		return errors.New("CCPoolStatus: deactivation did not retain the exact signed app generation")
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
		return errors.New("CCPoolStatus: deactivation returned the wrong retained activation plan")
	}
	return nil
}

func samePlan(left, right service.Plan) bool {
	return left.Digest() == right.Digest() && reflect.DeepEqual(left.Agents(), right.Agents())
}
