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
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
)

type installedGeneration struct {
	raw                   deployment.InstalledAttestation
	path                  string
	version               string
	teamID                string
	signingIdentifier     string
	designatedRequirement string
	cdHash                string
	bundleDigest          deployment.SHA256
	entitlementsDigest    deployment.SHA256
	device                string
	inode                 string
}

type installedOperation struct {
	id         string
	generation installedGeneration
	plan       service.Plan
}

type runtimeReadiness struct {
	runtimeBuild      string
	processGeneration proc.OwnerGeneration
	digest            deployment.SHA256
}

type activationReceipt struct {
	raw        deployment.ActivationReceipt
	id         string
	active     bool
	generation installedGeneration
	plan       service.Plan
	readiness  runtimeReadiness
}

type deactivationOperation struct {
	id         string
	activation activationReceipt
}

type runtimeProof struct {
	absent            bool
	processGeneration proc.OwnerGeneration
	digest            deployment.SHA256
}

type activationRequest struct {
	expected      installedGeneration
	consumerBuild string
	policyDigest  deployment.SHA256
	plan          service.Plan
	readiness     func(context.Context, installedOperation) (runtimeReadiness, error)
}

type deactivationRequest struct {
	expected       activationReceipt
	consumerBuild  string
	policyDigest   deployment.SHA256
	runtimeQuiesce func(context.Context, deployment.RuntimeStopper, deactivationOperation) (runtimeProof, error)
}

type installedStatus struct {
	state      deployment.InstalledState
	generation installedGeneration
	activation activationReceipt
	hasReceipt bool
}

type deploymentController interface {
	Attest(context.Context, deployment.InstalledSpec) (installedGeneration, error)
	Activate(context.Context, activationRequest) (activationReceipt, error)
	Deactivate(context.Context, deactivationRequest) (runtimeProof, error)
	Status(context.Context, deployment.InstalledSpec) (installedStatus, error)
}

type daemonkitDeploymentController struct{ inner *deployment.Controller }

func (c daemonkitDeploymentController) Attest(
	ctx context.Context,
	spec deployment.InstalledSpec,
) (installedGeneration, error) {
	attestation, err := c.inner.AttestInstalled(ctx, spec)
	if err != nil {
		return installedGeneration{}, err
	}
	return observeInstalledGeneration(attestation), nil
}

func (c daemonkitDeploymentController) Activate(
	ctx context.Context,
	request activationRequest,
) (activationReceipt, error) {
	receipt, err := c.inner.ActivateInstalled(ctx, deployment.ActivateInstalledConfig{
		Expected: request.expected.raw, ConsumerBuild: request.consumerBuild,
		PolicyDigest: request.policyDigest, Plan: request.plan,
		Readiness: func(ctx context.Context, operation deployment.InstalledOperation) (deployment.ReadinessProof, error) {
			proof, err := request.readiness(ctx, installedOperation{
				id: operation.OperationID(), generation: observeInstalledGeneration(operation.Generation()),
				plan: operation.Plan(),
			})
			if err != nil {
				return deployment.ReadinessProof{}, err
			}
			return deployment.NewReadinessProof(proof.runtimeBuild, proof.processGeneration, proof.digest)
		},
	})
	if err != nil {
		return activationReceipt{}, err
	}
	return observeActivationReceipt(receipt)
}

func (c daemonkitDeploymentController) Deactivate(
	ctx context.Context,
	request deactivationRequest,
) (runtimeProof, error) {
	receipt, err := c.inner.DeactivateInstalled(ctx, deployment.DeactivateInstalledConfig{
		Expected: request.expected.raw, ConsumerBuild: request.consumerBuild, PolicyDigest: request.policyDigest,
		RuntimeQuiesce: func(
			ctx context.Context,
			stopper deployment.RuntimeStopper,
			operation deployment.DeactivateInstalledOperation,
		) (deployment.RuntimeProof, error) {
			proof, err := request.runtimeQuiesce(ctx, stopper, deactivationOperation{
				id: operation.OperationID(), activation: request.expected,
			})
			if err != nil {
				return deployment.RuntimeProof{}, err
			}
			return deployment.NewRuntimeProof(proof.absent, proof.processGeneration, proof.digest)
		},
	})
	if err != nil {
		return runtimeProof{}, err
	}
	proof := receipt.RuntimeProof()
	return runtimeProof{
		absent: proof.Absent(), processGeneration: proof.ProcessGeneration(), digest: proof.Digest(),
	}, nil
}

func (c daemonkitDeploymentController) Status(
	ctx context.Context,
	spec deployment.InstalledSpec,
) (installedStatus, error) {
	status, err := c.inner.StatusInstalled(ctx, spec)
	if err != nil {
		return installedStatus{}, err
	}
	result := installedStatus{
		state: status.State(), generation: observeInstalledGeneration(status.Attestation()),
	}
	if receipt, ok := status.Receipt(); ok {
		result.activation, err = observeActivationReceipt(receipt)
		if err != nil {
			return installedStatus{}, err
		}
		result.hasReceipt = true
	}
	return result, nil
}

func observeInstalledGeneration(attestation deployment.InstalledAttestation) installedGeneration {
	return installedGeneration{
		raw: attestation, path: attestation.Path(), version: attestation.Version(),
		teamID: attestation.TeamID(), signingIdentifier: attestation.SigningIdentifier(),
		designatedRequirement: attestation.DesignatedRequirement(), cdHash: attestation.CDHash(),
		bundleDigest: attestation.BundleDigest(), entitlementsDigest: attestation.EntitlementsDigest(),
		device: attestation.Device(), inode: attestation.Inode(),
	}
}

func observeActivationReceipt(receipt deployment.ActivationReceipt) (activationReceipt, error) {
	readiness, ready := receipt.Readiness()
	if receipt.Active() != ready {
		return activationReceipt{}, errors.New("CCPoolStatus: activation receipt readiness is inconsistent")
	}
	result := activationReceipt{
		raw: receipt, id: receipt.OperationID(), active: receipt.Active(),
		generation: observeInstalledGeneration(receipt.Generation()), plan: receipt.Plan(),
	}
	if ready {
		result.readiness = runtimeReadiness{
			runtimeBuild: readiness.RuntimeBuild(), processGeneration: readiness.ProcessGeneration(),
			digest: readiness.ResourceDigest(),
		}
	}
	return result, nil
}

var (
	newDeployer      = func() deploymentController { return daemonkitDeploymentController{inner: deployment.New()} }
	makeProductHooks = newProductHooks
)

// ServiceDeploymentState is the exact durable holder activation state.
type ServiceDeploymentState string

const (
	ServiceDeploymentInactive ServiceDeploymentState = "inactive"
	ServiceDeploymentActive   ServiceDeploymentState = "active"
)

// ServiceGeneration identifies one exact attested fixed app generation.
type ServiceGeneration struct {
	Path                  string
	Version               string
	TeamID                string
	SigningIdentifier     string
	DesignatedRequirement string
	CDHash                string
	BundleDigest          deployment.SHA256
	EntitlementsDigest    deployment.SHA256
	Device                string
	Inode                 string
}

// ServiceDeployment identifies one exact durable holder activation.
type ServiceDeployment struct {
	State       ServiceDeploymentState
	OperationID string
	Holder      ServiceGeneration
}

// ServiceInstallReceipt binds one activation attempt to its exact prior and active states.
type ServiceInstallReceipt struct {
	Prior          ServiceDeployment
	Current        ServiceDeployment
	Changed        bool
	NewlyActivated bool
	activation     activationReceipt
}

// Rollback deactivates only the exact activation created by this attempt.
func (r ServiceInstallReceipt) Rollback(ctx context.Context) error {
	return rollbackService(ctx, newDeployer(), r)
}

type serviceDeploymentSpec struct {
	installed     deployment.InstalledSpec
	consumerBuild string
	policyDigest  deployment.SHA256
	hooks         productHooks
}

func statusAppCodeIdentity() codeidentity.CodeIdentity {
	return codeidentity.CodeIdentity{TeamID: holderbridge.TeamID, SigningIdentifier: holderbridge.BundleID}
}

func makeServiceDeploymentSpec() (serviceDeploymentSpec, error) {
	appVersion, err := statusAppVersion()
	if err != nil {
		return serviceDeploymentSpec{}, err
	}
	consumerBuild, policyDigest, err := holderbridge.DeploymentIdentity()
	if err != nil {
		return serviceDeploymentSpec{}, err
	}
	return serviceDeploymentSpec{
		installed: deployment.InstalledSpec{
			AppPath: pool.WidgetAppPath(), Version: appVersion, Identity: statusAppCodeIdentity(),
		},
		consumerBuild: consumerBuild, policyDigest: policyDigest,
		hooks: makeProductHooks(version.String(), policyDigest),
	}, nil
}

// InstallService activates the already-packaged exact signed status application.
func InstallService(ctx context.Context) (ServiceInstallReceipt, error) {
	return installService(ctx, newDeployer())
}

// DeactivateService durably retires the exact active status app runtime and service plan.
func DeactivateService(ctx context.Context) error {
	return deactivateService(ctx, newDeployer())
}

func installService(ctx context.Context, controller deploymentController) (ServiceInstallReceipt, error) {
	spec, err := makeServiceDeploymentSpec()
	if err != nil {
		return ServiceInstallReceipt{}, err
	}
	expected, err := controller.Attest(ctx, spec.installed)
	if err != nil {
		return ServiceInstallReceipt{}, fmt.Errorf("CCPoolStatus: attest packaged signed app: %w", err)
	}
	priorStatus, err := controller.Status(ctx, spec.installed)
	if err != nil {
		return ServiceInstallReceipt{}, fmt.Errorf("CCPoolStatus: observe prior activation: %w", err)
	}
	prior := publicServiceDeployment(priorStatus)
	plan, err := spec.hooks.servicePlanForBuild(expected, spec.hooks.buildID)
	if err != nil {
		return ServiceInstallReceipt{}, err
	}
	request := activationRequest{
		expected: expected, consumerBuild: spec.consumerBuild, policyDigest: spec.policyDigest,
		plan: plan, readiness: spec.hooks.readiness,
	}
	receipt, activateErr := controller.Activate(ctx, request)
	if activateErr != nil {
		observed, observeErr := controller.Status(ctx, spec.installed)
		if observeErr != nil || observed.state != deployment.InstalledActive || !observed.hasReceipt {
			return ServiceInstallReceipt{}, fmt.Errorf(
				"CCPoolStatus: activate signed app: %w", errors.Join(activateErr, observeErr),
			)
		}
		receipt = observed.activation
	}
	if err := validateActiveDeployment(expected, plan, receipt); err != nil {
		return ServiceInstallReceipt{}, err
	}
	current := publicActivation(receipt)
	return ServiceInstallReceipt{
		Prior: prior, Current: current, Changed: !reflect.DeepEqual(prior, current),
		NewlyActivated: prior.State != ServiceDeploymentActive, activation: receipt,
	}, nil
}

func publicServiceDeployment(status installedStatus) ServiceDeployment {
	if status.state == deployment.InstalledActive && status.hasReceipt {
		return publicActivation(status.activation)
	}
	return ServiceDeployment{State: ServiceDeploymentInactive, Holder: publicGeneration(status.generation)}
}

func publicActivation(receipt activationReceipt) ServiceDeployment {
	state := ServiceDeploymentInactive
	if receipt.active {
		state = ServiceDeploymentActive
	}
	return ServiceDeployment{
		State: state, OperationID: receipt.id, Holder: publicGeneration(receipt.generation),
	}
}

func publicGeneration(generation installedGeneration) ServiceGeneration {
	return ServiceGeneration{
		Path: generation.path, Version: generation.version, TeamID: generation.teamID,
		SigningIdentifier:     generation.signingIdentifier,
		DesignatedRequirement: generation.designatedRequirement, CDHash: generation.cdHash,
		BundleDigest: generation.bundleDigest, EntitlementsDigest: generation.entitlementsDigest,
		Device: generation.device, Inode: generation.inode,
	}
}

func validateActiveDeployment(
	expected installedGeneration,
	plan service.Plan,
	receipt activationReceipt,
) error {
	if !receipt.active || receipt.id == "" || !sameGeneration(receipt.generation, expected) ||
		receipt.plan.Digest() != plan.Digest() || receipt.readiness.runtimeBuild == "" ||
		receipt.readiness.processGeneration == (proc.OwnerGeneration{}) ||
		receipt.readiness.digest == (deployment.SHA256{}) {
		return errors.New("CCPoolStatus: activation receipt does not prove the exact packaged runtime")
	}
	return nil
}

func sameGeneration(left, right installedGeneration) bool {
	left.raw, right.raw = deployment.InstalledAttestation{}, deployment.InstalledAttestation{}
	return left == right
}

func rollbackService(
	ctx context.Context,
	controller deploymentController,
	receipt ServiceInstallReceipt,
) error {
	if !receipt.Changed || !receipt.NewlyActivated || receipt.activation.id == "" {
		return nil
	}
	spec, err := makeServiceDeploymentSpec()
	if err != nil {
		return err
	}
	status, err := controller.Status(ctx, spec.installed)
	if err != nil {
		return err
	}
	if status.state == deployment.InstalledVerifiedUnactivated {
		return nil
	}
	if status.state != deployment.InstalledActive || !status.hasReceipt ||
		status.activation.id != receipt.activation.id {
		return errors.New("CCPoolStatus: rollback activation ownership changed")
	}
	return deactivateExact(ctx, controller, spec, receipt.activation)
}

func deactivateService(ctx context.Context, controller deploymentController) error {
	spec, err := makeServiceDeploymentSpec()
	if err != nil {
		return err
	}
	status, err := controller.Status(ctx, spec.installed)
	if err != nil {
		return err
	}
	if status.state == deployment.InstalledVerifiedUnactivated {
		return nil
	}
	if status.state != deployment.InstalledActive || !status.hasReceipt {
		return errors.New("CCPoolStatus: packaged app activation is incomplete")
	}
	return deactivateExact(ctx, controller, spec, status.activation)
}

func deactivateExact(
	ctx context.Context,
	controller deploymentController,
	spec serviceDeploymentSpec,
	receipt activationReceipt,
) error {
	proof, err := controller.Deactivate(ctx, deactivationRequest{
		expected: receipt, consumerBuild: spec.consumerBuild, policyDigest: spec.policyDigest,
		runtimeQuiesce: spec.hooks.runtimeQuiesce,
	})
	if err != nil {
		return err
	}
	if !proof.absent || proof.digest == (deployment.SHA256{}) {
		return errors.New("CCPoolStatus: deactivation did not prove runtime absence")
	}
	return nil
}
