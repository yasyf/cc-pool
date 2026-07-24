package statusapp

import (
	"context"
	"errors"
	"fmt"

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

type candidateRequest struct {
	target         deployment.CurrentInstalledSpec
	sourcePath     string
	candidate      installedGeneration
	consumerBuild  string
	policyDigest   deployment.SHA256
	plan           deployment.CandidatePlan
	readiness      func(context.Context, installedOperation) (runtimeReadiness, error)
	runtimeQuiesce func(context.Context, deployment.RuntimeStopper, deactivationOperation) (runtimeProof, error)
}

type candidateReceipt struct {
	id         string
	activation activationReceipt
}

type uninstallRequest struct {
	current        deployment.CurrentInstalledSpec
	readiness      func(context.Context, installedOperation) (runtimeReadiness, error)
	runtimeQuiesce func(context.Context, deployment.RuntimeStopper, deactivationOperation) (runtimeProof, error)
}

type uninstallReceipt struct {
	id         string
	generation installedGeneration
	runtime    runtimeProof
}

type installedStatus struct {
	state      deployment.InstalledState
	generation installedGeneration
	activation activationReceipt
	hasReceipt bool
}

type deploymentController interface {
	Attest(context.Context, deployment.InstalledSpec) (installedGeneration, error)
	Apply(context.Context, candidateRequest) (candidateReceipt, error)
	Status(context.Context, deployment.InstalledSpec) (installedStatus, error)
	Uninstall(context.Context, uninstallRequest) (uninstallReceipt, error)
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

func (c daemonkitDeploymentController) Apply(
	ctx context.Context,
	request candidateRequest,
) (candidateReceipt, error) {
	receipt, err := c.inner.ApplyInstalledCandidate(ctx, deployment.ApplyInstalledCandidateConfig{
		Target: request.target, CandidateSourcePath: request.sourcePath,
		CandidateVersion: request.candidate.version, CandidateBundleDigest: request.candidate.bundleDigest,
		ConsumerBuild: request.consumerBuild, PolicyDigest: request.policyDigest, Plan: request.plan,
		RuntimeQuiesce: func(
			ctx context.Context,
			stopper deployment.RuntimeStopper,
			operation deployment.DeactivateInstalledOperation,
		) (deployment.RuntimeProof, error) {
			activation, err := observeActivationReceipt(operation.Activation())
			if err != nil {
				return deployment.RuntimeProof{}, err
			}
			proof, err := request.runtimeQuiesce(ctx, stopper, deactivationOperation{
				id: operation.OperationID(), activation: activation,
			})
			if err != nil {
				return deployment.RuntimeProof{}, err
			}
			return deployment.NewRuntimeProof(proof.absent, proof.processGeneration, proof.digest)
		},
		Readiness: func(
			ctx context.Context,
			operation deployment.InstalledOperation,
		) (deployment.ReadinessProof, error) {
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
		return candidateReceipt{}, err
	}
	activation, err := observeActivationReceipt(receipt.Activation())
	if err != nil {
		return candidateReceipt{}, err
	}
	return candidateReceipt{id: receipt.OperationID(), activation: activation}, nil
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

func (c daemonkitDeploymentController) Uninstall(
	ctx context.Context,
	request uninstallRequest,
) (uninstallReceipt, error) {
	receipt, err := c.inner.UninstallCurrentInstalled(ctx, deployment.UninstallCurrentInstalledConfig{
		Current: request.current,
		RuntimeQuiesce: func(
			ctx context.Context,
			stopper deployment.RuntimeStopper,
			operation deployment.DeactivateInstalledOperation,
		) (deployment.RuntimeProof, error) {
			activation, err := observeActivationReceipt(operation.Activation())
			if err != nil {
				return deployment.RuntimeProof{}, err
			}
			proof, err := request.runtimeQuiesce(ctx, stopper, deactivationOperation{
				id: operation.OperationID(), activation: activation,
			})
			if err != nil {
				return deployment.RuntimeProof{}, err
			}
			return deployment.NewRuntimeProof(proof.absent, proof.processGeneration, proof.digest)
		},
		Readiness: func(
			ctx context.Context,
			operation deployment.InstalledOperation,
		) (deployment.ReadinessProof, error) {
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
		return uninstallReceipt{}, err
	}
	proof := receipt.RuntimeProof()
	return uninstallReceipt{
		id: receipt.OperationID(), generation: observeInstalledGeneration(receipt.Generation()),
		runtime: runtimeProof{
			absent: proof.Absent(), processGeneration: proof.ProcessGeneration(), digest: proof.Digest(),
		},
	}, nil
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
	installedAppPath = pool.WidgetAppPath
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

// ServiceInstallReceipt binds one terminal daemonkit candidate apply to its active generation.
type ServiceInstallReceipt struct {
	OperationID string
	Activation  ServiceDeployment
}

// Rollback is idempotent because daemonkit completes rollback before ApplyPackagedApp returns.
func (ServiceInstallReceipt) Rollback(context.Context) error { return nil }

type serviceDeploymentSpec struct {
	installed     deployment.InstalledSpec
	current       deployment.CurrentInstalledSpec
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
	identity := statusAppCodeIdentity()
	appPath := installedAppPath()
	return serviceDeploymentSpec{
		installed: deployment.InstalledSpec{
			AppPath: appPath, Version: appVersion, Identity: identity,
		},
		current:       deployment.CurrentInstalledSpec{AppPath: appPath, Identity: identity},
		consumerBuild: consumerBuild, policyDigest: policyDigest,
		hooks: makeProductHooks(version.String(), policyDigest),
	}, nil
}

// ApplyPackagedApp installs or upgrades one exact packaged signed application candidate.
func ApplyPackagedApp(ctx context.Context, candidateSourcePath string) (ServiceInstallReceipt, error) {
	return applyPackagedApp(ctx, candidateSourcePath, newDeployer())
}

// RequireActiveService proves the exact installed application and live FuseKit runtime are ready.
func RequireActiveService(ctx context.Context) error {
	return requireActiveService(ctx, newDeployer())
}

// UninstallPackagedApp quiesces and removes the exact controller-sealed installed application.
func UninstallPackagedApp(ctx context.Context) error {
	return uninstallPackagedApp(ctx, newDeployer())
}

func applyPackagedApp(
	ctx context.Context,
	candidateSourcePath string,
	controller deploymentController,
) (ServiceInstallReceipt, error) {
	spec, err := makeServiceDeploymentSpec()
	if err != nil {
		return ServiceInstallReceipt{}, err
	}
	candidateSpec := spec.installed
	candidateSpec.AppPath = candidateSourcePath
	candidate, err := controller.Attest(ctx, candidateSpec)
	if err != nil {
		return ServiceInstallReceipt{}, fmt.Errorf("CCPoolStatus: attest packaged candidate: %w", err)
	}
	installedCandidate := candidate
	installedCandidate.path = spec.current.AppPath
	candidatePlan, err := spec.hooks.candidatePlanForBuild(
		installedCandidate, spec.hooks.buildID, candidateSourcePath,
	)
	if err != nil {
		return ServiceInstallReceipt{}, fmt.Errorf("CCPoolStatus: bind packaged candidate plan: %w", err)
	}
	receipt, err := controller.Apply(ctx, candidateRequest{
		target: spec.current, sourcePath: candidateSourcePath, candidate: candidate,
		consumerBuild: spec.consumerBuild, policyDigest: spec.policyDigest, plan: candidatePlan,
		readiness: spec.hooks.readiness, runtimeQuiesce: spec.hooks.runtimeQuiesce,
	})
	if err != nil {
		return ServiceInstallReceipt{}, fmt.Errorf("CCPoolStatus: apply packaged candidate: %w", err)
	}
	installedPlan, err := spec.hooks.servicePlanForBuild(receipt.activation.generation, spec.hooks.buildID)
	if err != nil {
		return ServiceInstallReceipt{}, err
	}
	if err := validateActiveDeployment(
		candidate, spec.current.AppPath, spec.hooks.buildID, installedPlan, receipt.activation,
	); err != nil {
		return ServiceInstallReceipt{}, err
	}
	if receipt.id == "" {
		return ServiceInstallReceipt{}, errors.New("CCPoolStatus: candidate apply did not return a terminal operation")
	}
	return ServiceInstallReceipt{
		OperationID: receipt.id, Activation: publicActivation(receipt.activation),
	}, nil
}

func requireActiveService(ctx context.Context, controller deploymentController) error {
	spec, err := makeServiceDeploymentSpec()
	if err != nil {
		return err
	}
	status, err := controller.Status(ctx, spec.installed)
	if err != nil {
		return fmt.Errorf("CCPoolStatus: observe installed service: %w", err)
	}
	if status.state != deployment.InstalledActive || !status.hasReceipt {
		return errors.New("CCPoolStatus: packaged application service is not active")
	}
	plan, err := spec.hooks.servicePlanForBuild(status.generation, spec.hooks.buildID)
	if err != nil {
		return err
	}
	if err := validateActiveDeployment(
		status.generation, spec.current.AppPath, spec.hooks.buildID, plan, status.activation,
	); err != nil {
		return err
	}
	fresh, err := spec.hooks.readiness(ctx, installedOperation{
		id: status.activation.id, generation: status.generation, plan: status.activation.plan,
	})
	if err != nil {
		return fmt.Errorf("CCPoolStatus: prove active service readiness: %w", err)
	}
	if fresh.runtimeBuild != spec.hooks.buildID || fresh.processGeneration == (proc.OwnerGeneration{}) ||
		fresh.digest == (deployment.SHA256{}) {
		return errors.New("CCPoolStatus: active service readiness proof is incomplete")
	}
	return nil
}

func uninstallPackagedApp(ctx context.Context, controller deploymentController) error {
	spec, err := makeServiceDeploymentSpec()
	if err != nil {
		return err
	}
	receipt, err := controller.Uninstall(ctx, uninstallRequest{
		current: spec.current, readiness: spec.hooks.readiness,
		runtimeQuiesce: spec.hooks.runtimeQuiesce,
	})
	if err != nil {
		return fmt.Errorf("CCPoolStatus: uninstall packaged application: %w", err)
	}
	if receipt.id == "" || receipt.generation.path != spec.current.AppPath ||
		!receipt.runtime.absent || receipt.runtime.digest == (deployment.SHA256{}) {
		return errors.New("CCPoolStatus: uninstall did not prove exact application removal")
	}
	return nil
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
	candidate installedGeneration,
	targetPath, runtimeBuild string,
	plan service.Plan,
	receipt activationReceipt,
) error {
	if !receipt.active || receipt.id == "" || receipt.generation.path != targetPath ||
		!sameApplicationBytes(receipt.generation, candidate) || receipt.plan.Digest() != plan.Digest() ||
		receipt.readiness.runtimeBuild != runtimeBuild ||
		receipt.readiness.processGeneration == (proc.OwnerGeneration{}) ||
		receipt.readiness.digest == (deployment.SHA256{}) {
		return errors.New("CCPoolStatus: activation receipt does not prove the exact packaged runtime")
	}
	return nil
}

func sameApplicationBytes(left, right installedGeneration) bool {
	left.raw, right.raw = deployment.InstalledAttestation{}, deployment.InstalledAttestation{}
	left.path, right.path = "", ""
	left.device, right.device = "", ""
	left.inode, right.inode = "", ""
	return left == right
}
