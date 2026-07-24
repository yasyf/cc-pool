package statusapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit/deployment"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
)

type recordingDeployer struct {
	attestation   installedGeneration
	attestErr     error
	statuses      []installedStatus
	statusCalls   int
	activation    activationReceipt
	activateErr   error
	activate      activationRequest
	activations   int
	deactivate    deactivationRequest
	deactivated   runtimeProof
	deactivateErr error
	deactivations int
}

func (d *recordingDeployer) Attest(context.Context, deployment.InstalledSpec) (installedGeneration, error) {
	return d.attestation, d.attestErr
}

func (d *recordingDeployer) Status(context.Context, deployment.InstalledSpec) (installedStatus, error) {
	index := d.statusCalls
	d.statusCalls++
	if len(d.statuses) == 0 {
		return installedStatus{}, errors.New("unexpected status")
	}
	if index >= len(d.statuses) {
		index = len(d.statuses) - 1
	}
	return d.statuses[index], nil
}

func (d *recordingDeployer) Activate(_ context.Context, request activationRequest) (activationReceipt, error) {
	d.activations++
	d.activate = request
	if d.activateErr != nil {
		return activationReceipt{}, d.activateErr
	}
	result := d.activation
	if result.plan.Digest() == (service.PlanDigest{}) {
		result.plan = request.plan
	}
	return result, nil
}

func (d *recordingDeployer) Deactivate(_ context.Context, request deactivationRequest) (runtimeProof, error) {
	d.deactivations++
	d.deactivate = request
	return d.deactivated, d.deactivateErr
}

func exactTestGeneration(t *testing.T) installedGeneration {
	t.Helper()
	directory, err := os.MkdirTemp("/private/tmp", "cc-pool-status-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	appPath := filepath.Join(directory, "CCPoolStatus.app")
	executable := holderExecutablePath(appPath)
	if err := os.MkdirAll(filepath.Dir(executable), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(executable, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return installedGeneration{
		path: appPath, version: "0.63.0",
		teamID: holderbridge.TeamID, signingIdentifier: holderbridge.BundleID,
		designatedRequirement: "designated => anchor apple generic", cdHash: "0123456789abcdef0123456789abcdef01234567",
		bundleDigest: deployment.SHA256{1}, entitlementsDigest: deployment.SHA256{2},
		device: "1", inode: "2",
	}
}

func exactTestActivation(t *testing.T, generation installedGeneration) activationReceipt {
	t.Helper()
	plan, err := testServicePlan(generation, "runtime-v1")
	if err != nil {
		t.Fatal(err)
	}
	return activationReceipt{
		id: "activation-1", active: true, generation: generation, plan: plan,
		readiness: runtimeReadiness{
			runtimeBuild: "runtime-v1", processGeneration: proc.OwnerGeneration{1}, digest: deployment.SHA256{4},
		},
	}
}

func useDeploymentMetadata(t *testing.T) {
	t.Helper()
	oldVersion, oldAppVersion := version.Version, version.StatusAppVersion
	version.Version, version.StatusAppVersion = "v0.63.0", "0.63.0"
	t.Cleanup(func() { version.Version, version.StatusAppVersion = oldVersion, oldAppVersion })
	oldHooks := makeProductHooks
	makeProductHooks = func(buildID string, digest deployment.SHA256) productHooks {
		hooks := newProductHooks("runtime-v1", digest)
		installTestBuilders(&hooks)
		return hooks
	}
	t.Cleanup(func() { makeProductHooks = oldHooks })
}

func testServicePlan(generation installedGeneration, buildID string) (service.Plan, error) {
	return service.NewPlan([]service.Agent{{
		Label: holderbridge.BundleID + ".fusekit", Program: holderExecutablePath(generation.path),
		LogPath: "/tmp/cc-pool-holder-test.log", Env: map[string]string{"FUSEKIT_BUILD_ID": buildID},
		AssociatedBundleIdentifiers: []string{holderbridge.BundleID}, RestartPolicy: service.RestartAlways,
		LimitLoadToSessionType: service.SessionTypeAqua,
	}})
}

func installTestBuilders(hooks *productHooks) {
	hooks.servicePlan = testServicePlan
	hooks.target = func(generation installedGeneration, buildID string) (runtimeTarget, error) {
		return runtimeTarget{
			executable: holderExecutablePath(generation.path), socket: "/tmp/fusekit-test.sock", buildID: buildID,
		}, nil
	}
}

func TestInstallServiceActivatesExactAttestedGeneration(t *testing.T) {
	useDeploymentMetadata(t)
	generation := exactTestGeneration(t)
	active := exactTestActivation(t, generation)
	controller := &recordingDeployer{
		attestation: generation,
		statuses:    []installedStatus{{state: deployment.InstalledVerifiedUnactivated, generation: generation}},
		activation:  active,
	}
	receipt, err := installService(t.Context(), controller)
	if err != nil {
		t.Fatal(err)
	}
	if controller.activations != 1 || !sameGeneration(controller.activate.expected, generation) ||
		controller.activate.consumerBuild == "" || controller.activate.policyDigest == (deployment.SHA256{}) ||
		controller.activate.plan.Digest() != active.plan.Digest() {
		t.Fatalf("activation = %#v", controller.activate)
	}
	if receipt.Prior.State != ServiceDeploymentInactive || receipt.Current.State != ServiceDeploymentActive ||
		!receipt.Changed || !receipt.NewlyActivated || receipt.Current.Holder.Path != generation.path {
		t.Fatalf("receipt = %#v", receipt)
	}
}

func TestInstallServiceAcceptsLostResponseOnlyFromExactActiveStatus(t *testing.T) {
	useDeploymentMetadata(t)
	generation := exactTestGeneration(t)
	active := exactTestActivation(t, generation)
	want := errors.New("lost response")
	controller := &recordingDeployer{
		attestation: generation,
		statuses: []installedStatus{
			{state: deployment.InstalledVerifiedUnactivated, generation: generation},
			{state: deployment.InstalledActive, generation: generation, activation: active, hasReceipt: true},
		},
		activateErr: want,
	}
	receipt, err := installService(t.Context(), controller)
	if err != nil || receipt.Current.OperationID != active.id || controller.statusCalls != 2 {
		t.Fatalf("receipt = %#v, status calls = %d, err = %v", receipt, controller.statusCalls, err)
	}

	controller.statusCalls = 0
	controller.statuses[1] = installedStatus{state: deployment.InstalledPrepared, generation: generation, activation: active, hasReceipt: true}
	if _, err := installService(t.Context(), controller); !errors.Is(err, want) {
		t.Fatalf("prepared lost response = %v", err)
	}
}

func TestServiceInstallRollbackDeactivatesOnlyOwnedActivation(t *testing.T) {
	useDeploymentMetadata(t)
	generation := exactTestGeneration(t)
	active := exactTestActivation(t, generation)
	controller := &recordingDeployer{
		statuses:    []installedStatus{{state: deployment.InstalledActive, generation: generation, activation: active, hasReceipt: true}},
		deactivated: runtimeProof{absent: true, digest: deployment.SHA256{9}},
	}
	receipt := ServiceInstallReceipt{
		Prior: ServiceDeployment{State: ServiceDeploymentInactive}, Current: publicActivation(active),
		Changed: true, NewlyActivated: true, activation: active,
	}
	if err := rollbackService(t.Context(), controller, receipt); err != nil {
		t.Fatal(err)
	}
	if controller.deactivations != 1 || controller.deactivate.expected.id != active.id {
		t.Fatalf("deactivation = %#v", controller.deactivate)
	}

	controller.deactivations = 0
	receipt.NewlyActivated = false
	if err := rollbackService(t.Context(), controller, receipt); err != nil || controller.deactivations != 0 {
		t.Fatalf("preexisting rollback calls = %d, err = %v", controller.deactivations, err)
	}
}

func TestDeactivateServiceRequiresExactActiveReceipt(t *testing.T) {
	useDeploymentMetadata(t)
	generation := exactTestGeneration(t)
	active := exactTestActivation(t, generation)
	controller := &recordingDeployer{
		statuses:    []installedStatus{{state: deployment.InstalledActive, generation: generation, activation: active, hasReceipt: true}},
		deactivated: runtimeProof{absent: true, digest: deployment.SHA256{7}},
	}
	if err := deactivateService(t.Context(), controller); err != nil {
		t.Fatal(err)
	}
	if controller.deactivations != 1 || !reflect.DeepEqual(controller.deactivate.expected, active) {
		t.Fatalf("deactivation = %#v", controller.deactivate)
	}

	controller.statusCalls, controller.deactivations = 0, 0
	controller.statuses = []installedStatus{{state: deployment.InstalledVerifiedUnactivated, generation: generation}}
	if err := deactivateService(t.Context(), controller); err != nil || controller.deactivations != 0 {
		t.Fatalf("inactive deactivation calls = %d, err = %v", controller.deactivations, err)
	}
}

func TestInstallServiceRejectsIncompleteActivationReceipt(t *testing.T) {
	useDeploymentMetadata(t)
	generation := exactTestGeneration(t)
	controller := &recordingDeployer{
		attestation: generation,
		statuses:    []installedStatus{{state: deployment.InstalledVerifiedUnactivated, generation: generation}},
		activation:  activationReceipt{id: "activation-1", active: true, generation: generation},
	}
	if _, err := installService(t.Context(), controller); err == nil {
		t.Fatal("incomplete activation was accepted")
	}
}
