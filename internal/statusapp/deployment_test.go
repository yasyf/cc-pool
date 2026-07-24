package statusapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit/deployment"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/fusekit/mountproto"
)

type recordingDeployer struct {
	attestation installedGeneration
	attestSpec  deployment.InstalledSpec
	attestErr   error

	apply      candidateRequest
	applied    candidateReceipt
	applyErr   error
	applyCalls int

	statuses    []installedStatus
	statusCalls int

	uninstall      uninstallRequest
	uninstalled    uninstallReceipt
	uninstallErr   error
	uninstallCalls int
}

func (d *recordingDeployer) Attest(
	_ context.Context,
	spec deployment.InstalledSpec,
) (installedGeneration, error) {
	d.attestSpec = spec
	return d.attestation, d.attestErr
}

func (d *recordingDeployer) Apply(
	_ context.Context,
	request candidateRequest,
) (candidateReceipt, error) {
	d.applyCalls++
	d.apply = request
	return d.applied, d.applyErr
}

func (d *recordingDeployer) Status(
	context.Context,
	deployment.InstalledSpec,
) (installedStatus, error) {
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

func (d *recordingDeployer) Uninstall(
	_ context.Context,
	request uninstallRequest,
) (uninstallReceipt, error) {
	d.uninstallCalls++
	d.uninstall = request
	return d.uninstalled, d.uninstallErr
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
		designatedRequirement: "designated => anchor apple generic",
		cdHash:                "0123456789abcdef0123456789abcdef01234567",
		bundleDigest:          deployment.SHA256{1},
		entitlementsDigest:    deployment.SHA256{2},
		device:                "1",
		inode:                 "2",
	}
}

func installedTestGeneration(source installedGeneration) installedGeneration {
	result := source
	result.path = installedAppPath()
	result.device = "3"
	result.inode = "4"
	return result
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
			runtimeBuild: "runtime-v1", processGeneration: proc.OwnerGeneration{1},
			digest: deployment.SHA256{4},
		},
	}
}

func useDeploymentMetadata(t *testing.T, configure ...func(*productHooks)) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	appPath := filepath.Join(root, "Applications", "CCPoolStatus.app")
	executable := holderExecutablePath(appPath)
	if err := os.MkdirAll(filepath.Dir(executable), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(executable, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	oldInstalledAppPath := installedAppPath
	installedAppPath = func() string { return appPath }
	t.Cleanup(func() { installedAppPath = oldInstalledAppPath })
	oldVersion, oldAppVersion := version.Version, version.StatusAppVersion
	version.Version, version.StatusAppVersion = "v0.63.0", "0.63.0"
	t.Cleanup(func() { version.Version, version.StatusAppVersion = oldVersion, oldAppVersion })
	oldHooks := makeProductHooks
	makeProductHooks = func(buildID string, digest deployment.SHA256) productHooks {
		hooks := newProductHooks("runtime-v1", digest)
		installTestBuilders(&hooks)
		for _, apply := range configure {
			apply(&hooks)
		}
		return hooks
	}
	t.Cleanup(func() { makeProductHooks = oldHooks })
}

func testServicePlan(generation installedGeneration, buildID string) (service.Plan, error) {
	return service.NewPlan([]service.Agent{{
		Label: holderbridge.BundleID + ".fusekit", Program: holderExecutablePath(generation.path),
		LogPath: "/tmp/cc-pool-holder-test.log", Env: map[string]string{"FUSEKIT_BUILD_ID": buildID},
		AssociatedBundleIdentifiers: []string{holderbridge.BundleID},
		RestartPolicy:               service.RestartAlways,
		LimitLoadToSessionType:      service.SessionTypeAqua,
	}})
}

func installTestBuilders(hooks *productHooks) {
	hooks.servicePlan = testServicePlan
	hooks.candidatePlan = func(
		generation installedGeneration,
		buildID, sourcePath string,
	) (deployment.CandidatePlan, error) {
		plan, err := testServicePlan(generation, buildID)
		if err != nil {
			return deployment.CandidatePlan{}, err
		}
		agents := plan.Agents()
		agents[0].Program = holderExecutablePath(sourcePath)
		return deployment.NewCandidatePlan(sourcePath, agents)
	}
	hooks.target = func(installedGeneration, string) (runtimeTarget, error) {
		return runtimeTarget{
			executable: "/tmp/CCPoolStatus", socket: "/tmp/fusekit-test.sock", buildID: "runtime-v1",
		}, nil
	}
	hooks.proveApp = func(context.Context, string) error { return nil }
	hooks.observe = func(context.Context, string) (mountproto.RuntimeHealthResponse, error) {
		return exactHealth(runtimeTarget{buildID: "runtime-v1"}), nil
	}
}

func TestApplyPackagedAppDelegatesExactCandidateAndReturnsTerminalApply(t *testing.T) {
	useDeploymentMetadata(t)
	candidate := exactTestGeneration(t)
	installed := installedTestGeneration(candidate)
	active := exactTestActivation(t, installed)
	controller := &recordingDeployer{
		attestation: candidate,
		applied: candidateReceipt{
			id: "apply-operation-1", activation: active,
		},
	}
	receipt, err := applyPackagedApp(t.Context(), candidate.path, controller)
	if err != nil {
		t.Fatal(err)
	}
	if controller.applyCalls != 1 || controller.attestSpec.AppPath != candidate.path ||
		controller.apply.sourcePath != candidate.path ||
		controller.apply.target.AppPath != installedAppPath() ||
		controller.apply.candidate.bundleDigest != candidate.bundleDigest ||
		controller.apply.consumerBuild == "" || controller.apply.policyDigest == (deployment.SHA256{}) {
		t.Fatalf("candidate apply = %#v", controller.apply)
	}
	if receipt.OperationID != "apply-operation-1" ||
		receipt.Activation.OperationID != active.id ||
		receipt.Activation.Holder.Path != installedAppPath() ||
		receipt.Activation.State != ServiceDeploymentActive {
		t.Fatalf("receipt = %#v", receipt)
	}
	if err := receipt.Rollback(t.Context()); err != nil {
		t.Fatalf("terminal daemonkit rollback = %v", err)
	}
}

func TestApplyPackagedAppRejectsIncompleteTerminalReceipt(t *testing.T) {
	useDeploymentMetadata(t)
	candidate := exactTestGeneration(t)
	installed := installedTestGeneration(candidate)
	controller := &recordingDeployer{
		attestation: candidate,
		applied: candidateReceipt{
			id: "apply-operation-1",
			activation: activationReceipt{
				id: "activation-1", active: true, generation: installed,
			},
		},
	}
	if _, err := applyPackagedApp(t.Context(), candidate.path, controller); err == nil {
		t.Fatal("incomplete terminal apply receipt was accepted")
	}
	controller.applied.id = ""
	controller.applied.activation = exactTestActivation(t, installed)
	if _, err := applyPackagedApp(t.Context(), candidate.path, controller); err == nil {
		t.Fatal("apply without terminal operation ID was accepted")
	}
}

func TestRequireActiveServiceProvesDurableReceiptAndFreshReadiness(t *testing.T) {
	useDeploymentMetadata(t)
	installed := installedTestGeneration(exactTestGeneration(t))
	active := exactTestActivation(t, installed)
	controller := &recordingDeployer{statuses: []installedStatus{{
		state: deployment.InstalledActive, generation: installed, activation: active, hasReceipt: true,
	}}}
	if err := requireActiveService(t.Context(), controller); err != nil {
		t.Fatal(err)
	}
	if controller.statusCalls != 1 {
		t.Fatalf("status calls = %d, want 1", controller.statusCalls)
	}

	controller.statusCalls = 0
	controller.statuses[0].state = deployment.InstalledPrepared
	if err := requireActiveService(t.Context(), controller); err == nil {
		t.Fatal("prepared service was accepted as active")
	}
}

func TestRequireActiveServiceRejectsStaleRuntimeReadiness(t *testing.T) {
	useDeploymentMetadata(t, func(hooks *productHooks) {
		hooks.observe = func(context.Context, string) (mountproto.RuntimeHealthResponse, error) {
			health := exactHealth(runtimeTarget{buildID: "other-runtime"})
			return health, nil
		}
	})
	installed := installedTestGeneration(exactTestGeneration(t))
	active := exactTestActivation(t, installed)
	controller := &recordingDeployer{statuses: []installedStatus{{
		state: deployment.InstalledActive, generation: installed, activation: active, hasReceipt: true,
	}}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := requireActiveService(ctx, controller); err == nil {
		t.Fatal("stale runtime generation was accepted")
	}
}

func TestUninstallPackagedAppDelegatesSealedRemoval(t *testing.T) {
	useDeploymentMetadata(t)
	installed := installedTestGeneration(exactTestGeneration(t))
	controller := &recordingDeployer{uninstalled: uninstallReceipt{
		id: "uninstall-operation-1", generation: installed,
		runtime: runtimeProof{absent: true, digest: deployment.SHA256{8}},
	}}
	if err := uninstallPackagedApp(t.Context(), controller); err != nil {
		t.Fatal(err)
	}
	if controller.uninstallCalls != 1 || controller.uninstall.current.AppPath != installedAppPath() ||
		controller.uninstall.readiness == nil || controller.uninstall.runtimeQuiesce == nil {
		t.Fatalf("uninstall request = %#v", controller.uninstall)
	}

	controller.uninstalled.runtime = runtimeProof{}
	if err := uninstallPackagedApp(t.Context(), controller); err == nil {
		t.Fatal("uninstall without absence proof was accepted")
	}
}

func TestCandidateApplyAndUninstallSurfaceControllerFailures(t *testing.T) {
	useDeploymentMetadata(t)
	candidate := exactTestGeneration(t)
	want := errors.New("sealed transaction failed")
	controller := &recordingDeployer{attestation: candidate, applyErr: want}
	if _, err := applyPackagedApp(t.Context(), candidate.path, controller); !errors.Is(err, want) {
		t.Fatalf("apply error = %v, want %v", err, want)
	}
	controller.uninstallErr = want
	if err := uninstallPackagedApp(t.Context(), controller); !errors.Is(err, want) {
		t.Fatalf("uninstall error = %v, want %v", err, want)
	}
}
