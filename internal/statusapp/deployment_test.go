package statusapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit/deployment"
	"github.com/yasyf/daemonkit/service"
)

type recordingDeployer struct {
	receipt         deploymentReceipt
	err             error
	calls           int
	config          deployment.Config
	deactivateCalls int
	deactivate      deployment.DeactivateConfig
	deactivation    deactivationResult
}

func (d *recordingDeployer) Deactivate(
	_ context.Context,
	config deployment.DeactivateConfig,
) (deactivationResult, error) {
	d.deactivateCalls++
	d.deactivate = config
	return d.deactivation, d.err
}

func (d *recordingDeployer) Deploy(
	_ context.Context,
	config deployment.Config,
) (deploymentReceipt, error) {
	d.calls++
	d.config = config
	return d.receipt, d.err
}

func useDeploymentHooks(t *testing.T, plan service.Plan) {
	t.Helper()
	original := makeProductHooks
	t.Cleanup(func() { makeProductHooks = original })
	makeProductHooks = func(buildID string, policyDigest deployment.SHA256) productHooks {
		hooks := newProductHooks(buildID, policyDigest)
		hooks.servicePlan = func(deployment.Operation) (service.Plan, error) { return plan, nil }
		hooks.servicePlanBuild = func(deployment.Operation, string) (service.Plan, error) { return plan, nil }
		return hooks
	}
}

func exactDeploymentReceipt(
	t *testing.T,
	release deployment.Release,
	state deployment.DeploymentState,
) (deploymentReceipt, service.Plan) {
	t.Helper()
	requirement, err := statusAppCodeIdentity().DRString()
	if err != nil {
		t.Fatal(err)
	}
	generation := deployment.CanonicalGeneration{
		Path: pool.WidgetAppPath(), Release: release,
		DesignatedRequirement: requirement, CDHash: "0123456789abcdef",
		BundleDigest: deployment.SHA256{2}, Device: "1", Inode: "2",
	}
	executable := holderExecutablePath(generation.Path)
	if err := os.MkdirAll(filepath.Dir(executable), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(executable, []byte("test helper"), 0o700); err != nil {
		t.Fatal(err)
	}
	operation := deployment.Operation{ID: "00112233445566778899aabbccddeeff", Generation: generation}
	plan := testServicePlan(t, operation, version.String())
	emptyPlan, err := service.NewPlan(nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt := deploymentReceipt{
		operationID: operation.ID, state: state, current: generation, hasCurrent: true,
		plan: emptyPlan, activationPlan: plan,
	}
	if state == deployment.DeploymentActive {
		receipt.plan = plan
	}
	return receipt, plan
}

func setCanonicalTestHome(t *testing.T) string {
	t.Helper()
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	return home
}

func exactTestRelease(t *testing.T) deployment.Release {
	t.Helper()
	digest, err := deployment.ParseSHA256(strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	return deployment.Release{
		Version: "0.63.0", URL: "https://example.invalid/CCPoolStatus.zip", SHA256: digest,
	}
}

func TestInstallServiceDeploysExactGeneration(t *testing.T) {
	setRelease(t, "v0.63.0", "0.63.0", strings.Repeat("ab", 32))
	home := setCanonicalTestHome(t)
	release, err := release()
	if err != nil {
		t.Fatal(err)
	}
	active, plan := exactDeploymentReceipt(t, release, deployment.DeploymentActive)
	useDeploymentHooks(t, plan)
	controller := &recordingDeployer{receipt: active}
	receipt, err := installService(t.Context(), controller)
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.hasCurrent || receipt.current.Path != pool.WidgetAppPath() || controller.calls != 1 {
		t.Fatalf("receipt/calls = %#v/%d", receipt, controller.calls)
	}
	config := controller.config
	if config.Dir != pool.WidgetAppDir() || config.AppName != appName || config.Release != release ||
		config.Identity.TeamID != holderbridge.TeamID || config.Identity.SigningIdentifier != holderbridge.BundleID ||
		config.ConsumerBuild == "" || config.PolicyDigest == (deployment.SHA256{}) ||
		config.RuntimeQuiesce == nil || config.PostInstallProof == nil || config.PriorAppRestoreProof == nil ||
		config.BuildPlan == nil || config.Readiness == nil {
		t.Fatalf("deployment config = %#v", config)
	}
	if got, want := receipt.current.Path, filepath.Join(home, "Applications", "CCPoolStatus.app"); got != want {
		t.Fatalf("app path = %q, want %q", got, want)
	}
}

func TestInstallServiceRequiresActiveExactReceipt(t *testing.T) {
	setRelease(t, "v0.63.0", "0.63.0", strings.Repeat("ab", 32))
	setCanonicalTestHome(t)
	release, err := release()
	if err != nil {
		t.Fatal(err)
	}
	active, plan := exactDeploymentReceipt(t, release, deployment.DeploymentActive)
	useDeploymentHooks(t, plan)
	for _, receipt := range []deploymentReceipt{
		{},
		{state: deployment.DeploymentRecoveryRequired},
		{state: deployment.DeploymentActive},
		{state: deployment.DeploymentActive, current: deployment.CanonicalGeneration{Path: "/wrong", Release: release}, hasCurrent: true},
		{state: deployment.DeploymentActive, current: active.current, hasCurrent: true, operationID: active.operationID},
	} {
		if _, err := installService(t.Context(), &recordingDeployer{receipt: receipt}); err == nil {
			t.Fatalf("accepted receipt %#v", receipt)
		}
	}
}

func TestInstallServiceReturnsDeploymentFailure(t *testing.T) {
	setRelease(t, "v0.63.0", "0.63.0", strings.Repeat("ab", 32))
	setCanonicalTestHome(t)
	want := errors.New("deployment failed")
	if _, err := installService(t.Context(), &recordingDeployer{err: want}); !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func TestInstallServiceValidatesReleaseBeforeCreatingApplications(t *testing.T) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	setRelease(t, "v0.63.0", "0.62.0", strings.Repeat("ab", 32))
	controller := &recordingDeployer{}
	if _, err := installService(t.Context(), controller); err == nil {
		t.Fatal("accepted mismatched release")
	}
	if controller.calls != 0 {
		t.Fatalf("deploy calls = %d, want 0", controller.calls)
	}
	if _, err := os.Lstat(filepath.Join(home, "Applications")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid release changed user Applications directory: %v", err)
	}
}

func TestDeactivateServiceRetiresExactDeploymentWithoutRemovingApplication(t *testing.T) {
	setCanonicalTestHome(t)
	inactive, plan := exactDeploymentReceipt(t, exactTestRelease(t), deployment.DeploymentInactive)
	useDeploymentHooks(t, plan)
	controller := &recordingDeployer{deactivation: deactivationResult{
		state: deployment.DeactivationInactive, receipt: inactive, hasReceipt: true,
	}}
	result, err := deactivateService(t.Context(), controller)
	if err != nil {
		t.Fatal(err)
	}
	if result.state != deployment.DeactivationInactive || !result.hasReceipt ||
		!result.receipt.hasCurrent || result.receipt.current.Path != pool.WidgetAppPath() ||
		controller.deactivateCalls != 1 {
		t.Fatalf("result/calls = %#v/%d", result, controller.deactivateCalls)
	}
	config := controller.deactivate
	if config.Dir != pool.WidgetAppDir() || config.AppName != appName ||
		config.Identity.TeamID != holderbridge.TeamID || config.Identity.SigningIdentifier != holderbridge.BundleID ||
		config.ConsumerBuild == "" || config.PolicyDigest == (deployment.SHA256{}) ||
		config.RuntimeQuiesce == nil || config.Readiness == nil {
		t.Fatalf("deactivation config = %#v", config)
	}
}

func TestDeactivateServiceAcceptsAbsentAndRequiresExactInactiveReceipt(t *testing.T) {
	setCanonicalTestHome(t)
	inactive, plan := exactDeploymentReceipt(t, exactTestRelease(t), deployment.DeploymentInactive)
	useDeploymentHooks(t, plan)
	absent, err := deactivateService(t.Context(), &recordingDeployer{deactivation: deactivationResult{
		state: deployment.DeactivationAbsent,
	}})
	if err != nil || absent.state != deployment.DeactivationAbsent || absent.hasReceipt {
		t.Fatalf("absent deactivation = %#v, %v", absent, err)
	}
	for _, result := range []deactivationResult{
		{state: deployment.DeactivationAbsent, hasReceipt: true},
		{state: deployment.DeactivationInactive},
		{state: deployment.DeactivationInactive, hasReceipt: true, receipt: deploymentReceipt{state: deployment.DeploymentActive, current: deployment.CanonicalGeneration{Path: pool.WidgetAppPath()}, hasCurrent: true}},
		{state: deployment.DeactivationInactive, hasReceipt: true, receipt: deploymentReceipt{state: deployment.DeploymentInactive}},
		{state: deployment.DeactivationInactive, hasReceipt: true, receipt: deploymentReceipt{state: deployment.DeploymentInactive, current: deployment.CanonicalGeneration{Path: "/wrong"}, hasCurrent: true}},
		{state: deployment.DeactivationInactive, hasReceipt: true, receipt: deploymentReceipt{state: deployment.DeploymentInactive, current: inactive.current, hasCurrent: true, operationID: inactive.operationID}},
	} {
		if _, err := deactivateService(t.Context(), &recordingDeployer{deactivation: result}); err == nil {
			t.Fatalf("accepted result %#v", result)
		}
	}
	invalidPlan := inactive
	invalidPlan.plan = service.Plan{}
	if _, err := deactivateService(t.Context(), &recordingDeployer{deactivation: deactivationResult{
		state: deployment.DeactivationInactive, receipt: invalidPlan, hasReceipt: true,
	}}); err == nil {
		t.Fatal("accepted zero-value inactive service plan")
	}
}

func TestDeactivateServiceFreshMachineIsZeroWrite(t *testing.T) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	result, err := deactivateService(t.Context(), daemonkitDeploymentController{inner: deployment.New()})
	if err != nil {
		t.Fatal(err)
	}
	if result.state != deployment.DeactivationAbsent || result.hasReceipt {
		t.Fatalf("deactivation result = %#v", result)
	}
	if _, err := os.Lstat(filepath.Join(home, "Applications")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fresh deactivation changed user Applications directory: %v", err)
	}
}

func TestDeactivateServiceReturnsDeploymentFailure(t *testing.T) {
	setCanonicalTestHome(t)
	want := errors.New("deactivation failed")
	if _, err := deactivateService(t.Context(), &recordingDeployer{err: want}); !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}
