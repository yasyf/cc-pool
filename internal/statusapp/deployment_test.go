package statusapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/deploy"
	"github.com/yasyf/daemonkit/launchd"
)

const (
	testBundleDigest       = "1111111111111111111111111111111111111111111111111111111111111111"
	testEntitlementsDigest = "2222222222222222222222222222222222222222222222222222222222222222"
)

type recordingDeployer struct {
	config deploy.Config

	installed      deploy.Generation
	installCalls   int
	superseded     deploy.Generation
	supersedeCalls int

	activation     deploy.Activation
	activateErr    error
	activateCalls  int
	uninstallErr   error
	uninstallCalls int
}

func (d *recordingDeployer) Install(_ context.Context, _ deploy.Candidate) (deploy.Generation, error) {
	d.installCalls++
	return d.installed, nil
}

func (d *recordingDeployer) Supersede(_ context.Context, _ deploy.Candidate) (deploy.Generation, error) {
	d.supersedeCalls++
	return d.superseded, nil
}

func (d *recordingDeployer) Activate(context.Context) (deploy.Activation, error) {
	d.activateCalls++
	return d.activation, d.activateErr
}

func (d *recordingDeployer) Uninstall(context.Context) (deploy.Removal, error) {
	d.uninstallCalls++
	return deploy.Removal{}, d.uninstallErr
}

func exactTestGeneration(appPath string) deploy.Generation {
	return deploy.Generation{
		Path: appPath, Version: "0.63.0",
		TeamID: holderbridge.TeamID, SigningIdentifier: holderbridge.BundleID,
		DesignatedRequirement: "designated => anchor apple generic",
		CDHash:                "0123456789abcdef0123456789abcdef01234567",
		EntitlementsDigest:    testEntitlementsDigest,
		BundleDigest:          testBundleDigest,
		FileID:                deploy.FileID{Device: "1", Inode: "2"},
	}
}

func useDeploymentMetadata(t *testing.T, controller *recordingDeployer) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	appPath := filepath.Join(root, "Applications", "CCPoolStatus.app")
	oldInstalledAppPath := installedAppPath
	installedAppPath = func() string { return appPath }
	t.Cleanup(func() { installedAppPath = oldInstalledAppPath })

	oldVersion, oldAppVersion := version.Version, version.StatusAppVersion
	version.Version, version.StatusAppVersion = "v0.63.0", "0.63.0"
	t.Cleanup(func() { version.Version, version.StatusAppVersion = oldVersion, oldAppVersion })

	agent := launchd.Agent{
		Label:                       holderbridge.DeploymentServiceLabel,
		Program:                     filepath.Join(appPath, "Contents", "MacOS", holderbridge.ExecutableName),
		LogPath:                     filepath.Join(root, "holder.log"),
		Env:                         map[string]string{"FUSEKIT_BUILD_ID": version.String()},
		AssociatedBundleIdentifiers: []string{holderbridge.BundleID},
		RestartPolicy:               launchd.RestartAlways,
	}
	oldCandidatePlan, oldInstalledAgents := makeCandidatePlan, makeInstalledAgents
	makeCandidatePlan = func(target, buildID, source string) (candidatePlan, error) {
		if target != appPath || buildID != version.String() {
			return candidatePlan{}, errors.New("candidate plan was not bound to the exact installed generation")
		}
		return candidatePlan{
			candidate: deploy.Candidate{
				Source: source, Version: version.StatusAppVersion, Digest: deploy.SHA256{7},
			},
			agents: []launchd.Agent{agent},
		}, nil
	}
	makeInstalledAgents = func(target, buildID string) ([]launchd.Agent, error) {
		if target != appPath || buildID != version.String() {
			return nil, errors.New("service plan was not bound to the exact installed generation")
		}
		return []launchd.Agent{agent}, nil
	}
	t.Cleanup(func() { makeCandidatePlan, makeInstalledAgents = oldCandidatePlan, oldInstalledAgents })

	oldDeployer := newDeployer
	newDeployer = func(config deploy.Config) (deploymentController, error) {
		controller.config = config
		return controller, nil
	}
	t.Cleanup(func() { newDeployer = oldDeployer })
	return appPath
}

func TestApplyPackagedAppInstallsAndActivatesTheExactCandidate(t *testing.T) {
	controller := &recordingDeployer{}
	appPath := useDeploymentMetadata(t, controller)
	installed := exactTestGeneration(appPath)
	controller.installed = installed
	controller.activation = deploy.Activation{Generation: installed}

	receipt, err := ApplyPackagedApp(t.Context(), "/tmp/packaged/CCPoolStatus.app")
	if err != nil {
		t.Fatal(err)
	}
	if controller.installCalls != 1 || controller.supersedeCalls != 0 || controller.activateCalls != 1 {
		t.Fatalf(
			"calls = install %d, supersede %d, activate %d",
			controller.installCalls, controller.supersedeCalls, controller.activateCalls,
		)
	}
	if receipt.Generation != installed || receipt.Activation.Generation != installed {
		t.Fatalf("receipt = %#v", receipt)
	}
	if err := receipt.Rollback(t.Context()); err != nil {
		t.Fatalf("sealed deploy rollback = %v", err)
	}
}

func TestApplyPackagedAppSupersedesAnOccupiedCanonicalPath(t *testing.T) {
	controller := &recordingDeployer{}
	appPath := useDeploymentMetadata(t, controller)
	if err := os.MkdirAll(appPath, 0o700); err != nil {
		t.Fatal(err)
	}
	installed := exactTestGeneration(appPath)
	controller.superseded = installed
	controller.activation = deploy.Activation{Generation: installed}

	if _, err := ApplyPackagedApp(t.Context(), "/tmp/packaged/CCPoolStatus.app"); err != nil {
		t.Fatal(err)
	}
	if controller.supersedeCalls != 1 || controller.installCalls != 0 {
		t.Fatalf("calls = install %d, supersede %d", controller.installCalls, controller.supersedeCalls)
	}
}

func TestApplyPackagedAppBindsTheExactSealedDeploymentConfig(t *testing.T) {
	controller := &recordingDeployer{}
	appPath := useDeploymentMetadata(t, controller)
	installed := exactTestGeneration(appPath)
	controller.installed = installed
	controller.activation = deploy.Activation{Generation: installed}

	if _, err := ApplyPackagedApp(t.Context(), "/tmp/packaged/CCPoolStatus.app"); err != nil {
		t.Fatal(err)
	}
	config := controller.config
	if config.App != appPath || config.Requirement.TeamID != holderbridge.TeamID ||
		config.Requirement.SigningIdentifier != holderbridge.BundleID ||
		config.Daemon.Label != daemonkit.Label(holderbridge.DeploymentServiceLabel) ||
		len(config.Agents) != 1 || config.Agents[0].Label != holderbridge.DeploymentServiceLabel {
		t.Fatalf("deploy config = %#v", config)
	}
	if err := config.Daemon.ValidateForClient(); err != nil {
		t.Fatalf("daemon identity is not client-valid: %v", err)
	}
}

func TestApplyPackagedAppRejectsAnActivationOffTheLandedGeneration(t *testing.T) {
	controller := &recordingDeployer{}
	appPath := useDeploymentMetadata(t, controller)
	installed := exactTestGeneration(appPath)
	replaced := installed
	replaced.BundleDigest = "3333333333333333333333333333333333333333333333333333333333333333"
	controller.installed = installed
	controller.activation = deploy.Activation{Generation: replaced}

	if _, err := ApplyPackagedApp(t.Context(), "/tmp/packaged/CCPoolStatus.app"); err == nil {
		t.Fatal("an activation of other bytes was accepted")
	}
}

func TestApplyPackagedAppRejectsACandidateOffTheRelease(t *testing.T) {
	controller := &recordingDeployer{}
	useDeploymentMetadata(t, controller)
	bound := makeCandidatePlan
	makeCandidatePlan = func(target, buildID, source string) (candidatePlan, error) {
		plan, err := bound(target, buildID, source)
		if err != nil {
			return candidatePlan{}, err
		}
		plan.candidate.Version = "0.62.0"
		return plan, nil
	}
	if _, err := ApplyPackagedApp(t.Context(), "/tmp/packaged/CCPoolStatus.app"); err == nil {
		t.Fatal("a candidate off the exact release was accepted")
	}
	if controller.installCalls != 0 || controller.supersedeCalls != 0 {
		t.Fatal("an off-release candidate reached the sealed deployment")
	}
}

func TestRequireActiveServiceTiesTheServingPIDToTheInstalledRuntime(t *testing.T) {
	swapDeploymentVar(t, &tenantLaneReady, func(context.Context) (daemonkit.Health, error) {
		return daemonkit.Health{Phase: daemonkit.PhaseReady, PID: 4242}, nil
	})
	swapDeploymentVar(t, &deployInventory, func(...string) (deploy.Survivors, error) {
		return deploy.Survivors{Live: []deploy.LiveProcess{{PID: 4242}}}, nil
	})
	if err := RequireActiveService(t.Context()); err != nil {
		t.Fatal(err)
	}

	swapDeploymentVar(t, &deployInventory, func(...string) (deploy.Survivors, error) {
		return deploy.Survivors{Live: []deploy.LiveProcess{{PID: 7}}}, nil
	})
	if err := RequireActiveService(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "does not run the installed application") {
		t.Fatalf("foreign serving pid = %v, want an installed-runtime refusal", err)
	}

	want := errors.New("daemon never published readiness")
	swapDeploymentVar(t, &tenantLaneReady, func(context.Context) (daemonkit.Health, error) {
		return daemonkit.Health{}, want
	})
	if err := RequireActiveService(t.Context()); !errors.Is(err, want) {
		t.Fatalf("readiness error = %v, want %v", err, want)
	}
}

func TestRequireActiveServiceRefusesAPIDNotPinnedAcrossTheObservation(t *testing.T) {
	swapDeploymentVar(t, &tenantLaneReady, func(context.Context) (daemonkit.Health, error) {
		return daemonkit.Health{Phase: daemonkit.PhaseReady, PID: 4242}, nil
	})
	tests := []struct {
		name        string
		inventories []deploy.Survivors
	}{
		{
			"the pid crossed the observation as two instances",
			[]deploy.Survivors{
				{Live: []deploy.LiveProcess{{PID: 4242, Start: 1, Boot: 1}}},
				{Live: []deploy.LiveProcess{{PID: 4242, Start: 2, Boot: 1}}},
			},
		},
		{
			"the pid kept arriving as a fresh instance across the retried bracket",
			[]deploy.Survivors{
				{},
				{Live: []deploy.LiveProcess{{PID: 4242, Start: 2, Boot: 1}}},
				{Live: []deploy.LiveProcess{{PID: 4242, Start: 3, Boot: 1}}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			swapDeploymentVar(t, &deployInventory, func(...string) (deploy.Survivors, error) {
				next := tt.inventories[calls]
				calls++
				return next, nil
			})
			if err := RequireActiveService(t.Context()); err == nil ||
				!strings.Contains(err.Error(), "does not run the installed application") {
				t.Fatalf("unpinned pid = %v, want an installed-runtime refusal", err)
			}
			if calls != len(tt.inventories) {
				t.Fatalf("inventory bracket ran %d calls, want %d", calls, len(tt.inventories))
			}
		})
	}
}

func TestRequireActiveServiceRetriesTheBracketForAHolderStartedMidObservation(t *testing.T) {
	observations := 0
	swapDeploymentVar(t, &tenantLaneReady, func(context.Context) (daemonkit.Health, error) {
		observations++
		return daemonkit.Health{Phase: daemonkit.PhaseReady, PID: 4242}, nil
	})
	inventories := []deploy.Survivors{
		{},
		{Live: []deploy.LiveProcess{{PID: 4242, Start: 2, Boot: 1}}},
		{Live: []deploy.LiveProcess{{PID: 4242, Start: 2, Boot: 1}}},
	}
	calls := 0
	swapDeploymentVar(t, &deployInventory, func(...string) (deploy.Survivors, error) {
		next := inventories[calls]
		calls++
		return next, nil
	})
	if err := RequireActiveService(t.Context()); err != nil {
		t.Fatalf("mid-observation holder start = %v, want a retried pass", err)
	}
	if calls != 3 || observations != 2 {
		t.Fatalf("bracket retry ran %d inventories and %d observations, want 3 and 2", calls, observations)
	}
}

func swapDeploymentVar[T any](t *testing.T, target *T, replacement T) {
	t.Helper()
	previous := *target
	*target = replacement
	t.Cleanup(func() { *target = previous })
}

func TestUninstallPackagedAppDelegatesTheSealedRemoval(t *testing.T) {
	controller := &recordingDeployer{}
	useDeploymentMetadata(t, controller)

	if err := UninstallPackagedApp(t.Context()); err != nil {
		t.Fatal(err)
	}
	if controller.uninstallCalls != 1 {
		t.Fatalf("uninstall calls = %d, want 1", controller.uninstallCalls)
	}

	want := errors.New("live processes remain")
	controller.uninstallErr = want
	if err := UninstallPackagedApp(t.Context()); !errors.Is(err, want) {
		t.Fatalf("uninstall error = %v, want %v", err, want)
	}
}

func TestSameApplicationBytesIgnoresOnlyLocation(t *testing.T) {
	base := exactTestGeneration("/Applications/CCPoolStatus.app")
	moved := base
	moved.Path = "/tmp/staged/CCPoolStatus.app"
	moved.FileID = deploy.FileID{Device: "9", Inode: "9"}
	rebuilt := base
	rebuilt.CDHash = "89abcdef0123456789abcdef0123456789abcdef"
	other := base
	other.BundleDigest = "3333333333333333333333333333333333333333333333333333333333333333"

	tests := []struct {
		name  string
		right deploy.Generation
		want  bool
	}{
		{"same bytes at another path and inode", moved, true},
		{"another code directory", rebuilt, false},
		{"another bundle tree", other, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sameApplicationBytes(base, tt.right); got != tt.want {
				t.Fatalf("sameApplicationBytes = %t, want %t", got, tt.want)
			}
		})
	}
}
