package statusapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/deploy"
	"github.com/yasyf/daemonkit/launchd"
)

const (
	testBundleDigest       = "1111111111111111111111111111111111111111111111111111111111111111"
	testEntitlementsDigest = "2222222222222222222222222222222222222222222222222222222222222222"
	testServingPID         = 4242
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
	resetErr       error
	resetCalls     int
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

func (d *recordingDeployer) Reset(context.Context) error {
	d.resetCalls++
	return d.resetErr
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

	swapDeploymentVar(t, &tenantLaneReady, func(context.Context) (daemonkit.Health, error) {
		return daemonkit.Health{Phase: daemonkit.PhaseReady, PID: testServingPID}, nil
	})
	swapDeploymentVar(t, &deployInventory, func(...string) (deploy.Survivors, error) {
		return deploy.Survivors{Live: []deploy.LiveProcess{{PID: testServingPID, Start: 1, Boot: 1}}}, nil
	})
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

// TestApplyPackagedAppProvesTheTenantLaneDownstreamOfActivation pins the
// install's proof to the lane the app serves last: the primary label publishes
// readiness before the source fleet is published and the tenant lane is
// started, so an activation against it alone can succeed on a startup that
// then unwinds.
func TestApplyPackagedAppProvesTheTenantLaneDownstreamOfActivation(t *testing.T) {
	tests := []struct {
		name        string
		laneErr     error
		wantRefusal string
	}{
		{"a tenant lane that answers lands the receipt", nil, ""},
		{
			"a tenant lane that never answers refuses the install",
			errors.New("tenant lane never admitted"),
			"require the live FuseKit runtime",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := &recordingDeployer{}
			appPath := useDeploymentMetadata(t, controller)
			installed := exactTestGeneration(appPath)
			controller.installed = installed
			controller.activation = deploy.Activation{Generation: installed}
			observations := 0
			swapDeploymentVar(t, &tenantLaneReady, func(context.Context) (daemonkit.Health, error) {
				observations++
				if tt.laneErr != nil {
					return daemonkit.Health{}, tt.laneErr
				}
				return daemonkit.Health{Phase: daemonkit.PhaseReady, PID: testServingPID}, nil
			})

			_, err := ApplyPackagedApp(t.Context(), "/tmp/packaged/CCPoolStatus.app")
			switch {
			case tt.wantRefusal == "" && err != nil:
				t.Fatalf("a proved tenant lane = %v, want the install receipt", err)
			case tt.wantRefusal != "" && (err == nil || !strings.Contains(err.Error(), tt.wantRefusal)):
				t.Fatalf("an unproved tenant lane = %v, want a refusal carrying %q", err, tt.wantRefusal)
			}
			if controller.activateCalls != 1 {
				t.Fatalf("activate calls = %d, want the tenant proof downstream of one activation", controller.activateCalls)
			}
			if observations == 0 {
				t.Fatal("the install cut its receipt without ever observing the tenant lane")
			}
		})
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
	foreignCtx, cancelForeign := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancelForeign()
	if err := RequireActiveService(foreignCtx); err == nil ||
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

func TestRequireActiveServiceRefusesAnUnpinnableServerAtTheDeadline(t *testing.T) {
	swapDeploymentVar(t, &tenantLaneReady, func(context.Context) (daemonkit.Health, error) {
		return daemonkit.Health{Phase: daemonkit.PhaseReady, PID: 4242}, nil
	})
	swapDeploymentVar(t, &deployInventory, func(...string) (deploy.Survivors, error) {
		return deploy.Survivors{}, nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := RequireActiveService(ctx); err == nil ||
		!strings.Contains(err.Error(), "does not run the installed application") {
		t.Fatalf("unpinnable server = %v, want an installed-runtime refusal at the deadline", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("caller-stated deadline was overridden: refusal took %v", elapsed)
	}
}

func TestRequireActiveServiceBoundsADeadlineFreeCaller(t *testing.T) {
	swapDeploymentVar(t, &readinessBudget, func() time.Duration { return 50 * time.Millisecond })
	swapDeploymentVar(t, &tenantLaneReady, func(context.Context) (daemonkit.Health, error) {
		return daemonkit.Health{Phase: daemonkit.PhaseReady, PID: 4242}, nil
	})
	swapDeploymentVar(t, &deployInventory, func(...string) (deploy.Survivors, error) {
		return deploy.Survivors{}, nil
	})
	started := time.Now()
	if err := RequireActiveService(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "does not run the installed application") {
		t.Fatalf("deadline-free unpinnable server = %v, want a budget-bounded refusal", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("package readiness budget did not bound the retry: refusal took %v", elapsed)
	}
}

func TestRequireActiveServiceRetriesTheBracketForAHolderReplacedMidObservation(t *testing.T) {
	pids := []int{42, 7}
	observations := 0
	swapDeploymentVar(t, &tenantLaneReady, func(context.Context) (daemonkit.Health, error) {
		pid := pids[min(observations, len(pids)-1)]
		observations++
		return daemonkit.Health{Phase: daemonkit.PhaseReady, PID: pid}, nil
	})
	inventories := []deploy.Survivors{
		{Live: []deploy.LiveProcess{{PID: 42, Start: 1, Boot: 1}}},
		{Live: []deploy.LiveProcess{{PID: 7, Start: 9, Boot: 1}}},
		{Live: []deploy.LiveProcess{{PID: 7, Start: 9, Boot: 1}}},
	}
	calls := 0
	swapDeploymentVar(t, &deployInventory, func(...string) (deploy.Survivors, error) {
		next := inventories[calls]
		calls++
		return next, nil
	})
	if err := RequireActiveService(t.Context()); err != nil {
		t.Fatalf("replaced holder = %v, want a retried pass", err)
	}
	if calls != 3 || observations != 2 {
		t.Fatalf("bracket retry ran %d inventories and %d observations, want 3 and 2", calls, observations)
	}
}

func TestRequireActiveServiceRetriesTheBracketForAPIDReusedByTheReplacement(t *testing.T) {
	swapDeploymentVar(t, &tenantLaneReady, func(context.Context) (daemonkit.Health, error) {
		return daemonkit.Health{Phase: daemonkit.PhaseReady, PID: 42}, nil
	})
	inventories := []deploy.Survivors{
		{Live: []deploy.LiveProcess{{PID: 42, Start: 1, Boot: 1}}},
		{Live: []deploy.LiveProcess{{PID: 42, Start: 2, Boot: 1}}},
		{Live: []deploy.LiveProcess{{PID: 42, Start: 2, Boot: 1}}},
	}
	calls := 0
	swapDeploymentVar(t, &deployInventory, func(...string) (deploy.Survivors, error) {
		next := inventories[calls]
		calls++
		return next, nil
	})
	if err := RequireActiveService(t.Context()); err != nil {
		t.Fatalf("reused pid = %v, want the replacement pinned on the retried bracket", err)
	}
	if calls != 3 {
		t.Fatalf("bracket retry ran %d inventories, want 3", calls)
	}
}

func TestObserverTenantDaemonRequiresTheInstalledSigningIdentity(t *testing.T) {
	observer := observerTenantDaemon()
	if !reflect.DeepEqual(observer.Trust.Serving, daemonkit.ServingSigned(statusAppRequirement())) {
		t.Fatalf("observer serving trust = %+v, want the installed application's signing requirement", observer.Trust.Serving)
	}
	shared := tenantfs.Daemon()
	if observer.Label != shared.Label {
		t.Fatalf("observer label = %q diverged from the shared lane %q", observer.Label, shared.Label)
	}
	if !reflect.DeepEqual(shared.Trust.Serving, daemonkit.ServingSameUser()) {
		t.Fatalf("shared serving trust = %+v; the observer requirement must not leak into the shared identity", shared.Trust.Serving)
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

// TestRequireActiveServiceSpendsAStatedDeadlineOnMoreObservations pins the
// observation timeout as a cap on one window rather than a ceiling on the
// whole wait: a holder that only publishes readiness after the first window
// expires is still accepted when the caller's own deadline affords a second
// bracket, and refused when it does not.
func TestRequireActiveServiceSpendsAStatedDeadlineOnMoreObservations(t *testing.T) {
	const observationWindow = 20 * time.Millisecond
	tests := []struct {
		name         string
		deadline     time.Duration
		wantRefusal  string
		observations int
	}{
		{"a deadline past one window buys a second observation", 5 * time.Second, "", 2},
		{"a deadline inside one window stops at the first", observationWindow / 2, "require the live FuseKit runtime", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observations := 0
			swapDeploymentVar(t, &tenantLaneReady, func(ctx context.Context) (daemonkit.Health, error) {
				observations++
				window, cancel := context.WithTimeout(ctx, observationWindow)
				defer cancel()
				if observations > 1 {
					return daemonkit.Health{Phase: daemonkit.PhaseReady, PID: 4242}, nil
				}
				<-window.Done()
				return daemonkit.Health{}, window.Err()
			})
			swapDeploymentVar(t, &deployInventory, func(...string) (deploy.Survivors, error) {
				return deploy.Survivors{Live: []deploy.LiveProcess{{PID: 4242, Start: 2, Boot: 1}}}, nil
			})
			ctx, cancel := context.WithTimeout(t.Context(), tt.deadline)
			defer cancel()

			err := RequireActiveService(ctx)
			if tt.wantRefusal == "" && err != nil {
				t.Fatalf("holder ready on the second observation = %v, want the stated deadline to buy it", err)
			}
			if tt.wantRefusal != "" && (err == nil || !strings.Contains(err.Error(), tt.wantRefusal)) {
				t.Fatalf("exhausted deadline = %v, want a refusal carrying %q", err, tt.wantRefusal)
			}
			if observations != tt.observations {
				t.Fatalf("observations = %d, want %d", observations, tt.observations)
			}
		})
	}
}

func TestReadinessBudgetAffordsTwoCompleteObservations(t *testing.T) {
	floor := 2*holderbridge.ReadinessContract().ObservationTimeout() + bracketRetryCadence
	if budget := readinessBudget(); budget <= floor {
		t.Fatalf(
			"readiness budget %v does not exceed two observation windows paced by the retry cadence (%v)",
			budget, floor,
		)
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

func TestResetPackagedAppDelegatesTheAgentRetirement(t *testing.T) {
	controller := &recordingDeployer{}
	useDeploymentMetadata(t, controller)

	if err := ResetPackagedApp(t.Context()); err != nil {
		t.Fatal(err)
	}
	if controller.resetCalls != 1 || controller.uninstallCalls != 0 {
		t.Fatalf("calls = reset %d, uninstall %d", controller.resetCalls, controller.uninstallCalls)
	}

	want := errors.New("no control lane can stop this daemon")
	controller.resetErr = want
	if err := ResetPackagedApp(t.Context()); !errors.Is(err, want) {
		t.Fatalf("reset error = %v, want %v", err, want)
	}
}

func TestResetPackagedAppAcceptsAWedgedInstalledApplication(t *testing.T) {
	tests := []struct {
		name    string
		install func(t *testing.T, appPath string)
	}{
		{"absent application", func(*testing.T, string) {}},
		{"non-executable runtime", func(t *testing.T, appPath string) {
			macos := filepath.Join(appPath, "Contents", "MacOS")
			if err := os.MkdirAll(macos, 0o750); err != nil {
				t.Fatal(err)
			}
			executable := filepath.Join(macos, holderbridge.ExecutableName)
			if err := os.WriteFile(executable, []byte("not a runtime"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			appPath := filepath.Join(root, "Applications", "CCPoolStatus.app")
			tt.install(t, appPath)
			swapDeploymentVar(t, &installedAppPath, func() string { return appPath })
			controller := &recordingDeployer{}
			swapDeploymentVar(t, &newDeployer, func(config deploy.Config) (deploymentController, error) {
				controller.config = config
				return controller, nil
			})

			if err := ResetPackagedApp(t.Context()); err != nil {
				t.Fatal(err)
			}
			if controller.resetCalls != 1 {
				t.Fatalf("reset calls = %d, want 1", controller.resetCalls)
			}
			agents := controller.config.Agents
			program := filepath.Join(appPath, "Contents", "MacOS", holderbridge.ExecutableName)
			if len(agents) != 1 || agents[0].Label != holderbridge.DeploymentServiceLabel ||
				agents[0].Program != program {
				t.Fatalf("reset deployment agents = %+v", agents)
			}
		})
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
