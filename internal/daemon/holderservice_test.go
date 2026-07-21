package daemon

import (
	"context"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/version"
	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/wire/lifeproto"
	"github.com/yasyf/fusekit/holder"
	"github.com/yasyf/fusekit/mountproto"
)

func TestValidateHolderHealthRequiresExactReadyLifecycle(t *testing.T) {
	healthy := dkdaemon.Health{
		Build: version.String(), Protocol: lifeproto.Version, PID: 42,
		State: dkdaemon.StateHealthy,
	}
	if err := validateHolderHealth(healthy); err != nil {
		t.Fatalf("healthy lifecycle: %v", err)
	}
	for _, test := range []struct {
		name string
		edit func(*dkdaemon.Health)
		want string
	}{
		{name: "build", edit: func(h *dkdaemon.Health) { h.Build = "v0.0.0" }, want: "identity is not exact"},
		{name: "protocol", edit: func(h *dkdaemon.Health) { h.Protocol++ }, want: "identity is not exact"},
		{name: "pid", edit: func(h *dkdaemon.Health) { h.PID = 0 }, want: "identity is not exact"},
		{name: "state", edit: func(h *dkdaemon.Health) { h.State = dkdaemon.StateDegraded }, want: "is not ready"},
		{name: "draining", edit: func(h *dkdaemon.Health) { h.Draining = true }, want: "is not ready"},
		{name: "busy", edit: func(h *dkdaemon.Health) { h.Busy = true }, want: "is not ready"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := healthy
			test.edit(&got)
			if err := validateHolderHealth(got); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateHolderHealth(%#v) = %v, want %q", got, err, test.want)
			}
		})
	}
}

func TestValidateHolderRuntimeHealthRequiresExactNativeMountProof(t *testing.T) {
	healthy := mountproto.RuntimeHealthResponse{
		Protocol:             mountproto.Version,
		Code:                 mountproto.ErrorCodeOk,
		ActivationGeneration: "activation-7",
		NativePhase:          mountproto.NativePhaseLive,
		NativeMount: &mountproto.NativeMountProof{
			PresentationRoot: pool.FuseKitPresentationRoot(),
			Filesystem:       mountproto.NativeMountFilesystem,
			Source:           mountproto.NativeMountSource,
			CatalogEpoch:     7,
		},
	}
	if err := validateHolderRuntimeHealth(healthy); err != nil {
		t.Fatalf("healthy runtime: %v", err)
	}
	for _, test := range []struct {
		name string
		edit func(*mountproto.RuntimeHealthResponse)
		want string
	}{
		{name: "activation", edit: func(h *mountproto.RuntimeHealthResponse) { h.ActivationGeneration = "" }, want: "activation generation is empty"},
		{name: "phase", edit: func(h *mountproto.RuntimeHealthResponse) { h.NativePhase = mountproto.NativePhaseStarting }, want: "presentation is not ready"},
		{name: "proof", edit: func(h *mountproto.RuntimeHealthResponse) { h.NativeMount = nil }, want: "presentation is not ready"},
		{name: "root", edit: func(h *mountproto.RuntimeHealthResponse) { h.NativeMount.PresentationRoot += "-wrong" }, want: "proof is not exact"},
		{name: "filesystem", edit: func(h *mountproto.RuntimeHealthResponse) { h.NativeMount.Filesystem = "fusefs" }, want: "proof is not exact"},
		{name: "source", edit: func(h *mountproto.RuntimeHealthResponse) { h.NativeMount.Source = "fuse-t:/wrong" }, want: "proof is not exact"},
		{name: "epoch", edit: func(h *mountproto.RuntimeHealthResponse) { h.NativeMount.CatalogEpoch = 0 }, want: "proof is not exact"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := healthy
			proof := *healthy.NativeMount
			got.NativeMount = &proof
			test.edit(&got)
			if err := validateHolderRuntimeHealth(got); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateHolderRuntimeHealth(%#v) = %v, want %q", got, err, test.want)
			}
		})
	}
}

func TestHolderReadyRequiresLifecycleBeforeRuntimeHealth(t *testing.T) {
	originalLifecycle, originalRuntime := holderLifecycleReady, holderRuntimeReady
	t.Cleanup(func() { holderLifecycleReady, holderRuntimeReady = originalLifecycle, originalRuntime })
	var order []string
	holderLifecycleReady = func(context.Context, string) error {
		order = append(order, "lifecycle")
		return nil
	}
	holderRuntimeReady = func(context.Context, string) error {
		order = append(order, "runtime")
		return nil
	}
	if err := holderReady(t.Context(), "/tmp/fusekit.sock"); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(order, []string{"lifecycle", "runtime"}) {
		t.Fatalf("readiness order = %v", order)
	}

	want := errors.New("lifecycle not ready")
	order = nil
	holderLifecycleReady = func(context.Context, string) error {
		order = append(order, "lifecycle")
		return want
	}
	holderRuntimeReady = func(context.Context, string) error {
		t.Fatal("runtime health queried before lifecycle readiness")
		return nil
	}
	if err := holderReady(t.Context(), "/tmp/fusekit.sock"); !errors.Is(err, want) {
		t.Fatalf("holderReady error = %v, want %v", err, want)
	}
	if !slices.Equal(order, []string{"lifecycle"}) {
		t.Fatalf("failed readiness order = %v", order)
	}
}

func TestHolderDeploymentPlanDerivesFixedApplicationAgentAndOpaqueTrust(t *testing.T) {
	if application := holderApplication(); application.AppPath != pool.WidgetAppPath() {
		t.Fatalf("production application path = %q", application.AppPath)
	}
	wantApplication := useTestHolderApplication(t)
	plan, err := HolderDeploymentPlan()
	if err != nil {
		t.Fatal(err)
	}
	application := plan.Application()
	if application != wantApplication || application.BundleID != holderbridge.BundleID ||
		application.TeamID != holderbridge.TeamID {
		t.Fatalf("application = %#v", application)
	}
	for role, executable := range map[string]holder.SignedExecutable{
		"broker":  application.Broker,
		"runtime": application.Runtime,
	} {
		if executable.ExecutableName != holderbridge.ExecutableName ||
			executable.SigningIdentifier != holderbridge.BundleID {
			t.Fatalf("%s executable = %#v", role, executable)
		}
	}
	broker, ok := plan.Broker()
	if !ok || broker.PolicyDigest != holderEntitlementPolicyDigest ||
		plan.RuntimePolicyDigest() != holderEntitlementPolicyDigest || !plan.SourceCapable() {
		t.Fatalf(
			"deployment trust/capability = broker %#v runtime %x source %t",
			broker, plan.RuntimePolicyDigest(), plan.SourceCapable(),
		)
	}
	runtimeSpec := holderbridge.RuntimePlanSpec(
		wantApplication.AppPath, pool.FuseKitRuntimeDir(), pool.FuseKitPresentationRoot(),
		plan.BuildID(), nil,
	)
	if runtimeSpec.Application != application || !runtimeSpec.SourceCapable ||
		runtimeSpec.PresentationRoot != pool.AccountsDir() ||
		runtimeSpec.BrokerPolicy.RequiredAppGroup != holderbridge.AppGroup ||
		runtimeSpec.RuntimePolicy.RequiredAppGroup != holderbridge.AppGroup {
		t.Fatal("signed runtime contract differs from daemon deployment identity")
	}
	agent := plan.Agent()
	if agent.Label != holderbridge.BundleID+".fusekit" ||
		agent.Program != filepath.Join(wantApplication.AppPath, "Contents", "MacOS", holderbridge.ExecutableName) ||
		len(agent.Args) != 0 || agent.Env["FUSEKIT_BUILD_ID"] != plan.BuildID() ||
		agent.LogPath != filepath.Join(pool.FuseKitRuntimeDir(), "holder.log") ||
		agent.RestartPolicy != service.RestartAlways ||
		agent.LimitLoadToSessionType != service.SessionTypeAqua ||
		!slices.Equal(agent.AssociatedBundleIdentifiers, []string{holderbridge.BundleID}) {
		t.Fatalf("agent = %#v", agent)
	}
	if plan.Paths().Directory != pool.FuseKitRuntimeDir() ||
		plan.Paths().Socket != pool.FuseKitSocketPath() ||
		plan.Paths().PresentationRoot != pool.AccountsDir() ||
		plan.Paths().ProcessStore != filepath.Join(pool.FuseKitRuntimeDir(), "processes.db") {
		t.Fatalf("runtime paths = %#v", plan.Paths())
	}
	for _, id := range []int{1, 7, 20} {
		if parent := filepath.Dir(pool.AccountPresentationDir(id)); parent != plan.Paths().PresentationRoot {
			t.Fatalf("account %d presentation parent = %q, want %q", id, parent, plan.Paths().PresentationRoot)
		}
	}
}

func TestEnsureHolderServiceConvergesExactAgentAndWaitsForReadiness(t *testing.T) {
	useTestHolderApplication(t)
	originalOpen, originalReady, originalPresent := holderControllerOpen, holderReady, holderServicePresent
	t.Cleanup(func() {
		holderControllerOpen, holderReady, holderServicePresent = originalOpen, originalReady, originalPresent
	})
	holderServicePresent = func(service.Agent) (bool, error) { return false, nil }
	var order []string
	controller := &testHolderServiceController{
		converge: func(_ context.Context, agents []service.Agent) error {
			order = append(order, "converge")
			if !reflect.DeepEqual(agents, []service.Agent{mustHolderDeploymentPlan(t).Agent()}) {
				t.Fatalf("desired agents = %#v", agents)
			}
			return nil
		},
		close: func(context.Context) error {
			order = append(order, "close")
			return nil
		},
	}
	holderControllerOpen = func(
		_ context.Context,
		config service.ControllerConfig,
	) (holderServiceController, error) {
		order = append(order, "open")
		if config != wantHolderControllerConfig() {
			t.Fatalf("controller config = %#v", config)
		}
		return controller, nil
	}
	readyCalls := 0
	holderReady = func(_ context.Context, socket string) error {
		readyCalls++
		if socket != pool.FuseKitSocketPath() {
			t.Fatalf("readiness socket = %q", socket)
		}
		if readyCalls == 1 {
			return errors.New("socket not ready")
		}
		return nil
	}
	if err := EnsureHolderService(t.Context()); err != nil {
		t.Fatal(err)
	}
	if readyCalls != 2 {
		t.Fatalf("ready calls = %d, want 2", readyCalls)
	}
	if !slices.Equal(order, []string{"open", "converge", "close"}) {
		t.Fatalf("controller order = %v", order)
	}
}

func TestEnsureHolderServiceRefusesMissingAppBeforeControllerEffects(t *testing.T) {
	useTestHolderApplication(t)
	originalStat, originalOpen, originalReady := holderAppStat, holderControllerOpen, holderReady
	t.Cleanup(func() {
		holderAppStat, holderControllerOpen, holderReady = originalStat, originalOpen, originalReady
	})
	holderAppStat = func(string) (os.FileInfo, error) { return nil, os.ErrNotExist }
	holderControllerOpen = func(
		context.Context,
		service.ControllerConfig,
	) (holderServiceController, error) {
		t.Fatal("opened controller before validating fixed app")
		return nil, nil
	}
	holderReady = func(context.Context, string) error {
		t.Fatal("declared readiness for missing app")
		return nil
	}
	if err := EnsureHolderService(t.Context()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("EnsureHolderService error = %v, want missing app", err)
	}
}

func TestEnsureHolderServiceConvergenceFailureClosesBeforeReadiness(t *testing.T) {
	useTestHolderApplication(t)
	originalOpen, originalReady, originalPresent := holderControllerOpen, holderReady, holderServicePresent
	t.Cleanup(func() {
		holderControllerOpen, holderReady, holderServicePresent = originalOpen, originalReady, originalPresent
	})
	holderServicePresent = func(service.Agent) (bool, error) { return false, nil }
	want := errors.New("launchctl convergence failed")
	closed, converged := 0, 0
	holderControllerOpen = func(
		context.Context,
		service.ControllerConfig,
	) (holderServiceController, error) {
		return &testHolderServiceController{
			converge: func(_ context.Context, agents []service.Agent) error {
				converged++
				if converged == 1 {
					if len(agents) != 1 {
						t.Fatalf("initial desired set = %#v, want holder", agents)
					}
					return want
				}
				if len(agents) != 0 {
					t.Fatalf("rollback desired set = %#v, want empty", agents)
				}
				return nil
			},
			close: func(context.Context) error {
				closed++
				return nil
			},
		}, nil
	}
	holderReady = func(context.Context, string) error {
		t.Fatal("declared readiness after convergence failure")
		return nil
	}
	if err := EnsureHolderService(t.Context()); !errors.Is(err, want) {
		t.Fatalf("EnsureHolderService error = %v, want %v", err, want)
	}
	if closed != 2 {
		t.Fatalf("controller close calls = %d, want initial plus rollback", closed)
	}
	if converged != 2 {
		t.Fatalf("converge calls = %d, want initial plus rollback", converged)
	}
}

func TestHolderServiceInstallReceiptRollsBackOnlyCreatedService(t *testing.T) {
	for _, test := range []struct {
		name         string
		preexisting  bool
		wantDesireds []int
	}{
		{name: "created", wantDesireds: []int{1, 0}},
		{name: "preexisting", preexisting: true, wantDesireds: []int{1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			useTestHolderApplication(t)
			originalOpen, originalReady, originalPresent := holderControllerOpen, holderReady, holderServicePresent
			t.Cleanup(func() {
				holderControllerOpen, holderReady, holderServicePresent = originalOpen, originalReady, originalPresent
			})
			holderServicePresent = func(service.Agent) (bool, error) { return test.preexisting, nil }
			holderReady = func(context.Context, string) error { return nil }
			var desiredSizes []int
			holderControllerOpen = func(
				context.Context,
				service.ControllerConfig,
			) (holderServiceController, error) {
				return &testHolderServiceController{
					converge: func(_ context.Context, agents []service.Agent) error {
						desiredSizes = append(desiredSizes, len(agents))
						return nil
					},
					close: func(context.Context) error { return nil },
				}, nil
			}
			install, err := InstallHolderService(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if err := install.Rollback(t.Context()); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(desiredSizes, test.wantDesireds) {
				t.Fatalf("desired set sizes = %v, want %v", desiredSizes, test.wantDesireds)
			}
		})
	}
}

func TestEnsureHolderServiceReadinessFailureRemovesNewService(t *testing.T) {
	useTestHolderApplication(t)
	originalOpen, originalReady, originalPresent := holderControllerOpen, holderReady, holderServicePresent
	t.Cleanup(func() {
		holderControllerOpen, holderReady, holderServicePresent = originalOpen, originalReady, originalPresent
	})
	holderServicePresent = func(service.Agent) (bool, error) { return false, nil }
	ctx, cancel := context.WithCancel(t.Context())
	want := errors.New("holder socket absent")
	holderReady = func(context.Context, string) error {
		cancel()
		return want
	}
	var desiredSizes []int
	holderControllerOpen = func(context.Context, service.ControllerConfig) (holderServiceController, error) {
		return &testHolderServiceController{
			converge: func(_ context.Context, agents []service.Agent) error {
				desiredSizes = append(desiredSizes, len(agents))
				return nil
			},
			close: func(closeCtx context.Context) error {
				if err := closeCtx.Err(); err != nil {
					t.Fatalf("controller close context = %v", err)
				}
				return nil
			},
		}, nil
	}
	err := EnsureHolderService(ctx)
	if !errors.Is(err, want) || !errors.Is(err, context.Canceled) {
		t.Fatalf("EnsureHolderService error = %v, want readiness cause and cancellation", err)
	}
	if !slices.Equal(desiredSizes, []int{1, 0}) {
		t.Fatalf("desired set sizes = %v, want install then empty rollback", desiredSizes)
	}
}

func TestEnsureHolderServiceReadinessFailurePreservesPreexistingService(t *testing.T) {
	useTestHolderApplication(t)
	originalOpen, originalReady, originalPresent := holderControllerOpen, holderReady, holderServicePresent
	t.Cleanup(func() {
		holderControllerOpen, holderReady, holderServicePresent = originalOpen, originalReady, originalPresent
	})
	holderServicePresent = func(service.Agent) (bool, error) { return true, nil }
	ctx, cancel := context.WithCancel(t.Context())
	holderReady = func(context.Context, string) error {
		cancel()
		return errors.New("holder socket absent")
	}
	var desiredSizes []int
	holderControllerOpen = func(context.Context, service.ControllerConfig) (holderServiceController, error) {
		return &testHolderServiceController{
			converge: func(_ context.Context, agents []service.Agent) error {
				desiredSizes = append(desiredSizes, len(agents))
				return nil
			},
			close: func(context.Context) error { return nil },
		}, nil
	}
	if err := EnsureHolderService(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("EnsureHolderService error = %v, want cancellation", err)
	}
	if !slices.Equal(desiredSizes, []int{1}) {
		t.Fatalf("desired set sizes = %v, want preexisting holder preserved", desiredSizes)
	}
}

func TestStopAndUninstallHolderServiceConvergesEmptyDesiredSet(t *testing.T) {
	originalOpen := holderControllerOpen
	t.Cleanup(func() { holderControllerOpen = originalOpen })
	var converged, closed int
	holderControllerOpen = func(
		_ context.Context,
		config service.ControllerConfig,
	) (holderServiceController, error) {
		if config != wantHolderControllerConfig() {
			t.Fatalf("controller config = %#v", config)
		}
		return &testHolderServiceController{
			converge: func(_ context.Context, agents []service.Agent) error {
				converged++
				if len(agents) != 0 {
					t.Fatalf("removal desired set = %#v", agents)
				}
				return nil
			},
			close: func(context.Context) error {
				closed++
				return nil
			},
		}, nil
	}
	if err := StopAndUninstallHolderService(t.Context()); err != nil {
		t.Fatal(err)
	}
	if converged != 1 || closed != 1 {
		t.Fatalf("converge/close calls = %d/%d, want 1/1", converged, closed)
	}
}

func TestStopAndUninstallHolderServiceReportsControllerFailure(t *testing.T) {
	originalOpen := holderControllerOpen
	t.Cleanup(func() { holderControllerOpen = originalOpen })
	want := errors.New("controller state unavailable")
	holderControllerOpen = func(
		context.Context,
		service.ControllerConfig,
	) (holderServiceController, error) {
		return nil, want
	}
	if err := StopAndUninstallHolderService(t.Context()); !errors.Is(err, want) {
		t.Fatalf("error = %v, want controller failure", err)
	}
}

func TestHolderControllerCloseOutlivesCallerCancellation(t *testing.T) {
	originalOpen := holderControllerOpen
	t.Cleanup(func() { holderControllerOpen = originalOpen })
	ctx, cancel := context.WithCancel(t.Context())
	controller := &testHolderServiceController{
		converge: func(context.Context, []service.Agent) error {
			cancel()
			return context.Canceled
		},
		close: func(closeCtx context.Context) error {
			if err := closeCtx.Err(); err != nil {
				t.Fatalf("controller close context = %v", err)
			}
			return nil
		},
	}
	holderControllerOpen = func(context.Context, service.ControllerConfig) (holderServiceController, error) {
		return controller, nil
	}
	if err := convergeHolderServices(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("convergeHolderServices error = %v, want cancellation", err)
	}
}

type testHolderServiceController struct {
	converge func(context.Context, []service.Agent) error
	close    func(context.Context) error
}

func (c *testHolderServiceController) Converge(ctx context.Context, agents []service.Agent) error {
	return c.converge(ctx, agents)
}

func (c *testHolderServiceController) Close(ctx context.Context) error { return c.close(ctx) }

func wantHolderControllerConfig() service.ControllerConfig {
	return service.ControllerConfig{
		StatePath:   filepath.Join(pool.FuseKitRuntimeDir(), "service-state.db"),
		ProcessPath: filepath.Join(pool.FuseKitRuntimeDir(), "service-processes.db"),
		WorkerLimit: holderServiceWorkers,
	}
}

func mustHolderDeploymentPlan(t *testing.T) holder.DeploymentPlan {
	t.Helper()
	plan, err := HolderDeploymentPlan()
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func useTestHolderApplication(t *testing.T) holder.SignedApplication {
	t.Helper()
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	applications := filepath.Join(account.HomeDir, "Applications")
	if err := os.MkdirAll(applications, 0o700); err != nil {
		t.Fatal(err)
	}
	appPath := filepath.Join(applications, ".cc-pool-holder-"+filepath.Base(t.TempDir())+".app")
	application := holderbridge.Application(appPath)
	executable := filepath.Join(appPath, "Contents", "MacOS", application.Runtime.ExecutableName)
	if err := os.MkdirAll(filepath.Dir(executable), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nexit 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// #nosec G302 -- the holder fixture must be executable and remains owner-only.
	if err := os.Chmod(executable, 0o700); err != nil {
		t.Fatal(err)
	}
	originalApplication := holderApplication
	holderApplication = func() holder.SignedApplication { return application }
	t.Cleanup(func() {
		holderApplication = originalApplication
		if err := os.RemoveAll(appPath); err != nil {
			t.Errorf("remove test holder app: %v", err)
		}
	})
	return application
}
