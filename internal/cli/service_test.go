package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/service"
)

func swapVar[T any](t *testing.T, target *T, val T) {
	t.Helper()
	old := *target
	*target = val
	t.Cleanup(func() { *target = old })
}

func tempHome(t *testing.T) string {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "ccp-home")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	return home
}

func seedAccounts(t *testing.T, accts ...store.Account) {
	t.Helper()
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(pool.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	for _, account := range accts {
		if account.InstanceID == "" {
			account.InstanceID = "instance-" + pool.AccountDirName(account.ID)
		}
		if account.Generation == 0 {
			account.Generation = 1
		}
		if account.KeychainService == "" {
			account.KeychainService = "ccp-test-missing"
		}
		if account.KeychainAccount == "" {
			account.KeychainAccount = "ccp-test"
		}
		if err := st.UpsertAccount(account); err != nil {
			t.Fatal(err)
		}
	}
}

func uninstallCmd() (*cobra.Command, *bytes.Buffer, *bytes.Buffer) {
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	return cmd, &out, &errOut
}

type testDaemonServiceController struct {
	desired     [][]service.Agent
	closed      int
	closeCtxErr error
	convergeErr error
	closeErr    error
}

func (c *testDaemonServiceController) Converge(_ context.Context, agents []service.Agent) error {
	c.desired = append(c.desired, append([]service.Agent(nil), agents...))
	return c.convergeErr
}

func (c *testDaemonServiceController) Close(ctx context.Context) error {
	c.closed++
	c.closeCtxErr = ctx.Err()
	return c.closeErr
}

func useDaemonServiceController(t *testing.T, controller daemonServiceController) {
	t.Helper()
	swapVar(t, &openDaemonServiceController, func(context.Context) (daemonServiceController, error) {
		return controller, nil
	})
}

func TestCCPAgentUsesPinnedExecutableAndTypedRestartPolicy(t *testing.T) {
	executable := filepath.Join(t.TempDir(), "ccp")
	agent, err := ccpAgent(executable)
	if err != nil {
		t.Fatal(err)
	}
	if agent.Program != executable {
		t.Fatalf("Program = %q, want %q", agent.Program, executable)
	}
	if agent.RestartPolicy != service.RestartOnFailure {
		t.Fatalf("RestartPolicy = %v, want RestartOnFailure", agent.RestartPolicy)
	}
	if agent.Env["PATH"] != daemonServicePATH {
		t.Fatalf("PATH = %q, want stable service PATH", agent.Env["PATH"])
	}
	if _, err := ccpAgent("ccp"); err == nil {
		t.Fatal("ccpAgent accepted relative executable")
	}
}

func TestResolveDaemonServiceExecutablePinsCurrentRoleTarget(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "ccp-v1")
	if err := os.WriteFile(target, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(target, 0o700); err != nil { //nolint:gosec // G302: private role fixture needs its owner execute bit
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "ccp")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root)
	executable, err := resolveDaemonServiceExecutable()
	if err != nil {
		t.Fatal(err)
	}
	want, err := filepath.EvalSymlinks(target)
	if err != nil {
		t.Fatal(err)
	}
	if executable != want {
		t.Fatalf("service executable = %q, want current role target %q", executable, want)
	}
}

func TestDaemonServiceControllerConfigIsStableAndDistinct(t *testing.T) {
	tempHome(t)
	config := daemonServiceControllerConfig()
	if !filepath.IsAbs(config.StatePath) || !filepath.IsAbs(config.ProcessPath) ||
		config.StatePath == config.ProcessPath {
		t.Fatalf("controller paths = %q / %q", config.StatePath, config.ProcessPath)
	}
	if config.WorkerLimit != daemonServiceWorkerLimit {
		t.Fatalf("WorkerLimit = %d, want %d", config.WorkerLimit, daemonServiceWorkerLimit)
	}
}

func TestRunServiceInstallConvergesExactExecutableAgent(t *testing.T) {
	tempHome(t)
	executable := filepath.Join(t.TempDir(), "ccp")
	swapVar(t, &serviceExecutable, func() (string, error) { return executable, nil })
	holderReady := false
	swapVar(t, &ensureHolder, func(context.Context) error {
		holderReady = true
		return nil
	})
	controller := &testDaemonServiceController{}
	useDaemonServiceController(t, controller)
	cmd, out, _ := uninstallCmd()
	cmd.SetContext(t.Context())
	if err := runServiceInstall(cmd); err != nil {
		t.Fatal(err)
	}
	if !holderReady {
		t.Fatal("daemon service converged before holder readiness")
	}
	if len(controller.desired) != 1 || len(controller.desired[0]) != 1 {
		t.Fatalf("desired = %+v", controller.desired)
	}
	agent := controller.desired[0][0]
	if agent.Program != executable || agent.Label != "com.yasyf.cc-pool" ||
		agent.RestartPolicy != service.RestartOnFailure {
		t.Fatalf("agent = %+v", agent)
	}
	if controller.closed != 1 || controller.closeCtxErr != nil {
		t.Fatalf("controller close = %d, context error = %v", controller.closed, controller.closeCtxErr)
	}
	if !strings.Contains(stripANSI(out.String()), "Installed and started the daemon") {
		t.Fatalf("output = %q", out.String())
	}
}

func TestRunServiceInstallStopsBeforeControllerWhenHolderFails(t *testing.T) {
	want := errors.New("holder not ready")
	swapVar(t, &ensureHolder, func(context.Context) error { return want })
	opened := false
	swapVar(t, &openDaemonServiceController, func(context.Context) (daemonServiceController, error) {
		opened = true
		return nil, errors.New("must not open")
	})
	cmd, _, _ := uninstallCmd()
	cmd.SetContext(t.Context())
	if err := runServiceInstall(cmd); !errors.Is(err, want) {
		t.Fatalf("error = %v, want holder failure", err)
	}
	if opened {
		t.Fatal("controller opened before holder readiness")
	}
}

func TestDaemonServiceControllerCloseJoinsAndOutlivesCancellation(t *testing.T) {
	runErr := errors.New("converge failed")
	closeErr := errors.New("close failed")
	controller := &testDaemonServiceController{closeErr: closeErr}
	useDaemonServiceController(t, controller)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := withDaemonServiceController(ctx, func(daemonServiceController) error { return runErr })
	if !errors.Is(err, runErr) || !errors.Is(err, closeErr) {
		t.Fatalf("error = %v, want converge and close failures", err)
	}
	if controller.closed != 1 || controller.closeCtxErr != nil {
		t.Fatalf("controller close = %d, context error = %v", controller.closed, controller.closeCtxErr)
	}
}

func TestUninstallWithoutPurgeStopsDaemonAndPreservesState(t *testing.T) {
	tempHome(t)
	seedAccounts(t, store.Account{ID: 1, ConfigDir: pool.AccountDir(1)})
	scanned := false
	swapVar(t, &scanSessions, func(context.Context) ([]procscan.Session, error) {
		scanned = true
		return nil, errors.New("must not scan")
	})
	stopped := false
	swapVar(t, &stopDaemon, func(*cobra.Command) error {
		stopped = true
		return nil
	})
	cmd, out, _ := uninstallCmd()
	if err := runServiceUninstall(cmd, false, false); err != nil {
		t.Fatal(err)
	}
	if scanned || !stopped {
		t.Fatalf("scanned = %t, stopped = %t", scanned, stopped)
	}
	if _, err := os.Stat(pool.DBPath()); err != nil {
		t.Fatalf("state was not preserved: %v", err)
	}
	if !strings.Contains(stripANSI(out.String()), "accounts and state are preserved") {
		t.Fatalf("output = %q", out.String())
	}
}

func TestPurgeSessionGateIsExactAndForceable(t *testing.T) {
	accounts := []store.Account{
		{ID: 1, ConfigDir: "/private/acct-01"},
		{ID: 2, ConfigDir: "/private/acct-02"},
	}
	swapVar(t, &scanSessions, func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 42, ConfigDir: accounts[1].ConfigDir}}, nil
	})
	err := gateUninstallSessions(accounts)
	if err == nil || !strings.Contains(err.Error(), "acct-02 (pid 42)") || strings.Contains(err.Error(), "acct-01") {
		t.Fatalf("error = %v", err)
	}

	tempHome(t)
	seedAccounts(t)
	scanned := false
	swapVar(t, &scanSessions, func(context.Context) ([]procscan.Session, error) {
		scanned = true
		return nil, errors.New("must not scan")
	})
	stopped := false
	swapVar(t, &stopDaemon, func(*cobra.Command) error {
		stopped = true
		return nil
	})
	cmd, _, _ := uninstallCmd()
	if err := runServiceUninstall(cmd, true, true); err != nil {
		t.Fatal(err)
	}
	if scanned || !stopped {
		t.Fatalf("scanned = %t, stopped = %t", scanned, stopped)
	}
}

func TestStopDaemonServiceControllerFailureIsFatal(t *testing.T) {
	want := errors.New("controller exploded")
	controller := &testDaemonServiceController{convergeErr: want}
	useDaemonServiceController(t, controller)
	holderCalled := false
	swapVar(t, &stopHolder, func(context.Context) error {
		holderCalled = true
		return nil
	})
	cmd, out, _ := uninstallCmd()
	err := stopDaemonService(cmd)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want controller failure", err)
	}
	if holderCalled || strings.Contains(stripANSI(out.String()), "Removed the daemon") {
		t.Fatalf("claimed success: %s", out.String())
	}
}

func TestStopDaemonServiceSettlesHolderBeforeSuccess(t *testing.T) {
	tempHome(t)
	controller := &testDaemonServiceController{}
	useDaemonServiceController(t, controller)
	swapVar(t, &stopHolder, func(context.Context) error { return nil })
	cmd, out, _ := uninstallCmd()
	cmd.SetContext(t.Context())
	if err := stopDaemonService(cmd); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stripANSI(out.String()), "Removed the daemon and holder LaunchAgents") {
		t.Fatalf("output = %q", out.String())
	}
	if len(controller.desired) != 1 || controller.desired[0] != nil || controller.closed != 1 {
		t.Fatalf("controller state: desired=%v closed=%d", controller.desired, controller.closed)
	}
}

func TestStopDaemonServiceDoesNotClaimSuccessWhenHolderStopFails(t *testing.T) {
	tempHome(t)
	controller := &testDaemonServiceController{}
	useDaemonServiceController(t, controller)
	want := errors.New("holder remained live")
	swapVar(t, &stopHolder, func(context.Context) error { return want })
	cmd, out, _ := uninstallCmd()
	cmd.SetContext(t.Context())
	if err := stopDaemonService(cmd); !errors.Is(err, want) {
		t.Fatalf("error = %v, want holder failure", err)
	}
	if strings.Contains(stripANSI(out.String()), "Removed the daemon") {
		t.Fatalf("claimed success: %s", out.String())
	}
}

func TestPurgeRefusesProvisionedAccount(t *testing.T) {
	tempHome(t)
	seedAccounts(t, store.Account{ID: 1, ConfigDir: pool.AccountPresentationDir(1)})
	cmd, _, _ := uninstallCmd()
	err := purgeAll(cmd)
	if err == nil || !strings.Contains(err.Error(), "accounts remain provisioned") {
		t.Fatalf("error = %v", err)
	}
	if _, err := os.Stat(pool.StateDir()); err != nil {
		t.Fatalf("state removed after refusal: %v", err)
	}
}

func TestPurgeAllRemovesCleanState(t *testing.T) {
	tempHome(t)
	if err := pool.EnsureAccountsDir(); err != nil {
		t.Fatal(err)
	}
	cmd, out, _ := uninstallCmd()
	if err := purgeAll(cmd); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(pool.StateDir()); !os.IsNotExist(err) {
		t.Fatalf("state still exists: %v", err)
	}
	if !strings.Contains(stripANSI(out.String()), "Purged all pool state") {
		t.Fatalf("output = %q", out.String())
	}
}

func TestUninstallHelpMentionsPurgeGate(t *testing.T) {
	cmd := newServiceUninstallCmd()
	help := cmd.Short + "\n" + cmd.Long
	for _, want := range []string{"live claude sessions", "--force", "~/.claude is never"} {
		if !strings.Contains(help, want) {
			t.Errorf("help missing %q", want)
		}
	}
	for _, flag := range []string{"purge", "force"} {
		if cmd.Flags().Lookup(flag) == nil {
			t.Errorf("missing --%s", flag)
		}
	}
}

func TestFuseKitPresentationRootIsInsideState(t *testing.T) {
	tempHome(t)
	if filepath.Dir(pool.FuseKitRuntimeDir()) != pool.StateDir() {
		t.Fatalf("runtime dir = %q", pool.FuseKitRuntimeDir())
	}
}
