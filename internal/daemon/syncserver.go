package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/version"
	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/worker"
	"github.com/yasyf/synckit/helperruntime"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

const (
	metaSyncEnabled         = "sync_enabled"
	syncHelperArgument      = "__host-sync-helper"
	syncHelperRecoveryID    = proc.RecoveryID("com.yasyf.cc-pool.sync-helper.v1")
	syncHelperReadyTimeout  = 10 * time.Second
	syncHelperProbeInterval = 25 * time.Millisecond
)

type syncHelperProduct struct{}

func (syncHelperProduct) Drain(context.Context) error { return nil }
func (syncHelperProduct) Close(context.Context) error { return nil }

// IsSyncHelperInvocation reports whether args select the resident host-sync helper role.
func IsSyncHelperInvocation(args []string) bool {
	return len(args) == 2 && args[0] == syncHelperArgument
}

// RunSyncHelper serves Synckit's exact consumer contract in an independent process runtime.
func RunSyncHelper(ctx context.Context, synckitdExecutable string) error {
	owners, err := pool.OpenHostSyncHelperOwners()
	if err != nil {
		return fmt.Errorf("open host-sync helper process owners: %w", err)
	}
	workerClient, err := hostsync.NewWorkerClient(owners.Workers(), owners.Executable(), synckitdExecutable)
	if err != nil {
		return err
	}
	runtime, err := newSyncHelperRuntime(
		pool.SyncSocketPath(), workerClient, owners.Workers(), owners.Children(),
		&proc.FileStore{Path: pool.HostSyncHelperStopStorePath()},
	)
	if err != nil {
		return err
	}
	return runtime.Run(ctx)
}

func newSyncHelperRuntime(
	socket string,
	consumer syncservice.SyncConsumer,
	workers *worker.Pool,
	children *proc.Manager,
	stopStore *proc.FileStore,
) (*helperruntime.Runtime, error) {
	if consumer == nil {
		return nil, errors.New("host-sync helper consumer is required")
	}
	dispatcher := rpc.NewDispatcher()
	syncservice.RegisterConsumer(dispatcher, consumer)
	return helperruntime.New(helperruntime.Config{
		App:        helperruntime.App{Name: hostsync.SyncServiceID, RuntimeBuild: version.String()},
		Socket:     socket,
		Dispatcher: dispatcher,
		Workers:    workers,
		Children:   children,
		StopStore:  stopStore,
		Prepare: func(dkdaemon.Activation) (helperruntime.Product, error) {
			return syncHelperProduct{}, nil
		},
	})
}

// startSyncHelper starts the exact child role and waits for its published runtime.
func (s *Server) startSyncHelper(ctx context.Context, executable, synckitdExecutable string) error {
	if s.launchSyncHelper != nil {
		return s.launchSyncHelper(ctx, executable, synckitdExecutable)
	}
	if s.m == nil || s.m.RuntimeChildren() == nil {
		return errors.New("host-sync helper requires daemon child ownership")
	}
	config, err := syncHelperSpawnConfig(executable, synckitdExecutable)
	if err != nil {
		return err
	}
	request, err := proc.NewSpawnRequest(config)
	if err != nil {
		return fmt.Errorf("build host-sync helper launch: %w", err)
	}
	child, _, err := s.m.RuntimeChildren().Prepare(ctx, request)
	if err != nil {
		return fmt.Errorf("prepare host-sync helper: %w", err)
	}
	stop := func(cause error) error {
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return errors.Join(cause, child.Stop(stopCtx))
	}
	if err := child.Start(ctx); err != nil {
		return stop(fmt.Errorf("start host-sync helper: %w", err))
	}
	readyCtx, cancel := context.WithTimeout(ctx, syncHelperReadyTimeout)
	defer cancel()
	client := rpc.NewClient(rpc.ClientConfig{
		Dial: wire.UnixDialer(s.syncSocket), WireBuild: rpc.WireBuild,
	})
	defer func() { _ = client.Close() }()
	if err := waitSyncHelperReady(readyCtx, child.Done(), child.Exit, client.RuntimeHealth); err != nil {
		return stop(err)
	}
	s.wg.Add(1)
	go s.monitorSyncHelper(ctx, child)
	return nil
}

func syncHelperSpawnConfig(executable, synckitdExecutable string) (proc.SpawnConfig, error) {
	if synckitdExecutable == "" || !filepath.IsAbs(synckitdExecutable) || filepath.Clean(synckitdExecutable) != synckitdExecutable {
		return proc.SpawnConfig{}, errors.New("host-sync helper requires an exact synckitd executable")
	}
	environment, err := hostsync.ProcessEnvironment()
	if err != nil {
		return proc.SpawnConfig{}, fmt.Errorf("resolve exact host-sync helper environment: %w", err)
	}
	return proc.SpawnConfig{
		RecoveryID: syncHelperRecoveryID, Executable: executable,
		Args: []string{syncHelperArgument, synckitdExecutable}, Env: environment,
		Stdin: proc.StdioNull, Stdout: proc.StdioNull, Stderr: proc.StdioNull,
	}, nil
}

func waitSyncHelperReady(
	ctx context.Context,
	done <-chan struct{},
	exit func() (proc.ProcessExit, bool),
	health func(context.Context) (rpc.RuntimeHealth, error),
) error {
	ticker := time.NewTicker(syncHelperProbeInterval)
	defer ticker.Stop()
	var lastErr error
	for {
		probe, err := health(ctx)
		if err == nil {
			err = validateSyncHelperHealth(probe)
		}
		if err == nil {
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("await host-sync helper readiness: %w", errors.Join(ctx.Err(), lastErr))
		case <-done:
			observed, _ := exit()
			return fmt.Errorf("host-sync helper exited before readiness: code=%d stopped=%t error=%s", observed.Code, observed.Stopped, observed.Error)
		case <-ticker.C:
		}
	}
}

func validateSyncHelperHealth(health rpc.RuntimeHealth) error {
	if health.RuntimeBuild != version.String() || health.RuntimeProtocol != int(rpc.Version) ||
		health.ProcessGeneration == "" || health.PID <= 0 {
		return fmt.Errorf("host-sync helper identity mismatch: %+v", health)
	}
	if health.State != string(dkdaemon.StateHealthy) || health.Draining || !health.Ready {
		return fmt.Errorf("host-sync helper is not ready: %+v", health)
	}
	return nil
}

func (s *Server) monitorSyncHelper(ctx context.Context, child *proc.PreparedChild) {
	defer s.wg.Done()
	select {
	case <-ctx.Done():
		return
	case <-child.Done():
	}
	if ctx.Err() != nil {
		return
	}
	exit, _ := child.Exit()
	s.log.Printf("host-sync helper exited: code=%d stopped=%t error=%s", exit.Code, exit.Stopped, exit.Error)
	if s.runtimeShutdown != nil {
		if err := s.runtimeShutdown(context.WithoutCancel(ctx)); err != nil {
			s.log.Printf("shut down after host-sync helper loss: %v", err)
		}
	}
}

// syncEnabled reports whether host sync is on, from the store's sync_enabled meta.
func (s *Server) syncEnabled() (bool, error) {
	v, ok, err := s.m.Store.GetMeta(metaSyncEnabled)
	if err != nil {
		return false, fmt.Errorf("read %s meta: %w", metaSyncEnabled, err)
	}
	return ok && v == "1", nil
}
