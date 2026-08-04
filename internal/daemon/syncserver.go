package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/workerexec"
	"github.com/yasyf/daemonkit"
	"github.com/yasyf/synckit/helperruntime"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

const (
	metaSyncEnabled        = "sync_enabled"
	syncHelperArgument     = "__host-sync-helper"
	syncHelperReadyTimeout = 10 * time.Second
	syncHelperSpawnTimeout = 10 * time.Second
	syncHelperStopTimeout  = 5 * time.Second
)

type syncHelperProduct struct{}

func (syncHelperProduct) Drain(context.Context) error { return nil }
func (syncHelperProduct) Close(context.Context) error { return nil }

// scopeRunner adapts one daemonkit process scope to cc-pool's disposable
// command boundary: every run is a dedicated-session child, reap included, and
// retains a whole maximal host-sync worker frame — daemonkit's 4 MiB default
// would turn a legitimate larger response into ErrTruncated.
type scopeRunner struct {
	scope daemonkit.Ctx
}

func (r scopeRunner) Run(
	ctx context.Context,
	request workerexec.CommandRequest,
) (workerexec.CommandResult, error) {
	if request.TotalTimeout <= 0 {
		return workerexec.CommandResult{}, errors.New("host-sync worker requires a positive total timeout")
	}
	runCtx, cancel := context.WithTimeout(ctx, request.TotalTimeout)
	defer cancel()
	result, err := r.scope.Run(runCtx, daemonkit.Cmd{
		Path: request.Path, Args: request.Args, Dir: request.Dir, Env: request.Env,
		Stdin: request.Stdin, MaxOutput: hostsync.MaxWorkerResponse,
		Exec: daemonkit.ServingSameUser(),
	})
	observed := workerexec.CommandResult{
		Stdout: result.Stdout, Stderr: result.Stderr, ExitCode: result.Exit.Code,
	}
	if err != nil {
		return observed, err
	}
	return observed, nil
}

// IsSyncHelperInvocation reports whether args select the resident host-sync helper role.
func IsSyncHelperInvocation(args []string) bool {
	return len(args) == 2 && args[0] == syncHelperArgument
}

// RunSyncHelper serves Synckit's exact consumer contract as a resident helper
// daemon: helperruntime derives the socket, lock, and state from the helper
// label, and the Prepare hook hands the helper its own process scope.
func RunSyncHelper(ctx context.Context, synckitdExecutable string) error {
	if synckitdExecutable == "" || !filepath.IsAbs(synckitdExecutable) || filepath.Clean(synckitdExecutable) != synckitdExecutable {
		return errors.New("host-sync helper requires an exact synckitd executable")
	}
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve host-sync helper executable: %w", err)
	}
	dispatcher := rpc.NewDispatcher()
	runtime, err := helperruntime.New(helperruntime.Config{
		App:        helperruntime.App{Name: hostsync.SyncServiceID},
		Dispatcher: dispatcher,
		Prepare: func(scope daemonkit.Ctx) (helperruntime.Product, error) {
			consumer, err := hostsync.NewWorkerClient(
				scopeRunner{scope: scope}, executable, synckitdExecutable,
			)
			if err != nil {
				return nil, err
			}
			syncservice.RegisterConsumer(dispatcher, consumer)
			return syncHelperProduct{}, nil
		},
	})
	if err != nil {
		return err
	}
	return runtime.Run(ctx)
}

// startSyncHelper spawns the exact child role and waits for its resident
// runtime to publish readiness on the helper's own label socket.
func (s *Server) startSyncHelper(ctx context.Context, executable, synckitdExecutable string) error {
	if s.launchSyncHelper != nil {
		return s.launchSyncHelper(ctx, executable, synckitdExecutable)
	}
	environment, err := hostsync.ProcessEnvironment()
	if err != nil {
		return fmt.Errorf("resolve exact host-sync helper environment: %w", err)
	}
	spawnCtx, cancel := context.WithTimeout(ctx, syncHelperSpawnTimeout)
	child, err := s.scope.Spawn(spawnCtx, daemonkit.Cmd{
		Path:    executable,
		Args:    []string{syncHelperArgument, synckitdExecutable},
		Env:     environment,
		Session: true,
		Exec:    daemonkit.ServingSameUser(),
	}, daemonkit.ChannelNone, nil)
	cancel()
	if err != nil {
		return fmt.Errorf("start host-sync helper: %w", err)
	}
	stop := func(cause error) error {
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), syncHelperStopTimeout)
		defer cancel()
		_, stopErr := child.Stop(stopCtx)
		return errors.Join(cause, stopErr)
	}
	spec, err := helperruntime.Spec(hostsync.SyncServiceID, daemonkit.Program{}, 0)
	if err != nil {
		return stop(err)
	}
	client, err := daemonkit.Open(spec)
	if err != nil {
		return stop(err)
	}
	readyCtx, cancelReady := context.WithTimeout(ctx, syncHelperReadyTimeout)
	defer cancelReady()
	ready := make(chan error, 1)
	go func() {
		_, err := client.WaitReady(readyCtx)
		ready <- err
	}()
	select {
	case err := <-ready:
		if err != nil {
			return stop(fmt.Errorf("await host-sync helper readiness: %w", err))
		}
	case exit := <-child.Done():
		return stop(fmt.Errorf(
			"host-sync helper exited before readiness: code=%d signal=%v", exit.Code, exit.Signal,
		))
	}
	s.wg.Add(1)
	go s.monitorSyncHelper(ctx, child)
	return nil
}

func (s *Server) monitorSyncHelper(ctx context.Context, child *daemonkit.Child) {
	defer s.wg.Done()
	var exit daemonkit.Exit
	select {
	case <-ctx.Done():
		return
	case exit = <-child.Done():
	}
	if ctx.Err() != nil {
		return
	}
	s.log.Printf("host-sync helper exited: code=%d signal=%v", exit.Code, exit.Signal)
	if s.stopServe != nil {
		s.stopServe(errors.New("daemon: host-sync helper exited"))
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
