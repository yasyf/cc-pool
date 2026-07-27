package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/testhome"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/worker"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

type blockingSyncConsumer struct {
	syncservice.SyncConsumer
	apply func(context.Context, syncservice.ChangeEnvelope) (syncservice.ApplyResult, error)
}

func (c blockingSyncConsumer) Apply(
	ctx context.Context,
	change syncservice.ChangeEnvelope,
) (syncservice.ApplyResult, error) {
	return c.apply(ctx, change)
}

func testSyncHelperOwners(t *testing.T) (*worker.Pool, *proc.Manager) {
	t.Helper()
	generation, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	workers, err := worker.NewPool(worker.Config{
		Capacity: 2, QueueCapacity: 2, MaxTotalRun: 3 * time.Minute,
		MaxStdinBytes: 1 << 20, MaxStdoutBytes: 1 << 20, MaxStderrBytes: 1 << 20,
	}, &proc.Reaper{
		Store: &proc.FileStore{Path: filepath.Join(t.TempDir(), "workers-v1.db")}, Generation: generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	children, err := proc.NewManager(2, &proc.Reaper{
		Store: &proc.FileStore{Path: filepath.Join(t.TempDir(), "children-v1.db")}, Generation: generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	return workers, children
}

func startTestSyncHelper(
	t *testing.T,
	consumer syncservice.SyncConsumer,
) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ccp-sync-helper")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "sync.sock")
	workers, children := testSyncHelperOwners(t)
	runtime, err := newSyncHelperRuntime(
		socket, consumer, workers, children,
		&proc.FileStore{Path: filepath.Join(dir, "stop-v1.db")},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	client := rpc.NewClient(rpc.ClientConfig{
		Dial: wire.UnixDialer(socket), WireBuild: rpc.WireBuild,
	})
	t.Cleanup(func() { _ = client.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for {
		probeCtx, probeCancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		health, probeErr := client.RuntimeHealth(probeCtx)
		probeCancel()
		if probeErr == nil && validateSyncHelperHealth(health) == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("helper never became ready: health=%+v err=%v", health, probeErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return socket, cancel, done
}

func TestSyncHelperRuntimeServesExactConsumerContract(t *testing.T) {
	regDir := t.TempDir()
	rf := hostsync.RegistryFile{
		Path: filepath.Join(regDir, "registry.json"), LockPath: filepath.Join(regDir, "registry.lock"),
	}
	service := &hostsync.Service{Registry: &rf, StampDir: filepath.Join(regDir, "stamps")}
	consumer := hostsync.NewConsumer(service, func() (bool, error) { return true, nil })
	socket, cancel, done := startTestSyncHelper(t, consumer)

	info, err := os.Stat(socket)
	if err != nil {
		t.Fatal(err)
	}
	if permissions := info.Mode().Perm(); permissions != 0o600 {
		t.Fatalf("socket mode = %o, want 600", permissions)
	}
	client := syncservice.NewClient(syncservice.Socket(socket))
	defer func() { _ = client.Close() }()
	capabilities, err := client.Capabilities(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := syncservice.DefaultCapabilities(hostsync.SyncServiceID)
	if !reflect.DeepEqual(capabilities, want) {
		t.Fatalf("capabilities = %+v, want %+v", capabilities, want)
	}
	for _, method := range []string{"ccp.fetch_credential", "ccp.fetch_stripped_credential"} {
		if err := client.Call(t.Context(), method, nil, nil); err == nil || !strings.Contains(err.Error(), "unknown method") {
			t.Fatalf("removed method %s = %v, want unknown method", method, err)
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run helper: %v", err)
	}
}

func TestSyncHelperRuntimeSettlesAdmittedApplyBeforeClose(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	regDir := t.TempDir()
	rf := hostsync.RegistryFile{
		Path: filepath.Join(regDir, "registry.json"), LockPath: filepath.Join(regDir, "registry.lock"),
	}
	base := hostsync.NewConsumer(
		&hostsync.Service{Registry: &rf, StampDir: filepath.Join(regDir, "stamps")},
		func() (bool, error) { return true, nil },
	)
	entered := make(chan struct{})
	release := make(chan struct{})
	ctxErr := make(chan error, 1)
	storeErr := make(chan error, 1)
	blocking := blockingSyncConsumer{SyncConsumer: base}
	blocking.apply = func(ctx context.Context, change syncservice.ChangeEnvelope) (syncservice.ApplyResult, error) {
		close(entered)
		<-release
		ctxErr <- ctx.Err()
		_, _, err := state.GetMeta("sync-drain-probe")
		storeErr <- err
		return syncservice.ApplyResult{AckedRevision: change.SourceRevision}, nil
	}
	socket, cancel, done := startTestSyncHelper(t, blocking)
	client := syncservice.NewClient(syncservice.Socket(socket))
	defer func() { _ = client.Close() }()
	change, err := syncservice.NewExportedChange(
		hostsync.SyncServiceID, hostsync.SyncSchemaFingerprint, syncservice.ChangeSnapshot,
		syncservice.NewRevision(0), syncservice.NewRevision(1), []byte(`{}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	change, err = syncservice.BindDelivery(change, "remote-host")
	if err != nil {
		t.Fatal(err)
	}
	called := make(chan error, 1)
	go func() {
		result, err := client.Apply(context.Background(), change)
		if err == nil && result.AckedRevision != change.SourceRevision {
			err = errors.New("apply returned the wrong revision")
		}
		called <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("apply never entered")
	}

	cancel()
	select {
	case err := <-done:
		t.Fatalf("helper returned before admitted apply settled: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-ctxErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("apply context after drain = %v, want cancellation", err)
	}
	if err := <-storeErr; err != nil {
		t.Fatalf("store access raced helper teardown: %v", err)
	}
	if err := <-called; err == nil {
		t.Fatal("draining helper delivered an apply response after canceling its session")
	}
	if err := <-done; err != nil {
		t.Fatalf("run helper: %v", err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncHelperSpawnConfigPreservesExactEnvironment(t *testing.T) {
	home := t.TempDir()
	configHome := filepath.Join(home, "xdg")
	testhome.Sandbox(t, home)
	t.Setenv("XDG_CONFIG_HOME", configHome)

	config, err := syncHelperSpawnConfig("/exact/cc-pool", "/exact/synckitd")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := config.Env, []string{"HOME=" + home, "XDG_CONFIG_HOME=" + configHome}; !reflect.DeepEqual(got, want) {
		t.Fatalf("helper environment = %v, want %v", got, want)
	}
	if got, want := config.Args, []string{syncHelperArgument, "/exact/synckitd"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("helper arguments = %v, want %v", got, want)
	}
	if !IsSyncHelperInvocation(config.Args) {
		t.Fatalf("helper arguments %v do not select the exact role", config.Args)
	}
	for _, executable := range []string{"", "synckitd", "/opt/homebrew/bin/../bin/synckitd"} {
		if _, err := syncHelperSpawnConfig("/exact/cc-pool", executable); err == nil {
			t.Fatalf("inexact synckitd executable %q accepted", executable)
		}
	}
}

func TestSyncEnabledMeta(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	server := &Server{cl: newClaims(), m: &pool.Manager{Store: state}}

	if on, err := server.syncEnabled(); err != nil || on {
		t.Fatalf("syncEnabled with no meta = %v (err %v), want false", on, err)
	}
	if err := state.SetMeta(metaSyncEnabled, "1"); err != nil {
		t.Fatal(err)
	}
	if on, err := server.syncEnabled(); err != nil || !on {
		t.Fatalf("syncEnabled after set 1 = %v (err %v), want true", on, err)
	}
}
