package hostsync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/worker"
	"github.com/yasyf/synckit/syncservice"
)

const testSynckitdExecutable = "/exact/synckitd"

type workerConsumerFixture struct {
	caps      syncservice.Capabilities
	items     []syncservice.WatchItem
	reconcile syncservice.ReconcileResult
	export    syncservice.ChangeEnvelope
	apply     syncservice.ApplyResult
	origins   []string
	exports   []syncservice.ExportRequest
	applies   []syncservice.ChangeEnvelope
	err       error
}

func (c *workerConsumerFixture) Capabilities(context.Context) (syncservice.Capabilities, error) {
	return c.caps, c.err
}

func (c *workerConsumerFixture) List(context.Context) ([]syncservice.WatchItem, error) {
	return c.items, c.err
}

func (c *workerConsumerFixture) Reconcile(_ context.Context, origin string) (syncservice.ReconcileResult, error) {
	c.origins = append(c.origins, origin)
	return c.reconcile, c.err
}

func (c *workerConsumerFixture) Export(_ context.Context, request syncservice.ExportRequest) (syncservice.ChangeEnvelope, error) {
	c.exports = append(c.exports, request)
	return c.export, c.err
}

func (c *workerConsumerFixture) Apply(_ context.Context, change syncservice.ChangeEnvelope) (syncservice.ApplyResult, error) {
	c.applies = append(c.applies, change)
	return c.apply, c.err
}

type inlineHostSyncRunner struct {
	t           *testing.T
	runtime     WorkerRuntime
	tasks       []worker.CommandRequest
	runtimeCall int
}

func (r *inlineHostSyncRunner) Run(ctx context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
	r.t.Helper()
	r.tasks = append(r.tasks, task)
	if !IsWorkerInvocation(task.Args) {
		r.t.Fatalf("task = %+v, want exact recovery worker role", task)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		r.t.Fatal(err)
	}
	wantEnv := []string{"HOME=" + home}
	if configHome, ok := os.LookupEnv("XDG_CONFIG_HOME"); ok {
		wantEnv = append(wantEnv, "XDG_CONFIG_HOME="+configHome)
	}
	if !slices.Equal(task.Env, wantEnv) {
		r.t.Fatalf("task env = %v, want %v", task.Env, wantEnv)
	}
	r.runtimeCall++
	var output bytes.Buffer
	err = RunWorker(ctx, bytes.NewReader(task.Stdin), &output, func(
		_ context.Context,
		synckitdExecutable string,
		run func(WorkerRuntime) error,
	) error {
		if synckitdExecutable != testSynckitdExecutable {
			r.t.Fatalf("synckitd executable = %q", synckitdExecutable)
		}
		return run(r.runtime)
	})
	return worker.CommandResult{Stdout: output.Bytes()}, err
}

func TestWorkerClientCarriesOnlyExactConfigIdentity(t *testing.T) {
	configHome := filepath.Join(t.TempDir(), "config")
	t.Setenv("XDG_CONFIG_HOME", configHome)
	runner := &inlineHostSyncRunner{
		t: t, runtime: WorkerRuntime{Consumer: &workerConsumerFixture{
			caps: syncservice.DefaultCapabilities(SyncServiceID),
		}, AuthKind: func(context.Context, int, string) (store.AuthKind, error) {
			return store.AuthKindOwned, nil
		}},
	}
	client, err := NewWorkerClient(runner, "/exact/ccp", testSynckitdExecutable)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Capabilities(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(runner.tasks) != 1 {
		t.Fatalf("tasks = %d, want one", len(runner.tasks))
	}
}

func TestWorkerClientRejectsInexactSynckitdExecutable(t *testing.T) {
	runner := &inlineHostSyncRunner{t: t}
	for _, executable := range []string{"", "synckitd", "/opt/homebrew/bin/../bin/synckitd"} {
		if _, err := NewWorkerClient(runner, "/exact/ccp", executable); err == nil {
			t.Fatalf("inexact synckitd executable %q accepted", executable)
		}
	}
}

func TestWorkerClientRejectsInexactConfigIdentity(t *testing.T) {
	for _, value := range []string{"", "relative/config", t.TempDir() + "/x/../config"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("XDG_CONFIG_HOME", value)
			runner := &inlineHostSyncRunner{t: t, runtime: WorkerRuntime{
				Consumer: &workerConsumerFixture{},
				AuthKind: func(context.Context, int, string) (store.AuthKind, error) {
					return store.AuthKindOwned, nil
				},
			}}
			client, err := NewWorkerClient(runner, "/exact/ccp", testSynckitdExecutable)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.Capabilities(t.Context()); err == nil || !strings.Contains(err.Error(), "XDG_CONFIG_HOME") {
				t.Fatalf("Capabilities with XDG_CONFIG_HOME=%q = %v", value, err)
			}
			if len(runner.tasks) != 0 {
				t.Fatal("invalid environment launched a worker")
			}
		})
	}
}

func TestWorkerClientExecutesExactOperations(t *testing.T) {
	exportRequest := syncservice.ExportRequest{
		ServiceID: SyncServiceID, SchemaFingerprint: SyncSchemaFingerprint,
		SinceRevision: syncservice.NewRevision(4),
	}
	exported, err := syncservice.NewExportedChange(
		SyncServiceID, SyncSchemaFingerprint, syncservice.ChangeSnapshot,
		syncservice.NewRevision(0), syncservice.NewRevision(5), []byte(`{"u1":{}}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := syncservice.BindDelivery(exported, "host-b")
	if err != nil {
		t.Fatal(err)
	}
	consumer := &workerConsumerFixture{
		caps:      syncservice.Capabilities{Name: "cc-pool", Methods: []string{"list"}},
		items:     []syncservice.WatchItem{{ID: "u1", WatchDirs: []string{"/stamps/u1"}, Fingerprint: "fp"}},
		reconcile: syncservice.ReconcileResult{Converged: 3, SkippedBusy: 1},
		export:    exported,
		apply:     syncservice.ApplyResult{AckedRevision: delivery.SourceRevision},
	}
	authKind := func(_ context.Context, accountID int, uuid string) (store.AuthKind, error) {
		if accountID != 7 || uuid != "u1" {
			t.Fatalf("auth-kind account = %d/%s", accountID, uuid)
		}
		return store.AuthKindAwaitingOrigin, nil
	}
	runner := &inlineHostSyncRunner{t: t, runtime: WorkerRuntime{
		Consumer: consumer, AuthKind: authKind,
	}}
	client, err := NewWorkerClient(runner, "/exact/ccp", testSynckitdExecutable)
	if err != nil {
		t.Fatal(err)
	}

	if got, err := client.Capabilities(t.Context()); err != nil || got.Name != consumer.caps.Name || len(got.Methods) != 1 {
		t.Fatalf("Capabilities = %+v, %v", got, err)
	}
	if got, err := client.List(t.Context()); err != nil || len(got) != 1 || got[0].ID != "u1" {
		t.Fatalf("List = %+v, %v", got, err)
	}
	if got, err := client.Reconcile(t.Context(), "host-a"); err != nil || got != consumer.reconcile {
		t.Fatalf("Reconcile = %+v, %v", got, err)
	}
	if got, err := client.Export(t.Context(), exportRequest); err != nil || got.ChangeID != exported.ChangeID || got.SourceRevision != exported.SourceRevision || !bytes.Equal(got.Payload, exported.Payload) {
		t.Fatalf("Export = %+v, %v", got, err)
	}
	if got, err := client.Apply(t.Context(), delivery); err != nil || got != consumer.apply {
		t.Fatalf("Apply = %+v, %v", got, err)
	}
	if runner.runtimeCall != 5 {
		t.Fatalf("worker calls = %d", runner.runtimeCall)
	}
	if strings.Join(consumer.origins, ",") != "host-a" {
		t.Fatalf("origins = %v", consumer.origins)
	}
	if len(consumer.exports) != 1 || consumer.exports[0] != exportRequest {
		t.Fatalf("export requests = %+v", consumer.exports)
	}
	if len(consumer.applies) != 1 || consumer.applies[0].ChangeID != delivery.ChangeID || consumer.applies[0].Origin != "host-b" {
		t.Fatalf("applied changes = %+v", consumer.applies)
	}
	if kind, err := client.AuthKind(t.Context(), 7, "u1"); err != nil || kind != store.AuthKindAwaitingOrigin {
		t.Fatalf("AuthKind = %q, %v", kind, err)
	}
	if runner.runtimeCall != 6 {
		t.Fatalf("worker calls after auth kind = %d", runner.runtimeCall)
	}
}

func staticWorkerRuntime(runtime WorkerRuntime) WorkerRuntimeScope {
	return func(_ context.Context, _ string, run func(WorkerRuntime) error) error {
		return run(runtime)
	}
}

func TestWorkerProtocolRejectsTrailingAndMismatchedFrames(t *testing.T) {
	runtime := WorkerRuntime{
		Consumer: &workerConsumerFixture{},
		AuthKind: func(context.Context, int, string) (store.AuthKind, error) { return store.AuthKindOwned, nil },
	}
	request := workerRequest{
		Protocol: hostSyncWorkerProtocol, SynckitdExecutable: testSynckitdExecutable,
		Operation: workerCapabilities,
		Params:    json.RawMessage(`{}`),
	}
	var framed bytes.Buffer
	if err := writeWorkerFrame(&framed, request); err != nil {
		t.Fatal(err)
	}
	framed.WriteByte(0)
	if err := RunWorker(t.Context(), &framed, &bytes.Buffer{}, staticWorkerRuntime(runtime)); err == nil || !strings.Contains(err.Error(), "trailing bytes") {
		t.Fatalf("trailing frame error = %v", err)
	}

	request.Protocol = "wrong"
	framed.Reset()
	if err := writeWorkerFrame(&framed, request); err != nil {
		t.Fatal(err)
	}
	if err := RunWorker(t.Context(), &framed, &bytes.Buffer{}, staticWorkerRuntime(runtime)); err == nil || !strings.Contains(err.Error(), "protocol mismatch") {
		t.Fatalf("protocol error = %v", err)
	}
}

func TestWorkerAuthKindPreservesTypedFailure(t *testing.T) {
	runner := &inlineHostSyncRunner{t: t, runtime: WorkerRuntime{
		Consumer: &workerConsumerFixture{},
		AuthKind: func(context.Context, int, string) (store.AuthKind, error) {
			return "", ErrAuthKindOriginForeign
		},
	}}
	client, err := NewWorkerClient(runner, "/exact/ccp", testSynckitdExecutable)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.AuthKind(t.Context(), 7, "u1"); !errors.Is(err, ErrAuthKindOriginForeign) {
		t.Fatalf("AuthKind error = %v, want ErrAuthKindOriginForeign", err)
	}
}

type canceledHostSyncRunner struct {
	task worker.CommandRequest
}

type gatedHostSyncRunner struct {
	inner   *inlineHostSyncRunner
	started chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

func (r *gatedHostSyncRunner) Run(ctx context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
	r.calls.Add(1)
	r.once.Do(func() { close(r.started) })
	select {
	case <-r.release:
		return r.inner.Run(ctx, task)
	case <-ctx.Done():
		return worker.CommandResult{}, ctx.Err()
	}
}

func TestWorkerClientSerializesExclusiveChildActivation(t *testing.T) {
	inner := &inlineHostSyncRunner{t: t, runtime: WorkerRuntime{
		Consumer: &workerConsumerFixture{},
		AuthKind: func(context.Context, int, string) (store.AuthKind, error) { return store.AuthKindOwned, nil },
	}}
	runner := &gatedHostSyncRunner{
		inner: inner, started: make(chan struct{}), release: make(chan struct{}),
	}
	client, err := NewWorkerClient(runner, "/exact/ccp", testSynckitdExecutable)
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := client.Export(t.Context(), testWorkerExportRequest())
		firstDone <- err
	}()
	<-runner.started
	secondCtx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := client.List(secondCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second operation = %v, want deadline while exclusive worker is active", err)
	}
	close(runner.release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if inner.runtimeCall != 1 {
		t.Fatalf("worker activations = %d, want one", inner.runtimeCall)
	}
}

func TestWorkerClientDefaultDeadlineIncludesLaneWait(t *testing.T) {
	inner := &inlineHostSyncRunner{t: t, runtime: WorkerRuntime{
		Consumer: &workerConsumerFixture{},
		AuthKind: func(context.Context, int, string) (store.AuthKind, error) { return store.AuthKindOwned, nil },
	}}
	runner := &gatedHostSyncRunner{
		inner: inner, started: make(chan struct{}), release: make(chan struct{}),
	}
	client, err := NewWorkerClient(runner, "/exact/ccp", testSynckitdExecutable)
	if err != nil {
		t.Fatal(err)
	}
	client.timeout = 20 * time.Millisecond
	firstCtx, cancelFirst := context.WithTimeout(t.Context(), time.Second)
	defer cancelFirst()
	firstDone := make(chan error, 1)
	go func() {
		_, err := client.Export(firstCtx, testWorkerExportRequest())
		firstDone <- err
	}()
	<-runner.started
	if _, err := client.List(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second operation = %v, want default deadline while waiting for lane", err)
	}
	if calls := runner.calls.Load(); calls != 1 {
		t.Fatalf("worker activations = %d, want no activation for timed-out waiter", calls)
	}
	close(runner.release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func (r *canceledHostSyncRunner) Run(ctx context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
	r.task = task
	<-ctx.Done()
	return worker.CommandResult{}, ctx.Err()
}

func TestWorkerDeadlineReturnsOnlyAfterRunnerSettlement(t *testing.T) {
	temp := t.TempDir()
	t.Setenv("TMPDIR", temp)
	runner := &canceledHostSyncRunner{}
	client, err := NewWorkerClient(runner, "/exact/ccp", testSynckitdExecutable)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err = client.Export(ctx, testWorkerExportRequest())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Export error = %v", err)
	}
	if !IsWorkerInvocation(runner.task.Args) {
		t.Fatalf("task = %+v", runner.task)
	}
	entries, readErr := os.ReadDir(temp)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("parent host-sync framing touched temp storage: %v", entries)
	}
}

func testWorkerExportRequest() syncservice.ExportRequest {
	return syncservice.ExportRequest{
		ServiceID: SyncServiceID, SchemaFingerprint: SyncSchemaFingerprint,
		SinceRevision: syncservice.NewRevision(0),
	}
}

var _ syncservice.SyncConsumer = (*workerConsumerFixture)(nil)
