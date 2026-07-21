package hostsync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

type workerConsumerFixture struct {
	caps      syncservice.Capabilities
	items     []syncservice.WatchItem
	reconcile syncservice.ReconcileResult
	sync      syncservice.SyncResult
	state     syncservice.RawRegistry
	origins   []string
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

func (c *workerConsumerFixture) Sync(_ context.Context, origin string) (syncservice.SyncResult, error) {
	c.origins = append(c.origins, origin)
	return c.sync, c.err
}

func (c *workerConsumerFixture) GetState(context.Context) (syncservice.RawRegistry, error) {
	return c.state, c.err
}

type inlineHostSyncRunner struct {
	t           *testing.T
	runtime     WorkerRuntime
	tasks       []supervise.Task
	runtimeCall int
}

func (r *inlineHostSyncRunner) Run(ctx context.Context, task supervise.Task) error {
	r.t.Helper()
	r.tasks = append(r.tasks, task)
	if task.RecoveryClass != proc.RecoverySourceOwner || !IsWorkerInvocation(task.Args) {
		r.t.Fatalf("task = %+v, want exact recovery worker role", task)
	}
	r.runtimeCall++
	return RunWorker(ctx, task.Stdin, task.Stdout, r.runtime)
}

func TestWorkerClientExecutesExactOperations(t *testing.T) {
	consumer := &workerConsumerFixture{
		caps:      syncservice.Capabilities{Name: "cc-pool", Methods: []string{"list", MethodFetchCredential}},
		items:     []syncservice.WatchItem{{ID: "u1", WatchDirs: []string{"/stamps/u1"}, Fingerprint: "fp"}},
		reconcile: syncservice.ReconcileResult{Converged: 3, SkippedBusy: 1},
		sync:      syncservice.SyncResult{Converged: 4, SkippedBusy: 2},
		state:     json.RawMessage(`{"u1":{"added":1}}`),
	}
	fetchCalls := 0
	fetch := rpc.Handler(func(_ context.Context, params map[string]any) (any, error) {
		fetchCalls++
		if params["uuid"] != "u1" {
			t.Fatalf("fetch params = %+v", params)
		}
		return map[string]any{"credential": "envelope"}, nil
	})
	authKind := func(_ context.Context, accountID int, uuid string) (store.AuthKind, error) {
		if accountID != 7 || uuid != "u1" {
			t.Fatalf("auth-kind account = %d/%s", accountID, uuid)
		}
		return store.AuthKindAwaitingOrigin, nil
	}
	runner := &inlineHostSyncRunner{t: t, runtime: WorkerRuntime{
		Consumer: consumer, Fetch: fetch, AuthKind: authKind,
	}}
	client, err := NewWorkerClient(runner, "/exact/ccp")
	if err != nil {
		t.Fatal(err)
	}

	if got, err := client.Capabilities(t.Context()); err != nil || got.Name != consumer.caps.Name || len(got.Methods) != 2 {
		t.Fatalf("Capabilities = %+v, %v", got, err)
	}
	if got, err := client.List(t.Context()); err != nil || len(got) != 1 || got[0].ID != "u1" {
		t.Fatalf("List = %+v, %v", got, err)
	}
	if got, err := client.Reconcile(t.Context(), "host-a"); err != nil || got != consumer.reconcile {
		t.Fatalf("Reconcile = %+v, %v", got, err)
	}
	if got, err := client.Sync(t.Context(), "host-b"); err != nil || got != consumer.sync {
		t.Fatalf("Sync = %+v, %v", got, err)
	}
	if got, err := client.GetState(t.Context()); err != nil || !bytes.Equal(got, consumer.state) {
		t.Fatalf("GetState = %s, %v", got, err)
	}
	got, err := client.FetchCredentialHandler(t.Context(), map[string]any{"uuid": "u1"})
	if err != nil {
		t.Fatal(err)
	}
	gotMap, ok := got.(map[string]any)
	if !ok || gotMap["credential"] != "envelope" {
		t.Fatalf("FetchCredentialHandler = %#v", got)
	}
	if fetchCalls != 1 || runner.runtimeCall != 6 {
		t.Fatalf("calls: fetch=%d worker=%d", fetchCalls, runner.runtimeCall)
	}
	if strings.Join(consumer.origins, ",") != "host-a,host-b" {
		t.Fatalf("origins = %v", consumer.origins)
	}
	if kind, err := client.AuthKind(t.Context(), 7, "u1"); err != nil || kind != store.AuthKindAwaitingOrigin {
		t.Fatalf("AuthKind = %q, %v", kind, err)
	}
	if runner.runtimeCall != 7 {
		t.Fatalf("worker calls after auth kind = %d", runner.runtimeCall)
	}
}

func TestWorkerProtocolRejectsTrailingAndMismatchedFrames(t *testing.T) {
	runtime := WorkerRuntime{
		Consumer: &workerConsumerFixture{},
		Fetch:    func(context.Context, map[string]any) (any, error) { return struct{}{}, nil },
		AuthKind: func(context.Context, int, string) (store.AuthKind, error) { return store.AuthKindOwned, nil },
	}
	request := workerRequest{
		Protocol: hostSyncWorkerProtocol, Operation: workerCapabilities,
		Params: json.RawMessage(`{}`),
	}
	var framed bytes.Buffer
	if err := writeWorkerFrame(&framed, request); err != nil {
		t.Fatal(err)
	}
	framed.WriteByte(0)
	if err := RunWorker(t.Context(), &framed, &bytes.Buffer{}, runtime); err == nil || !strings.Contains(err.Error(), "trailing bytes") {
		t.Fatalf("trailing frame error = %v", err)
	}

	request.Protocol = "wrong"
	framed.Reset()
	if err := writeWorkerFrame(&framed, request); err != nil {
		t.Fatal(err)
	}
	if err := RunWorker(t.Context(), &framed, &bytes.Buffer{}, runtime); err == nil || !strings.Contains(err.Error(), "protocol mismatch") {
		t.Fatalf("protocol error = %v", err)
	}
}

func TestWorkerAuthKindPreservesTypedFailure(t *testing.T) {
	runner := &inlineHostSyncRunner{t: t, runtime: WorkerRuntime{
		Consumer: &workerConsumerFixture{},
		Fetch:    func(context.Context, map[string]any) (any, error) { return struct{}{}, nil },
		AuthKind: func(context.Context, int, string) (store.AuthKind, error) {
			return "", ErrAuthKindOriginForeign
		},
	}}
	client, err := NewWorkerClient(runner, "/exact/ccp")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.AuthKind(t.Context(), 7, "u1"); !errors.Is(err, ErrAuthKindOriginForeign) {
		t.Fatalf("AuthKind error = %v, want ErrAuthKindOriginForeign", err)
	}
}

type canceledHostSyncRunner struct {
	task supervise.Task
}

func (r *canceledHostSyncRunner) Run(ctx context.Context, task supervise.Task) error {
	r.task = task
	<-ctx.Done()
	return ctx.Err()
}

func TestWorkerDeadlineReturnsOnlyAfterRunnerSettlement(t *testing.T) {
	temp := t.TempDir()
	t.Setenv("TMPDIR", temp)
	runner := &canceledHostSyncRunner{}
	client, err := NewWorkerClient(runner, "/exact/ccp")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err = client.GetState(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("GetState error = %v", err)
	}
	if runner.task.RecoveryClass != proc.RecoverySourceOwner || !IsWorkerInvocation(runner.task.Args) {
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

var _ syncservice.SyncConsumer = (*workerConsumerFixture)(nil)
