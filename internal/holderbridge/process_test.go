package holderbridge

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/yasyf/daemonkit/daemon"
)

func TestOwnedRuntimeUsesExactEmbeddedReadinessAndSettlement(t *testing.T) {
	runtime := newExactRuntime(nil)
	var cleanupCalls atomic.Int32
	owned, err := OwnRuntime(runtime, func() error {
		cleanupCalls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	process := &daemon.EmbeddedProcess{}
	started := make(chan error, 1)
	go func() {
		started <- process.Start(t.Context(), func(context.Context) (daemon.EmbeddedRuntime, error) {
			return owned, nil
		})
	}()
	<-runtime.runStarted
	select {
	case err := <-started:
		t.Fatalf("Start returned before exact readiness: %v", err)
	default:
	}
	runtime.publishReady()
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	if err := process.Ready(t.Context()); err != nil {
		t.Fatalf("Ready = %v", err)
	}
	if err := process.Close(t.Context()); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if err := process.Wait(t.Context()); err != nil {
		t.Fatalf("Wait = %v", err)
	}
	if calls := cleanupCalls.Load(); calls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", calls)
	}
	if calls := runtime.closeCalls.Load(); calls != 1 {
		t.Fatalf("runtime Close calls = %d, want 1", calls)
	}
}

func TestOwnedRuntimeStartupCancellationJoinsCleanup(t *testing.T) {
	runtime := newExactRuntime(nil)
	var cleanupCalls atomic.Int32
	owned, err := OwnRuntime(runtime, func() error {
		cleanupCalls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	process := &daemon.EmbeddedProcess{}
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan error, 1)
	go func() {
		started <- process.Start(ctx, func(context.Context) (daemon.EmbeddedRuntime, error) {
			return owned, nil
		})
	}()
	<-runtime.runStarted
	cancel()
	if err := <-started; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start = %v, want context.Canceled", err)
	}
	select {
	case <-runtime.done:
	default:
		t.Fatal("Start returned before canceled runtime settlement")
	}
	if err := process.Wait(t.Context()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = %v, want replayed context.Canceled", err)
	}
	if calls := cleanupCalls.Load(); calls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", calls)
	}
}

func TestOwnedRuntimeCloseCancellationStillJoinsCleanup(t *testing.T) {
	runtime := newExactRuntime(nil)
	runtime.publishReady()
	var cleanupCalls atomic.Int32
	owned, err := OwnRuntime(runtime, func() error {
		cleanupCalls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	process := &daemon.EmbeddedProcess{}
	if err := process.Start(t.Context(), func(context.Context) (daemon.EmbeddedRuntime, error) {
		return owned, nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := process.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close = %v, want context.Canceled after exact settlement", err)
	}
	select {
	case <-runtime.done:
	default:
		t.Fatal("Close returned caller cancellation before runtime settlement")
	}
	if calls := cleanupCalls.Load(); calls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", calls)
	}
}

func TestOwnedRuntimeConcurrentCloseAndWaitReplayExactResult(t *testing.T) {
	terminalErr := errors.New("runtime terminal failure")
	cleanupErr := errors.New("dependency cleanup failure")
	runtime := newExactRuntime(terminalErr)
	runtime.publishReady()
	runtime.closeEntered = make(chan struct{})
	runtime.closeRelease = make(chan struct{})
	var cleanupCalls atomic.Int32
	owned, err := OwnRuntime(runtime, func() error {
		cleanupCalls.Add(1)
		return cleanupErr
	})
	if err != nil {
		t.Fatal(err)
	}
	process := &daemon.EmbeddedProcess{}
	if err := process.Start(t.Context(), func(context.Context) (daemon.EmbeddedRuntime, error) {
		return owned, nil
	}); err != nil {
		t.Fatal(err)
	}

	const callers = 8
	results := make(chan error, callers*2)
	var callersWG sync.WaitGroup
	for range callers {
		callersWG.Add(2)
		go func() {
			defer callersWG.Done()
			results <- process.Close(t.Context())
		}()
		go func() {
			defer callersWG.Done()
			results <- process.Wait(t.Context())
		}()
	}
	<-runtime.closeEntered
	select {
	case err := <-results:
		t.Fatalf("terminal call returned before physical Close settlement: %v", err)
	default:
	}
	close(runtime.closeRelease)
	callersWG.Wait()
	close(results)
	for err := range results {
		if !errors.Is(err, terminalErr) || !errors.Is(err, cleanupErr) {
			t.Fatalf("concurrent terminal result = %v, want runtime and cleanup failures", err)
		}
	}
	if calls := runtime.closeCalls.Load(); calls != 1 {
		t.Fatalf("runtime Close calls = %d, want 1", calls)
	}
	if calls := cleanupCalls.Load(); calls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", calls)
	}
}

func TestOwnRuntimeRequiresRuntimeAndCleanup(t *testing.T) {
	runtime := newExactRuntime(nil)
	if _, err := OwnRuntime(nil, func() error { return nil }); err == nil {
		t.Fatal("OwnRuntime accepted nil runtime")
	}
	if _, err := OwnRuntime(runtime, nil); err == nil {
		t.Fatal("OwnRuntime accepted nil cleanup")
	}
}

type exactRuntime struct {
	ready      chan struct{}
	stop       chan struct{}
	done       chan struct{}
	runStarted chan struct{}
	terminal   error

	closeEntered chan struct{}
	closeRelease chan struct{}

	mu         sync.Mutex
	result     error
	readyOnce  sync.Once
	stopOnce   sync.Once
	runOnce    sync.Once
	finishOnce sync.Once
	closeOnce  sync.Once
	closeCalls atomic.Int32
}

func newExactRuntime(terminal error) *exactRuntime {
	return &exactRuntime{
		ready: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{}),
		runStarted: make(chan struct{}), terminal: terminal,
	}
}

func (r *exactRuntime) Run(ctx context.Context) error {
	ran := false
	r.runOnce.Do(func() {
		ran = true
		close(r.runStarted)
	})
	if !ran {
		return daemon.ErrRuntimeStarted
	}
	var result error
	select {
	case <-ctx.Done():
		result = errors.Join(r.terminal, ctx.Err())
	case <-r.stop:
		result = r.terminal
	}
	r.mu.Lock()
	r.result = result
	r.mu.Unlock()
	r.finishOnce.Do(func() { close(r.done) })
	return result
}

func (r *exactRuntime) WaitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.ready:
		return nil
	case <-r.done:
		return errors.Join(daemon.ErrRuntimeNotReady, r.terminalResult())
	}
}

func (r *exactRuntime) Close(ctx context.Context) error {
	r.closeCalls.Add(1)
	r.closeOnce.Do(func() {
		if r.closeEntered != nil {
			close(r.closeEntered)
		}
		if r.closeRelease != nil {
			<-r.closeRelease
		}
		r.stopOnce.Do(func() { close(r.stop) })
	})
	return r.Wait(ctx)
}

func (r *exactRuntime) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return r.terminalResult()
	}
}

func (r *exactRuntime) terminalResult() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.result
}

func (r *exactRuntime) publishReady() {
	r.readyOnce.Do(func() { close(r.ready) })
}
