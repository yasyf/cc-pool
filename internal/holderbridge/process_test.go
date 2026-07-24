package holderbridge

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestProcessUsesExactRuntimeReadinessAndSettlement(t *testing.T) {
	runtime := newExactRuntime(nil)
	process := &Process{}
	started := make(chan error, 1)
	go func() {
		started <- process.Start(t.Context(), func(context.Context) (ProcessRuntime, error) {
			return runtime, nil
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
	if calls := runtime.closeCalls.Load(); calls != 1 {
		t.Fatalf("runtime Close calls = %d, want 1", calls)
	}
}

func TestProcessStartupCancellationJoinsRuntime(t *testing.T) {
	runtime := newExactRuntime(nil)
	process := &Process{}
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan error, 1)
	go func() {
		started <- process.Start(ctx, func(context.Context) (ProcessRuntime, error) {
			return runtime, nil
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
	if err := process.Wait(t.Context()); err != nil {
		t.Fatalf("Wait = %v", err)
	}
}

func TestProcessConcurrentCloseAndWaitReplayExactResult(t *testing.T) {
	terminalErr := errors.New("runtime terminal failure")
	runtime := newExactRuntime(terminalErr)
	runtime.publishReady()
	runtime.closeEntered = make(chan struct{})
	runtime.closeRelease = make(chan struct{})
	process := &Process{}
	if err := process.Start(t.Context(), func(context.Context) (ProcessRuntime, error) {
		return runtime, nil
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
		if !errors.Is(err, terminalErr) {
			t.Fatalf("concurrent terminal result = %v, want %v", err, terminalErr)
		}
	}
}

func TestProcessRejectsInvalidOrRepeatedStart(t *testing.T) {
	process := &Process{}
	if err := process.Start(t.Context(), nil); err == nil {
		t.Fatal("Start accepted nil constructor")
	}
	want := errors.New("construct failed")
	if err := process.Start(t.Context(), func(context.Context) (ProcessRuntime, error) {
		return nil, want
	}); !errors.Is(err, want) {
		t.Fatalf("Start = %v, want %v", err, want)
	}
	if err := process.Start(t.Context(), func(context.Context) (ProcessRuntime, error) {
		return newExactRuntime(nil), nil
	}); !errors.Is(err, errProcessStarted) {
		t.Fatalf("repeated Start = %v, want process-started error", err)
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
	r.runOnce.Do(func() { close(r.runStarted) })
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
		return errors.New("runtime stopped before readiness")
	}
}

func (r *exactRuntime) Close(ctx context.Context) error {
	r.closeOnce.Do(func() {
		r.closeCalls.Add(1)
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
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.result
	}
}

func (r *exactRuntime) publishReady() { r.readyOnce.Do(func() { close(r.ready) }) }
