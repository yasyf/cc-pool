package holderbridge

import (
	"context"
	"errors"
	"sync"
)

var (
	errProcessStarted    = errors.New("FuseKit runtime process already started")
	errProcessNotStarted = errors.New("FuseKit runtime process is not started")
)

// ProcessRuntime is the exact signed-holder lifecycle consumed by Process.
type ProcessRuntime interface {
	Run(context.Context) error
	WaitReady(context.Context) error
	Close(context.Context) error
	Wait(context.Context) error
}

// Process owns one signed FuseKit runtime invocation for the app process.
type Process struct {
	mu      sync.Mutex
	started bool
	runtime ProcessRuntime
	done    chan struct{}
	result  error
}

// Start constructs, runs, and waits for one exact holder publication.
func (p *Process) Start(ctx context.Context, construct func(context.Context) (ProcessRuntime, error)) error {
	if construct == nil {
		return errors.New("FuseKit runtime process requires a constructor")
	}
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return errProcessStarted
	}
	p.started = true
	p.done = make(chan struct{})
	p.mu.Unlock()

	runtime, err := construct(ctx)
	if err != nil {
		p.finish(err)
		return err
	}
	if runtime == nil {
		err = errors.New("FuseKit runtime process constructor returned nil")
		p.finish(err)
		return err
	}
	p.mu.Lock()
	p.runtime = runtime
	p.mu.Unlock()
	runCtx := context.WithoutCancel(ctx)
	go func() { p.finish(runtime.Run(runCtx)) }()
	if err := runtime.WaitReady(ctx); err != nil {
		settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), RuntimeShutdownTimeout)
		defer cancel()
		return errors.Join(err, runtime.Close(settlement), p.Wait(settlement))
	}
	return nil
}

// Ready waits for the holder's committed runtime publication.
func (p *Process) Ready(ctx context.Context) error {
	runtime, err := p.current()
	if err != nil {
		return err
	}
	return runtime.WaitReady(ctx)
}

// Close drains and joins the holder runtime.
func (p *Process) Close(ctx context.Context) error {
	runtime, err := p.current()
	if err != nil {
		return err
	}
	return errors.Join(runtime.Close(ctx), p.Wait(ctx))
}

// Wait joins the holder runtime and replays its terminal result.
func (p *Process) Wait(ctx context.Context) error {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return errProcessNotStarted
	}
	done := p.done
	p.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.result
	}
}

func (p *Process) current() (ProcessRuntime, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.started || p.runtime == nil {
		return nil, errProcessNotStarted
	}
	return p.runtime, nil
}

func (p *Process) finish(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done == nil {
		return
	}
	p.result = err
	select {
	case <-p.done:
	default:
		close(p.done)
	}
}
