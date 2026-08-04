package tenantfs

import (
	"context"
	"errors"
	"sync"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/holder"
)

var errLaneNotStarted = errors.New("tenantfs: tenant lane is not started")

// Lane owns the tenant daemon the signed app serves beside FuseKit's own. Both
// daemons live in the app's process; they collide over nothing, because every
// path daemonkit derives — socket, flock, record file, state dir — derives from
// a Label, and each signal registration is a serve's own.
type Lane struct {
	mu      sync.Mutex
	started bool
	stop    context.CancelFunc
	done    chan struct{}
	result  error
}

// Start serves the tenant lane in the background and returns once it is
// admitting. ctx bounds only the wait; the lane runs until Close.
func (l *Lane) Start(ctx context.Context, controller *holder.LocalTenantController) error {
	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		return errors.New("tenantfs: tenant lane already started")
	}
	l.started = true
	serveCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
	l.stop, l.done = stop, make(chan struct{})
	done := l.done
	l.mu.Unlock()

	admitting := make(chan struct{})
	go func() {
		defer close(done)
		_, err := daemonkit.Serve(serveCtx, Daemon(), func(daemonkit.Ctx) (daemonkit.Product, error) {
			close(admitting)
			return NewProduct(controller), nil
		})
		l.mu.Lock()
		l.result = err
		l.mu.Unlock()
	}()
	select {
	case <-admitting:
		return nil
	case <-done:
		stop()
		return errors.Join(errors.New("tenantfs: tenant lane stopped before admitting"), l.Wait(ctx))
	case <-ctx.Done():
		stop()
		return ctx.Err()
	}
}

// Close drains the tenant lane and joins its serve.
func (l *Lane) Close(ctx context.Context) error {
	l.mu.Lock()
	stop := l.stop
	l.mu.Unlock()
	if stop == nil {
		return errLaneNotStarted
	}
	stop()
	return l.Wait(ctx)
}

// Wait joins the tenant lane and replays its terminal result.
func (l *Lane) Wait(ctx context.Context) error {
	l.mu.Lock()
	done := l.done
	l.mu.Unlock()
	if done == nil {
		return errLaneNotStarted
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.result
	}
}
