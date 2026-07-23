package holderbridge

import (
	"context"
	"errors"
	"sync"

	"github.com/yasyf/daemonkit/daemon"
)

// OwnRuntime binds dependency cleanup to exact runtime settlement.
func OwnRuntime(
	runtime daemon.EmbeddedRuntime,
	cleanup func() error,
) (daemon.EmbeddedRuntime, error) {
	if runtime == nil || cleanup == nil {
		return nil, errors.New("FuseKit runtime: owned runtime and cleanup are required")
	}
	return &ownedRuntime{
		EmbeddedRuntime: runtime,
		cleanup:         cleanup,
	}, nil
}

type ownedRuntime struct {
	daemon.EmbeddedRuntime

	cleanup     func() error
	cleanupOnce sync.Once
	cleanupErr  error
}

func (r *ownedRuntime) Run(ctx context.Context) error {
	return errors.Join(r.EmbeddedRuntime.Run(ctx), r.settleCleanup())
}

func (r *ownedRuntime) Wait(ctx context.Context) error {
	waitErr := r.EmbeddedRuntime.Wait(ctx)
	if waitErr != nil && ctx.Err() != nil {
		return waitErr
	}
	return errors.Join(waitErr, r.settleCleanup())
}

func (r *ownedRuntime) settleCleanup() error {
	r.cleanupOnce.Do(func() {
		r.cleanupErr = r.cleanup()
	})
	return r.cleanupErr
}

var _ daemon.EmbeddedRuntime = (*ownedRuntime)(nil)
