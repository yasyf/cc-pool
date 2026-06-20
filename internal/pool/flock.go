package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

// flockPollInterval is how often flockAcquire retries a contended lock while
// waiting for the holder (in another process) to release.
const flockPollInterval = 25 * time.Millisecond

// flockHandle owns an acquired advisory lock; release drops it.
type flockHandle struct {
	f *os.File
}

// release drops the advisory lock and closes the file. The lock file itself is
// left on disk on purpose: unlinking it under flock races other processes that
// have it open.
func (h *flockHandle) release() {
	_ = unix.Flock(int(h.f.Fd()), unix.LOCK_UN)
	_ = h.f.Close()
}

// flockAcquire takes an exclusive cross-process advisory lock on path, creating
// the file (and its parent dir) if needed. It blocks until the lock is held or
// ctx is done, polling rather than blocking in the syscall so cancellation is
// observed and no goroutine is leaked on a stuck holder.
func flockAcquire(ctx context.Context, path string) (*flockHandle, error) {
	//nolint:gosec // G703: path is a cc-pool-owned lock path under the state dir, not user-tainted input
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create lock dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // G304: path is a cc-pool-owned lock file under the state dir, not user input
	if err != nil {
		return nil, fmt.Errorf("open lock %s: %w", path, err)
	}
	for {
		err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return &flockHandle{f: f}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			_ = f.Close()
			return nil, fmt.Errorf("flock %s: %w", path, err)
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, fmt.Errorf("flock %s: %w", path, ctx.Err())
		case <-time.After(flockPollInterval):
		}
	}
}
