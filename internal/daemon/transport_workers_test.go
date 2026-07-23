package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/drain"
)

func TestServerWorkersWaitHonorsShutdownDeadline(t *testing.T) {
	server := &Server{syncIntake: &drain.Intake{}}
	server.wg.Add(1)
	t.Cleanup(server.wg.Done)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := (&serverWorkers{owner: server}).Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait() error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Wait() elapsed = %s, ignored shutdown deadline", elapsed)
	}
}

func TestServerWorkersCancelSettlesHolderMonitorBeforeLifetimeCancellation(t *testing.T) {
	server := &Server{syncIntake: &drain.Intake{}}
	lifetime, cancelLifetime := context.WithCancel(t.Context())
	defer cancelLifetime()
	monitorCtx, cancelMonitor := context.WithCancel(lifetime)
	server.holderMonitorMu.Lock()
	server.holderMonitorCancel = cancelMonitor
	server.holderMonitorMu.Unlock()
	server.wg.Add(1)
	go server.monitorHolderSession(monitorCtx, make(chan struct{}))

	workers := &serverWorkers{owner: server}
	workers.Cancel()
	waitCtx, cancelWait := context.WithTimeout(t.Context(), time.Second)
	defer cancelWait()
	if err := workers.Wait(waitCtx); err != nil {
		t.Fatalf("Wait() after Cancel() = %v", err)
	}
	if lifetime.Err() != nil {
		t.Fatalf("holder monitor required lifetime cancellation: %v", lifetime.Err())
	}
}
