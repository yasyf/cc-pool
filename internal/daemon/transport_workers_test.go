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
