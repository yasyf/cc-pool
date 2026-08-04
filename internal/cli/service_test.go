package cli

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit"
)

func swapVar[T any](t *testing.T, target *T, replacement T) {
	t.Helper()
	previous := *target
	*target = replacement
	t.Cleanup(func() { *target = previous })
}

func TestBudgetedKeepsACallerStatedDeadline(t *testing.T) {
	stated, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer cancel()
	ctx, release := budgeted(stated, time.Hour)
	defer release()
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 2*time.Minute {
		t.Fatalf("budgeted deadline = %v (ok=%t), want the caller's own minute", deadline, ok)
	}
}

func TestBudgetedStatesTheDefaultWhenNoneIsCarried(t *testing.T) {
	ctx, release := budgeted(context.Background(), time.Minute)
	defer release()
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 2*time.Minute {
		t.Fatalf("budgeted deadline = %v (ok=%t), want the stated default", deadline, ok)
	}
}

func TestInstallDaemonServiceRollsBackHolderOnEnsureFailure(t *testing.T) {
	ensureFailed := errors.New("ensure failed")
	rollbacks := 0
	swapVar(t, &removeLegacyDaemon, func(context.Context) error { return nil })
	swapVar(t, &ensureHolder, func(context.Context) (holderServiceInstall, error) {
		return testHolderInstall{rollback: func() { rollbacks++ }}, nil
	})
	swapVar(t, &ensureDaemonService, func(context.Context) (daemonkit.Ensured, error) {
		return daemonkit.Ensured{}, ensureFailed
	})
	if err := installDaemonService(t.Context()); !errors.Is(err, ensureFailed) {
		t.Fatalf("install = %v, want the ensure failure", err)
	}
	if rollbacks != 1 {
		t.Fatalf("holder rollbacks = %d, want exactly 1", rollbacks)
	}
}

func TestInstallDaemonServiceCommitsHolderOnEnsureSuccess(t *testing.T) {
	commits := 0
	swapVar(t, &removeLegacyDaemon, func(context.Context) error { return nil })
	swapVar(t, &ensureHolder, func(context.Context) (holderServiceInstall, error) {
		return testHolderInstall{commit: func() { commits++ }}, nil
	})
	swapVar(t, &ensureDaemonService, func(ctx context.Context) (daemonkit.Ensured, error) {
		if _, stated := ctx.Deadline(); !stated {
			t.Error("ensure ran without a stated deadline")
		}
		return daemonkit.Ensured{After: daemonkit.Health{Build: version.String()}}, nil
	})
	if err := installDaemonService(t.Context()); err != nil {
		t.Fatal(err)
	}
	if commits != 1 {
		t.Fatalf("holder commits = %d, want exactly 1", commits)
	}
}

type testHolderInstall struct {
	commit   func()
	rollback func()
}

func (h testHolderInstall) Commit() {
	if h.commit != nil {
		h.commit()
	}
}

func (h testHolderInstall) Rollback(context.Context) error {
	if h.rollback != nil {
		h.rollback()
	}
	return nil
}
