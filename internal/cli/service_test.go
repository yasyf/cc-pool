package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
)

func swapVar[T any](t *testing.T, target *T, replacement T) {
	t.Helper()
	previous := *target
	*target = replacement
	t.Cleanup(func() { *target = previous })
}

func TestBudgetedKeepsACallerStatedDeadline(t *testing.T) {
	stated := time.Now().Add(time.Minute)
	caller, cancel := context.WithDeadline(context.Background(), stated)
	defer cancel()
	ctx, release := budgeted(caller, time.Hour)
	defer release()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("budgeted dropped the caller's stated deadline")
	}
	if !deadline.Equal(stated) {
		t.Fatalf("budgeted deadline = %v, want the caller's own %v", deadline, stated)
	}
}

func TestBudgetedStatesTheDefaultWhenNoneIsCarried(t *testing.T) {
	const budget = 90 * time.Second
	before := time.Now()
	ctx, release := budgeted(context.Background(), budget)
	after := time.Now()
	defer release()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("budgeted stated no deadline on a context carrying none")
	}
	if deadline.Before(before.Add(budget)) || deadline.After(after.Add(budget)) {
		t.Fatalf("budgeted deadline = %v, want %v out from [%v, %v]", deadline, budget, before, after)
	}
}

func TestInstallDaemonServiceRollsBackHolderOnEnsureFailure(t *testing.T) {
	ensureFailed := errors.New("ensure failed")
	commits, rollbacks := 0, 0
	swapVar(t, &removeLegacyDaemon, func(context.Context) error { return nil })
	swapVar(t, &ensureHolder, func(context.Context) (holderServiceInstall, error) {
		return testHolderInstall{commit: func() { commits++ }, rollback: func() { rollbacks++ }}, nil
	})
	swapVar(t, &ensureDaemonService, func(context.Context) (daemonkit.Ensured, error) {
		return daemonkit.Ensured{}, ensureFailed
	})
	if err := installDaemonService(t.Context()); !errors.Is(err, ensureFailed) {
		t.Fatalf("install = %v, want the ensure failure", err)
	}
	if rollbacks != 1 {
		t.Errorf("holder rollbacks = %d, want exactly 1", rollbacks)
	}
	if commits != 0 {
		t.Errorf("holder commits = %d, want none behind a failed ensure", commits)
	}
}

func daemonkitExecutableDigest(image string) string {
	sum := sha256.Sum256([]byte(image))
	return hex.EncodeToString(sum[:])
}

func TestInstallDaemonServiceCommitsHolderOnEnsureSuccess(t *testing.T) {
	digest := daemonkitExecutableDigest("cc-pool daemon executable")
	tests := []struct {
		name    string
		ensured daemonkit.Ensured
	}{
		{
			name: "ensure started the wanted daemon",
			ensured: daemonkit.Ensured{
				Did:   daemonkit.ActionStarted,
				After: daemonkit.Health{Phase: daemonkit.PhaseReady, Build: digest},
			},
		},
		{
			name: "the wanted daemon already served",
			ensured: daemonkit.Ensured{
				Before: daemonkit.Health{Phase: daemonkit.PhaseReady, Build: digest},
				Did:    daemonkit.ActionNothing,
				After:  daemonkit.Health{Phase: daemonkit.PhaseReady, Build: digest},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			commits, rollbacks := 0, 0
			swapVar(t, &removeLegacyDaemon, func(context.Context) error { return nil })
			swapVar(t, &ensureHolder, func(context.Context) (holderServiceInstall, error) {
				return testHolderInstall{commit: func() { commits++ }, rollback: func() { rollbacks++ }}, nil
			})
			swapVar(t, &ensureDaemonService, func(ctx context.Context) (daemonkit.Ensured, error) {
				if _, stated := ctx.Deadline(); !stated {
					t.Error("ensure ran without a stated deadline")
				}
				return tt.ensured, nil
			})
			if err := installDaemonService(t.Context()); err != nil {
				t.Fatalf("install = %v, want nil", err)
			}
			if commits != 1 {
				t.Errorf("holder commits = %d, want exactly 1", commits)
			}
			if rollbacks != 0 {
				t.Errorf("holder rollbacks = %d, want none behind a successful ensure", rollbacks)
			}
		})
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
