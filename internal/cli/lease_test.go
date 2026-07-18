package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/lease"
)

func tempLeaseRoot(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "leases")
	swapVar(t, &leaseRoot, func() (string, error) { return root, nil })
	return root
}

func TestAcquireSessionLease(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	root := tempLeaseRoot(t)
	a := store.Account{ID: 3, ConfigDir: pool.AccountDir(3), OverlayKind: "symlink"}
	dir := pool.SessionLeaseDir(a)
	h, err := acquireSessionLease(a)
	if err != nil {
		t.Fatal(err)
	}
	if held, _, _ := lease.Probe(root, dir); !held {
		t.Fatal("session lease not held")
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireAndProbeSessionLeaseContextRequiresRemainingBudget(t *testing.T) {
	called := 0
	swapVar(t, &leaseRoot, func() (string, error) {
		called++
		return t.TempDir(), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := acquireAndProbeSessionLeaseContext(ctx, store.Account{ID: 1, ConfigDir: t.TempDir(), OverlayKind: "symlink"})
	if !errors.Is(err, context.DeadlineExceeded) || called != 0 {
		t.Fatalf("err=%v leaseRoot calls=%d", err, called)
	}
}

func TestProbeLeasedDir(t *testing.T) {
	dir := t.TempDir()
	if err := probeLeasedDir(dir, false); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "missing")
	if err := probeLeasedDir(missing, false); err == nil || !strings.Contains(err.Error(), "not answering") {
		t.Fatalf("missing probe = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".cc-pool-probe"), []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
}
