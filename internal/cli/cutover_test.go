package cli

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/daemonkit/proc"
)

func TestStoreCutoverTreatsDefaultDatabaseAliasAsLive(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pool.DBPath(), []byte("live database identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(home, "pool-alias.db")
	if err := os.Symlink(pool.DBPath(), alias); err != nil {
		t.Fatal(err)
	}
	lock, err := (proc.FileLockSpec{
		Path: pool.SocketPath() + ".lock", Mode: proc.FileLockExclusive, Deadline: time.Second,
	}).TryAcquire()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Close() }()

	cmd := newStoreCutoverCmd()
	cmd.SetArgs([]string{"--database", alias})
	err = cmd.Execute()
	if !errors.Is(err, proc.ErrLockBusy) {
		t.Fatalf("aliased default database cutover = %v, want live socket ErrLockBusy", err)
	}
}
