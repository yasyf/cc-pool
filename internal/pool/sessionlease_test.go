package pool

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/lease"
)

func muxDir(n int) string { return filepath.Join(MuxRootDir(), AccountDirName(n)) }

// bridgeAccount makes a mux fuse account whose ConfigDir is a real bridge symlink
// into the mux root, so SessionLeaseDir must key on the subtree.
func bridgeAccount(t *testing.T, id int) store.Account {
	t.Helper()
	a := store.Account{ID: id, ConfigDir: AccountDir(id), OverlayKind: "nfs"}
	if err := os.MkdirAll(filepath.Dir(a.ConfigDir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(muxDir(id), a.ConfigDir); err != nil {
		t.Fatal(err)
	}
	return a
}

// TestSessionLeaseDir pins the ROW-AWARE lease-key rule (F1): a fuse row keys on
// its mux subtree ONLY once its ConfigDir is the bridge symlink; a legacy per-dir
// (or bare/absent) fuse row and every symlink/File Provider row key on ConfigDir —
// the dir the holder actually mounts and fences at that point in the cutover.
func TestSessionLeaseDir(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	// A migrated MUX fuse row → the mux subtree, and it round-trips through the wire
	// translation back to the account ConfigDir (byte-identical to the holder's req.Dir).
	mux := bridgeAccount(t, 7)
	if got := SessionLeaseDir(mux); got != muxDir(7) {
		t.Fatalf("mux fuse row: SessionLeaseDir = %q, want the subtree %q", got, muxDir(7))
	}
	if got := ConfigDirForMount(SessionLeaseDir(mux)); got != mux.ConfigDir {
		t.Fatalf("mux lease dir not wire-identical: ConfigDirForMount = %q, want %q", got, mux.ConfigDir)
	}

	// A LEGACY per-dir fuse row: ConfigDir is a real dir (holder-mounted and fenced
	// there), NOT a bridge symlink → leases ConfigDir, so select/env/login and
	// migration all key on the live legacy dir.
	legacy := store.Account{ID: 8, ConfigDir: AccountDir(8), OverlayKind: "nfs"}
	if err := os.MkdirAll(legacy.ConfigDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if got := SessionLeaseDir(legacy); got != legacy.ConfigDir {
		t.Fatalf("legacy fuse row: SessionLeaseDir = %q, want the ConfigDir %q", got, legacy.ConfigDir)
	}

	// A bare half-migrated (absent) fuse dir also keys on ConfigDir.
	absent := store.Account{ID: 9, ConfigDir: AccountDir(9), OverlayKind: "fskit"}
	if got := SessionLeaseDir(absent); got != absent.ConfigDir {
		t.Fatalf("absent fuse dir: SessionLeaseDir = %q, want the ConfigDir %q", got, absent.ConfigDir)
	}

	for _, kind := range []string{"symlink", "fileprovider", "zfs"} {
		a := store.Account{ID: 3, ConfigDir: AccountDir(3), OverlayKind: kind}
		if got := SessionLeaseDir(a); got != a.ConfigDir {
			t.Fatalf("kind=%q: SessionLeaseDir = %q, want the ConfigDir %q", kind, got, a.ConfigDir)
		}
	}
}

// TestSeizeSessionLease pins the (a) local-op fence: a free key seizes and a live
// handout (a held shared lease) bounces ErrBusy so a cc-pool-performed destructive
// op defers instead of racing a live session.
func TestSeizeSessionLease(t *testing.T) {
	root := filepath.Join(t.TempDir(), "leases")
	m := &Manager{LeaseRoot: func() (string, error) { return root, nil }}
	a := store.Account{ID: 4, ConfigDir: AccountDir(4), OverlayKind: "symlink"}

	fence, err := m.SeizeSessionLease(a)
	if err != nil {
		t.Fatalf("SeizeSessionLease on a free key = %v, want nil", err)
	}
	held, err := m.SessionLeaseHeld(a)
	if err != nil || !held {
		t.Fatalf("SessionLeaseHeld under a held fence = (%v, %v), want (true, nil)", held, err)
	}
	if err := fence.Release(); err != nil {
		t.Fatal(err)
	}

	// A live handout (the launcher/agent holds the shared lease) bounces the seize.
	h, err := lease.Acquire(root, SessionLeaseDir(a), HolderOwner)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Close() }()
	if _, err := m.SeizeSessionLease(a); !errors.Is(err, lease.ErrBusy) {
		t.Fatalf("SeizeSessionLease under a live handout = %v, want lease.ErrBusy", err)
	}
	if held, _ := m.SessionLeaseHeld(a); !held {
		t.Fatal("SessionLeaseHeld under a live handout = false, want true")
	}
}
