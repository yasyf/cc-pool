package pool

import (
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/lease"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// SessionLeaseDir returns the directory a launcher takes account a's session
// lease on. It is ROW-AWARE: the key is derived from the dir's CURRENT mounted
// shape, so it stays byte-identical to the dir the holder journals and fences.
//
//   - A fuse row whose ConfigDir is the mux BRIDGE SYMLINK leases its mux
//     subtree mount (MuxRootDir()/acct-NN), the exact req.Dir the holder mounts
//     (the ConfigDir is only a bridge symlink into it).
//   - A fuse row not yet showing the bridge symlink (a bare or absent dir, mid
//     setup) leases its own ConfigDir until the symlink lands.
//   - A symlink or File Provider account has no holder mount, so it leases its
//     own ConfigDir — a uniform provenance key.
func SessionLeaseDir(a store.Account) string {
	return SessionLeaseDirFor(a.ID, a.ConfigDir, a.OverlayKind)
}

// SessionLeaseDirFor is SessionLeaseDir's primitive, keyed on the raw row fields
// so a pending add (no store.Account yet) computes the same key.
func SessionLeaseDirFor(id int, configDir, overlayKind string) string {
	b, err := fkoverlay.Parse(overlayKind)
	return SessionLeaseDirForShape(id, configDir, err == nil && b.IsFuse())
}

// SessionLeaseDirForShape derives the key from a row's raw shape inputs, reading
// IsBridgeSymlink so the key tracks the dir's current mounted shape.
func SessionLeaseDirForShape(id int, configDir string, fuse bool) string {
	if fuse && IsBridgeSymlink(configDir) {
		return filepath.Join(MuxRootDir(), AccountDirName(id))
	}
	return configDir
}

// SeizeSessionLease takes account a's current-shape session-lease key EXCLUSIVELY
// for a destructive op cc-pool performs itself on an unmounted/plain dir (bare-dir
// drain, dir replacement, cred move, symlink/FP removal). A held lease (a live
// session or its detached handout agent) returns a lease.ErrBusy error — defer.
// The caller HOLDS the returned fence across the whole mutation (a TOCTOU fence)
// and Release()s after. NEVER wrap a holder-delegated teardown of the SAME key in
// this fence — the holder's own Seize would self-bounce ClassBusy forever; a
// sequenced op does the holder teardown first (mountd.ErrBusy is that gate) and
// Seizes only after the holder confirms.
func (m *Manager) SeizeSessionLease(a store.Account) (*lease.Fence, error) {
	root, err := m.leaseRoot()
	if err != nil {
		return nil, err
	}
	return lease.Seize(root, SessionLeaseDir(a))
}

// SessionLeaseHeld reports whether account a's session-lease key is currently
// held — a non-destructive read (lease.Probe never seizes or unlinks) for
// advisory busy/UX surfaces. It NEVER stands in for SeizeSessionLease's fence on
// a destructive path.
func (m *Manager) SessionLeaseHeld(a store.Account) (bool, error) {
	root, err := m.leaseRoot()
	if err != nil {
		return false, err
	}
	held, _, err := lease.Probe(root, SessionLeaseDir(a))
	return held, err
}
