package daemon

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/synckit/converge"
)

// serverSessions answers the hostsync Sessions seam from daemon truth: a live
// session, select reservation, or in-flight conversion on any row sharing the
// uuid reads busy; errors propagate — teardown fails closed — see ccn 10bf17d.
type serverSessions struct{ s *Server }

// Busy implements hostsync.Sessions.
func (ss serverSessions) Busy(ctx context.Context, uuid string) (bool, string, error) {
	rows, err := ss.s.m.Store.AccountsByUUID(uuid)
	if err != nil {
		return false, "", fmt.Errorf("resolve accounts for %s: %w", uuid, err)
	}
	if len(rows) == 0 {
		return false, "", nil
	}
	sessions, err := ss.s.scan(ctx)
	if err != nil {
		return false, "", fmt.Errorf("scan sessions: %w", err)
	}
	for _, a := range rows {
		switch {
		case ss.s.cl.held(a.ID):
			return true, fmt.Sprintf("acct-%02d overlay conversion in flight", a.ID), nil
		case ss.s.cl.reservedCount(a.ID) > 0:
			return true, fmt.Sprintf("acct-%02d reserved by a launching select", a.ID), nil
		}
		if n := procscan.CountByConfigDir(sessions, a.ConfigDir); n > 0 {
			return true, fmt.Sprintf("acct-%02d has %d live session(s)", a.ID, n), nil
		}
		// A select handout holds the session lease before its claude is visible to
		// procscan; a held lease reports busy so a peer-driven removal defers. An
		// UNREADABLE lease root must fail CLOSED — read busy and surface the error — so
		// destructive removal never proceeds on an indeterminate probe.
		switch held, herr := ss.s.m.SessionLeaseHeld(a); {
		case herr != nil:
			return true, fmt.Sprintf("acct-%02d session-lease probe failed, assuming busy", a.ID),
				fmt.Errorf("probe session lease for acct-%02d: %w", a.ID, herr)
		case held:
			return true, fmt.Sprintf("acct-%02d has a held session lease (a live session or launch)", a.ID), nil
		}
	}
	return false, "", nil
}

var _ hostsync.Sessions = serverSessions{}

// setupSync constructs the host-sync engine and wires it onto the daemon.
// Always constructed — every acting path re-reads the sync_enabled meta, so
// enable needs no daemon restart — and it must run before serve spawns any
// worker or handler: they read s.sync unlocked.
func (s *Server) setupSync(ctx context.Context) error {
	if s.syncSocket == "" {
		return fmt.Errorf("no sync socket path configured")
	}
	manifestPath, err := hostsync.ManifestPath()
	if err != nil {
		return err
	}
	self, err := s.resolveSyncSelf(ctx)
	if err != nil {
		return err
	}

	svc := &hostsync.Service{
		M:        s.m,
		Registry: hostsync.NewRegistryFile(pool.SyncDir()),
		StampDir: pool.SyncStampsDir(),
		Log:      s.log,
		Locals:   hostsync.ManagerLocals(s.m, self, time.Now),
		Mesh:     hostsync.SynckitMesh{},
		Sessions: serverSessions{s: s},
		Status:   converge.NewPeerStatus(),
		Fetcher:  hostsync.NewSSHFetcher(),
	}
	pull := func(ctx context.Context, uuid string, chain hostsync.ChainStamp, localExpiresAt int64, localHash string, peers []string) (*creds.Credential, error) {
		return hostsync.FetchCredential(ctx, hostsync.PeerTransport, uuid, chain, localExpiresAt, localHash, peers)
	}
	svc.Driver = hostsync.NewDriver(svc, hostsync.DriverDeps{
		Store:      s.m.Store,
		Cred:       s.m,
		LocalIndex: hostsync.ManagerLocalIndex(s.m),
		Materialize: func(ctx context.Context, v hostsync.AccountValue, peers []string) (hostsync.MaterializeResult, error) {
			// A materializing account has no local chain: any verified envelope wins.
			noLocal := func(ctx context.Context, uuid string, chain hostsync.ChainStamp, peers []string) (*creds.Credential, error) {
				return pull(ctx, uuid, chain, 0, "", peers)
			}
			return svc.Materialize(ctx, v, peers, noLocal, manifestPath)
		},
		Pull: pull,
	})

	if err := s.startSyncServer(ctx, svc); err != nil {
		return err
	}
	s.sync = newSyncGate(svc, self, s.syncEnabledBool, s.log)
	s.syncPull = func(ctx context.Context) error {
		if !s.syncEnabledBool() {
			return nil
		}
		_, err := svc.Converge(ctx, "")
		return err
	}
	mirror := newCredMirror(s.gatedNote(svc), self, s.log)
	s.m.OnCredWrite = mirror.Hook
	s.wg.Add(1)
	go func() { defer s.wg.Done(); mirror.Run(ctx) }()
	return nil
}

// gatedNote wraps NoteCredWrite behind the per-call enable check, so a
// disabled pool's cred writes leave no registry or stamp residue.
func (s *Server) gatedNote(svc *hostsync.Service) func(context.Context, string, hostsync.ChainStamp) error {
	return func(ctx context.Context, uuid string, chain hostsync.ChainStamp) error {
		if !s.syncEnabledBool() {
			return nil
		}
		return svc.NoteCredWrite(ctx, uuid, chain)
	}
}

// syncEnabledBool adapts syncEnabled for the gate and mirror; a meta read
// failure reads disabled — sync must never take down single-host pooling.
func (s *Server) syncEnabledBool() bool {
	on, err := s.syncEnabled()
	if err != nil {
		s.log.Printf("read %s meta: %v", metaSyncEnabled, err)
		return false
	}
	return on
}

// resolveSyncSelf names this host in the registry: the mesh ssh target when
// joined — peers dial the chain holder by this name — else os.Hostname().
func (s *Server) resolveSyncSelf(ctx context.Context) (string, error) {
	self, _, merr := (hostsync.SynckitMesh{}).Resolve(ctx)
	if merr == nil {
		return self, nil
	}
	if s.syncEnabledBool() {
		s.log.Printf("sync self falls back to the hostname: %v", merr)
	}
	host, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("resolve sync self: %w", err)
	}
	return host, nil
}
