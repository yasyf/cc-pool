package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/syncservice"
)

// syncConsumerPlan carries the two executables the consumer half needs from
// the publication half, so neither is resolved twice nor parked on Server
// between the two calls.
type syncConsumerPlan struct {
	workerExecutable   string
	synckitdExecutable string
}

// setupSyncPublication wires host sync's credential-write publication onto the
// daemon and settles whatever the previous generation left pending. Always
// constructed — every acting path re-reads the sync_enabled meta, so enable
// needs no daemon restart — and it must run before serve admits any handler:
// handlers read s.authKind unlocked. It opens no socket: the resident helper
// stays unspawned until startSyncConsumer, so the foreign-lane claim scan that
// runs between the two halves cannot observe a reservation of ours.
func (s *Server) setupSyncPublication(ctx context.Context) (syncConsumerPlan, error) {
	self, err := s.resolveSyncSelf(ctx)
	if err != nil {
		return syncConsumerPlan{}, err
	}
	synckitdExecutable, err := resolveSynckitdExecutable()
	if err != nil {
		return syncConsumerPlan{}, err
	}
	if s.disposableWorkers == nil {
		return syncConsumerPlan{}, errors.New("sync requires daemon disposable workers")
	}
	workerExecutable, err := os.Executable()
	if err != nil {
		return syncConsumerPlan{}, fmt.Errorf("resolve credential settlement worker executable: %w", err)
	}

	registry := hostsync.NewRegistryFile(pool.SyncDir())
	stampDir := pool.SyncStampsDir()
	settler := newCredentialWriteSettler(
		s.disposableWorkers,
		workerExecutable,
		s.syncEnabled,
		*registry,
		stampDir,
		self,
	)
	previousBuilder := s.m.BuildCredentialWritePublication
	previousSettler := s.m.SettleCredentialWrite
	wired := false
	defer func() {
		if wired {
			return
		}
		s.m.BuildCredentialWritePublication = previousBuilder
		s.m.SettleCredentialWrite = previousSettler
	}()
	s.m.BuildCredentialWritePublication = credentialWritePublicationBuilder(self)
	s.m.SettleCredentialWrite = settler.Settle
	if err := s.m.SettlePendingCredentialWrites(ctx); err != nil {
		return syncConsumerPlan{}, fmt.Errorf("settle pending credential writes: %w", err)
	}

	worker, err := hostsync.NewWorkerClient(s.disposableWorkers, workerExecutable, synckitdExecutable)
	if err != nil {
		return syncConsumerPlan{}, err
	}
	s.syncSelf = self
	s.syncAuthKind = worker.AuthKind
	wired = true
	return syncConsumerPlan{
		workerExecutable:   workerExecutable,
		synckitdExecutable: synckitdExecutable,
	}, nil
}

// startSyncConsumer spawns the resident helper and publishes the converge
// path, which handlers read unlocked through s.syncPull. Readiness is
// exposure: synckitd's watcher drives a converge the moment the helper
// answers, and that converge's disposable worker mints an owner of its own —
// so every foreign lane must already be claimed when this runs, or the claim
// scan retires a live worker's pending add as a dead predecessor's.
func (s *Server) startSyncConsumer(ctx context.Context, plan syncConsumerPlan) error {
	if err := s.startSyncHelper(ctx, plan.workerExecutable, plan.synckitdExecutable); err != nil {
		return err
	}
	client := syncservice.NewClient(syncservice.Resident(hostsync.SyncServiceID))
	s.syncClient = client
	s.syncPull = func(ctx context.Context) error {
		enabled, err := s.syncEnabled()
		if err != nil {
			return fmt.Errorf("read sync enablement: %w", err)
		}
		if !enabled {
			return nil
		}
		_, err = client.Reconcile(ctx, "")
		return err
	}
	return nil
}

func resolveSynckitdExecutable() (string, error) {
	executable, err := exec.LookPath("synckitd")
	if err != nil {
		return "", fmt.Errorf("resolve installed synckitd executable: %w", err)
	}
	if !filepath.IsAbs(executable) || filepath.Clean(executable) != executable {
		return "", errors.New("installed synckitd executable is not clean and absolute")
	}
	return executable, nil
}

// authKind classifies a needs-login at persist time in the disposable worker.
// An absent worker or an unprovable registry owner is an error, never Owned.
func (s *Server) authKind(ctx context.Context, a store.Account) (store.AuthKind, error) {
	if a.AccountUUID == "" {
		return store.AuthKindOwned, nil
	}
	enabled, err := s.syncEnabled()
	if err != nil {
		return "", fmt.Errorf("read sync enablement: %w", err)
	}
	if !enabled {
		return store.AuthKindOwned, nil
	}
	if s.syncAuthKind == nil {
		return "", errors.New("auth-kind worker is unavailable")
	}
	return s.syncAuthKind(ctx, a.ID, a.AccountUUID)
}

// syncEnabledBool adapts syncEnabled for acting paths; a meta read
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
	if host == "" {
		return "", fmt.Errorf("resolve sync self: kernel hostname is empty")
	}
	return host, nil
}
