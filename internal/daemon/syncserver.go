package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

// metaSyncEnabled is the store-meta key `ccp sync enable` sets to "1"; read
// per request so enable/disable needs no daemon restart.
const metaSyncEnabled = "sync_enabled"

// syncSocketPerm keeps the sync socket private; rpc.Serve also rejects cross-UID peers.
const syncSocketPerm = 0o600

type onceCloseListener struct {
	net.Listener
	once sync.Once
}

func (l *onceCloseListener) Close() error {
	var err error
	l.once.Do(func() { err = l.Listener.Close() })
	return err
}

// startSyncServer stands up the daemon's second socket — the SyncConsumer
// contract plus the credential fetch method — on a wg-tracked goroutine, returning
// once the socket is bound; the broader sync setup wires the rest of the Service.
func (s *Server) startSyncServer(
	ctx context.Context,
	consumer syncservice.SyncConsumer,
	fetch rpc.Handler,
) error {
	if s.syncIntake == nil {
		s.syncIntake = &drain.Intake{}
	}
	ln, err := serveSyncSocket(ctx, &s.wg, s.syncIntake, s.syncSocket, consumer, fetch, s.log)
	if err != nil {
		return err
	}
	s.syncListener = ln
	return nil
}

// syncEnabled reports whether host sync is on, from the store's sync_enabled meta.
func (s *Server) syncEnabled() (bool, error) {
	v, ok, err := s.m.Store.GetMeta(metaSyncEnabled)
	if err != nil {
		return false, fmt.Errorf("read %s meta: %w", metaSyncEnabled, err)
	}
	return ok && v == "1", nil
}

// serveSyncSocket binds sockPath (0600) and serves the sync dispatcher until
// ctx is done; daemonkit wire admission remains held through terminal delivery.
// Returns once bound.
func serveSyncSocket(ctx context.Context, wg *sync.WaitGroup, intake *drain.Intake, sockPath string, consumer syncservice.SyncConsumer, fetch rpc.Handler, logger *log.Logger) (net.Listener, error) {
	ln, err := rpc.Listen(ctx, sockPath)
	if err != nil {
		return nil, fmt.Errorf("bind sync socket %s: %w", sockPath, err)
	}
	guarded := &onceCloseListener{Listener: ln}
	if err := os.Chmod(sockPath, syncSocketPerm); err != nil {
		_ = guarded.Close()
		return nil, fmt.Errorf("chmod sync socket %s: %w", sockPath, err)
	}
	d := rpc.NewDispatcher()
	syncservice.RegisterConsumer(d, consumer)
	d.Register(hostsync.MethodFetchCredential, fetch)
	server := rpc.NewServer(d)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.Wire.Serve(
			ctx, guarded, func() error { return nil }, intake.Admit, intake.AdmitProtected,
		); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Printf("sync socket serve: %v", err)
		}
	}()
	return guarded, nil
}
