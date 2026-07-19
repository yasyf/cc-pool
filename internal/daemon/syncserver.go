package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

// metaSyncEnabled is the store-meta key `ccp sync enable` sets to "1"; read
// per request so enable/disable needs no daemon restart.
const metaSyncEnabled = "sync_enabled"

// syncSocketPerm keeps the sync socket private; rpc.Serve also rejects cross-UID peers.
const syncSocketPerm = 0o600

// serverClaims adapts the daemon's convert claim (by account id) to
// hostsync.Claims (by registry uuid); byUUID is a field so tests fake it.
type serverClaims struct {
	s      *Server
	byUUID func(uuid string) (store.Account, bool, error)
}

// newServerClaims binds the claim adapter to s's store-backed uuid lookup.
func newServerClaims(s *Server) serverClaims {
	return serverClaims{s: s, byUUID: s.m.Store.GetAccountByUUID}
}

// TryClaim resolves uuid and claims it via beginConvert; a resolve miss/error
// or a live hold refuses the claim (release is then a safe no-op).
func (c serverClaims) TryClaim(uuid string) (func(), bool) {
	a, ok, err := c.byUUID(uuid)
	if err != nil || !ok {
		return func() {}, false
	}
	if !c.s.cl.own(a.ID) {
		return func() {}, false
	}
	return func() { c.s.cl.disownConvert(a.ID) }, true
}

var _ hostsync.Claims = serverClaims{}

type admittedSyncConsumer struct {
	drain *drain.Intake
	next  syncservice.SyncConsumer
}

func (c admittedSyncConsumer) Capabilities(ctx context.Context) (syncservice.Capabilities, error) {
	done, err := c.drain.Admit()
	if err != nil {
		return syncservice.Capabilities{}, err
	}
	defer done()
	return c.next.Capabilities(ctx)
}

func (c admittedSyncConsumer) List(ctx context.Context) ([]syncservice.WatchItem, error) {
	done, err := c.drain.Admit()
	if err != nil {
		return nil, err
	}
	defer done()
	return c.next.List(ctx)
}

func (c admittedSyncConsumer) Reconcile(ctx context.Context, origin string) (syncservice.ReconcileResult, error) {
	done, err := c.drain.Admit()
	if err != nil {
		return syncservice.ReconcileResult{}, err
	}
	defer done()
	return c.next.Reconcile(ctx, origin)
}

func (c admittedSyncConsumer) Sync(ctx context.Context, origin string) (syncservice.SyncResult, error) {
	done, err := c.drain.Admit()
	if err != nil {
		return syncservice.SyncResult{}, err
	}
	defer done()
	return c.next.Sync(ctx, origin)
}

func (c admittedSyncConsumer) GetState(ctx context.Context) (syncservice.RawRegistry, error) {
	done, err := c.drain.Admit()
	if err != nil {
		return nil, err
	}
	defer done()
	return c.next.GetState(ctx)
}

func admittedSyncHandler(d *drain.Intake, next rpc.Handler) rpc.Handler {
	return func(ctx context.Context, params map[string]any) (any, error) {
		done, err := d.Admit()
		if err != nil {
			return nil, err
		}
		defer done()
		return next(ctx, params)
	}
}

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
func (s *Server) startSyncServer(ctx context.Context, svc *hostsync.Service) error {
	if s.syncIntake == nil {
		s.syncIntake = &drain.Intake{}
	}
	svc.Claims = newServerClaims(s)
	consumer := hostsync.NewConsumer(svc, s.syncEnabled)
	fetch := hostsync.NewFetchCredentialHandler(s.m.Store.GetAccountByUUID, s.readCredentialForFetch)
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

// readCredentialForFetch reads a's credential under its lock with
// allowRefresh=false: serving a peer must never spend the refresh token.
func (s *Server) readCredentialForFetch(ctx context.Context, a store.Account) (*creds.Credential, error) {
	cred, _, err := s.m.EnsureFreshToken(ctx, a, 0, false)
	if err != nil {
		return nil, err
	}
	return cred, nil
}

// serveSyncSocket binds sockPath (0600) and serves the sync dispatcher until
// ctx is done; rpc.Serve owns the listener lifecycle. Returns once bound.
func serveSyncSocket(ctx context.Context, wg *sync.WaitGroup, intake *drain.Intake, sockPath string, consumer syncservice.SyncConsumer, fetch rpc.Handler, logger *log.Logger) (net.Listener, error) {
	ln, err := rpc.Listen(sockPath)
	if err != nil {
		return nil, fmt.Errorf("bind sync socket %s: %w", sockPath, err)
	}
	guarded := &onceCloseListener{Listener: ln}
	if err := os.Chmod(sockPath, syncSocketPerm); err != nil {
		_ = guarded.Close()
		return nil, fmt.Errorf("chmod sync socket %s: %w", sockPath, err)
	}
	d := rpc.NewDispatcher()
	syncservice.RegisterConsumer(d, admittedSyncConsumer{drain: intake, next: consumer})
	d.Register(hostsync.MethodFetchCredential, admittedSyncHandler(intake, fetch))

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := rpc.Serve(ctx, guarded, d); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Printf("sync socket serve: %v", err)
		}
	}()
	return guarded, nil
}
