package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/version"
	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/daemonrole"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/wire"
)

func operationLadder() (wire.Ladder, error) {
	server := map[wire.Op]time.Duration{
		wire.Op(OpSelect):             selectRequestTimeout,
		wire.Op(OpSelectCommit):       2 * time.Second,
		wire.Op(OpSelectAbort):        2 * time.Second,
		wire.Op(OpStatus):             4 * time.Second,
		wire.Op(OpCredMove):           140 * time.Second,
		wire.Op(OpAccountRemove):      3 * time.Minute,
		wire.Op(OpAccountIdentity):    31 * time.Second,
		wire.Op(OpAccountHealth):      61 * time.Second,
		wire.Op(OpAccountMutation):    30 * time.Minute,
		wire.Op(OpAccountMutationAck): 2 * time.Second,
	}
	client := map[wire.Op]time.Duration{
		wire.Op(OpSelect):             selectConnTimeout,
		wire.Op(OpSelectCommit):       3 * time.Second,
		wire.Op(OpSelectAbort):        3 * time.Second,
		wire.Op(OpStatus):             5 * time.Second,
		wire.Op(OpCredMove):           3 * time.Minute,
		wire.Op(OpAccountRemove):      4 * time.Minute,
		wire.Op(OpAccountIdentity):    32 * time.Second,
		wire.Op(OpAccountHealth):      62 * time.Second,
		wire.Op(OpAccountMutation):    31 * time.Minute,
		wire.Op(OpAccountMutationAck): 3 * time.Second,
	}
	return wire.NewLadder(server, client)
}

func (s *Server) runtime() (*wire.Server, *dkdaemon.Runtime, error) {
	ladder, err := operationLadder()
	if err != nil {
		return nil, nil, err
	}
	if s.wireIntake == nil {
		s.wireIntake = &drain.Intake{}
	}
	if s.syncIntake == nil {
		s.syncIntake = &drain.Intake{}
	}
	role, err := daemonRole()
	if err != nil {
		return nil, nil, err
	}
	wireServer := &wire.Server{
		Build: BusinessBuild, LifecycleBuild: version.String(), Ladder: ladder,
		ReservedProtectedSessions:  1,
		ProtectedSessionClassifier: role,
		Trust: func(_ context.Context, peer wire.Peer) error {
			if peer.UID != os.Geteuid() {
				return fmt.Errorf("%w: peer uid %d, daemon uid %d", wire.ErrUntrustedPeer, peer.UID, os.Geteuid())
			}
			return nil
		},
	}
	for _, op := range []Op{
		OpSelect,
		OpSelectCommit,
		OpSelectAbort,
		OpStatus,
		OpCredMove,
		OpAccountRemove,
		OpAccountIdentity,
		OpAccountHealth,
		OpAccountMutation,
		OpAccountMutationAck,
	} {
		op := op
		wireServer.RegisterConcurrent(wire.Op(op), func(ctx context.Context, wireRequest wire.Request) (any, error) {
			var request Request
			if err := decodeStrict(wireRequest.Payload, &request); err != nil {
				return nil, fmt.Errorf("decode %s request: %w", op, err)
			}
			request.Op = op
			if op == OpAccountMutation {
				return s.handleAccountMutationWire(ctx, wireRequest, request)
			}
			return s.dispatch(ctx, request), nil
		})
	}
	peer := &wire.LifecyclePeer{Config: wire.ClientConfig{
		Dial: wire.UnixDialer(s.socket), Build: BusinessBuild,
		LifecycleBuild: version.String(),
	}}
	runtime, err := dkdaemon.NewRuntime(dkdaemon.RuntimeConfig{
		Socket:       s.socket,
		Build:        version.String(),
		Protocol:     int(wire.ProtocolVersion),
		Peer:         peer,
		Contract:     dkdaemon.RequestDaemon,
		WaitMode:     dkdaemon.PIDExit,
		Grace:        s.evictTimeout,
		ListenerWait: s.evictTimeout,
		Admission:    s.wireIntake,
		Server:       &sessionServer{owner: s, wire: wireServer},
		Workers:      &serverWorkers{owner: s},
		State:        serverState{owner: s},
		Resources:    lifecycleResource{peer: peer, server: s},
		Activate:     s.activate,
	})
	if err != nil {
		_ = peer.Close()
		return nil, nil, err
	}
	return wireServer, runtime, nil
}

var serviceRoleExecutable = service.CanonicalExecutable

// ServiceRolePath returns the canonical current executable shared by launchd
// and daemon admission without consulting PATH or falling back to an alias.
func ServiceRolePath() (string, error) {
	rolePath, err := serviceRoleExecutable()
	if err != nil {
		return "", fmt.Errorf("resolve current ccp executable: %w", err)
	}
	if !filepath.IsAbs(rolePath) || filepath.Clean(rolePath) != rolePath {
		return "", fmt.Errorf("current ccp executable %q is not exact and absolute", rolePath)
	}
	return rolePath, nil
}

func daemonRole() (daemonrole.Classifier, error) {
	rolePath, err := ServiceRolePath()
	if err != nil {
		return daemonrole.Classifier{}, err
	}
	role := daemonrole.Classifier{
		RoleID: ServiceRoleID, RolePath: rolePath,
	}
	if err := role.Validate(); err != nil {
		return daemonrole.Classifier{}, err
	}
	return role, nil
}

type sessionServer struct {
	owner *Server
	wire  *wire.Server
}

func (s *sessionServer) Serve(
	ctx context.Context,
	listener net.Listener,
	ready func() error,
	admit, admitLifecycle func() (func(), error),
) error {
	execCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.owner.execMu.Lock()
	s.owner.execCancel = cancel
	s.owner.execMu.Unlock()

	if err := s.owner.setupSync(execCtx); err != nil {
		cancel()
		return fmt.Errorf("setup host sync publication: %w", err)
	}
	if s.owner.m.BuildCredentialWritePublication == nil || s.owner.m.SettleCredentialWrite == nil {
		cancel()
		return errors.New("host sync publication wiring is unavailable")
	}
	if err := s.owner.m.RecoverRetiredCredentialOwners(execCtx); err != nil {
		cancel()
		return fmt.Errorf("recover retired credential owners: %w", err)
	}
	s.owner.log.Printf("daemon %s started; socket=%s", version.String(), s.owner.socket)
	s.owner.wg.Add(1)
	go func() {
		defer s.owner.wg.Done()
		s.owner.runTable(execCtx, s.owner.newTick(execCtx), startupTable)
		if execCtx.Err() != nil {
			return
		}
		s.owner.scheduler(execCtx)
	}()
	err := s.wire.Serve(ctx, listener, ready, admit, admitLifecycle)
	s.owner.log.Printf("daemon stopped")
	return err
}

func (s *sessionServer) CloseIntake() error { return s.wire.CloseIntake() }

type serverWorkers struct{ owner *Server }

func (w *serverWorkers) Close() {
	w.owner.markClosing()
	w.owner.syncIntake.Close()
	if w.owner.syncListener != nil {
		_ = w.owner.syncListener.Close()
	}
	if w.owner.disposableWorkers != nil {
		w.owner.disposableWorkers.Close()
	}
}

func (w *serverWorkers) Cancel() {
	w.owner.execMu.Lock()
	cancel := w.owner.execCancel
	w.owner.execMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if w.owner.disposableWorkers != nil {
		w.owner.disposableWorkers.Cancel()
	}
}

func (w *serverWorkers) Wait(ctx context.Context) error {
	settleErr := w.owner.syncIntake.Settle(ctx)
	if settleErr != nil {
		if err := w.owner.syncIntake.Settle(context.WithoutCancel(ctx)); err != nil {
			settleErr = errors.Join(settleErr, err)
		}
	}
	if w.owner.disposableWorkers != nil {
		settleErr = errors.Join(settleErr, w.owner.disposableWorkers.Wait(ctx))
	}
	done := make(chan struct{})
	go func() {
		w.owner.wg.Wait()
		close(done)
	}()
	<-done
	return settleErr
}

type lifecycleResource struct {
	peer   *wire.LifecyclePeer
	server *Server
}

type serverState struct{ owner *Server }

func (s serverState) Close() error {
	if s.owner == nil || s.owner.m == nil {
		return nil
	}
	if s.owner.accountMutationLifetime == nil {
		return errors.New("daemon manager close requires an active lifecycle")
	}
	err := s.owner.m.Close(s.owner.accountMutationLifetime)
	s.owner.m = nil
	return err
}

func (r lifecycleResource) Close() error {
	var errs []error
	if r.peer != nil {
		errs = append(errs, r.peer.Close())
	}
	if r.server != nil && r.server.tenantClient != nil {
		errs = append(errs, r.server.tenantClient.Close())
	}
	return errors.Join(errs...)
}
