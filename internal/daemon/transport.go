package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/yasyf/cc-pool/internal/version"
	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/wire"
)

func operationLadder() (wire.Ladder, error) {
	server := map[wire.Op]time.Duration{
		wire.Op(OpSelect):        selectRequestTimeout,
		wire.Op(OpSelectCommit):  2 * time.Second,
		wire.Op(OpSelectAbort):   2 * time.Second,
		wire.Op(OpStatus):        4 * time.Second,
		wire.Op(OpMigrate):       140 * time.Second,
		wire.Op(OpCredMove):      140 * time.Second,
		wire.Op(OpFPRepair):      140 * time.Second,
		wire.Op(OpFPBridgeCheck): 13 * time.Second,
	}
	client := map[wire.Op]time.Duration{
		wire.Op(OpSelect):        selectConnTimeout,
		wire.Op(OpSelectCommit):  3 * time.Second,
		wire.Op(OpSelectAbort):   3 * time.Second,
		wire.Op(OpStatus):        5 * time.Second,
		wire.Op(OpMigrate):       migrateTimeout,
		wire.Op(OpCredMove):      migrateTimeout,
		wire.Op(OpFPRepair):      migrateTimeout,
		wire.Op(OpFPBridgeCheck): fpBridgeCheckTimeout,
	}
	return wire.NewLadder(server, client)
}

func (s *Server) runtime() (*wire.Server, *dkdaemon.Runtime, error) {
	ladder, err := operationLadder()
	if err != nil {
		return nil, nil, err
	}
	if s.m == nil {
		return nil, nil, errors.New("daemon manager is required")
	}
	if s.wireIntake == nil {
		s.wireIntake = &drain.Intake{}
	}
	if s.syncIntake == nil {
		s.syncIntake = &drain.Intake{}
	}
	wireServer := &wire.Server{
		Build:  version.String(),
		Ladder: ladder,
		Trust: func(peer wire.Peer) error {
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
		OpMigrate,
		OpCredMove,
		OpFPRepair,
		OpFPBridgeCheck,
	} {
		op := op
		wireServer.RegisterConcurrent(wire.Op(op), func(ctx context.Context, wireRequest wire.Request) (any, error) {
			var request Request
			if err := decodeStrict(wireRequest.Payload, &request); err != nil {
				return nil, fmt.Errorf("decode %s request: %w", op, err)
			}
			request.Op = op
			return s.dispatch(ctx, request), nil
		})
	}
	peer := &wire.LifecyclePeer{Config: wire.ClientConfig{
		Dial:  wire.UnixDialer(s.socket),
		Build: version.String(),
	}}
	runtime, err := dkdaemon.NewRuntime(dkdaemon.RuntimeConfig{
		Socket:       s.socket,
		Build:        version.String(),
		Protocol:     int(wire.ProtocolVersion),
		Peer:         peer,
		Contract:     dkdaemon.RequestDaemon,
		WaitMode:     dkdaemon.SocketRelease,
		Grace:        s.evictTimeout,
		ListenerWait: s.evictTimeout,
		Admission:    s.wireIntake,
		Server:       &sessionServer{owner: s, wire: wireServer},
		Workers:      &serverWorkers{owner: s},
		State:        s.m,
		Resources:    lifecycleResource{peer: peer},
	})
	if err != nil {
		_ = peer.Close()
		return nil, nil, err
	}
	return wireServer, runtime, nil
}

type sessionServer struct {
	owner *Server
	wire  *wire.Server
}

func (s *sessionServer) Serve(
	ctx context.Context,
	listener net.Listener,
	admit, admitLifecycle func() (func(), error),
) error {
	execCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.owner.execMu.Lock()
	s.owner.execCancel = cancel
	s.owner.execMu.Unlock()

	if err := s.owner.setupSync(execCtx); err != nil {
		s.owner.log.Printf("host sync disabled for this run: %v", err)
	}
	s.owner.log.Printf("daemon %s started; socket=%s", version.String(), s.owner.socket)
	s.owner.wg.Add(1)
	go func() {
		defer s.owner.wg.Done()
		s.owner.runTable(execCtx, s.owner.newTick(execCtx), startupTable)
		if execCtx.Err() != nil {
			return
		}
		s.owner.wg.Add(1)
		go func() {
			defer s.owner.wg.Done()
			s.owner.healFuseRows(execCtx)
		}()
		s.owner.scheduler(execCtx)
	}()
	err := s.wire.Serve(ctx, listener, admit, admitLifecycle)
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
}

func (w *serverWorkers) Cancel() {
	w.owner.execMu.Lock()
	cancel := w.owner.execCancel
	w.owner.execMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (w *serverWorkers) Wait(ctx context.Context) error {
	settleErr := w.owner.syncIntake.Settle(ctx)
	if settleErr != nil {
		if err := w.owner.syncIntake.Settle(context.WithoutCancel(ctx)); err != nil {
			settleErr = errors.Join(settleErr, err)
		}
	}
	done := make(chan struct{})
	go func() {
		w.owner.wg.Wait()
		close(done)
	}()
	<-done
	return settleErr
}

type lifecycleResource struct{ peer *wire.LifecyclePeer }

func (r lifecycleResource) Close() error { return r.peer.Close() }
