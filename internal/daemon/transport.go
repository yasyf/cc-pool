package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/version"
	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/daemonrole"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/wire"
)

var errHolderSessionLost = errors.New("daemon: FuseKit runtime session lost")

const daemonHealthMaxResponse = 16 << 10

func operationLadder() (wire.Ladder, error) {
	server := map[wire.Op]time.Duration{
		wire.Op(OpHealth):             1500 * time.Millisecond,
		wire.Op(OpSelect):             selectRequestTimeout,
		wire.Op(OpSelectCommit):       2 * time.Second,
		wire.Op(OpSelectAbort):        2 * time.Second,
		wire.Op(OpStatus):             4 * time.Second,
		wire.Op(OpAccountRemove):      3 * time.Minute,
		wire.Op(OpAccountIdentity):    31 * time.Second,
		wire.Op(OpAccountHealth):      61 * time.Second,
		wire.Op(OpAccountMutation):    30 * time.Minute,
		wire.Op(OpAccountMutationAck): 2 * time.Second,
	}
	client := map[wire.Op]time.Duration{
		wire.Op(OpHealth):             2 * time.Second,
		wire.Op(OpSelect):             selectConnTimeout,
		wire.Op(OpSelectCommit):       3 * time.Second,
		wire.Op(OpSelectAbort):        3 * time.Second,
		wire.Op(OpStatus):             5 * time.Second,
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
		WireBuild: WireBuild, Ladder: ladder,
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
	runtime, err := wire.NewRuntime(wire.RuntimeConfig{
		Socket:                    s.socket,
		RuntimeBuild:              version.String(),
		RuntimeProtocol:           int(wire.ProtocolVersion),
		Wire:                      wireServer,
		Classifier:                role,
		ReservedProtectedSessions: 1,
		StopVerifier: wire.StopVerifier{
			Classifier: role, Role: StopRoleID,
			Store: &proc.FileStore{Path: pool.DaemonServiceProcessStorePath()},
		},
		Observations: []wire.ObservationRoute{s.daemonHealthRoute()},
		Readiness:    serverReadiness{owner: s},
		ListenerWait: s.evictTimeout,
		Admission:    s.wireIntake,
		Workers:      &serverWorkers{owner: s},
		State:        serverState{owner: s},
		Resources:    lifecycleResource{server: s},
		Activate:     s.activate,
		HealthState:  s.runtimeHealthState,
		Busy:         s.runtimeBusy,
	})
	if err != nil {
		return nil, nil, err
	}
	s.runtimeShutdown = runtime.Shutdown
	s.runtimeHealth = runtime.Health
	return wireServer, runtime, nil
}

func (s *Server) daemonHealthRoute() wire.ObservationRoute {
	return wire.ObservationRoute{
		Op: wire.Op(OpHealth), MaxResponseBytes: daemonHealthMaxResponse,
		AvailableBeforeReady: true, Handler: s.daemonHealthObservation,
	}
}

func (s *Server) daemonHealthObservation(ctx context.Context, request wire.ObservationRequest) (wire.ObservationResponse, error) {
	if err := ctx.Err(); err != nil {
		return wire.ObservationResponse{}, err
	}
	if request.Op != wire.Op(OpHealth) || request.Tenant != "" {
		return wire.ObservationResponse{}, errors.New("daemon health observation route is not exact")
	}
	var body HealthRequest
	if err := decodeStrict(request.Payload, &body); err != nil {
		return wire.ObservationResponse{}, fmt.Errorf("decode daemon health observation: %w", err)
	}
	if body.Schema != DaemonHealthSchema {
		return wire.ObservationResponse{}, fmt.Errorf("daemon health schema %d is not exact", body.Schema)
	}
	if s.runtimeHealth == nil {
		return wire.ObservationResponse{}, errors.New("daemon runtime health is unavailable")
	}
	health, err := s.runtimeHealth(ctx)
	if err != nil {
		return wire.ObservationResponse{}, fmt.Errorf("read daemon runtime health: %w", err)
	}
	snapshot, err := daemonHealthSnapshot(health)
	if err != nil {
		return wire.ObservationResponse{}, fmt.Errorf("project daemon runtime health: %w", err)
	}
	if s.cl != nil {
		counts := s.cl.liveCounts()
		snapshot.ActiveReservations = counts.reservations
		snapshot.ExclusiveClaims = counts.exclusive
	}
	if s.m != nil && s.m.Store != nil {
		snapshot.ActiveSessions, err = s.m.Store.ActiveSessionTotal()
		if err != nil {
			return wire.ObservationResponse{}, fmt.Errorf("count active sessions: %w", err)
		}
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return wire.ObservationResponse{}, fmt.Errorf("encode daemon health observation: %w", err)
	}
	return wire.ObservationResponse{Payload: payload}, nil
}

func daemonHealthSnapshot(health dkdaemon.Health) (HealthResponse, error) {
	state, err := daemonRuntimeStateFromDaemon(health.State)
	if err != nil {
		return HealthResponse{}, err
	}
	return HealthResponse{
		Schema: DaemonHealthSchema, RuntimeBuild: health.RuntimeBuild, RuntimeProtocol: health.RuntimeProtocol,
		ProcessGeneration: health.ProcessGeneration, PID: health.PID, State: state,
		Draining: health.Draining, Busy: health.Busy, Ready: health.Ready,
	}, nil
}

func daemonRuntimeStateFromDaemon(state dkdaemon.State) (RuntimeState, error) {
	switch state {
	case dkdaemon.StateHealthy:
		return RuntimeStateHealthy, nil
	case dkdaemon.StateDegraded:
		return RuntimeStateDegraded, nil
	case dkdaemon.StateFailed:
		return RuntimeStateFailed, nil
	default:
		return "", fmt.Errorf("daemon runtime state %q is not exact", state)
	}
}

const serviceRolePath = "/opt/homebrew/bin/cc-pool"

var currentServiceExecutable = service.CanonicalExecutable

// ServiceRolePath returns the stable Homebrew alias re-resolved for each authorization.
func ServiceRolePath() string { return serviceRolePath }

// CurrentServiceExecutable returns the exact resolved binary installed into launchd.
func CurrentServiceExecutable() (string, error) {
	rolePath, err := currentServiceExecutable()
	if err != nil {
		return "", fmt.Errorf("resolve current ccp executable: %w", err)
	}
	if !filepath.IsAbs(rolePath) || filepath.Clean(rolePath) != rolePath {
		return "", fmt.Errorf("current ccp executable %q is not exact and absolute", rolePath)
	}
	return rolePath, nil
}

func daemonRole() (daemonrole.Classifier, error) {
	role := daemonrole.Classifier{
		RoleID: ServiceRoleID, RolePath: ServiceRolePath(),
	}
	if err := role.Validate(); err != nil {
		return daemonrole.Classifier{}, err
	}
	return role, nil
}

type serverReadiness struct{ owner *Server }

func (s serverReadiness) BeforeReady(ctx context.Context) error {
	execCtx, cancel := context.WithCancel(ctx)
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
	if s.owner.holderSessionDone == nil {
		cancel()
		return errors.New("FuseKit runtime session monitor is unavailable")
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
	return nil
}

func (s serverReadiness) AfterReady(err error) {
	if err == nil {
		s.owner.runtimePublished.Store(true)
		return
	}
	s.owner.runtimePublished.Store(false)
	s.owner.execMu.Lock()
	cancel := s.owner.execCancel
	s.owner.execMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s serverReadiness) Published() bool { return s.owner.runtimePublished.Load() }

type serverWorkers struct{ owner *Server }

func (w *serverWorkers) Close() {
	w.owner.markClosing()
	w.owner.runtimePublished.Store(false)
	w.owner.syncIntake.Close()
	if w.owner.syncListener != nil {
		_ = w.owner.syncListener.Close()
	}
	if w.owner.disposableWorkers != nil {
		w.owner.disposableWorkers.Close()
	}
}

func (w *serverWorkers) Cancel() {
	w.owner.cancelHolderMonitor()
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
	if w.owner.disposableWorkers != nil {
		settleErr = errors.Join(settleErr, w.owner.disposableWorkers.Wait(ctx))
	}
	done := make(chan struct{})
	go func() {
		w.owner.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		settleErr = errors.Join(settleErr, fmt.Errorf("daemon: await background workers: %w", ctx.Err()))
	}
	return settleErr
}

type lifecycleResource struct {
	server *Server
}

type serverState struct{ owner *Server }

func (s *Server) runtimeHealthState() dkdaemon.State {
	if s.holderLost.Load() {
		return dkdaemon.StateFailed
	}
	if !s.holderActive.Load() {
		return dkdaemon.StateDegraded
	}
	return dkdaemon.StateHealthy
}

func (s *Server) runtimeBusy() bool {
	return !s.holderActive.Load() || s.holderLost.Load()
}

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
	if r.server != nil && r.server.tenantClient != nil {
		r.server.holderActive.Store(false)
		errs = append(errs, r.server.tenantClient.Close())
	}
	return errors.Join(errs...)
}
