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
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/trust"
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
	policy, err := daemonTrustPolicy()
	if err != nil {
		return nil, nil, err
	}
	wireServer := &wire.Server{
		WireBuild: WireBuild, Ladder: ladder,
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
		wireServer.Register(wire.HandlerSpec{Op: wire.Op(op), Concurrent: true, Handler: func(ctx context.Context, wireRequest wire.Request) (any, error) {
			var request Request
			if err := decodeStrict(wireRequest.Payload, &request); err != nil {
				return nil, fmt.Errorf("decode %s request: %w", op, err)
			}
			request.Op = op
			if op == OpAccountMutation {
				return s.handleAccountMutationWire(ctx, wireRequest, request)
			}
			return s.dispatch(ctx, request), nil
		}})
	}
	if s.m == nil || s.m.DisposableWorkers() == nil || s.m.RuntimeChildren() == nil {
		return nil, nil, errors.New("daemon: runtime process ownership is unavailable")
	}
	runtime, err := wire.NewRuntime(wire.RuntimeConfig{
		Socket: s.socket, RuntimeBuild: version.String(), RuntimeProtocol: int(wire.ProtocolVersion),
		Wire: wireServer, TrustPolicy: policy,
		StopControlStore: &proc.FileStore{Path: pool.DaemonStopControlStorePath()},
		Observations:     []wire.ObservationRoute{s.daemonHealthRoute()}, ListenerWait: s.evictTimeout,
		Workers: s.m.DisposableWorkers(), Children: s.m.RuntimeChildren(),
		ShutdownTimeout: daemonShutdownTimeout,
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
		Handler: s.daemonHealthObservation,
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
		ProcessGeneration: health.ProcessGeneration.String(), PID: health.PID, State: state,
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

var currentServiceExecutable = service.CanonicalExecutable

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

func daemonTrustPolicy() (trust.TrustPolicy, error) {
	requirement := trust.Requirement{TeamID: ServiceTeamID, SigningIdentifier: ServiceRoleID}
	return trust.NewTrustPolicy(trust.TrustPolicyConfig{
		ExpectedUID: os.Geteuid(), AllowUnprotected: true,
		Roles: map[trust.PeerRole]trust.Requirement{
			trust.PeerRole(StopRoleID):      requirement,
			trust.PeerRole(ReceiptRoleID):   requirement,
			trust.PeerRole(ReadinessRoleID): requirement,
		},
		StopRoles:      []trust.PeerRole{trust.PeerRole(StopRoleID)},
		ReceiptRoles:   []trust.PeerRole{trust.PeerRole(ReceiptRoleID)},
		ReadinessRoles: []trust.PeerRole{trust.PeerRole(ReadinessRoleID)},
	})
}

func (s *Server) startProductRuntime(ctx context.Context) error {
	execCtx, cancel := context.WithCancel(ctx)
	s.execMu.Lock()
	s.execCancel = cancel
	s.execMu.Unlock()

	if err := s.setupSync(execCtx); err != nil {
		cancel()
		return fmt.Errorf("setup host sync publication: %w", err)
	}
	if s.m.BuildCredentialWritePublication == nil || s.m.SettleCredentialWrite == nil {
		cancel()
		return errors.New("host sync publication wiring is unavailable")
	}
	if s.holderSessionDone == nil {
		cancel()
		return errors.New("FuseKit runtime session monitor is unavailable")
	}
	if err := s.m.RecoverRetiredCredentialOwners(execCtx); err != nil {
		cancel()
		return fmt.Errorf("recover retired credential owners: %w", err)
	}
	s.log.Printf("daemon %s started; socket=%s", version.String(), s.socket)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runTable(execCtx, s.newTick(execCtx), startupTable)
		if execCtx.Err() != nil {
			return
		}
		s.scheduler(execCtx)
	}()
	return nil
}
