package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit"
)

var errHolderSessionLost = errors.New("daemon: FuseKit runtime session lost")

// Handle dispatches one business request. The client's own deadline rides the
// wire into ctx, so the retired per-op server ladder has no successor here:
// each op's budget is the one its caller stated.
func (s *Server) Handle(ctx context.Context, req daemonkit.Request) (daemonkit.Reply, error) {
	op := Op(req.Op)
	var request Request
	if err := decodeStrict(req.Body, &request); err != nil {
		return daemonkit.Reply{}, fmt.Errorf("decode %s request: %w", op, err)
	}
	request.Op = op
	switch op {
	case OpSelect, OpSelectCommit, OpSelectAbort, OpStatus,
		OpAccountRemove, OpAccountIdentity, OpAccountHealth,
		OpAccountMutationAck:
		return reply(s.dispatch(ctx, request))
	case OpAccountMutation:
		return reply(s.handleAccountMutation(ctx, req.Session, request))
	case OpAccountMutationPoll:
		return s.handleAccountMutationPoll(ctx, req, request)
	default:
		return daemonkit.Reply{}, &daemonkit.ProductError{
			Message: fmt.Sprintf("daemon: unknown operation %q", req.Op),
		}
	}
}

func reply(response Response) (daemonkit.Reply, error) {
	body, err := json.Marshal(response)
	if err != nil {
		return daemonkit.Reply{}, fmt.Errorf("encode daemon response: %w", err)
	}
	return daemonkit.Reply{Body: body}, nil
}

// Drain stops admitting work and joins every saga goroutine. The join is
// unconditional across every cleanup error: a successor may only claim this
// generation's owner-keyed rows once no saga of ours can still write one, and
// Serve holds the flock until this returns.
func (s *Server) Drain(budget daemonkit.Budget) error {
	ctx, cancel := budget.Context(context.Background())
	defer cancel()
	return s.drainProduct(ctx)
}

func (s *Server) drainProduct(ctx context.Context) error {
	s.markClosing()
	s.runtimePublished.Store(false)
	s.cancelHolderMonitor()
	s.execMu.Lock()
	execCancel := s.execCancel
	s.execMu.Unlock()
	if execCancel != nil {
		execCancel()
	}
	var result error
	if s.accountTerminals != nil {
		result = errors.Join(result, s.accountTerminals.Close(ctx))
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		result = errors.Join(result, fmt.Errorf("daemon: await product workers: %w", ctx.Err()))
	}
	return result
}

// Close releases the store and the holder session, after Drain proved no saga
// still holds either.
func (s *Server) Close(budget daemonkit.Budget) error {
	ctx, cancel := budget.Context(context.Background())
	defer cancel()
	return s.closeProduct(ctx)
}

func (s *Server) closeProduct(ctx context.Context) error {
	var result error
	if s.syncClient != nil {
		result = errors.Join(result, s.syncClient.Close())
		s.syncClient = nil
	}
	if s.tenantClient != nil {
		s.holderActive.Store(false)
		result = errors.Join(result, s.tenantClient.Close(ctx))
	}
	if s.m != nil {
		result = errors.Join(result, s.m.Close())
	}
	s.clearActivation()
	s.m = nil
	return result
}

// publishHealth hands daemonkit the product's half of Health.Detail. Readers
// reach it through Control.Health, which answers during the drain too.
func (s *Server) publishHealth() {
	if s.report == nil {
		return
	}
	snapshot := HealthResponse{
		Schema: DaemonHealthSchema, RuntimeBuild: version.String(),
		State: RuntimeStateHealthy,
		Ready: s.runtimePublished.Load(), Draining: s.closing.Load(),
	}
	if s.cl != nil {
		counts := s.cl.liveCounts()
		snapshot.ActiveReservations = counts.reservations
		snapshot.ExclusiveClaims = counts.exclusive
	}
	if s.m != nil && s.m.Store != nil {
		sessions, err := s.m.Store.ActiveSessionTotal()
		if err != nil {
			snapshot.State = RuntimeStateDegraded
		}
		snapshot.ActiveSessions = sessions
	}
	detail, err := json.Marshal(snapshot)
	if err != nil {
		return
	}
	s.report(detail)
}

// startProductRuntime opens business admission in the one order the exclusion
// argument needs: the owner is minted, the cross-era gate has already proven
// no pre-cut daemon survives, foreign lanes are claimed and retired mutations
// recovered while no worker of ours can hold a reservation, and only then do
// the resident sync helper and the scheduler go live.
func (s *Server) startProductRuntime(ctx context.Context) error {
	execCtx, cancel := context.WithCancel(ctx)
	s.execMu.Lock()
	s.execCancel = cancel
	s.execMu.Unlock()

	plan, err := s.setupSyncPublication(execCtx)
	if err != nil {
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
	if err := s.m.ClaimForeignLanes(execCtx); err != nil {
		cancel()
		return fmt.Errorf("claim foreign credential lanes: %w", err)
	}
	if err := s.recoverRetiredAccountMutations(execCtx); err != nil {
		cancel()
		return fmt.Errorf("recover account mutations: %w", err)
	}
	if err := s.recoverPendingAccountMutationPublications(execCtx); err != nil {
		cancel()
		return fmt.Errorf("recover account mutation publications: %w", err)
	}
	if err := s.startSyncConsumer(execCtx, plan); err != nil {
		cancel()
		return fmt.Errorf("start host sync consumer: %w", err)
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
