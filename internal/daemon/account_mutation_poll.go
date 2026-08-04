package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/yasyf/cc-pool/internal/accountterminal"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit"
)

// pollReplyMargin is withheld from every park so an empty page still crosses
// the wire inside the caller's own deadline (the fusekit broker-poll shape).
const pollReplyMargin = 500 * time.Millisecond

// pollKey identifies one client attachment: one accepted session driving one
// operation. TerminalAttachmentLimit bounds these per terminal inside Attach.
type pollKey struct {
	session   uint64
	operation store.AccountMutationID
}

// pollAttachment is the server half of one client attachment: a persistent
// terminal attachment whose replay cursor advances as polls page it, plus the
// at-most-one parked poll admission owns.
type pollAttachment struct {
	server *Server
	key    pollKey

	// pageMu serializes cursor validation through page completion. Without it
	// a superseding poll pages concurrently with its released predecessor and
	// returns its neighbour's chunks under its own request cursor — lost,
	// duplicated, or misordered terminal output.
	pageMu sync.Mutex

	mu         sync.Mutex
	attachment accountMutationTerminalAttachment
	controller bool
	renewing   bool
	closed     chan struct{}
	parked     chan struct{}
	next       uint64
	haveNext   bool
}

func (pa *pollAttachment) current() accountMutationTerminalAttachment {
	pa.mu.Lock()
	defer pa.mu.Unlock()
	return pa.attachment
}

func (s *Server) handleAccountMutationPoll(
	ctx context.Context,
	req daemonkit.Request,
	request Request,
) (daemonkit.Reply, error) {
	poll := request.MutationPoll
	if poll == nil {
		return daemonkit.Reply{}, &daemonkit.ProductError{Message: "account mutation poll request is required"}
	}
	if poll.Fence.CanonicalOperationID == ([32]byte{}) {
		return daemonkit.Reply{}, &daemonkit.ProductError{Message: "account mutation poll fence is required"}
	}
	if poll.WaitMillis < 0 || poll.WaitMillis > MaxPollWaitMillis {
		return daemonkit.Reply{}, &daemonkit.ProductError{
			Message: fmt.Sprintf("account mutation poll wait must lie in [0, %d]ms", MaxPollWaitMillis),
		}
	}
	operationID := store.AccountMutationID(poll.Fence.CanonicalOperationID)
	fence, state, done, err := s.accountMutationPollAnchor(operationID)
	if err != nil {
		return daemonkit.Reply{}, &daemonkit.ProductError{Message: err.Error()}
	}
	if fence != poll.Fence {
		return daemonkit.Reply{}, &daemonkit.ProductError{Message: "account mutation fence does not match the operation"}
	}
	s.accountMutationMu.Lock()
	running := s.accountMutationRuns[operationID]
	s.accountMutationMu.Unlock()
	if running != nil {
		select {
		case <-running.ready:
		case <-ctx.Done():
			return daemonkit.Reply{}, ctx.Err()
		}
		if running.terminal == nil {
			running = nil
		}
	}
	if running == nil {
		return pollReply(AccountMutationPollResponse{
			NextCursor: poll.TerminalCursor, State: state, Done: done,
		})
	}
	pa, err := s.accountMutationAttachment(req.Session, operationID, running, &poll.TerminalCursor)
	if err != nil {
		return daemonkit.Reply{}, &daemonkit.ProductError{Message: err.Error()}
	}
	token := pa.park()
	defer pa.unpark(token)
	pa.pageMu.Lock()
	defer pa.pageMu.Unlock()
	if err := pa.reposition(ctx, running, poll.TerminalCursor); err != nil {
		return daemonkit.Reply{}, &daemonkit.ProductError{Message: err.Error()}
	}
	chunks, settled, err := pa.page(s.pollParkContext(ctx, poll.WaitMillis, token))
	if err != nil {
		return daemonkit.Reply{}, &daemonkit.ProductError{Message: err.Error()}
	}
	state, done = s.accountMutationPollState(operationID, running, state, done)
	if settled && !done {
		done = accountMutationTerminalState(state)
	}
	next := poll.TerminalCursor + uint64(len(chunks))
	return pollReply(AccountMutationPollResponse{
		Chunks: chunks, NextCursor: next, State: state, Done: done,
	})
}

func pollReply(response AccountMutationPollResponse) (daemonkit.Reply, error) {
	body, err := json.Marshal(response)
	if err != nil {
		return daemonkit.Reply{}, fmt.Errorf("encode account mutation poll response: %w", err)
	}
	return daemonkit.Reply{Body: body}, nil
}

// pollParkContext bounds one park: the caller's conveyed deadline, the
// requested wait capped at the protocol ceiling minus the reply margin, the
// drain (Ctx.Context cancels at drain-begin), and a superseding poll all
// release it. nil means the margin left no time to park at all.
func (s *Server) pollParkContext(
	ctx context.Context,
	waitMillis int,
	superseded <-chan struct{},
) context.Context {
	wait := time.Duration(waitMillis) * time.Millisecond
	if wait <= 0 {
		wait = MaxPollWaitMillis * time.Millisecond
	}
	wait -= pollReplyMargin
	if wait <= 0 {
		return nil
	}
	lifetime := s.accountMutationLifetime
	if lifetime == nil {
		return nil
	}
	parkCtx, cancel := context.WithTimeout(ctx, wait)
	go func() {
		select {
		case <-superseded:
		case <-lifetime.Done():
		case <-parkCtx.Done():
			cancel()
			return
		}
		cancel()
	}()
	return parkCtx
}

// accountMutationPollAnchor validates the poll against the durable operation
// and answers its terminal state when no run is live.
func (s *Server) accountMutationPollAnchor(
	operationID store.AccountMutationID,
) (AccountMutationFence, AccountMutationState, bool, error) {
	if receipt, err := s.m.Store.AccountMutationReceipt(operationID); err == nil {
		result := accountMutationReceiptResult(receipt)
		return result.Fence, result.State, true, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return AccountMutationFence{}, "", false, err
	}
	active, err := s.m.Store.AccountMutation(operationID)
	if err != nil {
		return AccountMutationFence{}, "", false, err
	}
	result := accountMutationActiveResult(active)
	return result.Fence, result.State, false, nil
}

// accountMutationPollState re-reads the state after a page: a run that settled
// while the poll parked answers from its terminal result, not the stale
// pre-park anchor.
func (s *Server) accountMutationPollState(
	operationID store.AccountMutationID,
	running *accountMutationRun,
	state AccountMutationState,
	done bool,
) (AccountMutationState, bool) {
	select {
	case <-running.done:
		return running.result.State, accountMutationTerminalState(running.result.State)
	default:
	}
	if fence, current, terminal, err := s.accountMutationPollAnchor(operationID); err == nil && fence != (AccountMutationFence{}) {
		return current, terminal
	}
	return state, done
}

// accountMutationAttachment returns the session's attachment for one
// operation, creating it at the requested cursor; a cursor the existing
// attachment has moved past is the poll path's to reposition, under pageMu.
func (s *Server) accountMutationAttachment(
	session daemonkit.Session,
	operationID store.AccountMutationID,
	running *accountMutationRun,
	cursor *uint64,
) (*pollAttachment, error) {
	key := pollKey{session: session.ID(), operation: operationID}
	s.pollMu.Lock()
	pa := s.pollAttachments[key]
	s.pollMu.Unlock()
	if pa != nil {
		return pa, nil
	}
	lifetime := s.accountMutationLifetime
	if lifetime == nil {
		return nil, errors.New("account mutation lifetime is unavailable")
	}
	var replay *accountterminal.TerminalOutputCursor
	if cursor != nil {
		replay = &accountterminal.TerminalOutputCursor{NextSequence: *cursor}
	}
	attachment, err := running.terminal.Attach(lifetime, accountterminal.TerminalAttachmentSpec{
		Role:             accountterminal.TerminalObserver,
		DisconnectPolicy: accountterminal.DetachOnDisconnect,
		Cursor:           replay,
	})
	if err != nil {
		return nil, err
	}
	pa = &pollAttachment{server: s, key: key, attachment: attachment, closed: make(chan struct{})}
	if cursor != nil {
		pa.next, pa.haveNext = *cursor, true
	}
	s.registerPollAttachment(key, pa)
	s.armAccountMutationSessionTeardown(session)
	return pa, nil
}

// adoptAccountMutationAttachment registers the controller attachment the run
// start already claimed as the starting session's own.
func (s *Server) adoptAccountMutationAttachment(
	session daemonkit.Session,
	operationID store.AccountMutationID,
	attachment accountMutationTerminalAttachment,
) {
	key := pollKey{session: session.ID(), operation: operationID}
	pa := &pollAttachment{
		server: s, key: key, attachment: attachment,
		controller: true, closed: make(chan struct{}),
	}
	s.registerPollAttachment(key, pa)
	s.armAccountMutationSessionTeardown(session)
	pa.startControlRenewal()
}

func (s *Server) registerPollAttachment(key pollKey, pa *pollAttachment) {
	s.pollMu.Lock()
	if s.pollAttachments == nil {
		s.pollAttachments = make(map[pollKey]*pollAttachment)
	}
	previous := s.pollAttachments[key]
	s.pollAttachments[key] = pa
	s.pollMu.Unlock()
	if previous != nil && previous != pa {
		previous.close()
	}
}

// armAccountMutationSessionTeardown is the two-stage release: attachments
// close on Session.Disconnected — the transport is gone before in-flight
// handlers settle, so the controller frees within TerminalDisconnectPolicy —
// and the session-keyed entry drops on Session.Done.
func (s *Server) armAccountMutationSessionTeardown(session daemonkit.Session) {
	id := session.ID()
	s.pollMu.Lock()
	if s.pollSessions == nil {
		s.pollSessions = make(map[uint64]bool)
	}
	if s.pollSessions[id] {
		s.pollMu.Unlock()
		return
	}
	s.pollSessions[id] = true
	s.pollMu.Unlock()
	lifetime := s.accountMutationLifetime
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case <-session.Disconnected():
			s.releaseAccountMutationSession(id)
		case <-lifetime.Done():
			return
		}
		select {
		case <-session.Done():
		case <-lifetime.Done():
		}
		s.pollMu.Lock()
		delete(s.pollSessions, id)
		s.pollMu.Unlock()
	}()
}

// releaseAccountMutationOperation closes every session's attachment on one
// operation: acknowledgement retires the terminal, and a retained attachment
// would refuse the retirement it is stale against.
func (s *Server) releaseAccountMutationOperation(operationID store.AccountMutationID) {
	s.pollMu.Lock()
	attachments := make([]*pollAttachment, 0, len(s.pollAttachments))
	for key, pa := range s.pollAttachments {
		if key.operation == operationID {
			attachments = append(attachments, pa)
		}
	}
	s.pollMu.Unlock()
	for _, pa := range attachments {
		pa.close()
	}
}

func (s *Server) releaseAccountMutationSession(id uint64) {
	s.pollMu.Lock()
	attachments := make([]*pollAttachment, 0, len(s.pollAttachments))
	for key, pa := range s.pollAttachments {
		if key.session == id {
			attachments = append(attachments, pa)
		}
	}
	s.pollMu.Unlock()
	for _, pa := range attachments {
		pa.close()
	}
}

func (pa *pollAttachment) cursorMatches(cursor uint64) bool {
	pa.mu.Lock()
	defer pa.mu.Unlock()
	return !pa.haveNext || pa.next == cursor
}

// reposition re-attaches at cursor when the client's view diverged — a lost
// reply re-polls a cursor the attachment already passed — replaying from the
// terminal's retained window. Callers hold pageMu, so no page is in flight;
// a controller re-claims on the fresh attachment.
func (pa *pollAttachment) reposition(
	ctx context.Context,
	running *accountMutationRun,
	cursor uint64,
) error {
	if pa.cursorMatches(cursor) {
		return nil
	}
	lifetime := pa.server.accountMutationLifetime
	if lifetime == nil {
		return errors.New("account mutation lifetime is unavailable")
	}
	attachment, err := running.terminal.Attach(lifetime, accountterminal.TerminalAttachmentSpec{
		Role:             accountterminal.TerminalObserver,
		DisconnectPolicy: accountterminal.DetachOnDisconnect,
		Cursor:           &accountterminal.TerminalOutputCursor{NextSequence: cursor},
	})
	if err != nil {
		return err
	}
	pa.mu.Lock()
	previous := pa.attachment
	wasController := pa.controller
	pa.attachment = attachment
	pa.controller = false
	pa.next, pa.haveNext = cursor, true
	pa.mu.Unlock()
	_ = previous.Close()
	if wasController {
		return pa.ensureControl(ctx)
	}
	return nil
}

func (pa *pollAttachment) observe(output accountterminal.TerminalOutput) {
	pa.mu.Lock()
	pa.next, pa.haveNext = output.Sequence+1, true
	pa.mu.Unlock()
}

// park takes the attachment's one parked-poll slot, releasing whoever held it
// with their current page: supersede, never stack.
func (pa *pollAttachment) park() chan struct{} {
	pa.mu.Lock()
	defer pa.mu.Unlock()
	if pa.parked != nil {
		close(pa.parked)
	}
	token := make(chan struct{})
	pa.parked = token
	return token
}

func (pa *pollAttachment) unpark(token chan struct{}) {
	pa.mu.Lock()
	if pa.parked == token {
		pa.parked = nil
	}
	pa.mu.Unlock()
}

// page returns the buffered replay from the attachment's cursor, parking on
// parkCtx only when nothing is buffered. A released park returns an empty
// page, never an error; settled reports the terminal's own EOF.
func (pa *pollAttachment) page(parkCtx context.Context) (chunks [][]byte, settled bool, err error) {
	attachment := pa.current()
	buffered, cancel := context.WithCancel(context.Background())
	cancel()
	for len(chunks) < PollPageChunks {
		output, receiveErr := attachment.Receive(buffered)
		if receiveErr == nil {
			pa.observe(output)
			chunks = append(chunks, output.Data)
			continue
		}
		if errors.Is(receiveErr, io.EOF) {
			return chunks, true, nil
		}
		if errors.Is(receiveErr, context.Canceled) {
			break
		}
		return chunks, false, receiveErr
	}
	if len(chunks) > 0 || parkCtx == nil {
		return chunks, false, nil
	}
	output, receiveErr := attachment.Receive(parkCtx)
	if receiveErr != nil {
		if errors.Is(receiveErr, io.EOF) {
			return nil, true, nil
		}
		if parkCtx.Err() != nil {
			return nil, false, nil
		}
		return nil, false, receiveErr
	}
	pa.observe(output)
	chunks = [][]byte{output.Data}
	for len(chunks) < PollPageChunks {
		output, receiveErr := attachment.Receive(buffered)
		if receiveErr != nil {
			break
		}
		pa.observe(output)
		chunks = append(chunks, output.Data)
	}
	return chunks, false, nil
}

// ensureControl claims the terminal controller for this attachment, waiting
// out a still-leased predecessor under the caller's own deadline.
func (pa *pollAttachment) ensureControl(ctx context.Context) error {
	pa.mu.Lock()
	controller := pa.controller
	pa.mu.Unlock()
	if controller {
		return nil
	}
	retry := time.NewTicker(25 * time.Millisecond)
	defer retry.Stop()
	for {
		_, err := pa.current().ClaimControl(
			ctx, accountterminal.DetachOnDisconnect, accountterminal.DefaultTerminalControlLease,
		)
		switch {
		case err == nil:
			pa.mu.Lock()
			pa.controller = true
			pa.mu.Unlock()
			pa.startControlRenewal()
			return nil
		case errors.Is(err, accountterminal.ErrTerminalControllerAttached):
		default:
			return err
		}
		select {
		case <-retry.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (pa *pollAttachment) startControlRenewal() {
	pa.mu.Lock()
	if pa.renewing {
		pa.mu.Unlock()
		return
	}
	pa.renewing = true
	pa.mu.Unlock()
	s := pa.server
	lifetime := s.accountMutationLifetime
	if lifetime == nil {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		renew := time.NewTicker(accountterminal.DefaultTerminalControlLease / 3)
		defer renew.Stop()
		for {
			select {
			case <-lifetime.Done():
				return
			case <-pa.closed:
				return
			case <-renew.C:
				attachment := pa.current()
				renewCtx, cancel := context.WithTimeout(lifetime, accountMutationProbeWait)
				_, err := attachment.RenewControl(renewCtx)
				cancel()
				if err != nil {
					pa.mu.Lock()
					stale := pa.attachment != attachment
					pa.mu.Unlock()
					if stale {
						continue
					}
					pa.close()
					return
				}
			}
		}
	}()
}

func (pa *pollAttachment) close() {
	pa.mu.Lock()
	select {
	case <-pa.closed:
		pa.mu.Unlock()
		return
	default:
	}
	close(pa.closed)
	if pa.parked != nil {
		close(pa.parked)
		pa.parked = nil
	}
	attachment := pa.attachment
	pa.mu.Unlock()
	_ = attachment.Close()
	s := pa.server
	s.pollMu.Lock()
	if s.pollAttachments[pa.key] == pa {
		delete(s.pollAttachments, pa.key)
	}
	s.pollMu.Unlock()
}
