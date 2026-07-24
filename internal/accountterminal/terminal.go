package accountterminal

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/yasyf/daemonkit/proc"
)

const (
	// TerminalChunkSize is the largest input or output chunk retained in memory.
	TerminalChunkSize = 32 * 1024
	// TerminalQueueDepth bounds each direction of a terminal session.
	TerminalQueueDepth = 32
	// TerminalAttachmentLimit bounds observers plus the current controller.
	TerminalAttachmentLimit = 32
	// DefaultTerminalAttachTimeout bounds the durable but undispatched state.
	DefaultTerminalAttachTimeout = 5 * time.Second
	// DefaultTerminalControlLease bounds an unrenewed input-controller claim.
	DefaultTerminalControlLease = 30 * time.Second
	// DefaultTerminalSettledRetention bounds unacknowledged output replay.
	DefaultTerminalSettledRetention = 5 * time.Minute
	// DefaultTerminalDetachTimeout bounds a dispatched terminal without a controller.
	DefaultTerminalDetachTimeout = 5 * time.Minute

	defaultTerminalRows uint16 = 24
	defaultTerminalCols uint16 = 80
	terminationGrace           = 500 * time.Millisecond
)

// RecoveryID is the sole durable process class owned by account terminals.
const RecoveryID proc.RecoveryID = "com.yasyf.cc-pool.account-terminal.v1"

var (
	// ErrTerminalControllerAttached means a terminal already has an input controller.
	ErrTerminalControllerAttached = errors.New("terminal controller already attached")
	// ErrTerminalDetached means an operation targeted a closed attachment.
	ErrTerminalDetached = errors.New("terminal attachment is closed")
	// ErrTerminalNotController means an observer attempted an input operation.
	ErrTerminalNotController = errors.New("terminal attachment is not the controller")
	// ErrTerminalOutputLagged means an attachment fell behind the bounded replay window.
	ErrTerminalOutputLagged = errors.New("terminal attachment exceeded the output replay window")
	// ErrTerminalAttachmentLimit means the bounded attachment set is full.
	ErrTerminalAttachmentLimit = errors.New("terminal attachment limit reached")
	// ErrTerminalInputClosed means EOF has already closed terminal input.
	ErrTerminalInputClosed = errors.New("terminal input is closed")
	// ErrTerminalControlExpired means an unrenewed controller lease elapsed.
	ErrTerminalControlExpired = errors.New("terminal controller lease expired")
	// ErrTerminalOutputCursor means requested replay is outside the retained window.
	ErrTerminalOutputCursor = errors.New("terminal output cursor is unavailable")
	// ErrTerminalRetentionExpired means unacknowledged settled output was retired.
	ErrTerminalRetentionExpired = errors.New("terminal settled output retention expired")
	// ErrTerminalOutcomeMismatch means acknowledgement named another settlement.
	ErrTerminalOutcomeMismatch = errors.New("terminal outcome acknowledgement mismatch")
	// ErrTerminalSettled means a controller or input operation targeted a settled terminal.
	ErrTerminalSettled = errors.New("terminal is settled")
	// ErrTerminalDetachExpired means the terminal exceeded its no-controller deadline.
	ErrTerminalDetachExpired = errors.New("terminal detached deadline expired")
)

const terminalWrapper = `
printf r >&3
exec 3>&-
if ! IFS= read -r _ <&4; then
    exit 125
fi
exec 4<&-
exec "$@"
`

// TerminalSize is a PTY's character-cell geometry.
type TerminalSize struct {
	Rows uint16
	Cols uint16
}

func (s TerminalSize) normalized() TerminalSize {
	if s.Rows == 0 {
		s.Rows = defaultTerminalRows
	}
	if s.Cols == 0 {
		s.Cols = defaultTerminalCols
	}
	return s
}

// TerminalSpec describes one durably tracked interactive task.
type TerminalSpec struct {
	Path          string
	Args          []string
	Dir           string
	Env           []string
	Size          TerminalSize
	AttachTimeout time.Duration
	DetachTimeout time.Duration
	Retention     time.Duration
}

// Manager bounds account-login PTYs and owns their crash recovery boundary.
type Manager struct {
	capacity int
	reaper   *proc.Reaper

	mu         sync.Mutex
	recovered  bool
	recovering bool
	closed     bool
	nextID     uint64
	active     map[uint64]context.CancelFunc
	settleErrs []error
	changed    chan struct{}
}

// NewManager constructs an idle account-terminal manager.
func NewManager(capacity int, reaper *proc.Reaper) (*Manager, error) {
	if capacity <= 0 {
		return nil, errors.New("account terminal: capacity must be positive")
	}
	if reaper == nil {
		return nil, errors.New("account terminal: reaper is required")
	}
	return &Manager{
		capacity: capacity,
		reaper:   reaper,
		active:   make(map[uint64]context.CancelFunc),
		changed:  make(chan struct{}),
	}, nil
}

// Recover settles every account terminal retained by an earlier process generation.
func (m *Manager) Recover(ctx context.Context) error {
	m.mu.Lock()
	switch {
	case m.closed:
		m.mu.Unlock()
		return errors.New("account terminal: manager is closed")
	case m.recovered:
		m.mu.Unlock()
		return nil
	case m.recovering:
		m.mu.Unlock()
		return errors.New("account terminal: recovery is already running")
	case len(m.active) != 0:
		m.mu.Unlock()
		return errors.New("account terminal: recovery requires an idle manager")
	}
	m.recovering = true
	m.mu.Unlock()

	err := m.reaper.Reap(ctx)
	m.mu.Lock()
	m.recovering = false
	if err == nil {
		m.recovered = true
	}
	m.notifyLocked()
	m.mu.Unlock()
	if err != nil {
		return fmt.Errorf("account terminal: recover retained processes: %w", err)
	}
	return nil
}

// Close stops admission, cancels every live PTY, and joins exact settlement.
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	if !m.closed {
		m.closed = true
		cancels := make([]context.CancelFunc, 0, len(m.active))
		for _, cancel := range m.active {
			cancels = append(cancels, cancel)
		}
		m.notifyLocked()
		m.mu.Unlock()
		for _, cancel := range cancels {
			cancel()
		}
	} else {
		m.mu.Unlock()
	}

	done := ctx.Done()
	var ctxErr error
	for {
		m.mu.Lock()
		if len(m.active) == 0 && !m.recovering {
			errs := append([]error(nil), m.settleErrs...)
			m.mu.Unlock()
			if ctxErr == nil {
				ctxErr = ctx.Err()
			}
			return errors.Join(append(errs, ctxErr)...)
		}
		changed := m.changed
		m.mu.Unlock()
		select {
		case <-changed:
		case <-done:
			ctxErr = fmt.Errorf("account terminal: close: %w", ctx.Err())
			done = nil
		}
	}
}

// TerminalInputKind identifies one ordered client-to-PTY event.
type TerminalInputKind uint8

const (
	// TerminalInputBytes writes Data to the PTY.
	TerminalInputBytes TerminalInputKind = iota + 1
	// TerminalInputResize applies Size to the PTY.
	TerminalInputResize
	// TerminalInputEOF closes input with the terminal EOF character.
	TerminalInputEOF
)

// TerminalInput is one bounded, ordered terminal input event.
type TerminalInput struct {
	Kind TerminalInputKind
	Data []byte
	Size TerminalSize
}

// TerminalDisconnectPolicy controls what closing an attachment does to its task.
type TerminalDisconnectPolicy uint8

const (
	// CancelOnDisconnect terminates and settles the task on attachment loss.
	CancelOnDisconnect TerminalDisconnectPolicy = iota + 1
	// DetachOnDisconnect leaves the bounded session available for reattachment.
	DetachOnDisconnect
)

// TerminalAttachmentRole defines whether an attachment can write terminal input.
type TerminalAttachmentRole uint8

const (
	// TerminalObserver receives the bounded output stream without controlling input.
	TerminalObserver TerminalAttachmentRole = iota + 1
	// TerminalController is the terminal's single exact-fenced input owner.
	TerminalController
)

// TerminalAttachmentSpec describes one observer or input controller.
type TerminalAttachmentSpec struct {
	Role             TerminalAttachmentRole
	DisconnectPolicy TerminalDisconnectPolicy
	ControlLease     time.Duration
	Cursor           *TerminalOutputCursor
}

// TerminalOutputCursor identifies the next output sequence an attachment expects.
type TerminalOutputCursor struct {
	NextSequence uint64
}

// TerminalOutput is one sequenced terminal output chunk.
type TerminalOutput struct {
	Sequence uint64
	Data     []byte
}

// NextCursor returns the exact reconnect cursor after this chunk.
func (o TerminalOutput) NextCursor() TerminalOutputCursor {
	return TerminalOutputCursor{NextSequence: o.Sequence + 1}
}

// TerminalControllerLease is the current input-controller fence and expiry.
type TerminalControllerLease struct {
	Fence   uint64
	Expires time.Time
}

// TerminalOutcomeKind identifies how the interactive task ended.
type TerminalOutcomeKind uint8

const (
	// TerminalExited reports an ordinary process exit code.
	TerminalExited TerminalOutcomeKind = iota + 1
	// TerminalSignaled reports a process terminated by a signal.
	TerminalSignaled
	// TerminalCanceled reports daemon-requested process-group termination.
	TerminalCanceled
)

// TerminalOutcome is the non-secret, typed settlement result.
type TerminalOutcome struct {
	Kind     TerminalOutcomeKind
	ExitCode int
	Signal   syscall.Signal
	Record   proc.Record
	Digest   [32]byte
}

// Terminal is one durably tracked PTY task with bounded output replay,
// multiple observers, and one exact-fenced input controller.
type Terminal struct {
	manager   *Manager
	record    proc.Record
	sessionID uint64
	cancel    context.CancelFunc
	master    *os.File
	gate      *os.File
	write     func([]byte) error

	input      chan terminalInputRequest
	ioErr      chan error
	ioStop     chan struct{}
	ioStopOnce sync.Once
	inDone     chan struct{}
	outDone    chan struct{}
	done       chan struct{}

	gateOnce         sync.Once
	mu               sync.Mutex
	nextID           uint64
	nextFence        uint64
	controllerID     uint64
	attachments      map[uint64]*terminalAttachmentState
	outputs          [][]byte
	outputBase       uint64
	outputNext       uint64
	inputEOF         bool
	result           TerminalOutcome
	resultErr        error
	settled          bool
	retention        time.Duration
	retentionTimer   *time.Timer
	acknowledged     bool
	retentionExpired bool
	retired          chan struct{}
	retireOnce       sync.Once
	detachTimeout    time.Duration
	detachTimer      *time.Timer
	detachEpoch      uint64
	detachExpired    bool
	attachExpired    bool
	attachTimer      *time.Timer
}

type terminalAttachmentState struct {
	sendMu     sync.Mutex
	role       TerminalAttachmentRole
	policy     TerminalDisconnectPolicy
	fence      uint64
	lease      time.Duration
	expires    time.Time
	leaseTimer *time.Timer
	cursor     uint64
	notify     chan struct{}
	closed     chan struct{}
	closeError error
}

type terminalInputRequest struct {
	attachmentID uint64
	fence        uint64
	event        TerminalInput
	result       chan error
}

// TerminalAttachment is one exact-fenced view of a terminal's bounded output.
type TerminalAttachment struct {
	terminal *Terminal
	id       uint64
	state    *terminalAttachmentState
	stopCtx  func() bool
	once     sync.Once
}

// Start creates a tracked PTY task behind a dispatch gate. Attach must
// claim it before AttachTimeout or the task is canceled and reaped.
func (m *Manager) Start(startup context.Context, spec TerminalSpec) (*Terminal, error) {
	if spec.Path == "" {
		return nil, errors.New("account terminal: terminal path is required")
	}
	attachTimeout := spec.AttachTimeout
	if attachTimeout <= 0 {
		attachTimeout = DefaultTerminalAttachTimeout
	}
	retention := spec.Retention
	if retention <= 0 {
		retention = DefaultTerminalSettledRetention
	}
	detachTimeout := spec.DetachTimeout
	if detachTimeout <= 0 {
		detachTimeout = DefaultTerminalDetachTimeout
	}
	lifetime := context.WithoutCancel(startup)
	terminalCtx, sessionID, cancel, err := m.acquire(startup, lifetime)
	if err != nil {
		return nil, err
	}
	startupCtx, cancelStartup := context.WithCancel(startup)
	stopStartup := context.AfterFunc(terminalCtx, cancelStartup)
	defer func() {
		stopStartup()
		cancelStartup()
	}()
	cleanupSlot := true
	var cleanupErr error
	defer func() {
		if cleanupSlot {
			cancel()
			m.release(sessionID, cleanupErr)
		}
	}()

	args := make([]string, 0, len(spec.Args)+4)
	args = append(args, "-c", terminalWrapper, "daemonkit-terminal", spec.Path)
	args = append(args, spec.Args...)
	// #nosec G204 -- the fixed shell executes a fixed wrapper; Path and Args remain positional data.
	cmd := exec.Command("/bin/sh", args...)
	cmd.Dir = spec.Dir
	cmd.Env = spec.Env
	readyR, readyW, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("account terminal: terminal readiness pipe: %w", err)
	}
	gateR, gateW, err := os.Pipe()
	if err != nil {
		_ = readyR.Close()
		_ = readyW.Close()
		return nil, fmt.Errorf("account terminal: terminal dispatch gate: %w", err)
	}
	cmd.ExtraFiles = []*os.File{readyW, gateR}
	size := spec.Size.normalized()
	master, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: size.Rows, Cols: size.Cols})
	if err != nil {
		_ = readyR.Close()
		_ = readyW.Close()
		_ = gateR.Close()
		_ = gateW.Close()
		return nil, fmt.Errorf("account terminal: start terminal: %w", err)
	}
	_ = readyW.Close()
	_ = gateR.Close()
	if err := awaitWrapperReady(startupCtx, readyR); err != nil {
		_ = gateW.Close()
		_ = master.Close()
		killErr := killUntrackedGroup(cmd.Process.Pid)
		cleanupErr = killErr
		waitErr := cmd.Wait()
		return nil, errors.Join(fmt.Errorf("account terminal: await terminal readiness: %w", err), killErr, unexpectedWaitError(waitErr))
	}
	record, err := m.reaper.TrackGroup(terminalCtx, cmd.Process.Pid, RecoveryID)
	if err != nil {
		_ = gateW.Close()
		_ = master.Close()
		killErr := killUntrackedGroup(cmd.Process.Pid)
		cleanupErr = killErr
		waitErr := cmd.Wait()
		return nil, errors.Join(fmt.Errorf("account terminal: track terminal: %w", err), killErr, unexpectedWaitError(waitErr))
	}
	if recordErr := record.Validate(); recordErr != nil || record.PID != cmd.Process.Pid || !record.ProcessGroup || record.SessionID != record.PID {
		_ = gateW.Close()
		_ = master.Close()
		killErr := killUntrackedGroup(cmd.Process.Pid)
		waitErr := cmd.Wait()
		untrackErr := m.reaper.Untrack(context.WithoutCancel(terminalCtx), record)
		cleanupErr = errors.Join(killErr, untrackErr)
		return nil, errors.Join(errors.New("account terminal: registry returned an invalid terminal process record"), recordErr, untrackErr, killErr, unexpectedWaitError(waitErr))
	}
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	if err := startupCtx.Err(); err != nil {
		_ = gateW.Close()
		_ = master.Close()
		settleErr := m.reaper.TerminateWithin(context.WithoutCancel(terminalCtx), record, terminationGrace)
		cleanupErr = settleErr
		waitErr := <-waited
		return nil, errors.Join(
			fmt.Errorf("account terminal: terminal canceled after tracking: %w", err),
			settleErr,
			unexpectedWaitError(waitErr),
		)
	}

	t := &Terminal{
		manager: m, record: record, sessionID: sessionID, cancel: cancel, master: master, gate: gateW,
		input: make(chan terminalInputRequest, TerminalQueueDepth), ioErr: make(chan error, 1),
		ioStop: make(chan struct{}), inDone: make(chan struct{}), outDone: make(chan struct{}), done: make(chan struct{}),
		attachments: make(map[uint64]*terminalAttachmentState), retention: retention, retired: make(chan struct{}),
		detachTimeout: detachTimeout,
	}
	t.write = func(data []byte) error { return writeTerminal(master, data) }
	t.attachTimer = time.AfterFunc(attachTimeout, t.expireAttach)
	go t.pumpInput()
	go t.pumpOutput()
	go t.run(terminalCtx, waited)
	cleanupSlot = false
	return t, nil
}

func (m *Manager) acquire(
	wait context.Context,
	lifetime context.Context,
) (context.Context, uint64, context.CancelFunc, error) {
	for {
		m.mu.Lock()
		switch {
		case m.closed:
			m.mu.Unlock()
			return nil, 0, nil, errors.New("account terminal: manager is closed")
		case !m.recovered:
			m.mu.Unlock()
			return nil, 0, nil, errors.New("account terminal: recovery has not completed")
		case len(m.active) < m.capacity:
			terminalCtx, cancel := context.WithCancel(lifetime)
			m.nextID++
			id := m.nextID
			m.active[id] = cancel
			m.mu.Unlock()
			return terminalCtx, id, cancel, nil
		}
		changed := m.changed
		m.mu.Unlock()

		select {
		case <-wait.Done():
			return nil, 0, nil, fmt.Errorf("account terminal: wait for capacity: %w", wait.Err())
		case <-changed:
		}
	}
}

func (m *Manager) release(id uint64, settleErr error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.active[id]; !ok {
		panic(fmt.Sprintf("account terminal: release unknown session %d", id))
	}
	delete(m.active, id)
	if settleErr != nil {
		m.settleErrs = append(m.settleErrs, settleErr)
	}
	m.notifyLocked()
}

func (m *Manager) notifyLocked() {
	close(m.changed)
	m.changed = make(chan struct{})
}

// Record returns the immutable process-group identity recorded before dispatch.
func (t *Terminal) Record() proc.Record { return t.record }

// Attach adds one bounded observer or claims the single input-controller fence.
// The task dispatches only after a controller is attached or claimed.
func (t *Terminal) Attach(ctx context.Context, spec TerminalAttachmentSpec) (*TerminalAttachment, error) {
	if err := validateTerminalAttachmentSpec(spec); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("account terminal: attach terminal: %w", err)
	}
	t.mu.Lock()
	if t.acknowledged || t.retentionExpired {
		t.mu.Unlock()
		return nil, ErrTerminalRetentionExpired
	}
	if t.detachExpired {
		t.mu.Unlock()
		return nil, ErrTerminalDetachExpired
	}
	if t.attachExpired {
		t.mu.Unlock()
		return nil, errors.New("account terminal: terminal attach deadline expired")
	}
	if len(t.attachments) >= TerminalAttachmentLimit {
		t.mu.Unlock()
		return nil, ErrTerminalAttachmentLimit
	}
	if spec.Role == TerminalController && t.controllerID != 0 {
		t.mu.Unlock()
		return nil, ErrTerminalControllerAttached
	}
	if spec.Role == TerminalController && t.settled {
		t.mu.Unlock()
		return nil, ErrTerminalSettled
	}
	cursor := t.outputBase
	if spec.Cursor != nil {
		cursor = spec.Cursor.NextSequence
		if cursor < t.outputBase || cursor > t.outputNext {
			t.mu.Unlock()
			return nil, fmt.Errorf("%w: retained=[%d,%d] requested=%d", ErrTerminalOutputCursor, t.outputBase, t.outputNext, cursor)
		}
	}
	t.nextID++
	id := t.nextID
	state := &terminalAttachmentState{
		role: spec.Role, policy: spec.DisconnectPolicy, cursor: cursor,
		notify: make(chan struct{}, 1), closed: make(chan struct{}),
	}
	t.attachments[id] = state
	controller := spec.Role == TerminalController
	if controller {
		t.claimControllerLocked(id, spec.DisconnectPolicy, spec.ControlLease)
	}
	t.mu.Unlock()

	if controller {
		if err := t.releaseDispatchGate(); err != nil {
			t.detach(id, err)
			return nil, err
		}
	}
	a := &TerminalAttachment{terminal: t, id: id, state: state}
	a.stopCtx = context.AfterFunc(ctx, func() { _ = a.Close() })
	return a, nil
}

// ClaimControl atomically promotes an observer when no controller is attached.
func (a *TerminalAttachment) ClaimControl(
	ctx context.Context,
	policy TerminalDisconnectPolicy,
	lease time.Duration,
) (TerminalControllerLease, error) {
	if err := validateTerminalDisconnectPolicy(policy); err != nil {
		return TerminalControllerLease{}, err
	}
	lease = normalizeTerminalControlLease(lease)
	if err := ctx.Err(); err != nil {
		return TerminalControllerLease{}, fmt.Errorf("account terminal: claim terminal control: %w", err)
	}
	t := a.terminal
	t.mu.Lock()
	state, ok := t.attachments[a.id]
	if !ok || state != a.state {
		t.mu.Unlock()
		return TerminalControllerLease{}, a.detachedError()
	}
	if t.attachExpired {
		t.mu.Unlock()
		return TerminalControllerLease{}, errors.New("account terminal: terminal attach deadline expired")
	}
	if t.detachExpired {
		t.mu.Unlock()
		return TerminalControllerLease{}, ErrTerminalDetachExpired
	}
	if t.settled {
		t.mu.Unlock()
		return TerminalControllerLease{}, ErrTerminalSettled
	}
	if t.controllerID != 0 {
		t.mu.Unlock()
		return TerminalControllerLease{}, ErrTerminalControllerAttached
	}
	control := t.claimControllerLocked(a.id, policy, lease)
	t.mu.Unlock()
	if err := t.releaseDispatchGate(); err != nil {
		t.detach(a.id, err)
		return TerminalControllerLease{}, err
	}
	return control, nil
}

// HandoffControl atomically transfers the controller fence to an attached observer.
func (a *TerminalAttachment) HandoffControl(
	target *TerminalAttachment,
	policy TerminalDisconnectPolicy,
	lease time.Duration,
) (TerminalControllerLease, error) {
	if err := validateTerminalDisconnectPolicy(policy); err != nil {
		return TerminalControllerLease{}, err
	}
	lease = normalizeTerminalControlLease(lease)
	if target == nil || target.terminal != a.terminal || target.id == a.id {
		return TerminalControllerLease{}, errors.New("account terminal: terminal control target must be a distinct attachment on the same terminal")
	}
	t := a.terminal
	t.mu.Lock()
	defer t.mu.Unlock()
	current, currentOK := t.attachments[a.id]
	next, nextOK := t.attachments[target.id]
	if !currentOK || current != a.state {
		return TerminalControllerLease{}, a.detachedErrorLocked()
	}
	if !nextOK || next != target.state {
		return TerminalControllerLease{}, target.detachedErrorLocked()
	}
	if t.controllerID != a.id || current.role != TerminalController {
		return TerminalControllerLease{}, ErrTerminalNotController
	}
	if next.role != TerminalObserver {
		return TerminalControllerLease{}, ErrTerminalControllerAttached
	}
	stopTerminalLease(current)
	current.role = TerminalObserver
	current.policy = 0
	current.fence = 0
	return t.claimControllerLocked(target.id, policy, lease), nil
}

// ReleaseControl atomically demotes the controller to an observer without canceling the task.
func (a *TerminalAttachment) ReleaseControl() error {
	t := a.terminal
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.attachments[a.id]
	if !ok || state != a.state {
		return a.detachedErrorLocked()
	}
	if t.controllerID != a.id || state.role != TerminalController {
		return ErrTerminalNotController
	}
	stopTerminalLease(state)
	state.role = TerminalObserver
	state.policy = 0
	state.fence = 0
	t.controllerID = 0
	t.armDetachedTimeoutLocked()
	return nil
}

// ControllerLease returns the attachment's exact live input-controller fence.
func (a *TerminalAttachment) ControllerLease() (TerminalControllerLease, error) {
	t := a.terminal
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.attachments[a.id]
	if !ok || state != a.state {
		return TerminalControllerLease{}, a.detachedErrorLocked()
	}
	if t.controllerID != a.id || state.role != TerminalController {
		return TerminalControllerLease{}, ErrTerminalNotController
	}
	return TerminalControllerLease{Fence: state.fence, Expires: state.expires}, nil
}

// RenewControl renews the same exact controller fence.
func (a *TerminalAttachment) RenewControl(ctx context.Context) (TerminalControllerLease, error) {
	if err := ctx.Err(); err != nil {
		return TerminalControllerLease{}, fmt.Errorf("account terminal: renew terminal control: %w", err)
	}
	t := a.terminal
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.attachments[a.id]
	if !ok || state != a.state {
		return TerminalControllerLease{}, a.detachedErrorLocked()
	}
	if t.controllerID != a.id || state.role != TerminalController {
		return TerminalControllerLease{}, ErrTerminalNotController
	}
	t.armControllerLeaseLocked(a.id, state)
	return TerminalControllerLease{Fence: state.fence, Expires: state.expires}, nil
}

// Send enqueues one ordered input event with context-aware backpressure.
func (a *TerminalAttachment) Send(ctx context.Context, event TerminalInput) error {
	a.state.sendMu.Lock()
	defer a.state.sendMu.Unlock()
	fence, err := a.terminal.validateInput(a.id, event)
	if err != nil {
		if errors.Is(err, ErrTerminalDetached) {
			return a.detachedError()
		}
		return err
	}
	request := terminalInputRequest{
		attachmentID: a.id,
		fence:        fence,
		event:        cloneTerminalInput(event),
		result:       make(chan error, 1),
	}
	select {
	case a.terminal.input <- request:
	case <-a.state.closed:
		return a.detachedError()
	case <-a.terminal.done:
		return ErrTerminalSettled
	case <-ctx.Done():
		return fmt.Errorf("account terminal: send terminal input: %w", ctx.Err())
	}
	var contextErr error
	done := ctx.Done()
	for {
		select {
		case err := <-request.result:
			return errors.Join(err, contextErr)
		case <-a.terminal.done:
			select {
			case err := <-request.result:
				return errors.Join(err, contextErr)
			default:
				return errors.Join(ErrTerminalSettled, contextErr)
			}
		case <-done:
			contextErr = fmt.Errorf("account terminal: send terminal input: %w", ctx.Err())
			done = nil
		}
	}
}

// Receive returns the next sequenced raw merged PTY output chunk.
func (a *TerminalAttachment) Receive(ctx context.Context) (TerminalOutput, error) {
	for {
		t := a.terminal
		t.mu.Lock()
		state, ok := t.attachments[a.id]
		if !ok || state != a.state {
			t.mu.Unlock()
			return TerminalOutput{}, a.detachedError()
		}
		if state.cursor < t.outputNext {
			if state.cursor < t.outputBase {
				t.mu.Unlock()
				t.detach(a.id, ErrTerminalOutputLagged)
				return TerminalOutput{}, ErrTerminalOutputLagged
			}
			output := TerminalOutput{
				Sequence: state.cursor,
				Data:     append([]byte(nil), t.outputs[state.cursor-t.outputBase]...),
			}
			state.cursor++
			t.mu.Unlock()
			return output, nil
		}
		if t.settled {
			t.mu.Unlock()
			return TerminalOutput{}, io.EOF
		}
		notify, closed := state.notify, state.closed
		t.mu.Unlock()
		select {
		case <-notify:
		case <-closed:
			return TerminalOutput{}, a.detachedError()
		case <-ctx.Done():
			return TerminalOutput{}, fmt.Errorf("account terminal: receive terminal output: %w", ctx.Err())
		}
	}
}

// Close applies the attachment's explicit disconnect policy.
func (a *TerminalAttachment) Close() error {
	a.once.Do(func() {
		if a.stopCtx != nil {
			a.stopCtx()
		}
		a.terminal.detach(a.id, nil)
	})
	return nil
}

// Cancel terminates, reaps, and durably untracks the terminal task.
func (t *Terminal) Cancel(ctx context.Context) error {
	t.cancel()
	done := ctx.Done()
	var ctxErr error
	for {
		select {
		case <-t.done:
			t.mu.Lock()
			resultErr := t.resultErr
			t.mu.Unlock()
			return errors.Join(resultErr, ctxErr)
		case <-done:
			ctxErr = fmt.Errorf("account terminal: cancel terminal: %w", ctx.Err())
			done = nil
		}
	}
}

// Wait returns the typed result after complete PTY and process settlement.
func (t *Terminal) Wait(ctx context.Context) (TerminalOutcome, error) {
	select {
	case <-t.done:
		t.mu.Lock()
		defer t.mu.Unlock()
		return t.result, t.resultErr
	case <-ctx.Done():
		return TerminalOutcome{}, fmt.Errorf("account terminal: wait for terminal: %w", ctx.Err())
	}
}

// Acknowledge retires replay only after the exact outcome was delivered.
func (t *Terminal) Acknowledge(ctx context.Context, digest [32]byte) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("account terminal: acknowledge terminal outcome: %w", err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.settled {
		return errors.New("account terminal: terminal is not settled")
	}
	if digest != t.result.Digest {
		return ErrTerminalOutcomeMismatch
	}
	if t.acknowledged {
		return nil
	}
	if len(t.attachments) != 0 {
		return errors.New("account terminal: terminal attachments remain during acknowledgement")
	}
	t.acknowledged = true
	if t.retentionTimer != nil {
		t.retentionTimer.Stop()
		t.retentionTimer = nil
	}
	t.clearOutputLocked()
	t.retireOnce.Do(func() { close(t.retired) })
	return nil
}

// Retired closes after exact acknowledgement or bounded retention expiry.
func (t *Terminal) Retired() <-chan struct{} { return t.retired }

func (t *Terminal) validateInput(id uint64, event TerminalInput) (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.attachments[id]
	if !ok {
		return 0, ErrTerminalDetached
	}
	if t.controllerID != id || state.role != TerminalController {
		return 0, ErrTerminalNotController
	}
	if t.settled {
		return 0, ErrTerminalSettled
	}
	switch event.Kind {
	case TerminalInputBytes:
		if t.inputEOF {
			return 0, ErrTerminalInputClosed
		}
		if len(event.Data) == 0 || len(event.Data) > TerminalChunkSize {
			return 0, fmt.Errorf("account terminal: terminal input must contain 1..%d bytes", TerminalChunkSize)
		}
	case TerminalInputResize:
		size := event.Size.normalized()
		if size != event.Size {
			return 0, errors.New("account terminal: terminal resize requires non-zero rows and columns")
		}
	case TerminalInputEOF:
		if t.inputEOF {
			return 0, ErrTerminalInputClosed
		}
	default:
		return 0, errors.New("account terminal: unknown terminal input event")
	}
	return state.fence, nil
}

func (t *Terminal) validateInputFence(request terminalInputRequest) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.attachments[request.attachmentID]
	if !ok {
		return ErrTerminalDetached
	}
	if t.controllerID != request.attachmentID || state.role != TerminalController || state.fence != request.fence {
		return ErrTerminalNotController
	}
	if t.settled {
		return ErrTerminalSettled
	}
	if t.inputEOF && request.event.Kind != TerminalInputResize {
		return ErrTerminalInputClosed
	}
	return nil
}

func (t *Terminal) markInputEOF() {
	t.mu.Lock()
	t.inputEOF = true
	t.mu.Unlock()
}

func (t *Terminal) expireAttach() {
	t.mu.Lock()
	if t.controllerID != 0 || t.attachExpired {
		t.mu.Unlock()
		return
	}
	t.attachExpired = true
	t.mu.Unlock()
	t.cancel()
}

func validateTerminalAttachmentSpec(spec TerminalAttachmentSpec) error {
	switch spec.Role {
	case TerminalObserver:
		if spec.DisconnectPolicy != 0 || spec.ControlLease != 0 {
			return errors.New("account terminal: terminal observers cannot define controller policy")
		}
	case TerminalController:
		if err := validateTerminalDisconnectPolicy(spec.DisconnectPolicy); err != nil {
			return err
		}
	default:
		return errors.New("account terminal: terminal attachment role is required")
	}
	return nil
}

func validateTerminalDisconnectPolicy(policy TerminalDisconnectPolicy) error {
	if policy != CancelOnDisconnect && policy != DetachOnDisconnect {
		return errors.New("account terminal: terminal controller disconnect policy is required")
	}
	return nil
}

func normalizeTerminalControlLease(lease time.Duration) time.Duration {
	if lease <= 0 {
		return DefaultTerminalControlLease
	}
	return lease
}

func (t *Terminal) claimControllerLocked(
	id uint64,
	policy TerminalDisconnectPolicy,
	lease time.Duration,
) TerminalControllerLease {
	state := t.attachments[id]
	t.nextFence++
	state.role = TerminalController
	state.policy = policy
	state.fence = t.nextFence
	state.lease = normalizeTerminalControlLease(lease)
	t.controllerID = id
	t.detachEpoch++
	if t.detachTimer != nil {
		t.detachTimer.Stop()
		t.detachTimer = nil
	}
	t.armControllerLeaseLocked(id, state)
	if t.attachTimer != nil {
		t.attachTimer.Stop()
		t.attachTimer = nil
	}
	return TerminalControllerLease{Fence: state.fence, Expires: state.expires}
}

func (t *Terminal) armControllerLeaseLocked(id uint64, state *terminalAttachmentState) {
	if state.leaseTimer != nil {
		state.leaseTimer.Stop()
	}
	state.expires = time.Now().Add(state.lease)
	fence := state.fence
	state.leaseTimer = time.AfterFunc(state.lease, func() { t.expireController(id, fence) })
}

func stopTerminalLease(state *terminalAttachmentState) {
	if state.leaseTimer != nil {
		state.leaseTimer.Stop()
		state.leaseTimer = nil
	}
	state.expires = time.Time{}
	state.lease = 0
}

func (t *Terminal) expireController(id, fence uint64) {
	t.mu.Lock()
	state, ok := t.attachments[id]
	if !ok || state.fence != fence || t.controllerID != id || time.Now().Before(state.expires) {
		t.mu.Unlock()
		return
	}
	cancel := t.detachLocked(id, ErrTerminalControlExpired)
	t.mu.Unlock()
	if cancel {
		t.cancel()
	}
}

func (t *Terminal) releaseDispatchGate() error {
	var gateErr error
	t.gateOnce.Do(func() {
		gateErr = writePayload(t.gate, []byte("start\n"))
	})
	if gateErr == nil {
		return nil
	}
	t.cancel()
	<-t.done
	t.mu.Lock()
	resultErr := t.resultErr
	t.mu.Unlock()
	return errors.Join(fmt.Errorf("account terminal: release terminal dispatch gate: %w", gateErr), resultErr)
}

func (a *TerminalAttachment) detachedError() error {
	a.terminal.mu.Lock()
	defer a.terminal.mu.Unlock()
	return a.detachedErrorLocked()
}

func (a *TerminalAttachment) detachedErrorLocked() error {
	if a.state.closeError != nil {
		return a.state.closeError
	}
	return ErrTerminalDetached
}

func (t *Terminal) detach(id uint64, cause error) {
	t.mu.Lock()
	cancel := t.detachLocked(id, cause)
	t.mu.Unlock()
	if cancel {
		t.cancel()
	}
}

func (t *Terminal) detachLocked(id uint64, cause error) bool {
	state, ok := t.attachments[id]
	if !ok {
		return false
	}
	delete(t.attachments, id)
	stopTerminalLease(state)
	cancel := false
	if t.controllerID == id {
		t.controllerID = 0
		cancel = state.policy == CancelOnDisconnect
		if !cancel {
			t.armDetachedTimeoutLocked()
		}
	}
	if cause == nil {
		cause = ErrTerminalDetached
	}
	state.closeError = cause
	close(state.closed)
	return cancel
}

func (t *Terminal) run(ctx context.Context, waited <-chan error) {
	var outcome TerminalOutcome
	var resultErr error
	defer func() {
		t.cancel()
		close(t.done)
		t.manager.release(t.sessionID, resultErr)
	}()
	canceled := false
	select {
	case waitErr := <-waited:
		outcome = terminalOutcome(t.record, waitErr, false)
		resultErr = t.manager.reaper.TerminateWithin(
			context.WithoutCancel(ctx), t.record, terminationGrace,
		)
		var exitErr *exec.ExitError
		if waitErr != nil && !errors.As(waitErr, &exitErr) {
			resultErr = errors.Join(resultErr, fmt.Errorf("account terminal: wait for terminal process: %w", waitErr))
		}
	case <-ctx.Done():
		canceled = true
		t.gateOnce.Do(func() { _ = t.gate.Close() })
		t.stopIO()
		resultErr = t.manager.reaper.TerminateWithin(
			context.WithoutCancel(ctx), t.record, terminationGrace,
		)
		waitErr := <-waited
		outcome = terminalOutcome(t.record, waitErr, true)
		select {
		case ioErr := <-t.ioErr:
			resultErr = errors.Join(resultErr, ioErr)
		default:
		}
	}
	_ = t.gate.Close()
	if canceled || resultErr != nil {
		t.stopIO()
	}
	<-t.outDone
	t.stopIO()
	<-t.inDone
	outcome.Digest = terminalOutcomeDigest(outcome)
	t.mu.Lock()
	t.result = outcome
	t.resultErr = resultErr
	t.controllerID = 0
	t.settled = true
	if t.detachTimer != nil {
		t.detachTimer.Stop()
		t.detachTimer = nil
	}
	for _, state := range t.attachments {
		stopTerminalLease(state)
		notifyTerminalAttachment(state)
	}
	t.retentionTimer = time.AfterFunc(t.retention, t.expireRetention)
	t.mu.Unlock()
}

func (t *Terminal) stopIO() {
	t.ioStopOnce.Do(func() {
		close(t.ioStop)
		_ = t.master.Close()
	})
}

func (t *Terminal) expireRetention() {
	t.mu.Lock()
	if t.acknowledged || t.retentionExpired {
		t.mu.Unlock()
		return
	}
	t.retentionExpired = true
	t.controllerID = 0
	for id, state := range t.attachments {
		delete(t.attachments, id)
		stopTerminalLease(state)
		state.closeError = ErrTerminalRetentionExpired
		close(state.closed)
	}
	t.clearOutputLocked()
	t.mu.Unlock()
	t.retireOnce.Do(func() { close(t.retired) })
}

func (t *Terminal) clearOutputLocked() {
	for index := range t.outputs {
		t.outputs[index] = nil
	}
	t.outputs = nil
	t.outputBase = t.outputNext
}

func (t *Terminal) armDetachedTimeoutLocked() {
	if t.settled || t.controllerID != 0 || t.detachExpired {
		return
	}
	if t.detachTimer != nil {
		t.detachTimer.Stop()
	}
	t.detachEpoch++
	epoch := t.detachEpoch
	t.detachTimer = time.AfterFunc(t.detachTimeout, func() { t.expireDetached(epoch) })
}

func (t *Terminal) expireDetached(epoch uint64) {
	t.mu.Lock()
	if t.settled || t.controllerID != 0 || t.detachExpired || t.detachEpoch != epoch {
		t.mu.Unlock()
		return
	}
	t.detachExpired = true
	t.detachTimer = nil
	t.mu.Unlock()
	t.cancel()
}

func (t *Terminal) pumpInput() {
	defer close(t.inDone)
	for {
		select {
		case <-t.ioStop:
			return
		case request := <-t.input:
			if err := t.validateInputFence(request); err != nil {
				request.result <- err
				continue
			}
			var err error
			switch request.event.Kind {
			case TerminalInputBytes:
				err = t.write(request.event.Data)
			case TerminalInputResize:
				err = pty.Setsize(t.master, &pty.Winsize{Rows: request.event.Size.Rows, Cols: request.event.Size.Cols})
			case TerminalInputEOF:
				err = t.write([]byte{4})
				if err == nil {
					t.markInputEOF()
				}
			}
			if err != nil {
				request.result <- fmt.Errorf("account terminal: apply terminal input: %w", err)
				t.failIO(fmt.Errorf("account terminal: apply terminal input: %w", err))
				return
			}
			request.result <- nil
		}
	}
}

func (t *Terminal) failIO(err error) {
	select {
	case <-t.ioStop:
		return
	default:
	}
	select {
	case t.ioErr <- err:
	default:
	}
	t.cancel()
}

func writeTerminal(master *os.File, data []byte) error {
	for len(data) > 0 {
		n, err := master.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func (t *Terminal) pumpOutput() {
	defer close(t.outDone)
	buf := make([]byte, TerminalChunkSize)
	for {
		n, err := t.master.Read(buf)
		if n > 0 {
			t.publishOutput(buf[:n])
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, syscall.EIO) {
				select {
				case <-t.ioStop:
				default:
					t.failIO(fmt.Errorf("account terminal: read terminal output: %w", err))
				}
			}
			return
		}
	}
}

func (t *Terminal) publishOutput(payload []byte) {
	chunk := append([]byte(nil), payload...)
	cancel := false
	t.mu.Lock()
	if len(t.outputs) == TerminalQueueDepth {
		oldest := t.outputBase
		for id, state := range t.attachments {
			if state.cursor > oldest {
				continue
			}
			cancel = t.detachLocked(id, ErrTerminalOutputLagged) || cancel
		}
		t.outputs[0] = nil
		t.outputs = t.outputs[1:]
		t.outputBase++
	}
	t.outputs = append(t.outputs, chunk)
	t.outputNext++
	for _, state := range t.attachments {
		notifyTerminalAttachment(state)
	}
	t.mu.Unlock()
	if cancel {
		t.cancel()
	}
}

func notifyTerminalAttachment(state *terminalAttachmentState) {
	select {
	case state.notify <- struct{}{}:
	default:
	}
}

func terminalOutcome(record proc.Record, err error, canceled bool) TerminalOutcome {
	out := TerminalOutcome{Record: record, ExitCode: -1}
	if canceled {
		out.Kind = TerminalCanceled
		return out
	}
	if err == nil {
		out.Kind = TerminalExited
		out.ExitCode = 0
		return out
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			out.Kind = TerminalSignaled
			out.Signal = status.Signal()
			return out
		}
		out.Kind = TerminalExited
		out.ExitCode = exitErr.ExitCode()
		return out
	}
	return out
}

func terminalOutcomeDigest(out TerminalOutcome) [32]byte {
	payload, err := json.Marshal(struct {
		Kind     TerminalOutcomeKind `json:"kind"`
		ExitCode int                 `json:"exit_code"`
		Signal   syscall.Signal      `json:"signal"`
		Record   proc.Record         `json:"record"`
	}{Kind: out.Kind, ExitCode: out.ExitCode, Signal: out.Signal, Record: out.Record})
	if err != nil {
		panic(err)
	}
	return sha256.Sum256(payload)
}

func cloneTerminalInput(event TerminalInput) TerminalInput {
	clone := event
	clone.Data = append([]byte(nil), event.Data...)
	return clone
}

func awaitWrapperReady(ctx context.Context, ready *os.File) error {
	result := make(chan error, 1)
	go func() {
		var marker [1]byte
		_, err := io.ReadFull(ready, marker[:])
		if err == nil && marker[0] != 'r' {
			err = fmt.Errorf("unexpected marker %q", marker[0])
		}
		result <- err
	}()
	select {
	case err := <-result:
		_ = ready.Close()
		return err
	case <-ctx.Done():
		_ = ready.Close()
		<-result
		return ctx.Err()
	}
}

func killUntrackedGroup(pid int) error {
	err := syscall.Kill(-pid, syscall.SIGKILL)
	if err == nil || errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return fmt.Errorf("account terminal: kill untracked process group: %w", err)
}

func writePayload(writer io.WriteCloser, payload []byte) error {
	_, writeErr := writer.Write(payload)
	closeErr := writer.Close()
	return errors.Join(writeErr, closeErr)
}

func unexpectedWaitError(err error) error {
	if err == nil {
		return errors.New("account terminal: terminated process exited successfully")
	}
	return nil
}
