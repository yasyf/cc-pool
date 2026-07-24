package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yasyf/cc-pool/internal/accountterminal"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/cc-pool/internal/workerexec"
	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/synckit/syncservice"
)

// reservationTTL suppresses re-picking an account until its claude becomes
// visible to procscan.
const reservationTTL = 30 * time.Second

// provisionalSelectionTTL outlives the CLI's 60-second cross-account launch
// budget; committed reservations still use reservationTTL only until procscan sees claude.
const provisionalSelectionTTL = 90 * time.Second

// selectRequestTimeout bounds selection lifecycle repair below both the server
// connection and client transport deadlines, preventing late reservations after
// a client has fallen back.
const (
	selectRequestTimeout = 55 * time.Second
	selectConnTimeout    = 58 * time.Second
)

// preflightTimeout bounds required credential preflight below the selection
// reservation lifetime and the outer request deadline.
const preflightTimeout = 8 * time.Second

// defaultEvictTimeout bounds how long a starting daemon waits for a
// version-skewed holder to release the socket after being told to step down.
const defaultEvictTimeout = 5 * time.Second

const (
	daemonShutdownTimeout = 30 * time.Second
	accountTerminalLimit  = 4
)

// Server is the running daemon.
type Server struct {
	m          *pool.Manager
	socket     string
	syncSocket string // synckit consumer socket; tests point it into a short temp dir
	snapshot   string // status mirror path; tests point it into a temp dir
	log        *log.Logger

	// evictTimeout bounds the wait for a skewed holder to release the socket.
	evictTimeout time.Duration

	// wg tracks product background loops canceled by the activation lifetime.
	wg sync.WaitGroup

	execMu     sync.Mutex
	execCancel context.CancelFunc

	// closing (set by drain's MarkClosing after settle) stops runTable/runDueTable
	// starting a new pass in the teardown window before executors cancel.
	closing atomic.Bool

	// cl is the account-claim discipline: select reservations plus poll, convert,
	// and pool-wide claims (see claims.go). It owns its own mutex.
	cl *claims

	// netOutage is set when a full poll sweep found every attempted account
	// failing network-class (an outage). While set, the scheduler drops to a
	// short canary cadence (nextPollDelay) and pollOnce probes only one account
	// until connectivity returns. Scheduler-goroutine-local — no lock.
	netOutage bool
	// netProbeLogSkip throttles the per-canary "network unreachable" log while an
	// outage persists: only every netProbeLogEvery-th probe logs (the rest prove
	// liveness silently) so a multi-hour outage does not spam the log. Reset to 0
	// on each outage entry. Scheduler-goroutine-local — no lock.
	netProbeLogSkip int

	// scanSessions is the narrow deterministic test seam. Production installs
	// scanProcesses so heartbeat reconciliation observes one atomic process table.
	scanSessions  func(context.Context) ([]procscan.Session, error)
	scanProcesses func(context.Context) (procscan.Snapshot, error)
	// heartbeat is the one daemon-wide procscan cache shared by polling, healing,
	// content coordination, and selection. heartbeatMu only protects lazy setup.
	heartbeatMu       sync.Mutex
	heartbeat         *sessionHeartbeat
	heartbeatInterval time.Duration
	adoptionMu        sync.Mutex
	adoptionNext      map[string]time.Time
	adoptRotated      func(context.Context, store.Account) error

	// pollSpacing overrides perAccountSpacing (the inter-sample delay); zero means
	// the default. Tests shrink it so a multi-account sweep does not sleep for
	// seconds per account.
	pollSpacing time.Duration

	// startedAt stamps daemon start. The skew-replace gate requires uptime ≥
	// reservationTTL: a fresh daemon's reservation map is empty while a recent
	// select may not have exec'd its claude yet.
	startedAt time.Time

	tenantClient               *tenantfs.Client
	tenantCoordinator          *tenantCoordinator
	holderSessionDone          <-chan struct{}
	holderMonitorMu            sync.Mutex
	holderMonitorCancel        context.CancelFunc
	holderActive               atomic.Bool
	holderLost                 atomic.Bool
	runtimePublished           atomic.Bool
	runtimeShutdown            func(context.Context) error
	runtimeHealth              func(context.Context) (dkdaemon.Health, error)
	bootstrapMu                sync.Mutex
	bootstrap                  bootstrapState
	prepareAccount             func(context.Context, store.Account) (catalogproto.TenantPreparationProof, error)
	prepareReservedAccount     func(context.Context, store.PendingAccountReservation) (catalogproto.TenantPreparationProof, error)
	observePresentationBinding func(context.Context, store.Account, store.PresentationPreparationProof) error
	activatePrepared           func(context.Context, store.Account, catalogproto.TenantPreparationProof, func() error) error
	preflightCredential        func(context.Context, store.Account) error
	disposableWorkers          *worker.Pool
	accountTerminals           *accountterminal.Manager

	// led is the product self-heal ledger for auth.streak and the
	// ratelimit.acct / ratelimit.pool streaks. ledMu is its
	// serialization: every s.led access takes ledMu, never held across
	// mount/reconcile/re-register/bounce or usage-fetch/refresh I/O (bookkeeping in, I/O
	// out). See ccn doc 36b05ef.
	led   *ledgers
	ledMu sync.Mutex

	// syncClient is the persistent client to the dedicated host-sync helper.
	// syncSelf is this host's registry origin name.
	syncClient   *syncservice.Client
	syncSelf     string
	syncAuthKind func(context.Context, int, string) (store.AuthKind, error)
	// launchSyncHelper replaces managed child launch in focused runtime tests.
	launchSyncHelper func(context.Context, string, string) error

	// syncPull runs one converge pull for the invalid_grant self-heal (syncHeal)
	// and the on-demand preflight pull; nil ⇒ sync disabled.
	syncPull func(ctx context.Context) error

	accountMutationTerminal accountMutationTerminalRunner
	accountMutationLifetime context.Context
	accountMutationOwner    func() (proc.Record, error)
	accountMutationMu       sync.Mutex
	accountMutationRuns     map[store.AccountMutationID]*accountMutationRun
}

var ensureHolderRuntime = EnsureHolderService

// Run is the entry point for `cc-pool daemon`. It blocks until the process
// is signalled.
func Run(ctx context.Context) error {
	if err := pool.EnsureStateDir(); err != nil {
		return err
	}
	s := &Server{
		socket:       pool.SocketPath(),
		syncSocket:   pool.SyncSocketPath(),
		snapshot:     pool.StatusSnapshotPath(),
		log:          log.New(os.Stderr, "[cc-pool] ", log.LstdFlags),
		evictTimeout: defaultEvictTimeout,
		startedAt:    time.Now(),
		cl:           newClaims(),
		led:          newLedgers(),
	}
	return s.serve(ctx)
}

func (s *Server) activate(activation dkdaemon.Activation) (err error) {
	s.beginBootstrap()
	defer func() {
		if err != nil {
			s.finishBootstrap(err)
		}
	}()
	s.holderActive.Store(false)
	s.holderLost.Store(false)
	s.runtimePublished.Store(false)
	if err := ensureHolderRuntime(activation.Context()); err != nil {
		return err
	}
	if s.m == nil {
		return errors.New("daemon manager is unavailable")
	}
	generation, err := proc.ProcessGeneration()
	if err != nil {
		return fmt.Errorf("derive account terminal generation: %w", err)
	}
	terminals, err := accountterminal.NewManager(accountTerminalLimit, &proc.Reaper{
		Store: &proc.FileStore{Path: pool.AccountTerminalProcessStorePath()}, Generation: generation,
	})
	if err != nil {
		return err
	}
	s.accountTerminals = terminals
	if err := terminals.Recover(activation.Context()); err != nil {
		return fmt.Errorf("recover account terminals: %w", err)
	}
	tenantClient, err := tenantfs.NewClient(activation.Context(), pool.FuseKitSocketPath())
	if err != nil {
		return fmt.Errorf("connect FuseKit runtime: %w", err)
	}
	preparer, err := tenantfs.NewPreparer(tenantClient)
	if err != nil {
		return err
	}
	workers := s.m.DisposableWorkers()
	s.m.ClaimCredentialMutation = func(accountID int) (func(), error) {
		if !s.cl.ownExclusive(accountID) {
			return nil, errAccountExclusive
		}
		return func() { s.cl.releaseExclusive(accountID) }, nil
	}
	s.tenantClient = tenantClient
	s.holderSessionDone = tenantClient.Done()
	s.disposableWorkers = workers
	s.accountMutationTerminal = managedAccountMutationTerminalRunner{terminals: terminals, manager: s.m}
	s.accountMutationLifetime = activation.Context()
	s.scanSessions = s.m.ScanSessions
	s.scanProcesses = s.m.ScanProcesses
	s.tenantCoordinator = newTenantCoordinator(activation.Context(), s, preparer, tenantClient)
	if err := s.tenantCoordinator.initialize(activation.Context()); err != nil {
		return fmt.Errorf("initialize FuseKit tenants: %w", err)
	}
	if err := s.recoverRetiredAccountMutations(activation.Context()); err != nil {
		return fmt.Errorf("recover account mutations: %w", err)
	}
	if err := s.recoverPendingAccountMutationPublications(activation.Context()); err != nil {
		return fmt.Errorf("recover account mutation publications: %w", err)
	}
	s.holderActive.Store(true)
	monitorCtx, monitorCancel := context.WithCancel(activation.Context())
	s.holderMonitorMu.Lock()
	s.holderMonitorCancel = monitorCancel
	s.holderMonitorMu.Unlock()
	s.wg.Add(1)
	go s.monitorHolderSession(monitorCtx, s.holderSessionDone)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.monitorPendingAccountMutationPublications(monitorCtx)
	}()
	return nil
}

func (s *Server) cancelHolderMonitor() {
	s.holderMonitorMu.Lock()
	cancel := s.holderMonitorCancel
	s.holderMonitorMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *Server) monitorHolderSession(ctx context.Context, done <-chan struct{}) {
	defer s.wg.Done()
	select {
	case <-ctx.Done():
		return
	case <-done:
	}
	if ctx.Err() != nil {
		return
	}
	s.holderActive.Store(false)
	s.holderLost.Store(true)
	s.runtimePublished.Store(false)
	if s.runtimeShutdown == nil {
		s.log.Printf("FuseKit runtime session lost without shutdown ownership")
		return
	}
	if err := s.runtimeShutdown(context.WithoutCancel(ctx)); err != nil {
		s.log.Printf("shut down after FuseKit runtime session loss: %v", err)
	}
}

func (s *Server) clearActivation() {
	s.holderActive.Store(false)
	s.m = nil
	s.tenantClient = nil
	s.tenantCoordinator = nil
	s.holderSessionDone = nil
	s.disposableWorkers = nil
	s.accountTerminals = nil
	s.accountMutationTerminal = nil
	s.accountMutationLifetime = nil
	s.scanSessions = nil
	s.scanProcesses = nil
}

const maxVersionOutput = 64 << 10

// detectClaudeVersion runs `claude --version` in a disposable process group.
func detectClaudeVersion(ctx context.Context, workers *worker.Pool) string {
	if workers == nil {
		return ""
	}
	executable, err := exec.LookPath("claude")
	if err != nil || !filepath.IsAbs(executable) || filepath.Clean(executable) != executable {
		return ""
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	result, err := workers.Run(ctx, worker.CommandRequest{
		Path: executable, Dir: workerexec.TempDir(), Args: []string{"--version"}, TotalTimeout: 3 * time.Second,
	})
	if err != nil || len(result.Stdout) > maxVersionOutput {
		return ""
	}
	// `claude --version` emits "2.1.166 (Claude Code)".
	fields := strings.Fields(string(result.Stdout))
	if len(fields) > 0 {
		return fields[0]
	}
	return ""
}

// detectAndSetUserAgent stamps the OAuth user-agent with the detected claude
// version (the ua.detect startup row).
func (s *Server) detectAndSetUserAgent(ctx context.Context) {
	oauth.SetUserAgentVersion(detectClaudeVersion(ctx, s.disposableWorkers))
}

func (s *Server) serve(ctx context.Context) error {
	m, err := pool.OpenDaemon(ctx)
	if err != nil {
		return err
	}
	s.m = m
	_, runtime, err := s.runtime()
	if err != nil {
		return errors.Join(err, m.Close(ctx))
	}
	publication := dkdaemon.NewPublicationSlot[bool](runtime)
	activation, err := runtime.Begin(ctx)
	if err != nil {
		return errors.Join(err, m.Close(ctx))
	}
	settlement, err := activation.ClaimProductSettlement()
	if err != nil {
		_ = activation.Fail(err)
		return errors.Join(err, runtime.Wait(context.Background()), m.Close(ctx))
	}
	cleanupDone := make(chan error, 1)
	go func() {
		<-activation.Context().Done()
		cleanupDone <- s.settleProductRuntime(settlement)
	}()
	fail := func(cause error) error {
		failErr := activation.Fail(cause)
		return errors.Join(cause, failErr, runtime.Wait(context.Background()), <-cleanupDone)
	}
	if err := s.activate(activation); err != nil {
		return fail(err)
	}
	if err := s.startProductRuntime(activation.Context()); err != nil {
		return fail(err)
	}
	staged, err := publication.Stage(activation, true)
	if err != nil {
		return fail(err)
	}
	if err := activation.CommitReady(staged); err != nil {
		return fail(err)
	}
	s.runtimePublished.Store(true)
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultEvictTimeout)
			defer cancel()
			_ = runtime.Shutdown(shutdownCtx)
		case <-activation.Context().Done():
		}
	}()
	err = runtime.Wait(context.Background())
	cleanupErr := <-cleanupDone
	s.log.Printf("daemon stopped")
	if s.holderLost.Load() {
		err = errors.Join(errHolderSessionLost, err)
	}
	err = errors.Join(err, cleanupErr)
	if ctx.Err() != nil && (err == nil || errors.Is(err, ctx.Err())) {
		return nil
	}
	return err
}

func (s *Server) settleProductRuntime(settlement dkdaemon.ProductSettlement) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), daemonShutdownTimeout-defaultEvictTimeout)
	defer cancel()
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
		result = errors.Join(result, s.accountTerminals.Close(cleanupCtx))
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-cleanupCtx.Done():
		result = errors.Join(result, fmt.Errorf("daemon: await product workers: %w", cleanupCtx.Err()))
	}
	if s.syncClient != nil {
		result = errors.Join(result, s.syncClient.Close())
		s.syncClient = nil
	}
	if s.tenantClient != nil {
		s.holderActive.Store(false)
		result = errors.Join(result, s.tenantClient.Close())
	}
	if s.m != nil {
		result = errors.Join(result, s.m.Close(cleanupCtx))
	}
	s.clearActivation()
	s.m = nil
	if result != nil {
		return result
	}
	return settlement.Complete()
}

// markClosing prevents maintenance tables from starting another pass while the
// daemonkit worker phase is closing.
func (s *Server) markClosing() { s.closing.Store(true) }

func (s *Server) dispatch(ctx context.Context, req Request) Response {
	switch req.Op {
	case OpStatus:
		return s.handleStatus(ctx)
	case OpSelect:
		return s.handleSelect(ctx, req)
	case OpSelectCommit:
		return s.handleSelectCommit(ctx, req)
	case OpSelectAbort:
		return s.handleSelectAbort(ctx, req)
	case OpAccountRemove:
		return s.handleAccountRemove(ctx, req)
	case OpAccountIdentity:
		return s.handleAccountIdentity(ctx, req)
	case OpAccountHealth:
		return s.handleAccountHealth(ctx, req)
	case OpAccountMutationAck:
		return s.handleAccountMutationAck(ctx, req)
	default:
		return Response{OK: false, Error: "unknown op: " + string(req.Op)}
	}
}

func (s *Server) handleAccountIdentity(ctx context.Context, request Request) Response {
	if request.Account == nil || *request.Account <= 0 {
		return Response{Error: "account identity requires a positive account ID"}
	}
	account, err := s.m.Store.GetAccount(*request.Account)
	if err != nil {
		return Response{Error: err.Error()}
	}
	identity, err := s.m.AccountIdentity(ctx, account.ID, account.ConfigDir)
	if err != nil {
		return Response{Error: err.Error()}
	}
	if identity.AccountUUID == "" {
		return Response{Error: "account identity has no account UUID"}
	}
	return Response{OK: true, AccountIdentity: &AccountIdentityResult{
		AccountID: account.ID, AccountUUID: identity.AccountUUID, EmailAddress: identity.EmailAddress,
	}}
}

func (s *Server) handleAccountHealth(ctx context.Context, request Request) Response {
	if request.Account == nil || *request.Account <= 0 {
		return Response{Error: "account health requires a positive account ID"}
	}
	account, err := s.m.Store.GetAccount(*request.Account)
	if err != nil {
		return Response{Error: err.Error()}
	}
	if _, err := s.m.AccountIdentity(ctx, account.ID, account.ConfigDir); err != nil {
		return Response{Error: err.Error()}
	}
	if _, _, err := s.m.ReadCredential(ctx, account); err != nil {
		return Response{Error: err.Error()}
	}
	return Response{OK: true, AccountHealth: &AccountHealthResult{AccountID: account.ID}}
}

// handleStatus returns scored snapshots from cached samples (no live fetch).
func (s *Server) handleStatus(ctx context.Context) Response {
	accts, err := s.statuses(ctx)
	if err != nil {
		return Response{OK: false, Error: err.Error()}
	}
	// Version lets the client reject a pre-upgrade daemon before consuming fields
	// whose meaning changed; status clients then read the exact disk snapshot.
	resp := Response{OK: true, Version: version.String(), Accounts: accts}
	resp.Ledgers = s.ledgersWire()
	return resp
}

// statuses assembles the wire view of every account from cached samples — the
// single mapping shared by the socket status op and the on-disk snapshot.
func (s *Server) statuses(ctx context.Context) ([]AccountStatus, error) {
	snaps, err := s.m.Snapshots(ctx, false, 0)
	if err != nil {
		return nil, err
	}
	return ToStatuses(snaps), nil
}

// handleSelect picks the best available account from cached scores, applying
// short-lived reservations to avoid two selects colliding. Session and sticky
// effects remain provisional until handleSelectCommit consumes the token.
func (s *Server) handleSelect(ctx context.Context, req Request) Response {
	if req.PID > 0 && req.ProcessStartedAt <= 0 {
		return Response{OK: false, Error: "select with a process requires its kernel start time"}
	}
	if req.PID <= 0 && req.ProcessStartedAt != 0 {
		return Response{OK: false, Error: "select process start time requires a pid"}
	}
	var processStartedAt time.Time
	if req.ProcessStartedAt > 0 {
		processStartedAt = time.UnixMicro(req.ProcessStartedAt)
	}
	s.cl.pruneSelections(time.Now())
	snaps, err := s.m.Snapshots(ctx, false, 0)
	if err != nil {
		return Response{OK: false, Error: err.Error()}
	}
	if len(snaps) == 0 {
		return Response{OK: false, Error: pool.ErrNoAccounts.Error()}
	}

	excluded := make(map[int]bool, len(req.ExcludeIDs))
	for _, id := range req.ExcludeIDs {
		excluded[id] = true
	}
	if req.Account != nil {
		if excluded[*req.Account] {
			return Response{OK: false, Error: fmt.Sprintf("forced account %d is excluded", *req.Account)}
		}
		for _, sn := range snaps {
			if sn.Account.ID == *req.Account {
				if err := s.m.Store.SelectionEligible(sn.Account); err != nil {
					return Response{OK: false, Error: fmt.Sprintf("account %d is not selectable: %v", *req.Account, err)}
				}
				proof, err := s.prepareTenant(ctx, sn.Account)
				if err != nil {
					return Response{OK: false, Error: fmt.Sprintf("acct-%02d PrepareTenant: %v", sn.Account.ID, err)}
				}
				publicPath, err := s.selectionPublicPath(ctx, sn.Account, proof)
				if err != nil {
					return Response{OK: false, Error: fmt.Sprintf("acct-%02d PrepareTenant: %v", sn.Account.ID, err)}
				}
				if err := s.preflightSelectionCredential(ctx, sn.Account); err != nil {
					return Response{OK: false, Error: fmt.Sprintf("acct-%02d credential preflight: %v", sn.Account.ID, err)}
				}
				if err := s.m.Store.SelectionEligible(sn.Account); err != nil {
					return Response{OK: false, Error: fmt.Sprintf("acct-%02d selection eligibility: %v", sn.Account.ID, err)}
				}
				launch := selectionLaunch{
					pid: req.PID, processStartedAt: processStartedAt, cwd: req.Cwd,
					recordSticky: true,
				}
				token := ""
				if req.PID > 0 {
					token, err = s.cl.beginSelection(sn.Account, launch, provisionalSelectionTTL)
					if err != nil {
						return Response{OK: false, Error: fmt.Sprintf("reserve acct-%02d: %v", sn.Account.ID, err)}
					}
					if !s.cl.bindPreparation(token, proof) {
						return Response{OK: false, Error: fmt.Sprintf("acct-%02d reservation expired after PrepareTenant", sn.Account.ID)}
					}
				}
				id := sn.Account.ID
				return Response{
					OK: true, Dir: publicPath, SelectedID: &id,
					ReservationToken:  token,
					AccountInstanceID: sn.Account.InstanceID, AccountGeneration: sn.Account.Generation,
					Remaining5h: sn.Remaining5h, Remaining7d: sn.Remaining7d, HasUsage: sn.HasUsage,
					Scoped7dUtil: sn.Scoped7dUtil, Scoped7dModel: sn.Scoped7dModel,
				}
			}
		}
		return Response{OK: false, Error: fmt.Sprintf("account %d not found", *req.Account)}
	}

	// Refresh the one shared heartbeat before consulting stickiness so a claude
	// that just exited reads warm rather than live without starting a second scan.
	s.refreshHeartbeat(ctx, heartbeatOnDemandFreshness)

	usable := make([]pool.Snapshot, 0, len(snaps))
	excludedReady := false
	for _, sn := range snaps {
		if excluded[sn.Account.ID] {
			excludedReady = true
			continue
		}
		if err := s.m.Store.SelectionEligible(sn.Account); err != nil {
			if errors.Is(err, store.ErrAccountSelectionIneligible) {
				continue
			}
			return Response{OK: false, Error: fmt.Sprintf("acct-%02d selection eligibility: %v", sn.Account.ID, err)}
		}
		usable = append(usable, sn)
	}
	if len(usable) == 0 {
		if excludedReady {
			return Response{OK: false, Error: pool.ErrNoneAvailable.Error(), NoneAvailable: true}
		}
		soonest := soonestReset(snaps)
		resp := Response{OK: false, Error: pool.ErrNoneAvailable.Error(), NoneAvailable: true}
		if !soonest.IsZero() {
			resp.SoonestReset = &soonest
		}
		s.log.Printf("select: %s -> none available", req.Cwd)
		return resp
	}

	ranked, bySnap := s.rankWithReservations(usable)
	pin, outcome := s.m.StickyPick(req.Cwd, ranked, time.Now())
	r := pin
	fallback := false
	if outcome != pool.StickyBind {
		var ok bool
		r, ok = score.Pick(ranked)
		if !ok && !req.NoFallback {
			// Every account is exhausted (or worse): launch on the least-bad
			// exhausted one rather than refusing; the client warns loudly.
			r, ok = score.PickFallback(ranked)
			fallback = true
		}
		if !ok {
			soonest := soonestReset(snaps)
			resp := Response{OK: false, Error: pool.ErrNoneAvailable.Error(), NoneAvailable: true}
			when := "unknown"
			if !soonest.IsZero() {
				resp.SoonestReset = &soonest
				when = soonest.Format(time.RFC3339)
			}
			s.log.Printf("select: %s -> none available (soonest reset %s)", req.Cwd, when)
			return resp
		}
	}
	best := bySnap[r.AccountID]
	proof, err := s.prepareTenant(ctx, best.Account)
	if err != nil {
		return Response{OK: false, Error: fmt.Sprintf("acct-%02d PrepareTenant: %v", best.Account.ID, err)}
	}
	publicPath, err := s.selectionPublicPath(ctx, best.Account, proof)
	if err != nil {
		return Response{OK: false, Error: fmt.Sprintf("acct-%02d PrepareTenant: %v", best.Account.ID, err)}
	}
	id := best.Account.ID
	// Credential mutation takes the exclusive claim before the pending selection
	// is created; the claims mutex then orders every later mutation against it.
	err = s.preflightSelectionCredential(ctx, best.Account)
	if err != nil {
		return Response{OK: false, Error: fmt.Sprintf("acct-%02d credential preflight: %v", best.Account.ID, err)}
	}
	if err := s.m.Store.SelectionEligible(best.Account); err != nil {
		return Response{OK: false, Error: fmt.Sprintf("acct-%02d selection eligibility: %v", best.Account.ID, err)}
	}
	launch := selectionLaunch{
		pid: req.PID, processStartedAt: processStartedAt, cwd: req.Cwd,
		recordSticky: !outcome.Held() || best.Account.ID == pin.AccountID,
	}
	token := ""
	if req.PID > 0 {
		token, err = s.cl.beginSelection(best.Account, launch, provisionalSelectionTTL)
		if err != nil {
			return Response{OK: false, Error: fmt.Sprintf("reserve acct-%02d: %v", best.Account.ID, err)}
		}
		if !s.cl.bindPreparation(token, proof) {
			return Response{OK: false, Error: fmt.Sprintf("acct-%02d reservation expired after PrepareTenant", best.Account.ID)}
		}
	}
	s.log.Printf("select%s: %s -> acct-%02d (score %.1f · 5h %.0f%% used · 7d %.0f%% used%s)",
		selectKind(outcome, fallback), req.Cwd, best.Account.ID,
		r.Score, best.Util5h, best.Util7d, runnerUp(ranked, r.AccountID, fallback))
	resp := Response{
		OK: true, Dir: publicPath, SelectedID: &id,
		ReservationToken:  token,
		AccountInstanceID: best.Account.InstanceID, AccountGeneration: best.Account.Generation,
		Sticky:      outcome == pool.StickyBind,
		Remaining5h: best.Remaining5h, Remaining7d: best.Remaining7d, HasUsage: best.HasUsage,
		ExhaustedFallback: fallback, ExtraEnabled: best.ExtraEnabled,
		Scoped7dUtil: best.Scoped7dUtil, Scoped7dModel: best.Scoped7dModel,
	}
	if outcome == pool.StickyHoldManual {
		held := pin.AccountID
		resp.PinHeldAccount = &held
	}
	if fallback && !r.ExhaustedUntil.IsZero() {
		// Tell the client when the fallback pick actually recovers — the latest
		// reset among its pegged windows, not necessarily the 5h one.
		resp.SoonestReset = &r.ExhaustedUntil
	}
	return resp
}

func (s *Server) selectionPublicPath(
	ctx context.Context,
	account store.Account,
	proof catalogproto.TenantPreparationProof,
) (string, error) {
	storedProof, err := projectPreparationProof(proof)
	if err != nil {
		return "", err
	}
	observe := s.observePresentationBinding
	if observe == nil {
		observe = func(_ context.Context, account store.Account, proof store.PresentationPreparationProof) error {
			return s.m.Store.ObserveAccountPresentation(account, proof)
		}
	}
	if err := observe(ctx, account, storedProof); err != nil {
		return "", fmt.Errorf("observe account presentation: %w", err)
	}
	return storedProof.FileProvider.PublicPath, nil
}

func (s *Server) preflightSelectionCredential(
	ctx context.Context,
	account store.Account,
) error {
	pctx, cancel := context.WithTimeout(ctx, preflightTimeout)
	defer cancel()
	preflight := s.preflightCredential
	if preflight == nil {
		preflight = s.m.PreflightRefresh
	}
	err := preflight(pctx, account)
	// A synced token expired here: the origin's rotation may already be in the
	// registry but not yet pulled locally. Pull once, then re-evaluate.
	if !errors.Is(err, pool.ErrUnrefreshable) || s.syncPull == nil {
		return err
	}
	if pullErr := s.syncPull(pctx); pullErr != nil {
		return errors.Join(err, fmt.Errorf("preflight sync pull: %w", pullErr))
	}
	return preflight(pctx, account)
}

func (s *Server) handleSelectCommit(ctx context.Context, req Request) Response {
	if req.ReservationToken == "" {
		return Response{OK: false, Error: "select commit requires a reservation token"}
	}
	if s.cl.knowsSelection(req.ReservationToken) {
		return s.cl.commitSelection(ctx, req.ReservationToken, s.activateSelection)
	}
	committed, err := s.m.Store.SelectionCommitted(req.ReservationToken)
	if err != nil {
		return Response{OK: false, Error: fmt.Sprintf("read selection terminal: %v", err)}
	}
	if committed {
		return Response{OK: true}
	}
	return Response{OK: false, Error: "selection reservation is unknown or expired"}
}

func (s *Server) activateSelection(ctx context.Context, token string, reserved reservation, launch selectionLaunch) Response {
	if reserved.preparation == nil {
		return Response{OK: false, Error: "selection reservation has no preparation proof"}
	}
	publicPath, err := tenantfs.FileProviderPublicPath(*reserved.preparation)
	if err != nil {
		return Response{OK: false, Error: fmt.Sprintf("selection reservation has invalid preparation proof: %v", err)}
	}
	account := store.Account{
		ID: reserved.accountID, InstanceID: reserved.accountInstanceID, Generation: reserved.accountGeneration,
	}
	activate := func() error {
		return s.m.Store.ActivateSelection(store.SelectionActivation{
			Token:     token,
			AccountID: reserved.accountID, ExpectedInstanceID: reserved.accountInstanceID,
			ExpectedGeneration: reserved.accountGeneration,
			Process:            store.ProcessIdentity{PID: launch.pid, StartedAt: launch.processStartedAt},
			ConfigDir:          publicPath,
			Cwd:                launch.cwd, RecordSticky: launch.recordSticky, At: time.Now(),
		})
	}
	var activationErr error
	if s.activatePrepared != nil {
		activationErr = s.activatePrepared(ctx, account, *reserved.preparation, activate)
	} else if s.tenantCoordinator != nil {
		activationErr = s.tenantCoordinator.activatePrepared(ctx, account, *reserved.preparation, activate)
	} else {
		activationErr = errors.New("FuseKit tenant coordinator is unavailable")
	}
	if activationErr != nil {
		return Response{OK: false, Error: fmt.Sprintf("activate selection for account %d: %v", reserved.accountID, activationErr)}
	}
	return Response{OK: true}
}

func (s *Server) handleSelectAbort(ctx context.Context, req Request) Response {
	if req.ReservationToken == "" {
		return Response{OK: false, Error: "select abort requires a reservation token"}
	}
	return s.cl.abortSelection(ctx, req.ReservationToken)
}

// selectKind renders the select log qualifier. A bind and a fallback are
// mutually exclusive (an unusable pin never binds); a held pin coexists with
// fallback only when the free ranking itself collapsed to the least-bad pick,
// and the fallback warning is the more urgent of the two.
func selectKind(outcome pool.StickyOutcome, fallback bool) string {
	switch {
	case outcome == pool.StickyBind:
		return " (sticky)"
	case fallback:
		return " (exhausted-fallback)"
	case outcome.Held():
		return " (pin-held)"
	default:
		return ""
	}
}

// runnerUp renders the next-best servable account after winnerID for the
// select log, empty when there is none. A fallback pick means nothing is
// Available, so candidates widen to PickFallback's own predicate — otherwise
// the one select kind that most needs forensic context would never log one.
func runnerUp(ranked []score.Result, winnerID int, fallback bool) string {
	for _, r := range ranked {
		if r.AccountID == winnerID {
			continue
		}
		if !r.Available && (!fallback || r.RateLimited) {
			continue
		}
		return fmt.Sprintf(" · runner-up acct-%02d %.1f", r.AccountID, r.Score)
	}
	return ""
}

// scan resolves one atomic process observation. Narrow test fixtures synthesize
// the all-process set from their Claude sessions.
func (s *Server) scan(ctx context.Context) (procscan.Snapshot, error) {
	if s.scanProcesses != nil {
		return s.scanProcesses(ctx)
	}
	if s.scanSessions != nil {
		sessions, err := s.scanSessions(ctx)
		return procscan.Snapshot{Sessions: sessions, Processes: procscan.ClaudeProcesses(sessions)}, err
	}
	return procscan.ScanSnapshot(ctx)
}

// rankWithReservations re-ranks snapshots with the local reservation penalty
// applied, returning the ranking plus a snapshot lookup by account id. The
// cross-host live-session penalty is gone with the lease that carried it; usage
// samples re-converge the ranking within ~one poll — see cc-notes.
func (s *Server) rankWithReservations(snaps []pool.Snapshot) ([]score.Result, map[int]pool.Snapshot) {
	bySnap := map[int]pool.Snapshot{}
	inputs := make([]score.Input, 0, len(snaps))
	for _, sn := range snaps {
		bySnap[sn.Account.ID] = sn
		inputs = append(inputs, score.Input{
			AccountID:             sn.Account.ID,
			HasUsage:              sn.HasUsage,
			SampleTS:              time.Now().Add(-sn.SampleAge),
			Util5h:                sn.Util5h,
			Util7d:                sn.Util7d,
			Resets5h:              sn.Resets5h,
			Resets7d:              sn.Resets7d,
			Burn5hPerHour:         sn.Burn5hPerHour,
			ActiveSessions:        sn.ActiveSessions + s.cl.reservedCount(sn.Account.ID),
			RateLimited:           sn.RateLimited,
			RefreshFailed:         sn.Stale && !sn.HasUsage,
			NeedsLogin:            sn.NeedsLogin,
			CredentialQuarantined: sn.CredentialQuarantined,
			HasScoped7d:           sn.Scoped7dModel != "",
			Util7dScoped:          sn.Scoped7dUtil,
			Resets7dScoped:        sn.Scoped7dResets,
		})
	}
	return score.Rank(inputs, time.Now()), bySnap
}

func soonestReset(snaps []pool.Snapshot) time.Time {
	var best time.Time
	for _, sn := range snaps {
		if sn.Resets5h.IsZero() {
			continue
		}
		if best.IsZero() || sn.Resets5h.Before(best) {
			best = sn.Resets5h
		}
	}
	return best
}

// ToStatuses converts snapshots into wire AccountStatus values.
func ToStatuses(snaps []pool.Snapshot) []AccountStatus {
	out := make([]AccountStatus, 0, len(snaps))
	for _, sn := range snaps {
		out = append(out, AccountStatus{
			ID:                    sn.Account.ID,
			ConfigDir:             sn.Account.ConfigDir,
			Label:                 sn.Account.Label,
			Score:                 sn.Score,
			Remaining5h:           sn.Remaining5h,
			Remaining7d:           sn.Remaining7d,
			ActiveSessions:        sn.ActiveSessions,
			RateLimited:           sn.RateLimited,
			Exhausted:             sn.Exhausted,
			NeedsLogin:            sn.NeedsLogin,
			CredentialQuarantined: sn.CredentialQuarantined,
			AwaitingOrigin:        sn.AwaitingOrigin,
			HasUsage:              sn.HasUsage,
			Stale:                 sn.Stale,
			Resets5h:              sn.Resets5h,
			Resets7d:              sn.Resets7d,
			SampleAge:             sn.SampleAge.Round(time.Second).String(),
			// The wire ships the gated display forecast, never the raw
			// scoring burn (which stays live on stale samples).
			Burn5hPerHour:      sn.Forecast.BurnPerHour,
			Burn7dPerHour:      sn.Burn7dPerHour,
			Projected5hAtReset: sn.Forecast.AtReset,
			Depleted5hAt:       sn.Forecast.DepletedAt,
			ExtraEnabled:       sn.ExtraEnabled,
			ExtraUsed:          sn.ExtraUsed,
			ExtraLimit:         sn.ExtraLimit,
			Scoped7dUtil:       sn.Scoped7dUtil,
			Scoped7dResets:     sn.Scoped7dResets,
			Scoped7dModel:      sn.Scoped7dModel,
			WeeklyExhausted:    sn.WeeklyExhausted,
			Components:         ScoreComponentsFromDomain(sn.Components),
		})
	}
	return out
}
