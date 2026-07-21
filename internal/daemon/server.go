package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/cc-pool/internal/version"
	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/fusekit/catalogproto"
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

// sourceAuthorityOpenTimeout bounds startup on an untracked external holder of
// the cross-process authority lane after exact FuseKit worker recovery ran.
// Server is the running daemon.
type Server struct {
	m          *pool.Manager
	socket     string
	syncSocket string // synckit consumer socket; tests point it into a short temp dir
	snapshot   string // status mirror path; tests point it into a temp dir
	log        *log.Logger

	// evictTimeout bounds the wait for a skewed holder to release the socket.
	evictTimeout time.Duration

	// wg tracks only the background loops (startup, heal, scheduler, heartbeat,
	// sync mirror, fp.app ensure); serve Waits on it after the drain cancels their
	// context, before Run's deferred m.Close(). Request handlers settle through
	// drain, not here.
	wg sync.WaitGroup

	// wireIntake and syncIntake independently gate the persistent control plane
	// and synckit's consumer socket under daemonkit's one ordered shutdown.
	wireIntake *drain.Intake
	syncIntake *drain.Intake

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

	// scanSessions is a test seam over procscan.Scan; nil means the real scan.
	scanSessions func(context.Context) ([]procscan.Session, error)
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

	tenantClient        *tenantfs.Client
	tenantCoordinator   *tenantCoordinator
	prepareAccount      func(context.Context, store.Account) (catalogproto.TenantPreparationProof, error)
	activatePrepared    func(context.Context, store.Account, catalogproto.TenantPreparationProof, func() error) error
	preflightCredential func(context.Context, store.Account) error
	disposableWorkers   *supervise.Pool

	// led is the product self-heal ledger for auth.streak and the
	// ratelimit.acct / ratelimit.pool streaks. ledMu is its
	// serialization: every s.led access takes ledMu, never held across
	// mount/reconcile/re-register/bounce or usage-fetch/refresh I/O (bookkeeping in, I/O
	// out). See ccn doc 36b05ef.
	led   *ledgers
	ledMu sync.Mutex

	// syncSvc is the wired host-sync engine (registry, driver, mesh); nil ⇒ host
	// sync never wired this run. syncSelf is this host's registry origin name.
	// Both feed authKind's persist-time needs-login classification.
	syncSvc      *hostsync.Service
	syncSelf     string
	syncListener net.Listener
	syncAuthKind func(context.Context, int, string) (store.AuthKind, error)

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
		wireIntake:   &drain.Intake{},
		syncIntake:   &drain.Intake{},
	}
	return s.serve(ctx)
}

func (s *Server) activate(activation dkdaemon.Activation) (err error) {
	if err := ensureHolderRuntime(activation.Startup); err != nil {
		return err
	}
	m, err := pool.OpenDaemon(activation.Lifetime)
	if err != nil {
		return err
	}
	var tenantClient *tenantfs.Client
	published := false
	defer func() {
		if published {
			return
		}
		if tenantClient != nil {
			err = errors.Join(err, tenantClient.Close())
		}
		err = errors.Join(err, m.Close())
	}()

	tenantClient, err = tenantfs.NewClient(activation.Startup, pool.FuseKitSocketPath())
	if err != nil {
		return fmt.Errorf("connect FuseKit holder: %w", err)
	}
	preparer, err := tenantfs.NewPreparer(tenantClient)
	if err != nil {
		return err
	}
	workers := m.DisposableWorkers()
	s.m = m
	s.tenantClient = tenantClient
	s.disposableWorkers = workers
	s.accountMutationTerminal = daemonkitAccountMutationTerminalRunner{workers: workers, manager: m}
	s.accountMutationLifetime = activation.Lifetime
	s.scanSessions = m.ScanSessions
	if err := s.recoverRetiredAccountMutations(activation.Startup); err != nil {
		s.clearActivation()
		return fmt.Errorf("recover account mutations: %w", err)
	}
	s.tenantCoordinator = newTenantCoordinator(activation.Lifetime, s, preparer, tenantClient)
	if err := s.tenantCoordinator.initialize(activation.Startup); err != nil {
		s.clearActivation()
		return fmt.Errorf("initialize FuseKit tenants: %w", err)
	}
	published = true
	return nil
}

func (s *Server) clearActivation() {
	s.m = nil
	s.tenantClient = nil
	s.tenantCoordinator = nil
	s.disposableWorkers = nil
	s.accountMutationTerminal = nil
	s.accountMutationLifetime = nil
	s.scanSessions = nil
}

const maxVersionOutput = 64 << 10

type boundedVersionBuffer struct{ bytes.Buffer }

func (buffer *boundedVersionBuffer) Write(input []byte) (int, error) {
	length := len(input)
	remaining := maxVersionOutput - buffer.Len()
	if remaining > 0 {
		if len(input) > remaining {
			input = input[:remaining]
		}
		_, _ = buffer.Buffer.Write(input)
	}
	return length, nil
}

// detectClaudeVersion runs `claude --version` in a disposable process group.
func detectClaudeVersion(ctx context.Context, workers *supervise.Pool) string {
	if workers == nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var output boundedVersionBuffer
	if err := workers.Run(ctx, supervise.Task{
		RecoveryClass: proc.RecoveryTask,
		Path:          "claude", Args: []string{"--version"}, Stdout: &output,
	}); err != nil {
		return ""
	}
	// `claude --version` emits "2.1.166 (Claude Code)".
	fields := strings.Fields(output.String())
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
	wireServer, runtime, err := s.runtime()
	if err != nil {
		return err
	}
	wireServer.RegisterLifecycle(runtime)
	err = runtime.Run(ctx)
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return nil
	}
	return err
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
	case OpCredMove:
		return s.handleCredMove(ctx, req)
	case OpAccountRemove:
		return s.handleAccountRemove(ctx, req)
	case OpAccountIdentity:
		return s.handleAccountIdentity(ctx, req)
	case OpAccountHealth:
		return s.handleAccountHealth(ctx, req)
	case OpAccountMutationAck:
		return s.handleAccountMutationAck(req)
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
				}
				proof, err := s.prepareTenant(ctx, sn.Account)
				if err != nil {
					s.cl.abortReservation(token)
					return Response{OK: false, Error: fmt.Sprintf("acct-%02d PrepareTenant: %v", sn.Account.ID, err)}
				}
				if token != "" && !s.cl.bindPreparation(token, proof) {
					return Response{OK: false, Error: fmt.Sprintf("acct-%02d reservation expired during PrepareTenant", sn.Account.ID)}
				}
				if err := s.preflightSelectionCredential(ctx, sn.Account); err != nil {
					s.cl.abortReservation(token)
					return Response{OK: false, Error: fmt.Sprintf("acct-%02d credential preflight: %v", sn.Account.ID, err)}
				}
				id := sn.Account.ID
				return Response{
					OK: true, Dir: pool.AccountPresentationDir(sn.Account.ID), SelectedID: &id,
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
	}
	proof, err := s.prepareTenant(ctx, best.Account)
	if err != nil {
		s.cl.abortReservation(token)
		return Response{OK: false, Error: fmt.Sprintf("acct-%02d PrepareTenant: %v", best.Account.ID, err)}
	}
	if token != "" && !s.cl.bindPreparation(token, proof) {
		return Response{OK: false, Error: fmt.Sprintf("acct-%02d reservation expired during PrepareTenant", best.Account.ID)}
	}
	id := best.Account.ID
	// Finish credential preflight before returning the reservation. Credential
	// maintenance observes pending reservations, while maintenance that won the
	// durable lane first must settle before this selection may activate.
	err = s.preflightSelectionCredential(ctx, best.Account)
	if err != nil {
		s.cl.abortReservation(token)
		return Response{OK: false, Error: fmt.Sprintf("acct-%02d credential preflight: %v", best.Account.ID, err)}
	}
	s.log.Printf("select%s: %s -> acct-%02d (score %.1f · 5h %.0f%% used · 7d %.0f%% used%s)",
		selectKind(outcome, fallback), req.Cwd, best.Account.ID,
		r.Score, best.Util5h, best.Util7d, runnerUp(ranked, r.AccountID, fallback))
	resp := Response{
		OK: true, Dir: pool.AccountPresentationDir(best.Account.ID), SelectedID: &id,
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
	account := store.Account{
		ID: reserved.accountID, InstanceID: reserved.accountInstanceID, Generation: reserved.accountGeneration,
	}
	activate := func() error {
		return s.m.Store.ActivateSelection(store.SelectionActivation{
			Token:     token,
			AccountID: reserved.accountID, ExpectedInstanceID: reserved.accountInstanceID,
			ExpectedGeneration: reserved.accountGeneration,
			Process:            store.ProcessIdentity{PID: launch.pid, StartedAt: launch.processStartedAt},
			ConfigDir:          pool.AccountPresentationDir(reserved.accountID),
			Cwd:                launch.cwd, RecordSticky: launch.recordSticky, At: time.Now(),
		})
	}
	var err error
	if s.activatePrepared != nil {
		err = s.activatePrepared(ctx, account, *reserved.preparation, activate)
	} else if s.tenantCoordinator != nil {
		err = s.tenantCoordinator.activatePrepared(ctx, account, *reserved.preparation, activate)
	} else {
		err = errors.New("FuseKit tenant coordinator is unavailable")
	}
	if err != nil {
		return Response{OK: false, Error: fmt.Sprintf("activate selection for account %d: %v", reserved.accountID, err)}
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

// scan resolves session scanning through the test seam; nil means
// procscan.Scan.
func (s *Server) scan(ctx context.Context) ([]procscan.Session, error) {
	if s.scanSessions != nil {
		return s.scanSessions(ctx)
	}
	return procscan.Scan(ctx)
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
			ConfigDir:             pool.AccountPresentationDir(sn.Account.ID),
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
			Components:         sn.Components,
		})
	}
	return out
}
