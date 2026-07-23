package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalogproto"
)

var daemonTestToken atomic.Uint64

func nextDaemonTestToken() string {
	return fmt.Sprintf("%032x", daemonTestToken.Add(1))
}

func newDaemonTestManager(
	t *testing.T,
	st *store.Store,
	refresher pool.Refresher,
	credentials pool.Credentials,
) *pool.Manager {
	t.Helper()
	identity, err := proc.CurrentIdentity()
	if err != nil {
		t.Fatal(err)
	}
	owner := proc.Record{
		RecoveryClass: proc.RecoveryTask,
		PID:           identity.PID, StartTime: identity.StartTime, Boot: identity.Boot,
		Comm: identity.Comm, Executable: identity.Executable,
		AuditToken: identity.AuditToken, Generation: "daemon-test",
	}
	authority, err := pool.NewWorkerAuthority(
		accountMutationTestTaskRunner{credentials: credentials, refresher: refresher},
		identity.Executable, owner,
	)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := pool.NewManager(
		st, refresher,
		func(context.Context) ([]procscan.Session, error) { return nil, nil },
		authority,
	)
	if err != nil {
		t.Fatal(err)
	}
	manager.Creds = credentials
	manager.BuildCredentialWritePublication = credentialWritePublicationBuilder("daemon-test")
	manager.SettleCredentialWrite = func(context.Context, pool.CredentialWriteSettlement) error {
		return nil
	}
	return manager
}

func TestSelectPreflightSettlesBeforeReturnAndBlocksCredentialMove(t *testing.T) {
	s, _ := newTestServer(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	s.preflightCredential = func(context.Context, store.Account) error {
		close(entered)
		<-release
		return nil
	}
	forced := 1
	responses := make(chan Response, 1)
	go func() {
		responses <- s.handleSelect(t.Context(), Request{
			Op: OpSelect, Account: &forced, PID: 4242,
			ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj",
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("selection did not enter credential preflight")
	}
	if got := s.cl.reservedCount(forced); got != 1 {
		t.Fatalf("pending reservations = %d, want 1 during preflight", got)
	}
	account, err := s.m.Store.GetAccount(forced)
	if err != nil {
		t.Fatal(err)
	}
	move := s.moveAccountCred(t.Context(), account, creds.SourceFile, "file")
	if move.Outcome != CredentialMoveBusy || !strings.Contains(move.Detail, "pending selection") {
		t.Fatalf("credential move during preflight = %+v, want pending-selection busy", move)
	}
	select {
	case response := <-responses:
		t.Fatalf("selection returned before preflight settled: %+v", response)
	default:
	}
	close(release)
	select {
	case response := <-responses:
		if !response.OK {
			t.Fatalf("selection after preflight: %+v", response)
		}
		s.cl.abortReservation(response.ReservationToken)
	case <-time.After(time.Second):
		t.Fatal("selection did not return after preflight settled")
	}
}

func TestSelectPreflightFailureAbortsReservation(t *testing.T) {
	s, _ := newTestServer(t)
	s.preflightCredential = func(context.Context, store.Account) error {
		return errors.New("credential quarantine")
	}
	forced := 1
	response := s.handleSelect(t.Context(), Request{
		Op: OpSelect, Account: &forced, PID: 4242,
		ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj",
	})
	if response.OK || !strings.Contains(response.Error, "credential quarantine") {
		t.Fatalf("selection with failed credential preflight = %+v", response)
	}
	if got := s.cl.reservedCount(forced); got != 0 {
		t.Fatalf("pending reservations after failed preflight = %d, want 0", got)
	}
}

func TestBlockedPrepareDoesNotHoldClaimsOrStore(t *testing.T) {
	s, _ := newTestServer(t)
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	var callsMu sync.Mutex
	calls := map[int]int{}
	s.prepareAccount = func(
		ctx context.Context,
		account store.Account,
	) (catalogproto.TenantPreparationProof, error) {
		callsMu.Lock()
		calls[account.ID]++
		callsMu.Unlock()
		proof := daemonTestPreparationProof(account, pool.AccountDir(account.ID))
		if account.ID != 1 {
			return proof, nil
		}
		entered <- struct{}{}
		select {
		case <-ctx.Done():
			return catalogproto.TenantPreparationProof{}, ctx.Err()
		case <-release:
			return proof, nil
		}
	}

	forcedOne := 1
	startedAt := time.Now().Add(-time.Minute).UnixMicro()
	primaryResponses := make(chan Response, 1)
	go func() {
		primaryResponses <- s.handleSelect(t.Context(), Request{
			Op: OpSelect, Account: &forcedOne, PID: 4101,
			ProcessStartedAt: startedAt, Cwd: "/prepare-primary",
		})
	}()
	waitForBlockedPrepare(t, entered)
	primaryToken := exactPendingSelectionToken(t, s.cl, forcedOne)

	canceledContext, cancelCanceled := context.WithCancel(t.Context())
	canceledResponses := make(chan Response, 1)
	go func() {
		canceledResponses <- s.handleSelect(canceledContext, Request{
			Op: OpSelect, Account: &forcedOne, PID: 4102,
			ProcessStartedAt: startedAt, Cwd: "/prepare-canceled",
		})
	}()
	waitForBlockedPrepare(t, entered)
	if got := s.cl.reservedCount(forcedOne); got != 2 {
		t.Fatalf("same-account reservations during PrepareTenant = %d, want 2", got)
	}
	cancelCanceled()
	select {
	case response := <-canceledResponses:
		if response.OK || !strings.Contains(response.Error, context.Canceled.Error()) {
			t.Fatalf("canceled PrepareTenant response = %+v", response)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled same-account PrepareTenant retained the selection")
	}
	if got := s.cl.reservedCount(forcedOne); got != 1 {
		t.Fatalf("canceled PrepareTenant reservations = %d, want only the primary", got)
	}

	s.cl.mu.Lock()
	primary := s.cl.selections[primaryToken]
	if primary == nil {
		s.cl.mu.Unlock()
		t.Fatal("primary reservation disappeared before TTL expiry")
	}
	primary.expiresAt = s.cl.now().Add(-time.Second)
	s.cl.mu.Unlock()
	if got := s.cl.reservedCount(forcedOne); got != 0 {
		t.Fatalf("expired PrepareTenant reservation count = %d, want 0", got)
	}

	forcedTwo := 2
	unrelatedResponses := make(chan Response, 1)
	go func() {
		unrelatedResponses <- s.handleSelect(t.Context(), Request{
			Op: OpSelect, Account: &forcedTwo, PID: 4201,
			ProcessStartedAt: startedAt, Cwd: "/prepare-unrelated",
		})
	}()
	var unrelated Response
	select {
	case unrelated = <-unrelatedResponses:
		if !unrelated.OK || unrelated.ReservationToken == "" {
			t.Fatalf("unrelated selection during blocked PrepareTenant = %+v", unrelated)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked PrepareTenant held the account store across unrelated selection")
	}
	if response := s.handleSelectAbort(t.Context(), Request{
		Op: OpSelectAbort, ReservationToken: unrelated.ReservationToken,
	}); !response.OK {
		t.Fatalf("abort unrelated selection = %+v", response)
	}

	select {
	case response := <-primaryResponses:
		t.Fatalf("expired primary returned before PrepareTenant settled: %+v", response)
	default:
	}
	close(release)
	released = true
	select {
	case response := <-primaryResponses:
		if response.OK || !strings.Contains(response.Error, "reservation expired during PrepareTenant") {
			t.Fatalf("expired primary PrepareTenant response = %+v", response)
		}
	case <-time.After(time.Second):
		t.Fatal("primary PrepareTenant did not settle after release")
	}

	callsMu.Lock()
	accountOneCalls := calls[1]
	accountTwoCalls := calls[2]
	callsMu.Unlock()
	if accountOneCalls != 2 || accountTwoCalls != 1 {
		t.Fatalf("PrepareTenant calls = acct-1 %d acct-2 %d, want 2 and 1", accountOneCalls, accountTwoCalls)
	}
	for _, accountID := range []int{1, 2} {
		if got := s.cl.reservedCount(accountID); got != 0 || s.cl.held(accountID) {
			t.Fatalf("acct-%d retained reservation/claim: reservations=%d held=%v", accountID, got, s.cl.held(accountID))
		}
	}
	if sessions, err := s.m.Store.ListActiveSessions(); err != nil || len(sessions) != 0 {
		t.Fatalf("blocked PrepareTenant retained session leases: %+v, %v", sessions, err)
	}
	for _, token := range []string{primaryToken, unrelated.ReservationToken} {
		if committed, err := s.m.Store.SelectionCommitted(token); err != nil || committed {
			t.Fatalf("selection %s committed = %v, %v", token, committed, err)
		}
	}
}

func waitForBlockedPrepare(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("selection did not enter blocked PrepareTenant")
	}
}

func exactPendingSelectionToken(t *testing.T, claims *claims, accountID int) string {
	t.Helper()
	claims.mu.Lock()
	defer claims.mu.Unlock()
	var token string
	count := 0
	for candidate, selection := range claims.selections {
		if selection.accountID == accountID && selection.state == selectionPending {
			token = candidate
			count++
		}
	}
	if count != 1 {
		t.Fatalf("pending acct-%d selections = %d, want 1", accountID, count)
	}
	return token
}

// newTestServer builds a Server with acct-1 emptier than acct-2. scanSessions
// is stubbed: real `ps` can hang on a wedged mount.
func newTestServer(t *testing.T) (*Server, map[int]string) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	dirs := map[int]string{}
	fakeCreds := credstest.NewFake()
	now := time.Now()
	for id, util := range map[int]float64{1: 10, 2: 50} {
		configDir := pool.AccountDir(id)
		dirs[id] = configDir
		service := creds.ServiceName(configDir)
		if err := st.UpsertAccount(store.Account{
			ID: id, ConfigDir: configDir, InstanceID: fmt.Sprintf("instance-%d", id), Generation: 1,
			KeychainService: service, KeychainAccount: "ccp-test",
		}); err != nil {
			t.Fatal(err)
		}
		if err := st.InsertUsageSample(store.UsageSample{AccountID: id, TS: now, Util5h: util, Util7d: util}); err != nil {
			t.Fatal(err)
		}
		credential := &creds.Credential{}
		credential.ClaudeAiOauth.AccessToken = fmt.Sprintf("access-%d", id)
		credential.ClaudeAiOauth.RefreshToken = fmt.Sprintf("refresh-%d", id)
		credential.ClaudeAiOauth.ExpiresAt = now.Add(time.Hour).UnixMilli()
		fakeCreds.Put(service, "ccp-test", credential)
	}
	s := &Server{
		m:            newDaemonTestManager(t, st, &fakeOAuth{}, fakeCreds),
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		cl:           newClaims(),
		led:          newLedgers(),
		prepareAccount: func(ctx context.Context, account store.Account) (catalogproto.TenantPreparationProof, error) {
			return daemonTestPreparationProof(account, dirs[account.ID]), ctx.Err()
		},
		activatePrepared: func(_ context.Context, _ store.Account, _ catalogproto.TenantPreparationProof, activate func() error) error {
			return activate()
		},
	}
	return s, dirs
}

func daemonTestPreparationProof(account store.Account, publicPath string) catalogproto.TenantPreparationProof {
	return catalogproto.TenantPreparationProof{
		Catalog: catalogproto.CatalogLaneProof{Tenant: catalogproto.TenantID("test"), Generation: account.Generation},
		Presentation: catalogproto.PresentationProof{
			Kind: catalogproto.PresentationKindFileProvider,
			FileProvider: &catalogproto.FileProviderPresentationProof{
				TenantID: catalogproto.TenantID("test"), DomainID: catalogproto.DomainID(fmt.Sprintf("domain-%d", account.ID)),
				Generation: account.Generation, PublicPath: publicPath, ActivationGeneration: "activation-test",
			},
		},
	}
}

func TestPrepareReservedAccountPathReturnsOnlyValidatedFusePublicPath(t *testing.T) {
	s, dirs := newTestServer(t)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	reservation := store.PendingAccountReservation{
		ID: account.ID, InstanceID: account.InstanceID, Generation: account.Generation,
	}
	publicPath, err := s.prepareReservedAccountPath(t.Context(), reservation)
	if err != nil || publicPath != dirs[1] {
		t.Fatalf("prepare reserved account path = %q, %v; want %q", publicPath, err, dirs[1])
	}
	s.prepareReservedAccount = func(_ context.Context, got store.PendingAccountReservation) (string, error) {
		if got.ID != reservation.ID || got.InstanceID != reservation.InstanceID || got.Generation != reservation.Generation {
			t.Fatalf("reservation = %+v, want %+v", got, reservation)
		}
		return "relative/path", nil
	}
	if _, err := s.prepareReservedAccountPath(t.Context(), reservation); !errors.Is(err, tenantfs.ErrPreparationConflict) {
		t.Fatalf("invalid injected public path error = %v, want ErrPreparationConflict", err)
	}
}

func TestSelectionUsesPersistedFusePublicPathWithoutSynthesizing(t *testing.T) {
	s, _ := newTestServer(t)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	publicPath := "/Users/test/Library/CloudStorage/cc-pool-account-1"
	account.ConfigDir = publicPath
	if err := s.m.Store.UpsertAccount(account); err != nil {
		t.Fatal(err)
	}
	account, err = s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	s.prepareAccount = func(ctx context.Context, got store.Account) (catalogproto.TenantPreparationProof, error) {
		return daemonTestPreparationProof(got, publicPath), ctx.Err()
	}
	forced := account.ID
	response := s.handleSelect(t.Context(), Request{
		Op: OpSelect, Account: &forced, PID: 4242,
		ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proof-path",
	})
	if !response.OK || response.Dir != publicPath {
		t.Fatalf("selection = %+v, want proof path %q", response, publicPath)
	}
	commitSelectResponse(t, s, response)
	sessions, err := s.m.Store.ListActiveSessions()
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].ConfigDir != publicPath {
		t.Fatalf("committed sessions = %+v, want proof path %q", sessions, publicPath)
	}
}

func TestSelectionQuarantinesPresentationBindingDrift(t *testing.T) {
	s, _ := newTestServer(t)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	provenPath := "/Users/test/Library/CloudStorage/cc-pool-account-1"
	s.prepareAccount = func(ctx context.Context, got store.Account) (catalogproto.TenantPreparationProof, error) {
		return daemonTestPreparationProof(got, provenPath), ctx.Err()
	}
	forced := account.ID
	response := s.handleSelect(t.Context(), Request{
		Op: OpSelect, Account: &forced, PID: 4242,
		ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proof-drift",
	})
	if response.OK || !strings.Contains(response.Error, store.ErrAccountPresentationQuarantined.Error()) {
		t.Fatalf("selection = %+v, want quarantined drift", response)
	}
	quarantine, err := s.m.Store.AccountPresentationQuarantine(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	fileProvider := daemonTestPreparationProof(account, provenPath).Presentation.FileProvider
	if quarantine.AccountID != account.ID || quarantine.AccountInstanceID != account.InstanceID ||
		quarantine.AccountGeneration != account.Generation || quarantine.ExpectedConfigDir != account.ConfigDir ||
		quarantine.Observed.TenantID != string(fileProvider.TenantID) ||
		quarantine.Observed.DomainID != string(fileProvider.DomainID) ||
		quarantine.Observed.Generation != fileProvider.Generation ||
		quarantine.Observed.ActivationGeneration != fileProvider.ActivationGeneration ||
		quarantine.Observed.PublicPath != provenPath ||
		quarantine.Reason != store.AccountPresentationPublicPathDrift {
		t.Fatalf("presentation quarantine = %+v", quarantine)
	}
	if got := s.cl.reservedCount(account.ID); got != 0 {
		t.Fatalf("drifted selection retained %d reservations", got)
	}
	if sessions, err := s.m.Store.ListActiveSessions(); err != nil || len(sessions) != 0 {
		t.Fatalf("drifted selection sessions = %+v, %v", sessions, err)
	}
}

func activateDaemonTestSession(t *testing.T, s *Server, accountID, pid int, cwd string, started time.Time) int64 {
	t.Helper()
	started = started.Truncate(time.Microsecond)
	a, err := s.m.Store.GetAccount(accountID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.m.Store.ActivateSelection(store.SelectionActivation{
		Token:     nextDaemonTestToken(),
		AccountID: accountID, ExpectedInstanceID: a.InstanceID, ExpectedGeneration: a.Generation,
		Process:   store.ProcessIdentity{PID: pid, StartedAt: started},
		ConfigDir: pool.AccountDir(accountID),
		Cwd:       cwd, At: started,
	}); err != nil {
		t.Fatal(err)
	}
	sessions, err := s.m.Store.ListActiveSessions()
	if err != nil {
		t.Fatal(err)
	}
	for _, session := range sessions {
		if session.PID == pid && session.ProcessStartedAt.Equal(started) && session.Cwd == cwd {
			return session.ID
		}
	}
	t.Fatal("activated session was not stored")
	return 0
}

func expireCommittedReservations(c *claims, accountID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, selection := range c.selections {
		if selection.accountID == accountID {
			selection.expiresAt = time.Now().Add(-time.Second)
		}
	}
}

func TestReservedCountExpiresAfterTTL(t *testing.T) {
	s := &Server{cl: newClaims()}

	if got := s.cl.reservedCount(1); got != 0 {
		t.Fatalf("reservedCount before reserve = %d, want 0", got)
	}

	s.cl.reserve(1)
	if got := s.cl.reservedCount(1); got != 1 {
		t.Fatalf("reservedCount after reserve = %d, want 1", got)
	}

	expireCommittedReservations(s.cl, 1)
	if got := s.cl.reservedCount(1); got != 0 {
		t.Fatalf("reservedCount after TTL = %d, want 0", got)
	}
	s.cl.mu.Lock()
	left := len(s.cl.selections)
	s.cl.mu.Unlock()
	if left != 0 {
		t.Fatalf("expired reservations were not deleted: %d remain", left)
	}
}

func TestSelectionActivationRejectsStalePreparationProof(t *testing.T) {
	s, _ := newTestServer(t)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	launch := selectionLaunch{pid: 42, processStartedAt: time.Now(), cwd: "/work"}
	token, err := s.cl.beginSelection(account, launch, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	proof := catalogproto.TenantPreparationProof{
		Catalog:        catalogproto.CatalogLaneProof{Tenant: "test", Generation: account.Generation, Requested: 1},
		SourceRevision: 1,
		Presentation: catalogproto.PresentationProof{
			Kind: catalogproto.PresentationKindFileProvider,
			FileProvider: &catalogproto.FileProviderPresentationProof{
				PublicPath: "/Users/test/Library/CloudStorage/account-1",
			},
		},
	}
	if !s.cl.bindPreparation(token, proof) {
		t.Fatal("bind preparation failed")
	}
	s.activatePrepared = func(_ context.Context, _ store.Account, got catalogproto.TenantPreparationProof, _ func() error) error {
		if got.SourceRevision != 1 {
			t.Fatalf("proof source revision = %d", got.SourceRevision)
		}
		return tenantfs.ErrPreparationConflict
	}
	response := s.cl.commitSelection(t.Context(), token, s.activateSelection)
	if response.OK || !strings.Contains(response.Error, tenantfs.ErrPreparationConflict.Error()) {
		t.Fatalf("commit response = %+v", response)
	}
	sessions, err := s.m.Store.ListActiveSessions()
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 0 {
		t.Fatalf("sessions after stale preparation = %+v", sessions)
	}
}

func TestRawSelectHasNoActivationEffects(t *testing.T) {
	s, dirs := newTestServer(t)
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[1] {
		t.Fatalf("expected emptier acct-1 (%s), got %+v", dirs[1], resp)
	}
	if resp.Sticky {
		t.Fatal("first select must not report sticky")
	}
	if !resp.HasUsage || resp.Remaining5h <= 0 || resp.Remaining7d <= 0 {
		t.Fatalf("expected remaining headroom on a sampled pick, got HasUsage=%v Remaining5h=%.1f Remaining7d=%.1f", resp.HasUsage, resp.Remaining5h, resp.Remaining7d)
	}
	if resp.ReservationToken != "" {
		t.Fatalf("raw select returned reservation token %q", resp.ReservationToken)
	}
	if got := s.cl.reservedCount(1); got != 0 {
		t.Fatalf("raw select retained %d reservations, want zero", got)
	}
	if st, ok, err := s.m.Store.GetSticky("/proj"); err != nil || ok {
		t.Fatalf("raw select recorded sticky: %+v ok=%v err=%v", st, ok, err)
	}
	if sessions, err := s.m.Store.ListActiveSessions(); err != nil || len(sessions) != 0 {
		t.Fatalf("raw select sessions = %+v, err=%v; raw select has no process owner", sessions, err)
	}
}

func TestHandleSelectTransactionAbortAndExclude(t *testing.T) {
	s, dirs := newTestServer(t)
	first := s.handleSelect(t.Context(), Request{Op: OpSelect, PID: 4242, ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj"})
	if !first.OK || first.Dir != dirs[1] || first.ReservationToken == "" {
		t.Fatalf("provisional select = %+v, want acct-1 with token", first)
	}
	if live, _ := s.m.Store.ListActiveSessions(); len(live) != 0 {
		t.Fatalf("provisional select opened phantom sessions: %+v", live)
	}
	if _, ok, _ := s.m.Store.GetSticky("/proj"); ok {
		t.Fatal("provisional select recorded phantom sticky state")
	}
	if got := s.cl.reservedCount(1); got != 1 {
		t.Fatalf("pending reservation count = %d, want 1", got)
	}
	if resp := s.handleSelectAbort(context.Background(), Request{Op: OpSelectAbort, ReservationToken: first.ReservationToken}); !resp.OK {
		t.Fatalf("abort = %+v", resp)
	}
	if got := s.cl.reservedCount(1); got != 0 {
		t.Fatalf("reservation survived abort: %d", got)
	}

	second := s.handleSelect(t.Context(), Request{Op: OpSelect, PID: 4242, ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj", ExcludeIDs: []int{1}})
	if !second.OK || second.Dir != dirs[2] {
		t.Fatalf("excluded retry = %+v, want acct-2", second)
	}
	commitSelectResponse(t, s, second)
	if live, _ := s.m.Store.ListActiveSessions(); len(live) != 1 || live[0].AccountID != 2 || live[0].ConfigDir != dirs[2] {
		t.Fatalf("committed sessions = %+v, want only acct-2", live)
	}
	if sticky, ok, _ := s.m.Store.GetSticky("/proj"); !ok || sticky.AccountID != 2 {
		t.Fatalf("committed sticky = %+v ok=%v, want acct-2", sticky, ok)
	}
}

func TestCommitSelectionReplaySurvivesDaemonRestart(t *testing.T) {
	s, _ := newTestServer(t)
	selected := s.handleSelect(t.Context(), Request{
		Op: OpSelect, PID: 4242, ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj",
	})
	if !selected.OK || selected.ReservationToken == "" {
		t.Fatalf("select = %+v", selected)
	}
	if committed := s.handleSelectCommit(t.Context(), Request{
		Op: OpSelectCommit, ReservationToken: selected.ReservationToken,
	}); !committed.OK {
		t.Fatalf("commit = %+v", committed)
	}

	restarted := &Server{m: s.m, cl: newClaims()}
	if restarted.cl.knowsSelection(selected.ReservationToken) {
		t.Fatal("fresh daemon unexpectedly retained the old in-memory selection")
	}
	if replayed := restarted.handleSelectCommit(t.Context(), Request{
		Op: OpSelectCommit, ReservationToken: selected.ReservationToken,
	}); !replayed.OK {
		t.Fatalf("durable commit replay after daemon restart = %+v", replayed)
	}
	if sessions, err := s.m.Store.ListActiveSessions(); err != nil || len(sessions) != 1 {
		t.Fatalf("durable replay sessions = %+v, err=%v; want exactly one", sessions, err)
	}
	if sticky, ok, err := s.m.Store.GetSticky("/proj"); err != nil || !ok || sticky.AccountID != 1 {
		t.Fatalf("durable replay sticky = %+v ok=%v err=%v", sticky, ok, err)
	}
}

func TestCommitSelectionFailureReleasesPromotedReservation(t *testing.T) {
	s, _ := newTestServer(t)
	forced := 1
	started := time.Now().Add(-time.Minute).UnixMicro()
	first := s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &forced, PID: 4241, ProcessStartedAt: started, Cwd: "/first"})
	second := s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &forced, PID: 4242, ProcessStartedAt: started, Cwd: "/second"})
	if !first.OK || !second.OK {
		t.Fatalf("provisional selects = %+v / %+v", first, second)
	}
	if committed := s.handleSelectCommit(context.Background(), Request{Op: OpSelectCommit, ReservationToken: first.ReservationToken}); !committed.OK {
		t.Fatalf("first commit = %+v", committed)
	}
	s.wg.Wait()
	if err := s.m.Store.Close(); err != nil {
		t.Fatal(err)
	}
	failed := s.handleSelectCommit(context.Background(), Request{Op: OpSelectCommit, ReservationToken: second.ReservationToken})
	if failed.OK || !strings.Contains(failed.Error, "activate selection") {
		t.Fatalf("second commit against closed store = %+v", failed)
	}
	if got := s.cl.reservedCount(forced); got != 0 {
		t.Fatalf("terminal commits retained %d reservations, want zero", got)
	}
	replayed := s.handleSelectCommit(context.Background(), Request{Op: OpSelectCommit, ReservationToken: second.ReservationToken})
	if replayed.OK || replayed.Error != failed.Error {
		t.Fatalf("terminal failure replay = %+v, want %+v", replayed, failed)
	}
}

func TestProvisionalSelectionExpiresWithoutEffects(t *testing.T) {
	s, _ := newTestServer(t)
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, PID: 4242, ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj"})
	if !resp.OK {
		t.Fatalf("select = %+v", resp)
	}
	s.cl.mu.Lock()
	s.cl.selections[resp.ReservationToken].expiresAt = time.Now().Add(-time.Second)
	s.cl.mu.Unlock()

	s.cl.pruneSelections(time.Now())
	if committed := s.handleSelectCommit(context.Background(), Request{Op: OpSelectCommit, ReservationToken: resp.ReservationToken}); committed.OK {
		t.Fatalf("expired commit unexpectedly succeeded: %+v", committed)
	}
	if live, _ := s.m.Store.ListActiveSessions(); len(live) != 0 {
		t.Fatalf("expired selection opened sessions: %+v", live)
	}
	if _, ok, _ := s.m.Store.GetSticky("/proj"); ok {
		t.Fatal("expired selection recorded sticky state")
	}
}

func TestProvisionalSelectionDiesWithDaemonWithoutEffects(t *testing.T) {
	s, _ := newTestServer(t)
	response := s.handleSelect(t.Context(), Request{
		Op: OpSelect, PID: 4242,
		ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/dead-daemon",
	})
	if !response.OK || response.ReservationToken == "" {
		t.Fatalf("provisional selection = %+v", response)
	}
	restarted := &Server{m: s.m, cl: newClaims()}
	if got := restarted.cl.reservedCount(*response.SelectedID); got != 0 {
		t.Fatalf("fresh daemon retained %d in-memory reservations", got)
	}
	commit := restarted.handleSelectCommit(t.Context(), Request{
		Op: OpSelectCommit, ReservationToken: response.ReservationToken,
	})
	if commit.OK || !strings.Contains(commit.Error, "unknown or expired") {
		t.Fatalf("dead-daemon provisional commit = %+v", commit)
	}
	if committed, err := s.m.Store.SelectionCommitted(response.ReservationToken); err != nil || committed {
		t.Fatalf("dead-daemon token committed = %v, %v", committed, err)
	}
	if sessions, err := s.m.Store.ListActiveSessions(); err != nil || len(sessions) != 0 {
		t.Fatalf("dead-daemon provisional sessions = %+v, %v", sessions, err)
	}
	if _, ok, err := s.m.Store.GetSticky("/dead-daemon"); err != nil || ok {
		t.Fatalf("dead-daemon provisional sticky = ok %v, %v", ok, err)
	}
}

func TestRunCommitRejectsAccountGenerationChange(t *testing.T) {
	s, _ := newTestServer(t)
	forced := 1
	resp := s.handleSelect(t.Context(), Request{
		Op: OpSelect, Account: &forced, PID: 4242,
		ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj",
	})
	if !resp.OK {
		t.Fatalf("select = %+v", resp)
	}
	a, err := s.m.Store.GetAccount(forced)
	if err != nil {
		t.Fatal(err)
	}
	a.ConfigDir += "-replacement"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	committed := s.handleSelectCommit(context.Background(), Request{
		Op: OpSelectCommit, ReservationToken: resp.ReservationToken,
	})
	if committed.OK || !strings.Contains(committed.Error, "account generation changed") {
		t.Fatalf("commit after generation change = %+v", committed)
	}
	if live, err := s.m.Store.ListActiveSessions(); err != nil {
		t.Fatal(err)
	} else if len(live) != 0 {
		t.Fatalf("generation-raced run opened sessions: %+v", live)
	}
	if sticky, ok, err := s.m.Store.GetSticky("/proj"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("generation-raced run recorded sticky state: %+v", sticky)
	}
	if got := s.cl.reservedCount(forced); got != 0 {
		t.Fatalf("generation-raced run retained %d reservations, want zero", got)
	}
}

func TestHandleSelectHonorsSticky(t *testing.T) {
	s, dirs := newTestServer(t)
	// Sticky points at the WORSE account.
	if err := s.m.Store.UpsertSticky("/proj", 2, time.Now()); err != nil {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[2] || !resp.Sticky {
		t.Fatalf("expected sticky acct-2 (%s), got %+v", dirs[2], resp)
	}
}

// TestHandleSelectSkipsExhaustedStickyPin replays the 2026-06-10 incident:
// reset credit (eff5 ≈ 93, reset ~21m out) must not keep a pegged pin alive.
func TestHandleSelectSkipsExhaustedStickyPin(t *testing.T) {
	s, dirs := newTestServer(t)
	now := time.Now().Add(time.Minute) // newer than the harness samples
	if err := s.m.Store.InsertUsageSample(store.UsageSample{
		AccountID: 2, TS: now, Util5h: 100, Util7d: 21,
		Resets5h: now.Add(21 * time.Minute), Resets7d: now.Add(24 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.m.Store.UpsertSticky("/proj", 2, now); err != nil {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[1] {
		t.Fatalf("expected healthy acct-1 (%s) over the exhausted pin, got %+v", dirs[1], resp)
	}
	if resp.Sticky || resp.ExhaustedFallback {
		t.Fatalf("a fresh healthy pick must report neither sticky nor fallback: %+v", resp)
	}
	st, ok, err := s.m.Store.GetSticky("/proj")
	if err != nil || !ok || st.AccountID != 2 {
		t.Fatalf("raw inspection rewrote sticky row: %+v ok=%v err=%v", st, ok, err)
	}
}

// TestHandleSelectMarksSessionWithCwd: marked sessions feed the sticky
// activity rules.
func TestHandleSelectMarksSessionWithCwd(t *testing.T) {
	s, _ := newTestServer(t)
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, PID: 4242, ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj"})
	if !resp.OK || resp.SelectedID == nil {
		t.Fatalf("select failed: %+v", resp)
	}
	commitSelectResponse(t, s, resp)
	live, err := s.m.Store.ListActiveSessions()
	if err != nil || len(live) != 1 {
		t.Fatalf("sessions = %+v err=%v", live, err)
	}
	if live[0].PID != 4242 || live[0].Cwd != "/proj" || live[0].AccountID != *resp.SelectedID {
		t.Fatalf("session row = %+v, want pid 4242 cwd /proj acct %d", live[0], *resp.SelectedID)
	}
}

// TestHandleSelectBindsWarmEndedSession: a pin whose session ended minutes
// ago must still bind — the warm cache is what stickiness protects.
func TestHandleSelectBindsWarmEndedSession(t *testing.T) {
	s, dirs := newTestServer(t)
	now := time.Now()
	if err := s.m.Store.UpsertSticky("/proj", 2, now.Add(-3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	id := activateDaemonTestSession(t, s, 2, 800002, "/proj", now.Add(-3*time.Hour))
	if err := s.m.Store.CloseSession(id, now.Add(-10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[2] || !resp.Sticky {
		t.Fatalf("expected sticky acct-2 (%s) via warm ended session, got %+v", dirs[2], resp)
	}
}

// TestHandleSelectHoldsLiveOnlyPin: a still-live session cannot be resumed,
// so ranking runs free — but the pin is never repointed.
func TestHandleSelectHoldsLiveOnlyPin(t *testing.T) {
	s, dirs := newTestServer(t)
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)
	now := time.Now()
	if err := s.m.Store.UpsertSticky("/proj", 2, now); err != nil {
		t.Fatal(err)
	}
	started := now.Add(-10 * time.Minute).Truncate(time.Microsecond)
	activateDaemonTestSession(t, s, 2, 800002, "/proj", started)
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 800002, ConfigDir: dirs[2], StartedAt: started}}, nil
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[1] || resp.Sticky {
		t.Fatalf("expected free non-sticky acct-1 (%s), got %+v", dirs[1], resp)
	}
	if resp.PinHeldAccount != nil {
		t.Fatalf("an auto hold must not flag a held manual pin: %+v", resp.PinHeldAccount)
	}
	st, ok, _ := s.m.Store.GetSticky("/proj")
	if !ok || st.AccountID != 2 {
		t.Fatalf("held pin was repointed: %+v ok=%v", st, ok)
	}
	if !strings.Contains(buf.String(), "select (pin-held): /proj -> acct-01") {
		t.Fatalf("held pin not logged: %q", buf.String())
	}
}

// TestHandleSelectQuickResumeBindsAfterReap: handleSelect reconciles before
// deciding, so a just-died session reads as a warm end and the pin binds.
func TestHandleSelectQuickResumeBindsAfterReap(t *testing.T) {
	s, dirs := newTestServer(t)
	now := time.Now()
	if err := s.m.Store.UpsertSticky("/proj", 2, now.Add(-3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// pid 4000000 is impossible (macOS pids are 5-digit), so handleSelect's
	// sweep reaps the row; the -10m reconcile below makes the reap a warm end.
	activateDaemonTestSession(t, s, 2, 4000000, "/proj", now.Add(-3*time.Hour))
	if _, err := s.m.Store.CloseDeadSessions(map[int]time.Time{4000000: now.Add(-3 * time.Hour).Truncate(time.Microsecond)}, now.Add(-10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[2] || !resp.Sticky {
		t.Fatalf("quick resume must bind the pin via the reaped warm end, got %+v", resp)
	}
}

func TestHandleSelectForcedMarksSession(t *testing.T) {
	s, _ := newTestServer(t)
	forced := 2
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &forced, PID: 4242, ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj"})
	if !resp.OK {
		t.Fatalf("forced select failed: %+v", resp)
	}
	commitSelectResponse(t, s, resp)
	live, err := s.m.Store.ListActiveSessions()
	if err != nil || len(live) != 1 {
		t.Fatalf("sessions = %+v err=%v", live, err)
	}
	if live[0].PID != 4242 || live[0].Cwd != "/proj" || live[0].AccountID != 2 {
		t.Fatalf("session row = %+v, want pid 4242 cwd /proj acct 2", live[0])
	}
}

func TestHandleSelectHoldsUnusableManualPin(t *testing.T) {
	s, dirs := newTestServer(t)
	now := time.Now().Add(time.Minute)
	if err := s.m.Store.PinManual("/proj", 2, now); err != nil {
		t.Fatal(err)
	}
	if err := s.m.Store.InsertUsageSample(store.UsageSample{
		AccountID: 2, TS: now, Util5h: 100, Util7d: 21,
		Resets5h: now.Add(21 * time.Minute), Resets7d: now.Add(24 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[1] || resp.Sticky {
		t.Fatalf("expected free acct-1 (%s) over the exhausted manual pin, got %+v", dirs[1], resp)
	}
	if resp.PinHeldAccount == nil || *resp.PinHeldAccount != 2 {
		t.Fatalf("held manual pin must be surfaced, got %+v", resp.PinHeldAccount)
	}
	st, ok, _ := s.m.Store.GetSticky("/proj")
	if !ok || st.AccountID != 2 || !st.Manual {
		t.Fatalf("manual pin lost on hold: %+v ok=%v", st, ok)
	}
}

func TestHandleSelectForcedKeepsManualPin(t *testing.T) {
	s, dirs := newTestServer(t)
	now := time.Now()
	if err := s.m.Store.PinManual("/proj", 1, now); err != nil {
		t.Fatal(err)
	}
	forced := 2
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &forced, Cwd: "/proj"})
	if !resp.OK || resp.Dir != dirs[2] {
		t.Fatalf("forced select failed: %+v", resp)
	}
	st, ok, _ := s.m.Store.GetSticky("/proj")
	if !ok || st.AccountID != 1 || !st.Manual {
		t.Fatalf("forced select repointed the manual pin: %+v ok=%v", st, ok)
	}
}

// TestHandleSelectExhaustedFallback: an exhausted pool yields the least-bad
// pick flagged ExhaustedFallback — never an error.
func TestHandleSelectExhaustedFallback(t *testing.T) {
	s, dirs := newTestServer(t)
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)
	now := time.Now().Add(time.Minute)
	reset := now.Add(20 * time.Minute)
	for id, util7 := range map[int]float64{1: 90, 2: 10} {
		if err := s.m.Store.InsertUsageSample(store.UsageSample{
			AccountID: id, TS: now, Util5h: 100, Util7d: util7,
			Resets5h: reset, ExtraEnabled: id == 2,
		}); err != nil {
			t.Fatal(err)
		}
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
	if !resp.OK || !resp.ExhaustedFallback {
		t.Fatalf("expected a flagged fallback pick, got %+v", resp)
	}
	if resp.Dir != dirs[2] || !resp.ExtraEnabled {
		t.Fatalf("expected least-bad acct-2 (%s) with extra usage surfaced, got %+v", dirs[2], resp)
	}
	if resp.SoonestReset == nil || !resp.SoonestReset.Equal(reset.Truncate(time.Second)) {
		t.Fatalf("fallback must carry the pick's recovery time %v for the warning, got %v", reset, resp.SoonestReset)
	}
	s.wg.Wait()
	logged := buf.String()
	if !strings.Contains(logged, "select (exhausted-fallback): /proj -> acct-02") {
		t.Fatalf("fallback pick not logged as such: %q", logged)
	}
	if !strings.Contains(logged, "runner-up acct-01") {
		t.Fatalf("fallback log must name the exhausted runner-up: %q", logged)
	}
}

// TestHandleSelectNoFallback: --wait (NoFallback) must not commit the
// discarded pick's sticky or reservation.
func TestHandleSelectNoFallback(t *testing.T) {
	s, _ := newTestServer(t)
	now := time.Now().Add(time.Minute)
	for id := 1; id <= 2; id++ {
		if err := s.m.Store.InsertUsageSample(store.UsageSample{
			AccountID: id, TS: now, Util5h: 100, Resets5h: now.Add(20 * time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj", NoFallback: true})
	if resp.OK || !resp.NoneAvailable {
		t.Fatalf("NoFallback over an exhausted pool must report none available, got %+v", resp)
	}
	if _, ok, _ := s.m.Store.GetSticky("/proj"); ok {
		t.Fatal("a refused fallback pick must not rewrite the sticky record")
	}
	for id := 1; id <= 2; id++ {
		if s.cl.reservedCount(id) != 0 {
			t.Fatalf("a refused fallback pick must not reserve acct-%d", id)
		}
	}
}

func TestHandleStatusPropagatesExhaustionAndOverage(t *testing.T) {
	s, _ := newTestServer(t)
	now := time.Now().Add(time.Minute)
	if err := s.m.Store.InsertUsageSample(store.UsageSample{
		AccountID: 1, TS: now, Util5h: 100, Util7d: 21,
		Resets5h:     now.Add(20 * time.Minute),
		ExtraEnabled: true, ExtraUsed: 177, ExtraLimit: 5000,
	}); err != nil {
		t.Fatal(err)
	}
	resp := s.handleStatus(t.Context())
	if !resp.OK || len(resp.Accounts) != 2 {
		t.Fatalf("status failed: %+v", resp)
	}
	var acct1 *AccountStatus
	for i := range resp.Accounts {
		if resp.Accounts[i].ID == 1 {
			acct1 = &resp.Accounts[i]
		} else if resp.Accounts[i].Exhausted || resp.Accounts[i].ExtraEnabled {
			t.Fatalf("healthy acct must carry no exhaustion/overage: %+v", resp.Accounts[i])
		}
	}
	if acct1 == nil {
		t.Fatal("acct-1 missing from status")
	}
	if !acct1.Exhausted {
		t.Fatalf("pegged account must report exhausted: %+v", acct1)
	}
	if !acct1.ExtraEnabled || acct1.ExtraUsed != 177 || acct1.ExtraLimit != 5000 {
		t.Fatalf("overage state must survive the wire: %+v", acct1)
	}
}

// TestHandleSelectNoneAvailable: all rate-limited → structured NoneAvailable
// plus the soonest reset for --wait, read through to each account's last
// known-good sample.
func TestHandleSelectNoneAvailable(t *testing.T) {
	s, _ := newTestServer(t)
	now := time.Now().Add(time.Minute)
	reset := now.Add(30 * time.Minute)
	for id := 1; id <= 2; id++ {
		// A real prior reading carrying the window reset, then a rate-limit
		// marker on top (zeroed, as the production 429 path records it).
		if err := s.m.Store.InsertUsageSample(store.UsageSample{
			AccountID: id, TS: now, Util5h: 50, Resets5h: reset,
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.m.Store.InsertUsageSample(store.UsageSample{
			AccountID: id, TS: now.Add(time.Second), RateLimited: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
	if resp.OK || !resp.NoneAvailable {
		t.Fatalf("expected structured none-available, got %+v", resp)
	}
	if resp.SoonestReset == nil || !resp.SoonestReset.Equal(reset.Truncate(time.Second)) {
		t.Fatalf("expected soonest reset %v, got %v", reset, resp.SoonestReset)
	}
}

// TestHandleSelectLogsPick: every select logs its outcome — the 2026-06-10
// incident needed DB forensics because fresh picks logged nothing.
func TestHandleSelectLogsPick(t *testing.T) {
	s, _ := newTestServer(t)
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)
	if resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"}); !resp.OK {
		t.Fatalf("select failed: %+v", resp)
	}
	s.wg.Wait()
	logged := buf.String()
	if !strings.Contains(logged, "select: /proj -> acct-01") {
		t.Fatalf("fresh pick not logged: %q", logged)
	}
	if !strings.Contains(logged, "5h 10% used") || !strings.Contains(logged, "runner-up acct-02") {
		t.Fatalf("log line missing usage/runner-up: %q", logged)
	}
}

func TestHandleSelectForcedRecordsSticky(t *testing.T) {
	s, dirs := newTestServer(t)
	acct := 2
	resp := s.handleSelect(t.Context(), Request{
		Op: OpSelect, Account: &acct, PID: 4242,
		ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj",
	})
	if !resp.OK || resp.Dir != dirs[2] {
		t.Fatalf("expected forced acct-2 (%s), got %+v", dirs[2], resp)
	}
	if resp.Sticky {
		t.Fatal("forced select must not report sticky (ranking was not overridden)")
	}
	commitSelectResponse(t, s, resp)
	st, ok, err := s.m.Store.GetSticky("/proj")
	if err != nil || !ok || st.AccountID != 2 {
		t.Fatalf("forced account not recorded: %+v ok=%v err=%v", st, ok, err)
	}
}

func commitSelectResponse(t *testing.T, s *Server, resp Response) {
	t.Helper()
	committed := s.handleSelectCommit(context.Background(), Request{Op: OpSelectCommit, ReservationToken: resp.ReservationToken})
	if !committed.OK {
		t.Fatalf("commit selection: %+v", committed)
	}
}
