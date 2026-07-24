package daemon

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
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
		RecoveryID: pool.CredentialOwnerRecoveryID,
		PID:        identity.PID, StartTime: identity.StartTime, Boot: identity.Boot,
		Comm: identity.Comm, Executable: identity.Executable,
		AuditToken: identity.AuditToken, Generation: daemonTestGeneration("daemon-test"),
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
	manager.ClaimCredentialMutation = func(int) (func(), error) {
		return func() {}, nil
	}
	manager.BuildCredentialWritePublication = credentialWritePublicationBuilder("daemon-test")
	manager.SettleCredentialWrite = func(context.Context, pool.CredentialWriteSettlement) error {
		return nil
	}
	return manager
}

func TestSelectPreflightSettlesBeforeReturn(t *testing.T) {
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
		t.Fatalf("pending reservations = %d, want one during credential preflight", got)
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
		_ tenantfs.PreparationLease,
	) (catalogproto.TenantPreparationProof, error) {
		callsMu.Lock()
		calls[account.ID]++
		callsMu.Unlock()
		proof := daemonTestPreparationProof(account, testFileProviderConfigDir(account.ID))
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
	if got := s.cl.reservedCount(forcedOne); got != 1 {
		t.Fatalf("primary PrepareTenant reservations = %d, want 1", got)
	}

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
		t.Fatalf("canceled PrepareTenant reservation count = %d, want 1", got)
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
		t.Fatalf("primary returned before PrepareTenant settled: %+v", response)
	default:
	}
	close(release)
	released = true
	var primaryToken string
	select {
	case response := <-primaryResponses:
		if !response.OK || response.ReservationToken == "" {
			t.Fatalf("primary PrepareTenant response = %+v", response)
		}
		primaryToken = response.ReservationToken
		if abort := s.handleSelectAbort(t.Context(), Request{
			Op: OpSelectAbort, ReservationToken: response.ReservationToken,
		}); !abort.OK {
			t.Fatalf("abort primary selection = %+v", abort)
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

// newTestServer builds a Server with acct-1 emptier than acct-2. scanSessions
// is stubbed: real `ps` can hang on a wedged mount.
func newTestServer(t *testing.T) (*Server, map[int]string) {
	return newTestServerWithPaths(t, nil)
}

func newTestServerWithPaths(t *testing.T, paths map[int]string) (*Server, map[int]string) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	dirs := map[int]string{}
	presentationPaths := map[int]string{}
	fakeCreds := credstest.NewFake()
	now := time.Now()
	for _, id := range []int{1, 2} {
		util := map[int]float64{1: 10, 2: 50}[id]
		presentationPath := testFileProviderConfigDir(id)
		if paths[id] != "" {
			presentationPath = paths[id]
		}
		presentationPaths[id] = presentationPath
		account := admitDaemonTestAccount(t, st, store.Account{
			ID: id, ConfigDir: presentationPath, Generation: 1,
			KeychainAccount: "ccp-test",
		})
		dirs[id] = account.ConfigDir
		if err := st.InsertUsageSample(store.UsageSample{AccountID: id, TS: now, Util5h: util, Util7d: util}); err != nil {
			t.Fatal(err)
		}
		credential := &creds.Credential{}
		credential.ClaudeAiOauth.AccessToken = fmt.Sprintf("access-%d", id)
		credential.ClaudeAiOauth.RefreshToken = fmt.Sprintf("refresh-%d", id)
		credential.ClaudeAiOauth.ExpiresAt = now.Add(time.Hour).UnixMilli()
		fakeCreds.Put(account.KeychainService, "ccp-test", credential)
	}
	s := &Server{
		m:            newDaemonTestManager(t, st, &fakeOAuth{}, fakeCreds),
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		cl:           newClaims(),
		led:          newLedgers(),
		prepareAccount: func(ctx context.Context, account store.Account, _ tenantfs.PreparationLease) (catalogproto.TenantPreparationProof, error) {
			return daemonTestPreparationProof(account, presentationPaths[account.ID]), ctx.Err()
		},
		activatePrepared: func(_ context.Context, _ store.Account, _ tenantfs.PreparationLease, _ catalogproto.TenantPreparationProof, activate func() error) error {
			return activate()
		},
		sessionLeases: &testSessionLeaseManager{},
	}
	s.m.ClaimCredentialMutation = func(accountID int) (func(), error) {
		if !s.cl.ownExclusive(accountID) {
			return nil, errAccountExclusive
		}
		return func() { s.cl.releaseExclusive(accountID) }, nil
	}
	return s, dirs
}

func daemonTestPreparationProof(account store.Account, publicPath string) catalogproto.TenantPreparationProof {
	tenantID, err := (tenantfs.Account{InstanceID: account.InstanceID}).TenantID()
	if err != nil {
		panic(err)
	}
	domainID, err := catalogproto.DeriveDomainID(
		tenantfs.OwnerID,
		catalogproto.PresentationInstanceID(account.InstanceID),
	)
	if err != nil {
		panic(err)
	}
	const revision = 7
	const rootID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const policyDigest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const resolutionDigest = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	const sourcePublication = "dddddddddddddddddddddddddddddddd"
	presentationInstance := catalogproto.PresentationInstanceID(account.InstanceID)
	return catalogproto.TenantPreparationProof{
		Catalog: catalogproto.CatalogLaneProof{
			Tenant: catalogproto.TenantID(tenantID), Generation: account.Generation,
			Requested: revision, Desired: revision, Observed: revision, Verified: revision, Applied: revision,
		},
		Presentation: catalogproto.PresentationProof{
			Kind: catalogproto.PresentationKindFileProvider,
			FileProvider: &catalogproto.FileProviderPresentationProof{
				TenantID: catalogproto.TenantID(tenantID), DomainID: domainID,
				Generation: account.Generation, PublicPath: publicPath, ActivationGeneration: "activation-test",
				PresentationInstanceID: presentationInstance, RootID: rootID,
			},
		},
		SourceAuthority: catalogproto.SourceAuthorityID(tenantfs.ClaudeAuthorityID),
		SourceRevision:  revision, CatalogRevision: revision,
		ChangeID: "change-test", OperationID: "operation-test", SourcePublication: sourcePublication,
		CriticalReadiness: &catalogproto.CriticalReadinessProof{
			Lease: catalogproto.FileProviderLeaseReceipt{
				LeaseID: account.InstanceID, TenantID: catalogproto.TenantID(tenantID), DomainID: domainID,
				Generation: account.Generation, RootID: rootID, PresentationInstanceID: presentationInstance,
				State:        catalogproto.FileProviderLeaseStateProvisional,
				PolicyDigest: policyDigest, ResolutionDigest: resolutionDigest, CatalogHead: revision,
				SourceAuthority:   catalogproto.SourceAuthorityID(tenantfs.ClaudeAuthorityID),
				SourcePublication: sourcePublication, SourceRevision: revision,
				ActivationGeneration: "activation-test", ExpiresUnixNano: uint64(time.Now().Add(time.Minute).UnixNano()),
			},
		},
	}
}

func TestPrepareSelectionReturnsCompleteValidatedFuseProof(t *testing.T) {
	s, dirs := newTestServer(t)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	path, token, err := s.prepareSelection(t.Context(), account, selectionLaunch{})
	if err != nil || path != dirs[1] || token == "" {
		t.Fatalf("prepare selection = path %q token %q err %v; want path %q", path, token, err, dirs[1])
	}
	s.cl.abortReservation(token)
	s.prepareAccount = func(_ context.Context, got store.Account, _ tenantfs.PreparationLease) (catalogproto.TenantPreparationProof, error) {
		if got.ID != account.ID || got.InstanceID != account.InstanceID || got.Generation != account.Generation {
			t.Fatalf("account = %+v, want %+v", got, account)
		}
		invalid := daemonTestPreparationProof(account, "relative/path")
		return invalid, nil
	}
	if _, _, err := s.prepareSelection(t.Context(), account, selectionLaunch{}); !errors.Is(err, tenantfs.ErrPreparationConflict) {
		t.Fatalf("invalid injected proof error = %v, want ErrPreparationConflict", err)
	}
}

func TestPreparationIdentityProjectionRejectsDrift(t *testing.T) {
	account := store.Account{
		ID: 7, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 4,
	}
	raw := daemonTestPreparationProof(account, "/Users/test/Library/CloudStorage/proof-account-7")
	identity, err := projectPreparationIdentity(raw)
	if err != nil {
		t.Fatal(err)
	}
	if identity.TenantID == "" || identity.DomainID == "" || identity.Generation != account.Generation ||
		identity.PublicPath != "/Users/test/Library/CloudStorage/proof-account-7" {
		t.Fatalf("identity = %+v", identity)
	}
	raw.Presentation.FileProvider.PublicPath = "relative/path"
	if _, err := projectPreparationIdentity(raw); !errors.Is(err, tenantfs.ErrPreparationConflict) {
		t.Fatalf("relative identity error = %v", err)
	}
}

func TestSelectionUsesStableExecutionPathForVerifiedPresentation(t *testing.T) {
	publicPath := "/Users/test/Library/CloudStorage/cc-pool-account-1"
	s, _ := newTestServerWithPaths(t, map[int]string{1: publicPath})
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	s.prepareAccount = func(ctx context.Context, got store.Account, _ tenantfs.PreparationLease) (catalogproto.TenantPreparationProof, error) {
		return daemonTestPreparationProof(got, publicPath), ctx.Err()
	}
	forced := account.ID
	response := s.handleSelect(t.Context(), Request{
		Op: OpSelect, Account: &forced, PID: 4242,
		ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proof-path",
	})
	if !response.OK || response.Dir != account.ConfigDir {
		t.Fatalf("selection = %+v, want stable path %q", response, account.ConfigDir)
	}
	commitSelectResponse(t, s, response)
	sessions, err := s.m.Store.ListActiveSessions()
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].ConfigDir != account.ConfigDir {
		t.Fatalf("committed sessions = %+v, want stable path %q", sessions, account.ConfigDir)
	}
	if target, err := os.Readlink(account.ConfigDir); err != nil || target != publicPath {
		t.Fatalf("stable config target = %q, %v; want %q", target, err, publicPath)
	}
}

func TestSelectionRepairsPresentationPathDriftWithoutChangingExecutionPath(t *testing.T) {
	s, _ := newTestServer(t)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	provenPath := "/Users/test/Library/CloudStorage/cc-pool-account-1"
	s.prepareAccount = func(ctx context.Context, got store.Account, _ tenantfs.PreparationLease) (catalogproto.TenantPreparationProof, error) {
		return daemonTestPreparationProof(got, provenPath), ctx.Err()
	}
	forced := account.ID
	response := s.handleSelect(t.Context(), Request{
		Op: OpSelect, Account: &forced, PID: 4242,
		ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proof-drift",
	})
	if !response.OK || response.Dir != account.ConfigDir {
		t.Fatalf("selection = %+v, want stable path %q", response, account.ConfigDir)
	}
	commitSelectResponse(t, s, response)
	presentation, err := s.m.Store.AccountPresentation(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	fileProvider := daemonTestPreparationProof(account, provenPath).Presentation.FileProvider
	if presentation.AccountInstanceID != account.InstanceID ||
		presentation.AccountGeneration != account.Generation ||
		presentation.Identity.TenantID != string(fileProvider.TenantID) ||
		presentation.Identity.DomainID != string(fileProvider.DomainID) ||
		presentation.Identity.Generation != fileProvider.Generation ||
		presentation.Identity.PublicPath != provenPath {
		t.Fatalf("repaired presentation = %+v", presentation)
	}
	if target, err := os.Readlink(account.ConfigDir); err != nil || target != provenPath {
		t.Fatalf("stable config target = %q, %v; want %q", target, err, provenPath)
	}
	if _, err := s.m.Store.AccountPresentationQuarantine(account.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("presentation quarantine remains after repair: %v", err)
	}
	if sessions, err := s.m.Store.ListActiveSessions(); err != nil || len(sessions) != 1 || sessions[0].ConfigDir != account.ConfigDir {
		t.Fatalf("repaired selection sessions = %+v, %v", sessions, err)
	}
}

func activateDaemonTestSession(t *testing.T, s *Server, accountID, pid int, cwd string, started time.Time) int64 {
	t.Helper()
	started = started.Truncate(time.Microsecond)
	a, err := s.m.Store.GetAccount(accountID)
	if err != nil {
		t.Fatal(err)
	}
	token := nextDaemonTestToken()
	provisional := store.FileProviderLeaseReceipt("daemon-test-provisional:" + token)
	staged, err := s.m.Store.StageSelection(store.SelectionActivation{
		Token:     token,
		AccountID: accountID, ExpectedInstanceID: a.InstanceID, ExpectedGeneration: a.Generation,
		Process:   store.ProcessIdentity{PID: pid, StartedAt: started},
		ConfigDir: a.ConfigDir,
		Cwd:       cwd, At: started,
		FileProviderLease: provisional, LeaseExpiresAt: started.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	committed := store.FileProviderLeaseReceipt("daemon-test-committed:" + token)
	if err := s.m.Store.CommitSelection(token, provisional, committed, false, started); err != nil {
		t.Fatal(err)
	}
	return staged.ID
}

func closeDaemonTestSession(t *testing.T, s *Server, id int64, at time.Time) {
	t.Helper()
	sessions, err := s.m.Store.ListActiveSessions()
	if err != nil {
		t.Fatal(err)
	}
	for _, session := range sessions {
		if session.ID != id {
			continue
		}
		released := append(store.FileProviderLeaseReceipt(nil), session.FileProviderLease...)
		released = append(released, []byte(":released")...)
		if err := s.m.Store.CompleteSessionLeaseRelease(id, session.FileProviderLease, released, at); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatalf("session %d not found", id)
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
	presentation, err := s.m.Store.AccountPresentation(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	proof := daemonTestPreparationProof(account, presentation.Identity.PublicPath)
	proof.SourceRevision = 1
	if !s.cl.bindPreparation(token, proof) {
		t.Fatal("bind preparation failed")
	}
	s.activatePrepared = func(_ context.Context, _ store.Account, _ tenantfs.PreparationLease, got catalogproto.TenantPreparationProof, _ func() error) error {
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
	s, _ := newTestServer(t)
	s.prepareAccount = func(context.Context, store.Account, tenantfs.PreparationLease) (catalogproto.TenantPreparationProof, error) {
		t.Fatal("metadata-only selection prepared a tenant")
		return catalogproto.TenantPreparationProof{}, nil
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
	if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 1 || resp.Prepared || resp.Dir != "" {
		t.Fatalf("metadata-only select = %+v, want unprepared acct-1 with no path", resp)
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
	if !first.OK || !first.Prepared || first.Dir != dirs[1] || first.ReservationToken == "" {
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
	if !first.OK {
		t.Fatalf("first provisional select = %+v", first)
	}
	account, err := s.m.Store.GetAccount(forced)
	if err != nil {
		t.Fatal(err)
	}
	secondToken, err := s.cl.beginSelection(account, selectionLaunch{
		pid: 4242, processStartedAt: time.UnixMicro(started), cwd: "/second",
	}, reservationTTL)
	if err != nil {
		t.Fatal(err)
	}
	s.cl.mu.Lock()
	firstSelection := s.cl.selections[first.ReservationToken]
	var preparation *catalogproto.TenantPreparationProof
	if firstSelection != nil && firstSelection.preparation != nil {
		proof := *firstSelection.preparation
		preparation = &proof
	}
	s.cl.mu.Unlock()
	if preparation == nil || !s.cl.bindPreparation(secondToken, *preparation) {
		t.Fatal("copy first selection preparation into commit-failure fixture")
	}
	second := Response{OK: true, ReservationToken: secondToken}
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

func TestRunCommitRejectsReservedGenerationMismatch(t *testing.T) {
	s, _ := newTestServer(t)
	forced := 1
	resp := s.handleSelect(t.Context(), Request{
		Op: OpSelect, Account: &forced, PID: 4242,
		ProcessStartedAt: time.Now().Add(-time.Minute).UnixMicro(), Cwd: "/proj",
	})
	if !resp.OK {
		t.Fatalf("select = %+v", resp)
	}
	s.cl.mu.Lock()
	s.cl.selections[resp.ReservationToken].accountGeneration++
	s.cl.mu.Unlock()

	committed := s.handleSelectCommit(context.Background(), Request{
		Op: OpSelectCommit, ReservationToken: resp.ReservationToken,
	})
	if committed.OK || !strings.Contains(committed.Error, "identity changed") {
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
	s, _ := newTestServer(t)
	// Sticky points at the WORSE account.
	if err := s.m.Store.UpsertSticky("/proj", 2, time.Now()); err != nil {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
	if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 2 || resp.Prepared || resp.Dir != "" || !resp.Sticky {
		t.Fatalf("expected metadata-only sticky acct-2, got %+v", resp)
	}
}

// TestHandleSelectSkipsExhaustedStickyPin replays the 2026-06-10 incident:
// reset credit (eff5 ≈ 93, reset ~21m out) must not keep a pegged pin alive.
func TestHandleSelectSkipsExhaustedStickyPin(t *testing.T) {
	s, _ := newTestServer(t)
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
	if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 1 || resp.Prepared || resp.Dir != "" {
		t.Fatalf("expected metadata-only healthy acct-1 over the exhausted pin, got %+v", resp)
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
	s, _ := newTestServer(t)
	now := time.Now()
	if err := s.m.Store.UpsertSticky("/proj", 2, now.Add(-3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	id := activateDaemonTestSession(t, s, 2, 800002, "/proj", now.Add(-3*time.Hour))
	closeDaemonTestSession(t, s, id, now.Add(-10*time.Minute))
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
	if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 2 || resp.Prepared || resp.Dir != "" || !resp.Sticky {
		t.Fatalf("expected metadata-only sticky acct-2 via warm ended session, got %+v", resp)
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
	if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 1 || resp.Prepared || resp.Dir != "" || resp.Sticky {
		t.Fatalf("expected metadata-only free non-sticky acct-1, got %+v", resp)
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
	s, _ := newTestServer(t)
	now := time.Now()
	if err := s.m.Store.UpsertSticky("/proj", 2, now.Add(-3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// pid 4000000 is impossible (macOS pids are 5-digit), so handleSelect's
	// sweep reaps the row; the -10m reconcile below makes the reap a warm end.
	activateDaemonTestSession(t, s, 2, 4000000, "/proj", now.Add(-3*time.Hour))
	alive := map[int]time.Time{4000000: now.Add(-3 * time.Hour).Truncate(time.Microsecond)}
	if result, err := s.m.Store.ReconcileSessions(alive, alive, now.Add(-10*time.Minute)); err != nil || len(result.Live) != 1 {
		t.Fatal(err)
	}
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Cwd: "/proj"})
	if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 2 || resp.Prepared || resp.Dir != "" || !resp.Sticky {
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
	s, _ := newTestServer(t)
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
	if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 1 || resp.Prepared || resp.Dir != "" || resp.Sticky {
		t.Fatalf("expected metadata-only free acct-1 over the exhausted manual pin, got %+v", resp)
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
	s, _ := newTestServer(t)
	now := time.Now()
	if err := s.m.Store.PinManual("/proj", 1, now); err != nil {
		t.Fatal(err)
	}
	forced := 2
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &forced, Cwd: "/proj"})
	if !resp.OK || resp.SelectedID == nil || *resp.SelectedID != 2 || resp.Prepared || resp.Dir != "" {
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
	s, _ := newTestServer(t)
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
	if resp.SelectedID == nil || *resp.SelectedID != 2 || resp.Prepared || resp.Dir != "" || !resp.ExtraEnabled {
		t.Fatalf("expected metadata-only least-bad acct-2 with extra usage surfaced, got %+v", resp)
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
