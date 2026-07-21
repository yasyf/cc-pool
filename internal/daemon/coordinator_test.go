package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
)

type lifecycleRuntimeStub struct {
	mu                 sync.Mutex
	provision          mountproto.ProvisionTenantResponse
	provisionErr       error
	provisionResponses []mountproto.ProvisionTenantResponse
	provisionErrors    []error
	provisionCalls     int
	state              mountproto.StateResponse
	stateErr           error
	stateCalls         int
	replace            mountproto.ReplaceTenantResponse
	replaceErr         error
	replaceExpected    uint64
	remove             mountproto.RemoveTenantResponse
	removeErr          error
	removeExpected     uint64
	removed            bool
}

func (r *lifecycleRuntimeStub) ProvisionTenant(
	context.Context,
	tenantfs.Account,
) (mountproto.ProvisionTenantResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	index := r.provisionCalls
	r.provisionCalls++
	if index < len(r.provisionResponses) {
		return r.provisionResponses[index], r.provisionErrors[index]
	}
	return r.provision, r.provisionErr
}

func (r *lifecycleRuntimeStub) ReplaceTenant(
	_ context.Context,
	_ tenantfs.Account,
	expected uint64,
) (mountproto.ReplaceTenantResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.replaceExpected = expected
	return r.replace, r.replaceErr
}

func (r *lifecycleRuntimeStub) RemoveTenant(
	_ context.Context,
	_ tenantfs.Account,
	expected uint64,
) (mountproto.RemoveTenantResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeExpected = expected
	if r.removeErr == nil {
		r.removed = true
	}
	return r.remove, r.removeErr
}

func (r *lifecycleRuntimeStub) TenantState(
	context.Context,
	tenantfs.Account,
) (mountproto.StateResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stateCalls++
	if r.stateErr != nil {
		return mountproto.StateResponse{}, r.stateErr
	}
	if r.removed {
		return mountproto.StateResponse{}, remoteError(mountproto.ErrorCodeNotFound)
	}
	return r.state, nil
}

type sourcePreparerStub struct {
	proof       catalogproto.TenantPreparationProof
	prepareErr  error
	validateErr error
	prepared    int
	validated   int
}

type blockingLifecycleRuntime struct {
	mu      sync.Mutex
	started chan struct{}
	release chan struct{}
	calls   int
	active  int
	maximum int
}

func newBlockingLifecycleRuntime(total int) *blockingLifecycleRuntime {
	return &blockingLifecycleRuntime{
		started: make(chan struct{}, total),
		release: make(chan struct{}),
	}
}

func (r *blockingLifecycleRuntime) ProvisionTenant(
	ctx context.Context,
	account tenantfs.Account,
) (mountproto.ProvisionTenantResponse, error) {
	r.mu.Lock()
	r.calls++
	r.active++
	if r.active > r.maximum {
		r.maximum = r.active
	}
	r.mu.Unlock()
	r.started <- struct{}{}
	select {
	case <-r.release:
	case <-ctx.Done():
		r.mu.Lock()
		r.active--
		r.mu.Unlock()
		return mountproto.ProvisionTenantResponse{}, ctx.Err()
	}
	r.mu.Lock()
	r.active--
	r.mu.Unlock()
	id, err := account.TenantID()
	if err != nil {
		return mountproto.ProvisionTenantResponse{}, err
	}
	return mountproto.ProvisionTenantResponse{
		Protocol:   mountproto.Version,
		Code:       mountproto.ErrorCodeOk,
		TenantID:   mountproto.TenantID(id),
		Generation: account.Generation,
	}, nil
}

func (*blockingLifecycleRuntime) ReplaceTenant(
	context.Context,
	tenantfs.Account,
	uint64,
) (mountproto.ReplaceTenantResponse, error) {
	return mountproto.ReplaceTenantResponse{}, errors.New("unexpected replace")
}

func (*blockingLifecycleRuntime) RemoveTenant(
	context.Context,
	tenantfs.Account,
	uint64,
) (mountproto.RemoveTenantResponse, error) {
	return mountproto.RemoveTenantResponse{}, errors.New("unexpected remove")
}

func (*blockingLifecycleRuntime) TenantState(
	context.Context,
	tenantfs.Account,
) (mountproto.StateResponse, error) {
	return mountproto.StateResponse{}, errors.New("unexpected state")
}

func (r *blockingLifecycleRuntime) counts() (calls, active, maximum int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls, r.active, r.maximum
}

func (p *sourcePreparerStub) Prepare(
	context.Context,
	tenantfs.Account,
) (catalogproto.TenantPreparationProof, error) {
	p.prepared++
	return p.proof, p.prepareErr
}

func (p *sourcePreparerStub) Validate(
	tenantfs.Account,
	catalogproto.TenantPreparationProof,
) error {
	p.validated++
	return p.validateErr
}

func exactState(id mountproto.TenantID, generation uint64) mountproto.StateResponse {
	return mountproto.StateResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
		State: &mountproto.TenantState{
			OwnerID: mountproto.OwnerID(tenantfs.OwnerID), TenantID: id, Generation: generation,
			Desired: 11, Applied: 11, StateVersion: 1, ReplacementEligible: true,
		},
	}
}

func remoteError(code mountproto.ErrorCode) error {
	return &mountservice.RemoteError{Code: code, Message: string(code)}
}

func allAccountRemovals(t *testing.T, st *store.Store) []store.AccountRemoval {
	t.Helper()
	var removals []store.AccountRemoval
	for after := 0; ; {
		page, err := st.PageAccountRemovals(t.Context(), after, store.AccountRemovalPageLimit)
		if err != nil {
			t.Fatal(err)
		}
		removals = append(removals, page.Removals...)
		if page.Next == 0 {
			return removals
		}
		after = page.Next
	}
}

func testTenantAccount(t *testing.T) (store.Account, tenantfs.Account, mountproto.TenantID) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	account := store.Account{
		ID: 18, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 7,
	}
	tenantAccount := pool.TenantAccount(account)
	tenantID, err := tenantAccount.TenantID()
	if err != nil {
		t.Fatal(err)
	}
	return account, tenantAccount, mountproto.TenantID(tenantID)
}

func bulkInsertInactiveAccounts(t *testing.T, database string, total int) {
	t.Helper()
	db, err := sql.Open("sqlite", database)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	statement, err := tx.PrepareContext(t.Context(), `
		INSERT INTO accounts(
			id,instance_id,generation,config_dir,keychain_service,keychain_account,created_at
		) VALUES(?,?,?,?,?,?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = statement.Close() }()
	for id := 1; id <= total; id++ {
		if _, err := statement.ExecContext(
			t.Context(), id, fmt.Sprintf("%032x", id), 1,
			fmt.Sprintf("/accounts/%d", id), "service", "account", 1,
		); err != nil {
			t.Fatalf("insert account %d: %v", id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureTenantReplacesPriorGenerationAfterProvisionConflict(t *testing.T) {
	account, tenantAccount, tenantID := testTenantAccount(t)
	runtime := &lifecycleRuntimeStub{
		provisionErr: remoteError(mountproto.ErrorCodeConflict),
		state:        exactState(tenantID, 2),
		replace: mountproto.ReplaceTenantResponse{
			Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
			TenantID: tenantID, Generation: account.Generation,
		},
	}
	coordinator := &tenantCoordinator{runtime: runtime}
	if err := coordinator.ensureTenant(t.Context(), account, tenantAccount); err != nil {
		t.Fatal(err)
	}
	if runtime.replaceExpected != 2 {
		t.Fatalf("replace expected generation = %d, want 2", runtime.replaceExpected)
	}
}

func TestEnsureTenantRetriesConflictThatRacesWithAbsence(t *testing.T) {
	account, tenantAccount, tenantID := testTenantAccount(t)
	runtime := &lifecycleRuntimeStub{
		provisionResponses: []mountproto.ProvisionTenantResponse{
			{},
			{
				Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
				TenantID: tenantID, Generation: account.Generation,
			},
		},
		provisionErrors: []error{remoteError(mountproto.ErrorCodeConflict), nil},
		stateErr:        remoteError(mountproto.ErrorCodeNotFound),
	}
	if err := (&tenantCoordinator{runtime: runtime}).ensureTenant(t.Context(), account, tenantAccount); err != nil {
		t.Fatal(err)
	}
	if runtime.provisionCalls != 2 || runtime.replaceExpected != 0 {
		t.Fatalf("provision calls = %d, replace expected = %d", runtime.provisionCalls, runtime.replaceExpected)
	}
}

func TestPrepareProvisionsBeforeOnDemandConvergence(t *testing.T) {
	account, tenantAccount, tenantID := testTenantAccount(t)
	runtime := &lifecycleRuntimeStub{provision: mountproto.ProvisionTenantResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
		TenantID: tenantID, Generation: account.Generation,
	}}
	preparer := &sourcePreparerStub{}
	coordinator := &tenantCoordinator{runtime: runtime, preparer: preparer}
	if _, err := coordinator.prepare(t.Context(), account); err != nil {
		t.Fatal(err)
	}
	if runtime.provisionCalls != 1 || preparer.prepared != 1 {
		t.Fatalf("provision calls = %d, prepare calls = %d", runtime.provisionCalls, preparer.prepared)
	}
	_ = tenantAccount
}

func TestInitializeAdmissionDoesNotReplayHundredThousandInactiveAccounts(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	database := filepath.Join(home, "pool-v1.db")
	st, err := store.Open(database)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	bulkInsertInactiveAccounts(t, database, 100_000)
	runtime := &lifecycleRuntimeStub{provisionErr: errors.New("unexpected eager provision")}
	server := &Server{m: &pool.Manager{Store: st}}
	coordinator := newTenantCoordinator(t.Context(), server, nil, runtime)
	if err := coordinator.initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	if runtime.provisionCalls != 0 {
		t.Fatalf("startup provision calls = %d, want 0", runtime.provisionCalls)
	}
}

func TestRemovalRecoveryIsBounded(t *testing.T) {
	const total = 3 * accountRemovalRecoveryConcurrency
	removals := make([]store.AccountRemoval, total)
	for index := range removals {
		removals[index].AccountID = index + 1
	}
	started := make(chan struct{}, total)
	release := make(chan struct{})
	var (
		mu      sync.Mutex
		active  int
		maximum int
	)
	done := make(chan error, 1)
	go func() {
		done <- recoverAccountRemovals(
			t.Context(),
			func(context.Context, int, int) (store.AccountRemovalPage, error) {
				return store.AccountRemovalPage{Removals: removals}, nil
			},
			func(context.Context, store.AccountRemoval) error {
				mu.Lock()
				active++
				if active > maximum {
					maximum = active
				}
				mu.Unlock()
				started <- struct{}{}
				<-release
				mu.Lock()
				active--
				mu.Unlock()
				return nil
			},
		)
	}()
	for range accountRemovalRecoveryConcurrency {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("removal recovery did not fill its bounded capacity")
		}
	}
	mu.Lock()
	if active != accountRemovalRecoveryConcurrency || maximum != accountRemovalRecoveryConcurrency {
		mu.Unlock()
		t.Fatalf("filled removal concurrency = active %d maximum %d", active, maximum)
	}
	mu.Unlock()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if active != 0 || maximum != accountRemovalRecoveryConcurrency {
		t.Fatalf("removal concurrency = active %d maximum %d", active, maximum)
	}
}

func TestRemovalRecoveryRestartsCursorAndReplaysFailedClaims(t *testing.T) {
	const total = 2*store.AccountRemovalPageLimit + 1
	claims := make(map[int]struct{}, total)
	for id := 1; id <= total; id++ {
		claims[id] = struct{}{}
	}
	var (
		mu          sync.Mutex
		fail        = true
		pageCursors []int
	)
	page := func(
		_ context.Context,
		after, limit int,
	) (store.AccountRemovalPage, error) {
		mu.Lock()
		defer mu.Unlock()
		pageCursors = append(pageCursors, after)
		result := store.AccountRemovalPage{Removals: make([]store.AccountRemoval, 0, limit)}
		for id := after + 1; id <= total; id++ {
			if _, present := claims[id]; !present {
				continue
			}
			if len(result.Removals) == limit {
				result.Next = result.Removals[len(result.Removals)-1].AccountID
				break
			}
			result.Removals = append(result.Removals, store.AccountRemoval{AccountID: id})
		}
		return result, nil
	}
	finish := func(_ context.Context, removal store.AccountRemoval) error {
		mu.Lock()
		defer mu.Unlock()
		if removal.AccountID == store.AccountRemovalPageLimit/2 && fail {
			fail = false
			return errors.New("injected interruption")
		}
		delete(claims, removal.AccountID)
		return nil
	}
	if err := recoverAccountRemovals(t.Context(), page, finish); err == nil {
		t.Fatal("interrupted recovery succeeded")
	}
	if err := recoverAccountRemovals(t.Context(), page, finish); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(claims) != 0 {
		t.Fatalf("recovery left %d durable claims", len(claims))
	}
	if len(pageCursors) < 2 || pageCursors[0] != 0 || pageCursors[1] != 0 {
		t.Fatalf("restart cursors = %v, want prefix [0 0]", pageCursors)
	}
}

func TestInitializePagesOnlyInterruptedRemovalClaims(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	st, err := store.Open(filepath.Join(home, "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	const total = store.AccountRemovalPageLimit + 1
	for id := 1; id <= total; id++ {
		if err := st.UpsertAccount(store.Account{
			ID: id, ConfigDir: pool.AccountPresentationDir(id),
			KeychainService: "service", KeychainAccount: "account",
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.BeginAccountRemoval(id, true); err != nil {
			t.Fatal(err)
		}
	}
	runtime := &lifecycleRuntimeStub{stateErr: remoteError(mountproto.ErrorCodeNotFound)}
	server := &Server{
		m: newDaemonTestManager(t, st, accountMutationTestRefresher{}, credstest.NewFake()),
	}
	coordinator := newTenantCoordinator(t.Context(), server, nil, runtime)
	if err := coordinator.initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	accounts, err := st.ListAccounts()
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != 0 || len(allAccountRemovals(t, st)) != 0 {
		t.Fatalf("restart recovery left accounts=%d removals=%d", len(accounts), len(allAccountRemovals(t, st)))
	}
	if runtime.provisionCalls != 0 || runtime.stateCalls != total {
		t.Fatalf("startup calls: provision=%d state=%d", runtime.provisionCalls, runtime.stateCalls)
	}
}

func TestOnDemandProvisioningIsBounded(t *testing.T) {
	const requests = 3 * tenantProvisionConcurrency
	runtime := newBlockingLifecycleRuntime(requests)
	coordinator := newTenantCoordinator(t.Context(), nil, nil, runtime)
	errs := make(chan error, requests)
	for id := 1; id <= requests; id++ {
		account := store.Account{
			ID: id, InstanceID: fmt.Sprintf("%032x", id), Generation: 1,
		}
		go func() {
			tenantAccount := pool.TenantAccount(account)
			errs <- coordinator.ensureTenant(t.Context(), account, tenantAccount)
		}()
	}
	for range tenantProvisionConcurrency {
		select {
		case <-runtime.started:
		case <-time.After(time.Second):
			t.Fatal("bounded provisioning did not fill its capacity")
		}
	}
	if calls, active, maximum := runtime.counts(); calls != tenantProvisionConcurrency ||
		active != tenantProvisionConcurrency || maximum != tenantProvisionConcurrency {
		t.Fatalf("filled provision counts = calls %d active %d maximum %d", calls, active, maximum)
	}
	close(runtime.release)
	for range requests {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	calls, active, maximum := runtime.counts()
	if calls != requests || active != 0 || maximum != tenantProvisionConcurrency {
		t.Fatalf("provision counts = calls %d active %d maximum %d", calls, active, maximum)
	}
}

func TestOnDemandProvisioningCoalescesOneTenantGeneration(t *testing.T) {
	const requests = 32
	runtime := newBlockingLifecycleRuntime(requests)
	coordinator := newTenantCoordinator(t.Context(), nil, nil, runtime)
	account := store.Account{
		ID: 18, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 7,
	}
	tenantAccount := pool.TenantAccount(account)
	ready := sync.WaitGroup{}
	ready.Add(requests)
	errs := make(chan error, requests)
	start := make(chan struct{})
	for range requests {
		go func() {
			ready.Done()
			<-start
			errs <- coordinator.ensureTenant(t.Context(), account, tenantAccount)
		}()
	}
	ready.Wait()
	close(start)
	select {
	case <-runtime.started:
	case <-time.After(time.Second):
		t.Fatal("coalesced provision did not start")
	}
	close(runtime.release)
	for range requests {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if calls, _, _ := runtime.counts(); calls != 1 {
		t.Fatalf("coalesced provision calls = %d, want 1", calls)
	}
}

func TestCanceledProvisionWaiterDoesNotAbandonSharedSettlement(t *testing.T) {
	runtime := newBlockingLifecycleRuntime(2)
	coordinator := newTenantCoordinator(t.Context(), nil, nil, runtime)
	account := store.Account{
		ID: 18, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 7,
	}
	tenantAccount := pool.TenantAccount(account)
	waiterContext, cancelWaiter := context.WithCancel(t.Context())
	waiter := make(chan error, 1)
	go func() {
		waiter <- coordinator.ensureTenant(waiterContext, account, tenantAccount)
	}()
	select {
	case <-runtime.started:
	case <-time.After(time.Second):
		t.Fatal("provision did not start")
	}
	cancelWaiter()
	if err := <-waiter; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter = %v", err)
	}
	follower := make(chan error, 1)
	go func() {
		follower <- coordinator.ensureTenant(t.Context(), account, tenantAccount)
	}()
	close(runtime.release)
	if err := <-follower; err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ensureTenant(t.Context(), account, tenantAccount); err != nil {
		t.Fatal(err)
	}
	if calls, active, maximum := runtime.counts(); calls != 1 || active != 0 || maximum != 1 {
		t.Fatalf("settled provision counts = calls %d active %d maximum %d", calls, active, maximum)
	}
}

func TestActivatePreparedValidatesBeforeSessionActivation(t *testing.T) {
	account, _, _ := testTenantAccount(t)
	validationErr := errors.New("stale proof")
	preparer := &sourcePreparerStub{validateErr: validationErr}
	activated := false
	err := (&tenantCoordinator{preparer: preparer}).activatePrepared(
		t.Context(), account, catalogproto.TenantPreparationProof{},
		func() error {
			activated = true
			return nil
		},
	)
	if !errors.Is(err, validationErr) || activated || preparer.validated != 1 {
		t.Fatalf("activate = err %v, activated %v, validations %d", err, activated, preparer.validated)
	}
}

func TestFinishRemovalNeedsOnlyTenantAbsenceProof(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(home, "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	if err := st.UpsertAccount(store.Account{
		ID: 1, ConfigDir: pool.AccountPresentationDir(1),
		KeychainService: "service", KeychainAccount: "account",
	}); err != nil {
		t.Fatal(err)
	}
	account, err := st.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(pool.AccountBackingDir(account.ID), 0o700); err != nil {
		t.Fatal(err)
	}
	removal, err := st.BeginAccountRemoval(account.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	tenantID, err := pool.TenantAccount(account).TenantID()
	if err != nil {
		t.Fatal(err)
	}
	runtime := &lifecycleRuntimeStub{
		state: exactState(mountproto.TenantID(tenantID), account.Generation),
		remove: mountproto.RemoveTenantResponse{
			Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
			TenantID: mountproto.TenantID(tenantID), Generation: account.Generation,
			FileProviderAbsent: true,
		},
	}
	server := &Server{
		m:   newDaemonTestManager(t, st, accountMutationTestRefresher{}, credstest.NewFake()),
		log: log.New(io.Discard, "", 0),
	}
	if err := (&tenantCoordinator{server: server, runtime: runtime}).finishRemoval(t.Context(), removal); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetAccount(account.ID); !errors.Is(err, store.ErrAccountNotFound) {
		t.Fatalf("account after removal = %v", err)
	}
	if _, err := os.Lstat(pool.AccountBackingDir(account.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("backing after removal = %v", err)
	}
}
