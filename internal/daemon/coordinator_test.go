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
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/daemonkit/wire"
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

type fleetLifecycleRuntime struct {
	mu          sync.Mutex
	provisioned []string
}

type absentRemovalRuntime struct {
	lifecycleRuntimeStub
	mu          sync.Mutex
	removeCalls int
}

func (r *absentRemovalRuntime) RemoveTenant(
	_ context.Context,
	account tenantfs.Account,
	expected uint64,
) (mountproto.RemoveTenantResponse, error) {
	id, err := account.TenantID()
	if err != nil {
		return mountproto.RemoveTenantResponse{}, err
	}
	r.mu.Lock()
	r.removeCalls++
	r.mu.Unlock()
	return mountproto.RemoveTenantResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
		TenantID: mountproto.TenantID(id), Generation: expected,
		FileProviderAbsent: true,
	}, nil
}

func (r *fleetLifecycleRuntime) ProvisionTenant(
	_ context.Context,
	account tenantfs.Account,
) (mountproto.ProvisionTenantResponse, error) {
	id, err := account.TenantID()
	if err != nil {
		return mountproto.ProvisionTenantResponse{}, err
	}
	r.mu.Lock()
	r.provisioned = append(r.provisioned, account.FileProviderDisplayName)
	r.mu.Unlock()
	return mountproto.ProvisionTenantResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
		TenantID: mountproto.TenantID(id), Generation: account.Generation,
	}, nil
}

func (*fleetLifecycleRuntime) ReplaceTenant(
	context.Context,
	tenantfs.Account,
	uint64,
) (mountproto.ReplaceTenantResponse, error) {
	return mountproto.ReplaceTenantResponse{}, errors.New("unexpected replace")
}

func (*fleetLifecycleRuntime) RemoveTenant(
	_ context.Context,
	account tenantfs.Account,
	expected uint64,
) (mountproto.RemoveTenantResponse, error) {
	id, err := account.TenantID()
	if err != nil {
		return mountproto.RemoveTenantResponse{}, err
	}
	return mountproto.RemoveTenantResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
		TenantID: mountproto.TenantID(id), Generation: expected,
		FileProviderAbsent: true,
	}, nil
}

func (*fleetLifecycleRuntime) TenantState(
	context.Context,
	tenantfs.Account,
) (mountproto.StateResponse, error) {
	return mountproto.StateResponse{}, remoteError(mountproto.ErrorCodeNotFound)
}

func (r *fleetLifecycleRuntime) provisionedAccounts() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.provisioned...)
}

type fleetSourcePreparer struct {
	mu          sync.Mutex
	started     chan string
	release     <-chan struct{}
	failName    string
	activation  string
	mutateProof func(*catalogproto.TenantPreparationProof)
	prepared    []string
	active      int
	maximum     int
}

func (p *fleetSourcePreparer) Prepare(
	ctx context.Context,
	account tenantfs.Account,
	_ tenantfs.PreparationLease,
) (catalogproto.TenantPreparationProof, error) {
	name := account.FileProviderDisplayName
	p.mu.Lock()
	p.prepared = append(p.prepared, name)
	p.active++
	if p.active > p.maximum {
		p.maximum = p.active
	}
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.active--
		p.mu.Unlock()
	}()
	if p.started != nil {
		p.started <- name
	}
	if name == p.failName {
		return catalogproto.TenantPreparationProof{}, errors.New("injected prepare failure")
	}
	if p.release != nil {
		select {
		case <-p.release:
		case <-ctx.Done():
			return catalogproto.TenantPreparationProof{}, ctx.Err()
		}
	}
	proof := daemonTestPreparationProof(store.Account{
		InstanceID: account.InstanceID, Generation: account.Generation,
	}, filepath.Join("/Users/test/Library/CloudStorage", "CCPoolStatus-"+name))
	if p.activation != "" {
		proof.Presentation.FileProvider.ActivationGeneration = p.activation
	}
	if p.mutateProof != nil {
		p.mutateProof(&proof)
	}
	return proof, nil
}

func (*fleetSourcePreparer) Validate(
	context.Context,
	tenantfs.Account,
	tenantfs.PreparationLease,
	catalogproto.TenantPreparationProof,
) error {
	return nil
}

func (p *fleetSourcePreparer) counts() (prepared []string, active, maximum int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.prepared...), p.active, p.maximum
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
	tenantfs.PreparationLease,
) (catalogproto.TenantPreparationProof, error) {
	p.prepared++
	return p.proof, p.prepareErr
}

func (p *sourcePreparerStub) Validate(
	context.Context,
	tenantfs.Account,
	tenantfs.PreparationLease,
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

func testPreparationLease() tenantfs.PreparationLease {
	return tenantfs.PreparationLease{
		ID: "00112233445566778899aabbccddeeff", ExpiresAt: time.Now().Add(time.Minute),
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

func insertCoordinatorAccounts(t *testing.T, st *store.Store, total int) {
	t.Helper()
	for id := 1; id <= total; id++ {
		admitDaemonTestAccount(t, st, store.Account{
			ID: id, InstanceID: fmt.Sprintf("%032x", id), Generation: 1,
			ConfigDir:       testFileProviderConfigDir(id),
			KeychainService: "service", KeychainAccount: "account",
		})
	}
}

func openDesiredCoordinatorStore(t *testing.T, total int) *store.Store {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	path := filepath.Join(home, "pool-v1.db")
	initial, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := initial.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=foreign_keys(on)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	for id := 1; id <= total; id++ {
		if _, err := db.Exec(
			`INSERT INTO accounts(
			 id,instance_id,generation,config_dir,keychain_service,keychain_account,label,account_uuid,created_at
			 ) VALUES(?,?,?,?,?,?,?,?,1)`,
			id, fmt.Sprintf("%032x", id), 1, testFileProviderConfigDir(id),
			fmt.Sprintf("service-%d", id), fmt.Sprintf("account-%d", id),
			fmt.Sprintf("label-%d", id), fmt.Sprintf("uuid-%d", id),
		); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
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
	if _, err := coordinator.prepare(t.Context(), account, testPreparationLease()); err != nil {
		t.Fatal(err)
	}
	if runtime.provisionCalls != 1 || preparer.prepared != 1 {
		t.Fatalf("provision calls = %d, prepare calls = %d", runtime.provisionCalls, preparer.prepared)
	}
	_ = tenantAccount
}

func TestInitializeProvisionsEveryActiveAccountWithoutPreparingContent(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	st, err := store.Open(filepath.Join(home, "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	const total = 19
	insertCoordinatorAccounts(t, st, total)
	runtime := &fleetLifecycleRuntime{}
	preparer := &fleetSourcePreparer{}
	server := &Server{m: &pool.Manager{Store: st}}
	coordinator := newTenantCoordinator(t.Context(), server, preparer, runtime)
	if err := coordinator.initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, active, maximum := preparer.counts()
	provisioned := runtime.provisionedAccounts()
	if len(prepared) != 0 || len(provisioned) != total || active != 0 || maximum != 0 {
		t.Fatalf(
			"startup fleet = prepared %d provisioned %d active %d maximum %d",
			len(prepared), len(provisioned), active, maximum,
		)
	}
	slices.Sort(provisioned)
	for id := 1; id <= total; id++ {
		want := pool.AccountDirName(id)
		if provisioned[id-1] != want {
			t.Fatalf("startup account %d = provisioned %q, want %q", id, provisioned[id-1], want)
		}
	}
}

func TestInitializeBindsImmutableIdentityForFreshDesiredAccounts(t *testing.T) {
	st := openDesiredCoordinatorStore(t, 2)
	runtime := &fleetLifecycleRuntime{}
	preparer := &fleetSourcePreparer{}
	server := &Server{m: &pool.Manager{Store: st}}
	server.beginBootstrap()
	coordinator := newTenantCoordinator(t.Context(), server, preparer, runtime)
	if err := coordinator.initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	active, err := st.ListActiveAccounts()
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 2 {
		t.Fatalf("active accounts = %d, want 2", len(active))
	}
	for _, account := range active {
		presentation, err := st.AccountPresentation(account.ID)
		if err != nil {
			t.Fatal(err)
		}
		expected, err := expectedPresentationIdentity(account)
		if err != nil {
			t.Fatal(err)
		}
		if presentation.AccountInstanceID != account.InstanceID ||
			presentation.AccountGeneration != account.Generation ||
			presentation.Identity != expected {
			t.Fatalf("acct-%02d presentation = %+v", account.ID, presentation)
		}
	}
	progress := server.bootstrapSnapshot()
	if progress.Total != 2 || progress.Settled != 2 || progress.Quarantined != 0 ||
		!progress.Terminal || progress.FailureCount != 0 {
		t.Fatalf("bootstrap progress = %+v", progress)
	}
}

func TestInitializeBindsIdentityWithoutPreparingContent(t *testing.T) {
	st := openDesiredCoordinatorStore(t, 1)
	release := make(chan struct{})
	preparer := &fleetSourcePreparer{started: make(chan string, 1), release: release}
	server := &Server{m: &pool.Manager{Store: st}}
	server.beginBootstrap()
	coordinator := newTenantCoordinator(t.Context(), server, preparer, &fleetLifecycleRuntime{})
	if err := coordinator.initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AccountPresentation(1); err != nil {
		t.Fatal(err)
	}
	if prepared, _, _ := preparer.counts(); len(prepared) != 0 {
		t.Fatalf("startup prepared content: %v", prepared)
	}
	close(release)
}

func TestInitializeRetainsDesiredPresentationIdentityAfterRestart(t *testing.T) {
	st := openDesiredCoordinatorStore(t, 1)
	server := &Server{m: &pool.Manager{Store: st}}
	first := newTenantCoordinator(t.Context(), server, &fleetSourcePreparer{}, &fleetLifecycleRuntime{})
	if err := first.initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted := newTenantCoordinator(t.Context(), server, &fleetSourcePreparer{}, &fleetLifecycleRuntime{})
	if err := restarted.initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	presentation, err := st.AccountPresentation(1)
	if err != nil {
		t.Fatal(err)
	}
	account, err := st.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := expectedPresentationIdentity(account)
	if err != nil {
		t.Fatal(err)
	}
	if presentation.Identity != expected {
		t.Fatalf("restart identity = %+v, want %+v", presentation.Identity, expected)
	}
}

func TestConcurrentInitializeConvergesOneDesiredPresentation(t *testing.T) {
	st := openDesiredCoordinatorStore(t, 1)
	server := &Server{m: &pool.Manager{Store: st}}
	start := make(chan struct{})
	errs := make(chan error, 2)
	for range 2 {
		coordinator := newTenantCoordinator(
			t.Context(), server, &fleetSourcePreparer{release: start}, &fleetLifecycleRuntime{},
		)
		go func() { errs <- coordinator.initialize(t.Context()) }()
	}
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	active, err := st.ListActiveAccounts()
	if err != nil || len(active) != 1 {
		t.Fatalf("active accounts = %+v err=%v", active, err)
	}
	if _, err := st.AccountPresentation(1); err != nil {
		t.Fatal(err)
	}
}

func TestInitializeSettlesDesiredIdentityMismatchWithDurableQuarantine(t *testing.T) {
	st := openDesiredCoordinatorStore(t, 1)
	account, err := st.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := expectedPresentationIdentity(account)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.BindDesiredAccountPresentation(account, expected); err != nil {
		t.Fatal(err)
	}
	observed := expected
	observed.Generation++
	if err := st.ObserveAccountPresentation(account, observed); !errors.Is(err, store.ErrAccountPresentationQuarantined) {
		t.Fatalf("seed identity quarantine: %v", err)
	}
	preparer := &fleetSourcePreparer{}
	server := &Server{m: &pool.Manager{Store: st}}
	server.beginBootstrap()
	coordinator := newTenantCoordinator(t.Context(), server, preparer, &fleetLifecycleRuntime{})
	if err := coordinator.initialize(t.Context()); err != nil {
		t.Fatalf("initialize mismatch = %v, want settled quarantine", err)
	}
	quarantine, err := st.AccountPresentationQuarantine(1)
	if err != nil || quarantine.Reason != store.AccountPresentationGenerationDrift {
		t.Fatalf("quarantine = %+v err=%v", quarantine, err)
	}
	if active, err := st.ListActiveAccounts(); err != nil || len(active) != 0 {
		t.Fatalf("active after mismatch = %+v err=%v", active, err)
	}
	progress := server.bootstrapSnapshot()
	if progress.Total != 0 || progress.Settled != 0 || progress.Quarantined != 0 ||
		!progress.Terminal || progress.FailureCount != 0 {
		t.Fatalf("quarantine bootstrap progress = %+v", progress)
	}
}

func TestInitializeDoesNotRetryRawHolderTransitions(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "starting", err: wire.ErrNotReady},
		{name: "draining", err: wire.ErrDraining},
	} {
		t.Run(test.name, func(t *testing.T) {
			st := openDesiredCoordinatorStore(t, 1)
			runtime := &lifecycleRuntimeStub{provisionErr: test.err}
			preparer := &fleetSourcePreparer{}
			server := &Server{m: &pool.Manager{Store: st}}
			coordinator := newTenantCoordinator(t.Context(), server, preparer, runtime)
			if err := coordinator.initialize(t.Context()); !errors.Is(err, test.err) {
				t.Fatalf("initialize %s = %v", test.name, err)
			}
			prepared, _, _ := preparer.counts()
			if runtime.provisionCalls != 1 || len(prepared) != 0 {
				t.Fatalf("%s attempts: provision=%d prepare=%d", test.name, runtime.provisionCalls, len(prepared))
			}
			if _, err := st.AccountPresentation(1); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("presentation after %s = %v", test.name, err)
			}
		})
	}
}

func TestInitializeRecoversRemovalBeforePreparingActiveFleet(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	st, err := store.Open(filepath.Join(home, "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	insertCoordinatorAccounts(t, st, 2)
	if err := os.MkdirAll(pool.AccountBackingDir(2), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := st.BeginAccountRemoval(2, true); err != nil {
		t.Fatal(err)
	}
	runtime := &fleetLifecycleRuntime{}
	preparer := &fleetSourcePreparer{}
	server := &Server{
		m: newDaemonTestManager(t, st, accountMutationTestRefresher{}, credstest.NewFake()),
	}
	coordinator := newTenantCoordinator(t.Context(), server, preparer, runtime)
	if err := coordinator.initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, _, _ := preparer.counts()
	provisioned := runtime.provisionedAccounts()
	if len(prepared) != 0 || !slices.Equal(provisioned, []string{"acct-01"}) {
		t.Fatalf("active recovery = prepared %v provisioned %v, want provision only acct-01", prepared, provisioned)
	}
	if _, err := st.GetAccount(2); !errors.Is(err, store.ErrAccountNotFound) {
		t.Fatalf("removed account after recovery = %v", err)
	}
}

func TestInitializeIgnoresContentPreparationFailuresAndRestartReprovisionsFleet(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	st, err := store.Open(filepath.Join(home, "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	insertCoordinatorAccounts(t, st, 2)
	server := &Server{m: &pool.Manager{Store: st}}
	firstPreparer := &fleetSourcePreparer{failName: "acct-02"}
	firstRuntime := &fleetLifecycleRuntime{}
	first := newTenantCoordinator(t.Context(), server, firstPreparer, firstRuntime)
	if err := first.initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	firstPrepared, _, _ := firstPreparer.counts()
	if len(firstPrepared) != 0 {
		t.Fatalf("startup prepared content: %v", firstPrepared)
	}
	firstProvisioned := firstRuntime.provisionedAccounts()
	slices.Sort(firstProvisioned)
	if !slices.Equal(firstProvisioned, []string{"acct-01", "acct-02"}) {
		t.Fatalf("startup provisioned %v", firstProvisioned)
	}

	restartRuntime := &fleetLifecycleRuntime{}
	restartPreparer := &fleetSourcePreparer{}
	restarted := newTenantCoordinator(t.Context(), server, restartPreparer, restartRuntime)
	if err := restarted.initialize(t.Context()); err != nil {
		t.Fatalf("restart recovery: %v", err)
	}
	retried, _, _ := restartPreparer.counts()
	reprovisioned := restartRuntime.provisionedAccounts()
	slices.Sort(retried)
	slices.Sort(reprovisioned)
	want := []string{"acct-01", "acct-02"}
	if len(retried) != 0 || !slices.Equal(reprovisioned, want) {
		t.Fatalf("restart fleet = prepared %v provisioned %v, want %v", retried, reprovisioned, want)
	}
}

func TestInitializeNeverStartsContentPreparation(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	st, err := store.Open(filepath.Join(home, "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	insertCoordinatorAccounts(t, st, 2)
	server := &Server{m: &pool.Manager{Store: st}}
	preparer := &fleetSourcePreparer{failName: "acct-02"}
	coordinator := newTenantCoordinator(t.Context(), server, preparer, &fleetLifecycleRuntime{})
	if err := coordinator.initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, active, _ := preparer.counts()
	if len(prepared) != 0 || active != 0 {
		t.Fatalf("settled startup fleet = prepared %v active %d", prepared, active)
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
		admitDaemonTestAccount(t, st, store.Account{
			ID: id, ConfigDir: testFileProviderConfigDir(id),
			KeychainService: "service", KeychainAccount: "account",
		})
		if _, err := st.BeginAccountRemoval(id, true); err != nil {
			t.Fatal(err)
		}
	}
	runtime := &absentRemovalRuntime{
		lifecycleRuntimeStub: lifecycleRuntimeStub{
			stateErr: remoteError(mountproto.ErrorCodeNotFound),
		},
	}
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
	if runtime.provisionCalls != 0 || runtime.stateCalls != total || runtime.removeCalls != total {
		t.Fatalf(
			"startup calls: provision=%d state=%d retirement=%d",
			runtime.provisionCalls, runtime.stateCalls, runtime.removeCalls,
		)
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
		t.Context(), account, testPreparationLease(), catalogproto.TenantPreparationProof{},
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
	admitDaemonTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: testFileProviderConfigDir(1),
		KeychainService: "service", KeychainAccount: "account",
	})
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

func TestFinishRemovalRequiresExplicitTenantAbsenceProofWhenStateIsAbsent(t *testing.T) {
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
	account := admitDaemonTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: testFileProviderConfigDir(1),
		KeychainService: "service", KeychainAccount: "account",
	})
	removal, err := st.BeginAccountRemoval(account.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	tenantID, err := pool.TenantAccount(account).TenantID()
	if err != nil {
		t.Fatal(err)
	}
	runtime := &lifecycleRuntimeStub{
		stateErr: remoteError(mountproto.ErrorCodeNotFound),
		remove: mountproto.RemoveTenantResponse{
			Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
			TenantID: mountproto.TenantID(tenantID), Generation: account.Generation,
		},
	}
	server := &Server{
		m:   newDaemonTestManager(t, st, accountMutationTestRefresher{}, credstest.NewFake()),
		log: log.New(io.Discard, "", 0),
	}
	coordinator := &tenantCoordinator{server: server, runtime: runtime}
	if err := coordinator.finishRemoval(t.Context(), removal); err == nil {
		t.Fatal("absent state completed removal without explicit File Provider absence proof")
	}
	if _, err := st.GetAccount(account.ID); err != nil {
		t.Fatalf("account after rejected absence proof = %v", err)
	}
	runtime.remove.FileProviderAbsent = true
	if err := coordinator.finishRemoval(t.Context(), removal); err != nil {
		t.Fatal(err)
	}
	if !runtime.removed || runtime.removeExpected != account.Generation {
		t.Fatalf("explicit retirement proof = removed %v expected generation %d", runtime.removed, runtime.removeExpected)
	}
	if _, err := st.GetAccount(account.ID); !errors.Is(err, store.ErrAccountNotFound) {
		t.Fatalf("account after removal = %v", err)
	}
}
