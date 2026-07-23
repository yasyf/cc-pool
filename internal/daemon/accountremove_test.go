package daemon

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/fusekit/mountproto"
)

type blockingRemovalRuntime struct {
	lifecycleRuntimeStub
	entered chan struct{}
	release <-chan struct{}
}

func (r *blockingRemovalRuntime) RemoveTenant(
	ctx context.Context,
	_ tenantfs.Account,
	expected uint64,
) (mountproto.RemoveTenantResponse, error) {
	r.removeExpected = expected
	close(r.entered)
	select {
	case <-ctx.Done():
		return mountproto.RemoveTenantResponse{}, ctx.Err()
	case <-r.release:
		if r.removeErr == nil {
			r.removed = true
		}
		return r.remove, r.removeErr
	}
}

func newAccountRemovalTestServer(
	t *testing.T,
	database string,
) (*Server, store.Account, *store.Store) {
	t.Helper()
	st, err := store.Open(database)
	if err != nil {
		t.Fatal(err)
	}
	account := admitDaemonTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: testFileProviderConfigDir(1),
		KeychainService: "service", KeychainAccount: "account",
	})
	server := &Server{
		m:   newDaemonTestManager(t, st, accountMutationTestRefresher{}, credstest.NewFake()),
		cl:  newClaims(),
		log: log.New(io.Discard, "", 0),
	}
	return server, account, st
}

func waitRemovalStage(t *testing.T, stage string, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", stage)
	}
}

func assertRemovalClaimReleased(t *testing.T, server *Server, id int) {
	t.Helper()
	if !server.cl.ownExclusive(id) {
		t.Fatalf("acct-%02d claim remained held across external work", id)
	}
	server.cl.releaseExclusive(id)
}

func assertRemovalFencesSelection(t *testing.T, manager *pool.Manager) {
	t.Helper()
	accounts, err := manager.Store.ListActiveAccounts()
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != 0 {
		t.Fatalf("selection candidates while removal is pending = %+v, want none", accounts)
	}
}

func TestAccountRemovalReleasesClaimBeforeTenantRemoval(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	server, account, st := newAccountRemovalTestServer(t, filepath.Join(home, "pool-v1.db"))
	defer func() { _ = st.Close() }()
	backing := pool.AccountBackingDir(account.ID)
	if err := os.MkdirAll(backing, 0o700); err != nil {
		t.Fatal(err)
	}
	tenantID, err := pool.TenantAccount(account).TenantID()
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	runtime := &blockingRemovalRuntime{
		lifecycleRuntimeStub: lifecycleRuntimeStub{
			state: exactState(mountproto.TenantID(tenantID), account.Generation),
			remove: mountproto.RemoveTenantResponse{
				Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
				TenantID: mountproto.TenantID(tenantID), Generation: account.Generation,
				FileProviderAbsent: true,
			},
		},
		entered: entered,
		release: release,
	}
	server.tenantCoordinator = &tenantCoordinator{server: server, runtime: runtime}
	response := make(chan Response, 1)
	go func() {
		id := account.ID
		response <- server.handleAccountRemove(t.Context(), Request{Account: &id, DeleteCredential: true})
	}()

	waitRemovalStage(t, "tenant removal", entered)
	assertRemovalClaimReleased(t, server, account.ID)
	assertRemovalFencesSelection(t, server.m)
	close(release)

	select {
	case got := <-response:
		if !got.OK || got.Error != "" {
			t.Fatalf("account removal response = %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for account removal")
	}
	assertRemovalClaimReleased(t, server, account.ID)
	if _, err := st.GetAccount(account.ID); !errors.Is(err, store.ErrAccountNotFound) {
		t.Fatalf("account after removal = %v", err)
	}
	if _, err := os.Lstat(backing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("private backing after removal = %v", err)
	}
}

func TestCancelledAccountRemovalReleasesClaimAndRestartResumesIntent(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(home, "pool-v1.db")
	server, account, st := newAccountRemovalTestServer(t, database)
	backing := pool.AccountBackingDir(account.ID)
	if err := os.MkdirAll(backing, 0o700); err != nil {
		t.Fatal(err)
	}
	tenantID, err := pool.TenantAccount(account).TenantID()
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	server.tenantCoordinator = &tenantCoordinator{
		server: server,
		runtime: &blockingRemovalRuntime{
			lifecycleRuntimeStub: lifecycleRuntimeStub{
				state: exactState(mountproto.TenantID(tenantID), account.Generation),
			},
			entered: entered,
			release: make(chan struct{}),
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	response := make(chan Response, 1)
	go func() {
		id := account.ID
		response <- server.handleAccountRemove(ctx, Request{Account: &id, DeleteCredential: true})
	}()
	waitRemovalStage(t, "cancelled tenant removal", entered)
	cancel()
	select {
	case got := <-response:
		if got.OK || got.Error == "" {
			t.Fatalf("cancelled removal response = %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cancelled removal")
	}
	assertRemovalClaimReleased(t, server, account.ID)
	assertRemovalFencesSelection(t, server.m)
	removals := allAccountRemovals(t, st)
	if len(removals) != 1 || removals[0].AccountID != account.ID {
		t.Fatalf("durable removal intents = %+v", removals)
	}
	removal := removals[0]
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := store.Open(database)
	if err != nil {
		t.Fatalf("reopen store after cancelled removal: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	restarted := &Server{
		m:   newDaemonTestManager(t, reopened, accountMutationTestRefresher{}, credstest.NewFake()),
		cl:  newClaims(),
		log: log.New(io.Discard, "", 0),
	}
	runtime := &lifecycleRuntimeStub{
		state: exactState(mountproto.TenantID(tenantID), removal.AccountGeneration),
		remove: mountproto.RemoveTenantResponse{
			Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
			TenantID: mountproto.TenantID(tenantID), Generation: removal.AccountGeneration,
			FileProviderAbsent: true,
		},
	}
	restarted.tenantCoordinator = &tenantCoordinator{
		server: restarted, runtime: runtime, preparer: &sourcePreparerStub{},
	}
	if err := restarted.tenantCoordinator.initialize(t.Context()); err != nil {
		t.Fatalf("resume durable removal after restart: %v", err)
	}
	assertRemovalClaimReleased(t, restarted, account.ID)
	if _, err := reopened.GetAccount(account.ID); !errors.Is(err, store.ErrAccountNotFound) {
		t.Fatalf("account after restarted removal = %v", err)
	}
	removals = allAccountRemovals(t, reopened)
	if len(removals) != 0 {
		t.Fatalf("removal intent survived completed restart recovery: %+v", removals)
	}
	if _, err := os.Lstat(backing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("private backing after restarted removal = %v", err)
	}
}
