package daemon

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/holder"
)

type testSessionLeaseManager struct {
	mu sync.Mutex

	commit  func(store.FileProviderLeaseReceipt, int64, store.ProcessIdentity, time.Time) (store.FileProviderLeaseReceipt, error)
	renew   func(store.FileProviderLeaseReceipt, time.Time) (store.FileProviderLeaseReceipt, error)
	release func(store.Session) (store.FileProviderLeaseReceipt, error)
}

func (m *testSessionLeaseManager) ReleaseProvisional(
	context.Context,
	store.FileProviderLeaseReceipt,
) (store.FileProviderLeaseReceipt, error) {
	return store.FileProviderLeaseReceipt("released-provisional"), nil
}

func (m *testSessionLeaseManager) Commit(
	_ context.Context,
	receipt store.FileProviderLeaseReceipt,
	sessionID int64,
	process store.ProcessIdentity,
	expires time.Time,
) (store.FileProviderLeaseReceipt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.commit != nil {
		return m.commit(receipt, sessionID, process, expires)
	}
	return store.FileProviderLeaseReceipt(fmt.Sprintf("committed-file-provider-lease:%d:%x", sessionID, receipt)), nil
}

func (m *testSessionLeaseManager) Renew(
	_ context.Context,
	receipt store.FileProviderLeaseReceipt,
	expires time.Time,
) (store.FileProviderLeaseReceipt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.renew != nil {
		return m.renew(receipt, expires)
	}
	return store.FileProviderLeaseReceipt(fmt.Sprintf("renewed-file-provider-lease:%x:%d", receipt, expires.UnixNano())), nil
}

func (m *testSessionLeaseManager) Release(
	_ context.Context,
	session store.Session,
) (store.FileProviderLeaseReceipt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.release != nil {
		return m.release(session)
	}
	return append(store.FileProviderLeaseReceipt(nil), session.FileProviderLease...), nil
}

type testSessionLeaseRuntime struct {
	commit func(holder.LocalFileProviderLeaseCommit) catalogproto.FileProviderLeaseReceipt
}

func (r testSessionLeaseRuntime) CommitFileProviderLease(
	_ context.Context,
	request holder.LocalFileProviderLeaseCommit,
) (catalogproto.FileProviderLeaseReceipt, error) {
	if r.commit != nil {
		return r.commit(request), nil
	}
	committed := request.Lease
	committed.State = catalogproto.FileProviderLeaseStateCommitted
	committed.SessionID = request.SessionID
	committed.ProcessIdentity = request.ProcessIdentity
	committed.ExpiresUnixNano = uint64(request.ExpiresAt.UnixNano())
	return committed, nil
}

func (testSessionLeaseRuntime) RenewFileProviderLease(
	_ context.Context,
	request holder.LocalFileProviderLeaseRenew,
) (catalogproto.FileProviderLeaseReceipt, error) {
	renewed := request.Lease
	renewed.ExpiresUnixNano = uint64(request.ExpiresAt.UnixNano())
	return renewed, nil
}

func (testSessionLeaseRuntime) ReleaseFileProviderLease(
	_ context.Context,
	request catalogproto.FileProviderLeaseReceipt,
) (catalogproto.FileProviderLeaseReceipt, error) {
	released := request
	released.State = catalogproto.FileProviderLeaseStateReleased
	return released, nil
}

func TestCatalogSessionLeaseManagerExactLifecycle(t *testing.T) {
	provisional := testCatalogLease(t)
	raw, err := encodeSessionLease(provisional)
	if err != nil {
		t.Fatal(err)
	}
	manager := catalogSessionLeaseManager{runtime: testSessionLeaseRuntime{}}
	process := store.ProcessIdentity{PID: 4242, StartedAt: time.Unix(1_700_000_000, 123_000).UTC()}
	expires := time.Unix(1_800_000_000, 456).UTC()
	committedRaw, err := manager.Commit(t.Context(), raw, 17, process, expires)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := decodeSessionLease(committedRaw)
	if err != nil {
		t.Fatal(err)
	}
	if committed.State != catalogproto.FileProviderLeaseStateCommitted || committed.SessionID != "17" ||
		committed.ProcessIdentity != sessionProcessIdentity(process) ||
		committed.ExpiresUnixNano != uint64(expires.UnixNano()) {
		t.Fatalf("committed receipt = %+v", committed)
	}
	renewedExpiry := expires.Add(time.Minute)
	renewedRaw, err := manager.Renew(t.Context(), committedRaw, renewedExpiry)
	if err != nil {
		t.Fatal(err)
	}
	renewed, err := decodeSessionLease(renewedRaw)
	if err != nil || renewed.ExpiresUnixNano != uint64(renewedExpiry.UnixNano()) {
		t.Fatalf("renewed receipt = %+v, %v", renewed, err)
	}
	releasedRaw, err := manager.Release(t.Context(), store.Session{
		ID: 17, PID: process.PID, ProcessStartedAt: process.StartedAt,
		LeaseState: store.SessionLeaseActive, FileProviderLease: renewedRaw, LeaseExpiresAt: renewedExpiry,
	})
	if err != nil {
		t.Fatal(err)
	}
	released, err := decodeSessionLease(releasedRaw)
	if err != nil || released.State != catalogproto.FileProviderLeaseStateReleased ||
		released.SessionID != committed.SessionID || released.ProcessIdentity != committed.ProcessIdentity {
		t.Fatalf("released receipt = %+v, %v", released, err)
	}
}

func TestCatalogSessionLeaseManagerRejectsChangedResponseAndReleasesPending(t *testing.T) {
	provisional := testCatalogLease(t)
	raw, err := encodeSessionLease(provisional)
	if err != nil {
		t.Fatal(err)
	}
	changed := catalogSessionLeaseManager{runtime: testSessionLeaseRuntime{commit: func(
		request holder.LocalFileProviderLeaseCommit,
	) catalogproto.FileProviderLeaseReceipt {
		request.Lease.DomainID += "-changed"
		request.Lease.State = catalogproto.FileProviderLeaseStateCommitted
		request.Lease.SessionID = request.SessionID
		request.Lease.ProcessIdentity = request.ProcessIdentity
		request.Lease.ExpiresUnixNano = uint64(request.ExpiresAt.UnixNano())
		return request.Lease
	}}}
	process := store.ProcessIdentity{PID: 42, StartedAt: time.Unix(1_700_000_000, 0).UTC()}
	if _, err := changed.Commit(t.Context(), raw, 7, process, time.Unix(1_800_000_000, 0)); err == nil {
		t.Fatal("changed commit response was accepted")
	}
	manager := catalogSessionLeaseManager{runtime: testSessionLeaseRuntime{}}
	releasedRaw, err := manager.Release(t.Context(), store.Session{
		ID: 7, PID: process.PID, ProcessStartedAt: process.StartedAt,
		LeaseState: store.SessionLeasePending, FileProviderLease: raw,
		LeaseExpiresAt: time.Unix(1_800_000_000, 0).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	released, err := decodeSessionLease(releasedRaw)
	if err != nil || released.State != catalogproto.FileProviderLeaseStateReleased ||
		released.SessionID != "7" || released.ProcessIdentity != sessionProcessIdentity(process) {
		t.Fatalf("released pending receipt = %+v, %v", released, err)
	}
}

func testCatalogLease(t *testing.T) catalogproto.FileProviderLeaseReceipt {
	t.Helper()
	instance := catalogproto.PresentationInstanceID("0123456789abcdef0123456789abcdef")
	tenantID, err := (tenantfs.Account{InstanceID: string(instance)}).TenantID()
	if err != nil {
		t.Fatal(err)
	}
	domainID, err := catalogproto.DeriveDomainID(tenantfs.OwnerID, instance)
	if err != nil {
		t.Fatal(err)
	}
	return catalogproto.FileProviderLeaseReceipt{
		LeaseID: "abababababababababababababababab", TenantID: catalogproto.TenantID(tenantID), DomainID: domainID,
		Generation: 1, RootID: "11111111111111111111111111111111", PresentationInstanceID: instance,
		State:            catalogproto.FileProviderLeaseStateProvisional,
		PolicyDigest:     "2222222222222222222222222222222222222222222222222222222222222222",
		ResolutionDigest: "3333333333333333333333333333333333333333333333333333333333333333",
		CatalogHead:      1, SourceAuthority: catalogproto.SourceAuthorityID(tenantfs.ClaudeAuthorityID),
		SourcePublication: "44444444444444444444444444444444", SourceRevision: 1,
		ActivationGeneration: "activation-test", ExpiresUnixNano: uint64(time.Unix(1_800_000_000, 0).UnixNano()),
	}
}
