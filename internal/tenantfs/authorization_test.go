package tenantfs

import (
	"context"
	"testing"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
)

func TestExternalMountControlIsExhaustivelyRejected(t *testing.T) {
	authorizer := MountAuthorizer{}
	lifecycle := mountservice.Identity{Caller: daemonkit.Caller{PID: 7, UID: 42}}
	if _, err := authorizer.Authorize(
		context.Background(), lifecycle, mountproto.OperationTenantState, "tenant", 1,
	); err == nil {
		t.Fatal("tenant lifecycle was externally authorized")
	}
}

func TestNativeOperationsAreExhaustivelyRejected(t *testing.T) {
	authorizer := MountAuthorizer{}
	identity := mountservice.Identity{Caller: daemonkit.Caller{PID: 7, UID: 42}}
	operations := []mountproto.Operation{
		mountproto.OperationNativeBind,
		mountproto.OperationNativeMounted,
		mountproto.OperationNativeReady,
		mountproto.OperationNativeUnbind,
		mountproto.OperationNativeRoutePage,
		mountproto.OperationNativePin,
		mountproto.OperationNativeRelease,
		mountproto.OperationNativeSnapshotOpen,
		mountproto.OperationNativeSnapshotRead,
		mountproto.OperationNativeSnapshotClose,
		mountproto.OperationNativeWriteOpen,
		mountproto.OperationNativeWriteRead,
		mountproto.OperationNativeWriteWrite,
		mountproto.OperationNativeWriteTruncate,
		mountproto.OperationNativeWriteSync,
		mountproto.OperationNativeWriteCommit,
		mountproto.OperationNativeWriteAbort,
	}
	for _, operation := range operations {
		if err := authorizer.AuthorizeNative(t.Context(), identity, operation); err == nil {
			t.Errorf("native operation %q was authorized", operation)
		}
	}
}

func TestUnforwardedTenantCatalogRequestsAreRejected(t *testing.T) {
	if _, err := catalogAuthorization(
		catalogproto.OperationCatalogLookup,
		catalogservice.Route{Tenant: "tenant"},
	); err == nil {
		t.Fatal("unforwarded tenant catalog request was authorized")
	}
}

func TestProductAdminCatalogOperationsAreExternallyRejected(t *testing.T) {
	for _, operation := range []catalogproto.Operation{
		catalogproto.OperationTenantPrepare,
		catalogproto.OperationPresentationLeaseCommit,
		catalogproto.OperationPresentationLeaseRenew,
		catalogproto.OperationPresentationLeaseRelease,
		catalogproto.OperationSourceAuthorityPublishDesiredFleet,
		catalogproto.OperationSourceAuthorityReadDesiredFleet,
	} {
		for _, route := range []catalogservice.Route{{}, {Tenant: "tenant"}, {Tenant: "tenant", Domain: "domain"}} {
			if _, err := catalogAuthorization(operation, route); err == nil {
				t.Fatalf("product admin operation %q accepted route %+v", operation, route)
			}
		}
	}
}
