package tenantfs

import (
	"context"
	"testing"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/transportproto"
)

func TestRuntimeHealthObservationUsesImmutablePeerIdentity(t *testing.T) {
	authorizer := MountAuthorizer{UID: 42}
	identity := mountservice.ObservationIdentity{
		Peer: wire.Peer{PID: 7, UID: 42}, WireBuild: transportproto.WireBuild,
	}
	if err := authorizer.AuthorizeObservation(t.Context(), identity, mountproto.OperationRuntimeHealth); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []mountservice.ObservationIdentity{
		{Peer: wire.Peer{PID: 1, UID: 42}, WireBuild: transportproto.WireBuild},
		{Peer: wire.Peer{PID: 7, UID: 43}, WireBuild: transportproto.WireBuild},
		{Peer: wire.Peer{PID: 7, UID: 42}, WireBuild: "wrong"},
	} {
		if err := authorizer.AuthorizeObservation(context.Background(), invalid, mountproto.OperationRuntimeHealth); err == nil {
			t.Fatalf("authorized invalid observation identity %+v", invalid)
		}
	}
}

func TestNativeOperationsAreExhaustivelyRejected(t *testing.T) {
	authorizer := MountAuthorizer{UID: 42}
	identity := mountservice.Identity{
		Peer: wire.Peer{PID: 7, UID: 42}, WireBuild: transportproto.WireBuild,
		Session: &wire.AcceptedSession{},
	}
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

func TestFileProviderLeaseOperationsRequireExactTenantOwnerRoute(t *testing.T) {
	for _, operation := range []catalogproto.Operation{
		catalogproto.OperationPresentationLeaseCommit,
		catalogproto.OperationPresentationLeaseRenew,
		catalogproto.OperationPresentationLeaseRelease,
	} {
		authorization, err := catalogAuthorization(operation, catalogservice.Route{Tenant: "tenant"})
		if err != nil {
			t.Fatal(err)
		}
		if authorization.Role != catalogservice.RoleTenantOwner || authorization.Principal != "cc-pool-owner" {
			t.Fatalf("lease authorization = %+v", authorization)
		}
		for _, route := range []catalogservice.Route{
			{},
			{Tenant: "tenant", Domain: "domain"},
			{Tenant: "tenant", Forwarded: true},
		} {
			if _, err := catalogAuthorization(operation, route); err == nil {
				t.Fatalf("lease operation %q accepted route %+v", operation, route)
			}
		}
	}
}

func TestSourceFleetOperationsRequireExactUnroutedProductAdminRole(t *testing.T) {
	for _, operation := range []catalogproto.Operation{
		catalogproto.OperationSourceAuthorityPublishDesiredFleet,
		catalogproto.OperationSourceAuthorityReadDesiredFleet,
	} {
		authorization, err := catalogAuthorization(operation, catalogservice.Route{})
		if err != nil {
			t.Fatal(err)
		}
		if authorization.Principal != string(SourceAuthorityFleetOwner) ||
			authorization.Role != catalogservice.RoleProductAdmin ||
			authorization.Route != (catalogservice.Route{}) {
			t.Fatalf("source fleet authorization = %+v", authorization)
		}
		if _, err := catalogAuthorization(operation, catalogservice.Route{Tenant: "foreign"}); err == nil {
			t.Fatal("routed source fleet operation was authorized")
		}
	}
}
