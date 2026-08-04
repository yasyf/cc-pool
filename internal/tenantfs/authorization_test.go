package tenantfs

import (
	"context"
	"errors"
	"testing"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
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

func testAccountRoute(t *testing.T) (catalogservice.Route, catalogservice.Authorization) {
	t.Helper()
	tenantID, err := Account{InstanceID: testInstanceID}.TenantID()
	if err != nil {
		t.Fatalf("TenantID: %v", err)
	}
	domain, err := catalogproto.DeriveDomainID(OwnerID, catalogproto.PresentationInstanceID(testInstanceID))
	if err != nil {
		t.Fatalf("DeriveDomainID: %v", err)
	}
	route := catalogservice.Route{Tenant: tenantID, Generation: 3}
	return route, catalogservice.Authorization{
		Principal:    "cc-pool-fileprovider",
		Role:         catalogservice.RoleFileProvider,
		Presentation: catalog.PresentationFileProvider,
		Route: catalogservice.Route{
			Tenant: route.Tenant, Generation: route.Generation, Domain: domain, Forwarded: true,
		},
	}
}

// Product admin and tenant owner operations are refused one layer up, by
// FuseKit's own role check: this authorizer issues RoleFileProvider whatever
// the operation, and RoleFileProvider admits no admin operation.
func TestCatalogAuthorizationGrantsOnlyTheFileProviderRole(t *testing.T) {
	route, want := testAccountRoute(t)
	for _, operation := range []catalogproto.Operation{
		catalogproto.OperationCatalogLookupPrivate,
		catalogproto.OperationTenantPrepare,
		catalogproto.OperationPresentationLeaseCommit,
		catalogproto.OperationPresentationLeaseRenew,
		catalogproto.OperationPresentationLeaseRelease,
		catalogproto.OperationSourceAuthorityPublishDesiredFleet,
		catalogproto.OperationSourceAuthorityReadDesiredFleet,
	} {
		t.Run(string(operation), func(t *testing.T) {
			authorization, err := catalogAuthorization(operation, route)
			if err != nil {
				t.Fatalf("catalogAuthorization: %v", err)
			}
			if authorization != want {
				t.Fatalf("catalogAuthorization = %+v, want %+v", authorization, want)
			}
		})
	}
}

func TestCatalogAuthorizationDerivesTheDomainFromTheTenant(t *testing.T) {
	route, want := testAccountRoute(t)
	route.Domain = "com.yasyf.cc-pool.chosen-by-the-caller"
	authorization, err := catalogAuthorization(catalogproto.OperationCatalogLookupPrivate, route)
	if err != nil {
		t.Fatalf("catalogAuthorization: %v", err)
	}
	if authorization != want {
		t.Fatalf("catalogAuthorization = %+v, want the tenant-derived route %+v", authorization, want)
	}
}

func TestCatalogAuthorizationRequiresAGenerationFencedAccountTenant(t *testing.T) {
	route, _ := testAccountRoute(t)
	foreign, err := catalog.NewTenantID("tenant")
	if err != nil {
		t.Fatalf("NewTenantID: %v", err)
	}
	malformed, err := catalog.NewTenantID(accountTenantPrefix + "beef")
	if err != nil {
		t.Fatalf("NewTenantID: %v", err)
	}
	tests := []struct {
		name  string
		route catalogservice.Route
	}{
		{"no tenant", catalogservice.Route{Generation: route.Generation}},
		{"no generation", catalogservice.Route{Tenant: route.Tenant}},
		{"tenant outside the account namespace", catalogservice.Route{Tenant: foreign, Generation: route.Generation}},
		{"account tenant with a malformed instance", catalogservice.Route{Tenant: malformed, Generation: route.Generation}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := catalogAuthorization(catalogproto.OperationCatalogLookupPrivate, tt.route)
			if !errors.Is(err, errUnauthorized) {
				t.Fatalf("catalogAuthorization(%+v) = %v, want %v", tt.route, err, errUnauthorized)
			}
		})
	}
}

func TestBrokerCatalogOperationsRequireAnUntenantedRoute(t *testing.T) {
	route, _ := testAccountRoute(t)
	for _, operation := range []catalogproto.Operation{
		catalogproto.OperationBrokerPoll,
		catalogproto.OperationBrokerResult,
	} {
		t.Run(string(operation), func(t *testing.T) {
			authorization, err := catalogAuthorization(operation, catalogservice.Route{})
			if err != nil {
				t.Fatalf("catalogAuthorization: %v", err)
			}
			want := catalogservice.Authorization{
				Principal:    "cc-pool-fileprovider",
				Role:         catalogservice.RoleFileProvider,
				Presentation: catalog.PresentationFileProvider,
			}
			if authorization != want {
				t.Fatalf("catalogAuthorization = %+v, want the untenanted broker session %+v", authorization, want)
			}
			if _, err := catalogAuthorization(operation, route); !errors.Is(err, errUnauthorized) {
				t.Fatalf("broker operation accepted tenant route %+v: %v", route, err)
			}
		})
	}
}
