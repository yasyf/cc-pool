package tenantfs

import (
	"testing"

	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/mountproto"
)

func TestNativeOperationsAreExhaustivelyAuthorized(t *testing.T) {
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
		if !nativeOperation(operation) {
			t.Errorf("native operation %q was rejected", operation)
		}
	}

	for _, operation := range []mountproto.Operation{
		mountproto.OperationRuntimeHealth,
		mountproto.OperationTenantProvision,
		mountproto.OperationTenantReplace,
		mountproto.OperationTenantRemove,
		mountproto.OperationTenantState,
		"unknown",
	} {
		if nativeOperation(operation) {
			t.Errorf("non-native operation %q was authorized", operation)
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
