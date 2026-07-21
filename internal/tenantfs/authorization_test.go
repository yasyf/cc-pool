package tenantfs

import (
	"testing"

	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
)

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
