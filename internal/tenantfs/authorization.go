package tenantfs

import (
	"context"
	"errors"
	"os"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/tenant"
	"github.com/yasyf/fusekit/transportproto"
)

var errUnauthorized = errors.New("tenantfs: unauthorized FuseKit peer")

// MountAuthorizer binds every local lifecycle request to cc-pool's owner.
type MountAuthorizer struct{ UID int }

// NewMountAuthorizer returns the current-user product authorizer.
func NewMountAuthorizer() MountAuthorizer { return MountAuthorizer{UID: os.Getuid()} }

// AuthorizeRuntime admits one exact local runtime-health request.
func (a MountAuthorizer) AuthorizeRuntime(
	_ context.Context,
	identity mountservice.Identity,
	operation mountproto.Operation,
) error {
	if !validMountIdentity(a.UID, identity) || operation != mountproto.OperationRuntimeHealth {
		return errUnauthorized
	}
	return nil
}

// Authorize admits one exact local tenant lifecycle request.
func (a MountAuthorizer) Authorize(
	_ context.Context,
	identity mountservice.Identity,
	operation mountproto.Operation,
	_ catalog.TenantID,
	_ catalog.Generation,
) (tenant.OwnerID, error) {
	if !validMountIdentity(a.UID, identity) || !tenantLifecycleOperation(operation) {
		return "", errUnauthorized
	}
	return tenant.OwnerID(OwnerID), nil
}

// AuthorizeNative admits the signed holder's exact native child session.
func (a MountAuthorizer) AuthorizeNative(_ context.Context, identity mountservice.Identity, operation mountproto.Operation) error {
	if !validMountIdentity(a.UID, identity) || !nativeOperation(operation) {
		return errUnauthorized
	}
	return nil
}

// CatalogAuthorizer maps authenticated product operations to closed FuseKit roles.
type CatalogAuthorizer struct{ UID int }

// NewCatalogAuthorizer returns the current-user product authorizer.
func NewCatalogAuthorizer() CatalogAuthorizer { return CatalogAuthorizer{UID: os.Getuid()} }

// Authorize derives the one role permitted for operation and route.
func (a CatalogAuthorizer) Authorize(
	_ context.Context,
	identity catalogservice.Identity,
	operation catalogproto.Operation,
	route catalogservice.Route,
) (catalogservice.Authorization, error) {
	if !validCatalogIdentity(a.UID, identity) {
		return catalogservice.Authorization{}, errUnauthorized
	}
	return catalogAuthorization(operation, route)
}

func catalogAuthorization(
	operation catalogproto.Operation,
	route catalogservice.Route,
) (catalogservice.Authorization, error) {
	if operation == catalogproto.OperationSourceAuthorityPublishDesiredFleet ||
		operation == catalogproto.OperationSourceAuthorityReadDesiredFleet {
		if route != (catalogservice.Route{}) {
			return catalogservice.Authorization{}, errUnauthorized
		}
		return catalogservice.Authorization{
			Principal: string(SourceAuthorityFleetOwner), Role: catalogservice.RoleProductAdmin,
		}, nil
	}
	authorization := catalogservice.Authorization{Route: route}
	switch {
	case operation == catalogproto.OperationTenantPrepare && route.Tenant != "" && !route.Forwarded && route.Domain == "":
		authorization.Principal = "cc-pool-owner"
		authorization.Role = catalogservice.RoleTenantOwner
	case operation == catalogproto.OperationBrokerOpen && route == (catalogservice.Route{}):
		authorization.Principal = "cc-pool-fileprovider"
		authorization.Role = catalogservice.RoleFileProvider
		authorization.Presentation = catalog.PresentationFileProvider
	case route.Forwarded && route.Tenant != "" && route.Domain != "":
		authorization.Principal = "cc-pool-fileprovider"
		authorization.Role = catalogservice.RoleFileProvider
		authorization.Presentation = catalog.PresentationFileProvider
	case !route.Forwarded && route.Tenant != "" && route.Domain == "":
		authorization.Principal = "cc-pool-mount"
		authorization.Role = catalogservice.RoleMount
		authorization.Presentation = catalog.PresentationMount
	default:
		return catalogservice.Authorization{}, errUnauthorized
	}
	return authorization, nil
}

func validMountIdentity(uid int, identity mountservice.Identity) bool {
	return identity.Build == transportproto.Build && identity.Session != nil &&
		identity.Peer.PID > 1 && identity.Peer.UID == uid
}

func validCatalogIdentity(uid int, identity catalogservice.Identity) bool {
	return identity.Build == transportproto.Build && identity.Session != nil &&
		identity.Peer.PID > 1 && identity.Peer.UID == uid
}

func tenantLifecycleOperation(operation mountproto.Operation) bool {
	return operation == mountproto.OperationTenantProvision || operation == mountproto.OperationTenantReplace ||
		operation == mountproto.OperationTenantRemove || operation == mountproto.OperationTenantState
}

func nativeOperation(operation mountproto.Operation) bool {
	return operation == mountproto.OperationNativeBind || operation == mountproto.OperationNativeReady ||
		operation == mountproto.OperationNativeUnbind || operation == mountproto.OperationNativeRoutePage ||
		operation == mountproto.OperationNativePin || operation == mountproto.OperationNativeRelease ||
		operation == mountproto.OperationNativeSnapshotOpen || operation == mountproto.OperationNativeSnapshotRead ||
		operation == mountproto.OperationNativeSnapshotClose || operation == mountproto.OperationNativeWriteOpen ||
		operation == mountproto.OperationNativeWriteRead || operation == mountproto.OperationNativeWriteWrite ||
		operation == mountproto.OperationNativeWriteTruncate || operation == mountproto.OperationNativeWriteSync ||
		operation == mountproto.OperationNativeWriteCommit || operation == mountproto.OperationNativeWriteAbort
}

var (
	_ mountservice.Authorizer   = MountAuthorizer{}
	_ catalogservice.Authorizer = CatalogAuthorizer{}
)
