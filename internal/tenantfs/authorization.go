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

// MountAuthorizer rejects every external mount-control request.
type MountAuthorizer struct{}

// NewMountAuthorizer returns the deny-all external mount-control authorizer.
func NewMountAuthorizer() MountAuthorizer { return MountAuthorizer{} }

// AuthorizeObservation rejects external runtime observation.
func (a MountAuthorizer) AuthorizeObservation(
	_ context.Context,
	_ mountservice.ObservationIdentity,
	_ mountproto.Operation,
) error {
	return errUnauthorized
}

// Authorize rejects external tenant lifecycle control.
func (a MountAuthorizer) Authorize(
	_ context.Context,
	_ mountservice.Identity,
	_ mountproto.Operation,
	_ catalog.TenantID,
	_ catalog.Generation,
) (tenant.OwnerID, error) {
	return "", errUnauthorized
}

// AuthorizeNative rejects native presentations for this File Provider-only product.
func (MountAuthorizer) AuthorizeNative(context.Context, mountservice.Identity, mountproto.Operation) error {
	return errUnauthorized
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
	authorization := catalogservice.Authorization{Route: route}
	switch {
	case operation == catalogproto.OperationBrokerOpen && route == (catalogservice.Route{}):
		authorization.Principal = "cc-pool-fileprovider"
		authorization.Role = catalogservice.RoleFileProvider
		authorization.Presentation = catalog.PresentationFileProvider
	case route.Forwarded && route.Tenant != "" && route.Domain != "":
		authorization.Principal = "cc-pool-fileprovider"
		authorization.Role = catalogservice.RoleFileProvider
		authorization.Presentation = catalog.PresentationFileProvider
	default:
		return catalogservice.Authorization{}, errUnauthorized
	}
	return authorization, nil
}

func validCatalogIdentity(uid int, identity catalogservice.Identity) bool {
	return identity.WireBuild == transportproto.WireBuild && identity.Session != nil &&
		identity.Peer.PID > 1 && identity.Peer.UID == uid
}

var (
	_ mountservice.Authorizer   = MountAuthorizer{}
	_ catalogservice.Authorizer = CatalogAuthorizer{}
)
