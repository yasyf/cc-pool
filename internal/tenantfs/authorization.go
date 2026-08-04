package tenantfs

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/tenant"
)

var errUnauthorized = errors.New("tenantfs: unauthorized FuseKit peer")

// MountAuthorizer rejects every external mount-control request.
type MountAuthorizer struct{}

// NewMountAuthorizer returns the deny-all external mount-control authorizer.
func NewMountAuthorizer() MountAuthorizer { return MountAuthorizer{} }

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

// catalogAuthorization admits the File Provider role and nothing else. The
// broker's forward wrapper died with v0.20, so a tenant request's domain
// binding is asserted here: cc-pool's domain is a pure function of the tenant,
// which leaves no session binding to track.
func catalogAuthorization(
	operation catalogproto.Operation,
	route catalogservice.Route,
) (catalogservice.Authorization, error) {
	authorization := catalogservice.Authorization{
		Principal:    "cc-pool-fileprovider",
		Role:         catalogservice.RoleFileProvider,
		Presentation: catalog.PresentationFileProvider,
		Route:        route,
	}
	if operation == catalogproto.OperationBrokerPoll || operation == catalogproto.OperationBrokerResult {
		if route != (catalogservice.Route{}) {
			return catalogservice.Authorization{}, errUnauthorized
		}
		return authorization, nil
	}
	if route.Tenant == "" || route.Generation == 0 {
		return catalogservice.Authorization{}, errUnauthorized
	}
	domain, err := boundDomain(route.Tenant)
	if err != nil {
		return catalogservice.Authorization{}, err
	}
	authorization.Route.Domain = domain
	authorization.Route.Forwarded = true
	return authorization, nil
}

func boundDomain(id catalog.TenantID) (catalogproto.DomainID, error) {
	instance, found := strings.CutPrefix(string(id), accountTenantPrefix)
	if !found || !validAccountInstanceID(instance) {
		return "", errUnauthorized
	}
	return catalogproto.DeriveDomainID(OwnerID, catalogproto.PresentationInstanceID(instance))
}

func validCatalogIdentity(uid int, identity catalogservice.Identity) bool {
	return identity.Caller.PID > 1 && identity.Caller.UID == uint32(uid)
}

var (
	_ mountservice.Authorizer   = MountAuthorizer{}
	_ catalogservice.Authorizer = CatalogAuthorizer{}
)
