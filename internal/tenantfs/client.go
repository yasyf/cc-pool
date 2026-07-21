package tenantfs

import (
	"context"
	"errors"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/transportproto"
)

// PreparationRuntime converges one exact committed tenant source revision.
type PreparationRuntime interface {
	PrepareTenant(context.Context, catalogproto.TenantID, catalogproto.PrepareTenantRequest) (catalogproto.PrepareTenantResponse, error)
}

// Client owns one persistent exact-suite session for cc-pool's FuseKit operations.
type Client struct {
	wire    *wire.Client
	mount   *mountservice.Client
	catalog *catalogservice.Client
}

// NewClient opens one persistent exact-suite FuseKit session.
func NewClient(ctx context.Context, socket string) (*Client, error) {
	if socket == "" {
		return nil, errors.New("tenantfs: FuseKit socket is empty")
	}
	session, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial: wire.UnixDialer(socket), Build: transportproto.Build,
	})
	if err != nil {
		return nil, err
	}
	mountClient, err := mountservice.NewClientOn(session)
	if err != nil {
		_ = session.Close()
		return nil, err
	}
	catalogClient, err := catalogservice.NewClientOn(session)
	if err != nil {
		_ = session.Close()
		return nil, err
	}
	return &Client{wire: session, mount: mountClient, catalog: catalogClient}, nil
}

// Close settles and closes the shared persistent session.
func (c *Client) Close() error { return c.wire.Close() }

// RuntimeHealth returns the holder's exact activation and native presentation state.
func (c *Client) RuntimeHealth(ctx context.Context) (mountproto.RuntimeHealthResponse, error) {
	return c.mount.RuntimeHealth(ctx)
}

// ProvisionTenant durably provisions one account before publishing its source state.
func (c *Client) ProvisionTenant(ctx context.Context, account Account) (mountproto.ProvisionTenantResponse, error) {
	id, err := account.TenantID()
	if err != nil {
		return mountproto.ProvisionTenantResponse{}, err
	}
	definition, err := account.Definition()
	if err != nil {
		return mountproto.ProvisionTenantResponse{}, err
	}
	return c.mount.ProvisionTenant(ctx, id, definition)
}

// ReplaceTenant generation-fences one durable account definition replacement.
func (c *Client) ReplaceTenant(ctx context.Context, account Account, expected uint64) (mountproto.ReplaceTenantResponse, error) {
	id, err := account.TenantID()
	if err != nil {
		return mountproto.ReplaceTenantResponse{}, err
	}
	definition, err := account.Definition()
	if err != nil {
		return mountproto.ReplaceTenantResponse{}, err
	}
	return c.mount.ReplaceTenant(ctx, id, catalog.Generation(expected), definition)
}

// RemoveTenant generation-fences one durable account removal.
func (c *Client) RemoveTenant(ctx context.Context, account Account, expected uint64) (mountproto.RemoveTenantResponse, error) {
	id, err := account.TenantID()
	if err != nil {
		return mountproto.RemoveTenantResponse{}, err
	}
	return c.mount.RemoveTenant(ctx, id, catalog.Generation(expected))
}

// TenantState returns the authenticated owner's exact durable tenant state.
func (c *Client) TenantState(ctx context.Context, account Account) (mountproto.StateResponse, error) {
	id, err := account.TenantID()
	if err != nil {
		return mountproto.StateResponse{}, err
	}
	return c.mount.State(ctx, id)
}

// PrepareTenant converges one exact committed tenant source revision.
func (c *Client) PrepareTenant(
	ctx context.Context,
	tenant catalogproto.TenantID,
	request catalogproto.PrepareTenantRequest,
) (catalogproto.PrepareTenantResponse, error) {
	return c.catalog.PrepareTenant(ctx, tenant, request)
}
