package tenantfs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/holder"
)

// Label names cc-pool's tenant lane, served inside the signed app beside
// FuseKit's own daemon. It is a lane of its own because FuseKit's business
// trust admits only the signed File Provider extension and broker, while every
// caller here is the same user's unsigned cc-pool daemon.
const Label daemonkit.Label = "com.yasyf.cc-pool.tenants"

// Schema names the tenant lane's application protocol era.
const Schema daemonkit.Schema = "com.yasyf.cc-pool.tenants/v1"

// concurrency is one in-flight call per caller — the daemon's tenant
// coordinator, its sync worker, and the status app's deployment observer —
// plus 5 headroom for the retry each may have in flight during a holder
// restart.
const concurrency = 3 + 5

// Daemon is the tenant lane's whole identity, read by the signed app that
// serves it and the cc-pool daemon that calls it.
func Daemon() daemonkit.Daemon {
	return daemonkit.Daemon{
		Label:       Label,
		Schemas:     []daemonkit.Schema{Schema},
		Trust:       daemonkit.Trust{Serving: daemonkit.ServingSameUser()},
		Restart:     daemonkit.RestartAlways,
		Concurrency: concurrency,
	}
}

// Product serves the tenant lane over one holder tenant controller.
type Product struct {
	controller *holder.LocalTenantController
}

// NewProduct binds the tenant lane to the signed app's holder controller.
func NewProduct(controller *holder.LocalTenantController) *Product {
	return &Product{controller: controller}
}

// Handle dispatches one tenant operation. Every failure the controller reports
// is in-band, carried by the response's own header code, so the lane's own
// errors name only an unknown operation or a result that will not encode.
func (p *Product) Handle(ctx context.Context, request daemonkit.Request) (daemonkit.Reply, error) {
	handler, known := controlHandlers[request.Op]
	if !known {
		return daemonkit.Reply{}, &daemonkit.ProductError{
			Code:    string(ControlErrorInvalid),
			Message: fmt.Sprintf("tenantfs: unknown cc-pool tenant operation %q", request.Op),
		}
	}
	body, err := json.Marshal(handler(ctx, request, p.controller))
	if err != nil {
		return daemonkit.Reply{}, fmt.Errorf("tenantfs: encode cc-pool tenant result: %w", err)
	}
	return daemonkit.Reply{Body: body}, nil
}

// Drain stops admitting tenant work. The controller's own convergence is the
// holder's to settle, so the lane has nothing of its own to join.
func (*Product) Drain(daemonkit.Budget) error { return nil }

// Close releases the lane. The controller outlives it, owned by the holder.
func (*Product) Close(daemonkit.Budget) error { return nil }

var _ daemonkit.Product = (*Product)(nil)
