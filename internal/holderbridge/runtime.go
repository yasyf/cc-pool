package holderbridge

import (
	"context"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/holder"
	"github.com/yasyf/fusekit/mountservice"
)

const (
	// NativeReadinessTimeout is cc-pool's hard native-presentation startup budget.
	NativeReadinessTimeout = 30 * time.Second
	// CatalogReadinessTimeout is cc-pool's hard catalog-process startup budget.
	CatalogReadinessTimeout = 30 * time.Second
	// CatalogOperationTimeout is cc-pool's hard catalog request budget.
	CatalogOperationTimeout = 30 * time.Second
	// RuntimeShutdownTimeout is cc-pool's hard runtime settlement budget.
	RuntimeShutdownTimeout = 30 * time.Second
)

// RuntimeSpec defines cc-pool's complete FuseKit runtime policy.
type RuntimeSpec struct {
	Plan              holder.RuntimePlan
	TrustRequirements holder.RuntimeTrustRequirements
	StopControlStore  *proc.FileStore
	Owner             catalog.SourceAuthorityFleetOwnerID
	Drivers           holder.DriverFactories
	CatalogAuthorizer catalogservice.Authorizer
	Authorizer        mountservice.Authorizer
	BusinessHandlers  []holder.BusinessHandlerSpec
}

// NewRuntime constructs the signed app's FuseKit runtime.
func NewRuntime(ctx context.Context, spec RuntimeSpec) (*holder.Runtime, error) {
	return newRuntime(ctx, spec, holder.New)
}

func newRuntime(
	ctx context.Context,
	spec RuntimeSpec,
	construct func(context.Context, holder.Config) (*holder.Runtime, error),
) (*holder.Runtime, error) {
	config := holder.Config{
		Plan: spec.Plan, RuntimeBuild: spec.Plan.BuildID(),
		TrustRequirements: spec.TrustRequirements, StopControlStore: spec.StopControlStore,
		Owner: spec.Owner, Drivers: spec.Drivers,
		CatalogAuthorizer:       spec.CatalogAuthorizer,
		Authorizer:              spec.Authorizer,
		BusinessHandlers:        spec.BusinessHandlers,
		NativeReadinessTimeout:  NativeReadinessTimeout,
		CatalogReadinessTimeout: CatalogReadinessTimeout,
		CatalogOperationTimeout: CatalogOperationTimeout,
		ShutdownTimeout:         RuntimeShutdownTimeout,
	}
	return construct(ctx, config)
}
