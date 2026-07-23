package holderbridge

import (
	"context"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/holder"
	"github.com/yasyf/fusekit/mountservice"
)

const (
	// NativeReadinessTimeout is cc-pool's hard native-presentation startup budget.
	NativeReadinessTimeout = 30 * time.Second
	// SourceReadinessTimeout is cc-pool's hard source-observer startup budget.
	SourceReadinessTimeout = 30 * time.Second
	// CatalogReadinessTimeout is cc-pool's hard catalog-process startup budget.
	CatalogReadinessTimeout = 30 * time.Second
	// CatalogOperationTimeout is cc-pool's hard catalog request budget.
	CatalogOperationTimeout = 30 * time.Second
	// RuntimeShutdownTimeout is cc-pool's hard runtime settlement budget.
	RuntimeShutdownTimeout = 30 * time.Second
)

// EmbeddedRuntimeSpec defines cc-pool's complete FuseKit runtime policy.
type EmbeddedRuntimeSpec struct {
	Plan              holder.RuntimePlan
	StopRole          string
	StopControlStore  proc.StopControlStore
	Owner             catalog.SourceAuthorityFleetOwnerID
	Drivers           holder.DriverFactories
	CatalogAuthorizer catalogservice.Authorizer
	Authorizer        mountservice.Authorizer
}

// NewEmbeddedRuntime constructs the signed app's FuseKit runtime.
func NewEmbeddedRuntime(ctx context.Context, spec EmbeddedRuntimeSpec) (daemon.EmbeddedRuntime, error) {
	return newEmbeddedRuntime(ctx, spec, holder.New)
}

func newEmbeddedRuntime(
	ctx context.Context,
	spec EmbeddedRuntimeSpec,
	construct func(context.Context, holder.Config) (*holder.Runtime, error),
) (daemon.EmbeddedRuntime, error) {
	config := holder.Config{
		Plan: spec.Plan, RuntimeBuild: spec.Plan.BuildID(),
		StopRole: spec.StopRole, StopControlStore: spec.StopControlStore,
		Owner: spec.Owner, Drivers: spec.Drivers,
		CatalogAuthorizer:       spec.CatalogAuthorizer,
		Authorizer:              spec.Authorizer,
		NativeReadinessTimeout:  NativeReadinessTimeout,
		SourceReadinessTimeout:  SourceReadinessTimeout,
		CatalogReadinessTimeout: CatalogReadinessTimeout,
		CatalogOperationTimeout: CatalogOperationTimeout,
		ShutdownTimeout:         RuntimeShutdownTimeout,
	}
	return constructEmbeddedRuntime(ctx, config, construct)
}

func constructEmbeddedRuntime(
	ctx context.Context,
	config holder.Config,
	construct func(context.Context, holder.Config) (*holder.Runtime, error),
) (daemon.EmbeddedRuntime, error) {
	runtime, err := construct(ctx, config)
	if err != nil {
		return nil, err
	}
	return runtime, nil
}
