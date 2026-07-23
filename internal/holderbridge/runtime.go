package holderbridge

import (
	"context"
	"os"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/holder"
	"github.com/yasyf/fusekit/mountservice"
)

const catalogOperationTimeout = 30 * time.Second

// EmbeddedRuntimeSpec defines cc-pool's complete FuseKit runtime policy.
type EmbeddedRuntimeSpec struct {
	Plan              holder.RuntimePlan
	StopRole          string
	StopControlStore  proc.StopControlStore
	Owner             catalog.SourceAuthorityFleetOwnerID
	Drivers           holder.DriverFactories
	CatalogAuthorizer catalogservice.Authorizer
	Authorizer        mountservice.Authorizer
	ShutdownTimeout   time.Duration
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
		NativeStdout:            os.Stdout,
		NativeStderr:            os.Stderr,
		CatalogOperationTimeout: catalogOperationTimeout,
		ShutdownTimeout:         spec.ShutdownTimeout,
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
