// Package holderbridge binds cc-pool's signed application to FuseKit.
package holderbridge

import (
	"context"
	"fmt"

	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/fusekit/fuset"
	"github.com/yasyf/fusekit/holder"
)

const (
	// BundleID is the fixed signed holder application's bundle identifier.
	BundleID = "com.yasyf.cc-pool.status"
	// TeamID is the fixed signing team for every protected holder role.
	TeamID = "SXKCTF23Q2"
	// ExecutableName is the one embedded holder and broker executable.
	ExecutableName = "CCPoolStatus"
	// AppGroup is the one protected broker transport container.
	AppGroup = TeamID + ".ccp"
)

// Application returns cc-pool's exact fixed signed application identity.
func Application(appPath string) holder.SignedApplication {
	executable := holder.SignedExecutable{
		ExecutableName:    ExecutableName,
		SigningIdentifier: BundleID,
	}
	return holder.SignedApplication{
		AppPath: appPath, BundleID: BundleID, TeamID: TeamID,
		Broker: executable, Runtime: executable,
	}
}

// RuntimePlanSpec returns the concrete signed-side cc-pool holder contract.
func RuntimePlanSpec(
	appPath, runtimeDirectory, buildID string,
	verifier *holder.FUSEVerifier,
) holder.RuntimePlanSpec {
	policy := holder.EntitlementPolicy{RequiredAppGroup: AppGroup}
	return holder.RuntimePlanSpec{
		Application: Application(appPath), RuntimeDirectory: runtimeDirectory,
		BuildID: buildID, SourceCapable: true,
		BrokerPolicy: policy, RuntimePolicy: policy, FUSEVerifier: verifier,
	}
}

// PackageFUSE delegates the reviewed FUSE-T bundle transaction to FuseKit.
func PackageFUSE(
	ctx context.Context,
	runner supervise.TaskRunner,
	signingIdentity, appPath string,
) error {
	packager, err := holder.NewFUSEPackager(runner, signingIdentity)
	if err != nil {
		return fmt.Errorf("holderbridge: create FUSE packager: %w", err)
	}
	_, err = packager.Package(ctx, Application(appPath), fuset.CaskDylib)
	if err != nil {
		return fmt.Errorf("holderbridge: package FUSE bundle: %w", err)
	}
	return nil
}

// NewRuntimePlan verifies FuseKit's installed bundle before deriving the holder plan.
func NewRuntimePlan(
	runner supervise.TaskRunner,
	appPath, runtimeDirectory, buildID string,
) (holder.RuntimePlan, error) {
	verifier, err := holder.NewFUSEVerifier(runner)
	if err != nil {
		return holder.RuntimePlan{}, fmt.Errorf("holderbridge: create FUSE verifier: %w", err)
	}
	plan, err := holder.NewRuntimePlan(RuntimePlanSpec(appPath, runtimeDirectory, buildID, verifier))
	if err != nil {
		return holder.RuntimePlan{}, fmt.Errorf("holderbridge: verify FUSE runtime plan: %w", err)
	}
	return plan, nil
}
