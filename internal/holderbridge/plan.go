// Package holderbridge binds cc-pool's signed application to FuseKit.
package holderbridge

import (
	"fmt"

	"github.com/yasyf/fusekit/holder"
)

const (
	// BundleID is the fixed signed status helper application's bundle identifier.
	BundleID = "com.yasyf.cc-pool.status"
	// StopRoleID is the controller-launched one-shot signed runtime settlement role.
	StopRoleID = "com.yasyf.cc-pool.status.fusekit.stop-control"
	// TeamID is the fixed signing team for every protected helper role.
	TeamID = "SXKCTF23Q2"
	// ExecutableName is the embedded status helper and broker executable.
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

// ReadinessContract returns cc-pool's one signed-runtime and service-observer budget.
func ReadinessContract() holder.ReadinessContract { return holder.StandardReadinessContract() }

// RuntimePlanSpec returns the concrete signed-side cc-pool helper contract.
func RuntimePlanSpec(
	appPath, runtimeDirectory, buildID string,
) holder.RuntimePlanSpec {
	policy := holder.EntitlementPolicy{RequiredAppGroup: AppGroup}
	return holder.RuntimePlanSpec{
		Application: Application(appPath), RuntimeDirectory: runtimeDirectory,
		BuildID: buildID, Readiness: ReadinessContract(), SourceCapable: true,
		BrokerPolicy: policy, RuntimePolicy: policy,
	}
}

// NewRuntimePlan derives the File Provider-only signed helper plan.
func NewRuntimePlan(appPath, runtimeDirectory, buildID string) (holder.RuntimePlan, error) {
	plan, err := holder.NewRuntimePlan(RuntimePlanSpec(appPath, runtimeDirectory, buildID))
	if err != nil {
		return holder.RuntimePlan{}, fmt.Errorf("holderbridge: create runtime plan: %w", err)
	}
	return plan, nil
}
