// Package holderbridge binds cc-pool's signed application to FuseKit.
package holderbridge

import "github.com/yasyf/fusekit/holder"

const (
	// BundleID is the fixed signed status helper application's bundle identifier.
	BundleID = "com.yasyf.cc-pool.status"
	// TeamID is the fixed signing team for every protected helper role.
	TeamID = "SXKCTF23Q2"
	// ExecutableName is the embedded status helper and broker executable.
	ExecutableName = "CCPoolStatus"
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
	return holder.RuntimePlanSpec{
		Application: Application(appPath), RuntimeDirectory: runtimeDirectory,
		BuildID: buildID, Readiness: ReadinessContract(), SourceCapable: true,
	}
}

// DeploymentPlanSpec returns the daemon-facing contract for one exact app generation.
func DeploymentPlanSpec(appPath, runtimeDirectory, buildID string) holder.DeploymentPlanSpec {
	return holder.DeploymentPlanSpec{
		Application: Application(appPath), RuntimeDirectory: runtimeDirectory,
		BuildID: buildID, Readiness: ReadinessContract(), SourceCapable: true,
	}
}

// NewDeploymentPlan derives the exact daemon-facing signed helper plan.
func NewDeploymentPlan(appPath, runtimeDirectory, buildID string) (holder.DeploymentPlan, error) {
	return holder.NewDeploymentPlan(DeploymentPlanSpec(appPath, runtimeDirectory, buildID))
}

// NewRuntimePlan derives the File Provider-only signed helper plan.
func NewRuntimePlan(
	appPath, runtimeDirectory, buildID string,
) (holder.RuntimePlan, error) {
	plan, err := holder.NewRuntimePlan(RuntimePlanSpec(
		appPath, runtimeDirectory, buildID,
	))
	if err != nil {
		return holder.RuntimePlan{}, err
	}
	return plan, nil
}
