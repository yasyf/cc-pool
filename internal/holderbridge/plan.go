// Package holderbridge binds cc-pool's signed application to FuseKit.
package holderbridge

import (
	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/holder"
)

const (
	// BundleID is the fixed signed status helper application's bundle identifier.
	BundleID = "com.yasyf.cc-pool.status"
	// TeamID is the fixed signing team for every protected helper role.
	TeamID = "SXKCTF23Q2"
	// ExecutableName is the embedded status helper and broker executable.
	ExecutableName = "CCPoolStatus"
)

const fileProviderBundleID = "com.yasyf.cc-pool.status.fileprovider"

// Control's only peer is the Homebrew CLI; the app drives its runtime in process and opens no wire
// session, so pinning the app's identity here rejects the lane the requirement exists to protect.
const controllerBundleID = "com.yasyf.cc-pool"

const runtimePolicyDigest daemonkit.PolicyDigest = "cca1c08f02957912b419b602a6c4b943b8ef9d65906c7b5f3d38d29d2633b838"

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

// RuntimeTrust returns the signed peers admitted to FuseKit's two trust lanes.
func RuntimeTrust(requiredAppGroup string) holder.RuntimeTrust {
	return holder.RuntimeTrust{
		Controller: daemonkit.Requirement{TeamID: TeamID, SigningIdentifier: controllerBundleID},
		FileProviderExtension: daemonkit.Requirement{
			TeamID: TeamID, SigningIdentifier: fileProviderBundleID, RequiredAppGroup: requiredAppGroup,
		},
	}
}

// RuntimePlanSpec returns the concrete signed-side cc-pool helper contract.
func RuntimePlanSpec(
	appPath, runtimeDirectory, buildID, requiredAppGroup string,
) holder.RuntimePlanSpec {
	policy := holder.EntitlementPolicy{RequiredAppGroup: requiredAppGroup}
	return holder.RuntimePlanSpec{
		Application: Application(appPath), RuntimeDirectory: runtimeDirectory,
		BuildID: buildID, Readiness: ReadinessContract(), SourceCapable: true,
		BrokerPolicy: policy, RuntimePolicy: policy,
	}
}

// DeploymentPlanSpec returns the daemon-facing contract for one exact app generation.
func DeploymentPlanSpec(appPath, runtimeDirectory, buildID string) holder.DeploymentPlanSpec {
	return holder.DeploymentPlanSpec{
		Application: Application(appPath), RuntimeDirectory: runtimeDirectory,
		BuildID: buildID, Readiness: ReadinessContract(), SourceCapable: true,
		BrokerPolicyDigest: runtimePolicyDigest, RuntimePolicyDigest: runtimePolicyDigest,
	}
}

// NewDeploymentPlan derives the exact daemon-facing signed helper plan.
func NewDeploymentPlan(appPath, runtimeDirectory, buildID string) (holder.DeploymentPlan, error) {
	return holder.NewDeploymentPlan(DeploymentPlanSpec(appPath, runtimeDirectory, buildID))
}

// NewRuntimePlan derives the File Provider-only signed helper plan.
func NewRuntimePlan(
	appPath, runtimeDirectory, buildID, requiredAppGroup string,
) (holder.RuntimePlan, error) {
	plan, err := holder.NewRuntimePlan(RuntimePlanSpec(
		appPath, runtimeDirectory, buildID, requiredAppGroup,
	))
	if err != nil {
		return holder.RuntimePlan{}, err
	}
	return plan, nil
}
