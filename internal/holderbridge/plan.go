// Package holderbridge binds cc-pool's signed application to FuseKit.
package holderbridge

import (
	"github.com/yasyf/daemonkit/codeidentity"
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

var runtimePolicyDigest = codeidentity.PolicyDigest{
	0x48, 0xe0, 0x64, 0xe5, 0xc6, 0xcd, 0x4f, 0xa0,
	0x85, 0xd9, 0x91, 0x31, 0x57, 0x59, 0x35, 0x1d,
	0x8a, 0xb7, 0x6f, 0xb2, 0x23, 0xaf, 0x6b, 0x9f,
	0x8c, 0x6f, 0x25, 0xa6, 0xcf, 0x7f, 0xbd, 0x9b,
}

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
func NewRuntimePlan(appPath, runtimeDirectory, buildID string) (holder.RuntimePlan, error) {
	plan, err := holder.NewRuntimePlan(RuntimePlanSpec(appPath, runtimeDirectory, buildID))
	if err != nil {
		return holder.RuntimePlan{}, err
	}
	return plan, nil
}
