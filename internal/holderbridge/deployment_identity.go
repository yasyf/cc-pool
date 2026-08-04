package holderbridge

import "time"

const (
	// DeploymentEvidenceIdentity is the v1 product-evidence digest domain.
	DeploymentEvidenceIdentity = "cc-pool.deployment-evidence.v1"
	// DeploymentServiceLabel is the exact status app launch-agent label.
	DeploymentServiceLabel = BundleID + ".fusekit"
	// DeploymentElectionTimeout is the exact File Provider election deadline.
	DeploymentElectionTimeout = 5 * time.Second
	// DeploymentPollInterval is the exact deployment observation cadence.
	DeploymentPollInterval = 100 * time.Millisecond
)
