package holderbridge

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	consumerBuildDomain = "cc-pool.deployment-callbacks.v1@sha256:"
	// DeploymentEvidenceIdentity is the v1 product-evidence digest domain.
	DeploymentEvidenceIdentity = "cc-pool.deployment-evidence.v1"
	// DeploymentServiceLabel is the exact status app launch-agent label.
	DeploymentServiceLabel = BundleID + ".fusekit"
	// DeploymentElectionTimeout is the exact File Provider election deadline.
	DeploymentElectionTimeout = 5 * time.Second
	// DeploymentPollInterval is the exact deployment observation cadence.
	DeploymentPollInterval = 100 * time.Millisecond
)

var startupConsumerBuild, startupConsumerBuildErr = currentConsumerBuild()

// DeploymentIdentity returns the startup-frozen updater build identity.
func DeploymentIdentity() (string, error) {
	if startupConsumerBuildErr != nil {
		return "", fmt.Errorf("CCPoolStatus: cache deployment consumer build: %w", startupConsumerBuildErr)
	}
	return startupConsumerBuild, nil
}

func currentConsumerBuild() (string, error) {
	executable, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("CCPoolStatus: resolve updater executable: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(executable)
	if err != nil {
		return "", fmt.Errorf("CCPoolStatus: resolve updater executable links: %w", err)
	}
	return consumerBuildForExecutable(resolved)
}

func consumerBuildForExecutable(path string) (_ string, resultErr error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", errors.New("CCPoolStatus: updater executable path is not exact and absolute")
	}
	file, err := os.OpenInRoot(filepath.Dir(path), filepath.Base(path))
	if err != nil {
		return "", fmt.Errorf("CCPoolStatus: open updater executable: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("CCPoolStatus: close updater executable: %w", err))
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("CCPoolStatus: inspect updater executable: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return "", errors.New("CCPoolStatus: updater executable is not an executable regular file")
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", fmt.Errorf("CCPoolStatus: hash updater executable: %w", err)
	}
	return consumerBuildDomain + hex.EncodeToString(digest.Sum(nil)), nil
}
