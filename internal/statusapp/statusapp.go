// Package statusapp reconciles cc-pool's exact signed application release.
package statusapp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/fetch"
)

const (
	appName              = "CCPoolStatus"
	fileProviderBundleID = "com.yasyf.cc-pool.status.fileprovider"
	electionWait         = 5 * time.Second
	electionPoll         = 100 * time.Millisecond
)

type fetcher interface {
	Fetch(context.Context, fetch.Config) (fetch.Installation, error)
}

type runner interface {
	Run(context.Context, string, ...string) error
	Output(context.Context, string, ...string) ([]byte, error)
}

type commandRunner struct{}

func (commandRunner) Run(ctx context.Context, name string, arguments ...string) error {
	_, err := (commandRunner{}).Output(ctx, name, arguments...)
	return err
}

func (commandRunner) Output(ctx context.Context, name string, arguments ...string) ([]byte, error) {
	//nolint:gosec // The caller supplies only the fixed /usr/bin/pluginkit command and fixed-shape arguments.
	command := exec.CommandContext(ctx, name, arguments...)
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w: %s", name, strings.Join(arguments, " "), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

func release() (fetch.Release, error) {
	if version.StatusAppVersion == "" || strings.TrimSpace(version.StatusAppVersion) != version.StatusAppVersion {
		return fetch.Release{}, errors.New("CCPoolStatus: release bundle version is not exact")
	}
	releasePrefix := "v" + version.StatusAppVersion
	if version.Version != releasePrefix && !strings.HasPrefix(version.Version, releasePrefix+"-") {
		return fetch.Release{}, fmt.Errorf(
			"CCPoolStatus: application version %q does not match release %q",
			version.StatusAppVersion, version.Version,
		)
	}
	digest, err := fetch.ParseSHA256(version.StatusAppSHA256)
	if err != nil {
		return fetch.Release{}, fmt.Errorf("CCPoolStatus: parse release digest: %w", err)
	}
	return fetch.Release{
		Version: version.StatusAppVersion,
		URL: fmt.Sprintf(
			"https://github.com/yasyf/cc-pool/releases/download/%s/cc-pool-status-%s-darwin.zip",
			version.Version, version.Version,
		),
		SHA256: digest,
	}, nil
}

// Reconcile installs the exact signed application release and registers its
// File Provider extension before any tenant or service can use it.
func Reconcile(ctx context.Context) (string, error) {
	return reconcile(ctx, fetch.New(), commandRunner{})
}

func reconcile(ctx context.Context, source fetcher, commands runner) (string, error) {
	directory := pool.WidgetAppDir()
	if err := ensureInstallDirectory(directory); err != nil {
		return "", err
	}
	bundle, err := release()
	if err != nil {
		return "", err
	}
	installation, err := source.Fetch(ctx, fetch.Config{
		Release: bundle,
		Dir:     directory,
		AppName: appName,
		Identity: codeidentity.CodeIdentity{
			TeamID: holderbridge.TeamID, SigningIdentifier: holderbridge.BundleID,
		},
	})
	if err != nil {
		return "", fmt.Errorf("CCPoolStatus: fetch signed app %s: %w", version.Version, err)
	}
	if installation.Path != pool.WidgetAppPath() {
		return "", fmt.Errorf("CCPoolStatus: installed path %q differs from fixed path %q", installation.Path, pool.WidgetAppPath())
	}
	extension := filepath.Join(installation.Path, "Contents", "PlugIns", "CCPoolFileProvider.appex")
	if err := commands.Run(ctx, "/usr/bin/pluginkit", "-a", extension); err != nil {
		return "", fmt.Errorf("CCPoolStatus: register File Provider extension: %w", err)
	}
	if err := commands.Run(ctx, "/usr/bin/pluginkit", "-e", "use", "-i", fileProviderBundleID); err != nil {
		return "", fmt.Errorf("CCPoolStatus: enable File Provider extension: %w", err)
	}
	if err := waitForElection(ctx, commands, extension); err != nil {
		return "", err
	}
	return installation.Path, nil
}

func waitForElection(ctx context.Context, commands runner, expectedPath string) error {
	electionCtx, cancel := context.WithTimeout(ctx, electionWait)
	defer cancel()
	var lastErr error
	for {
		output, err := commands.Output(
			electionCtx, "/usr/bin/pluginkit", "-m", "-v", "-i", fileProviderBundleID,
		)
		if err == nil {
			lastErr = verifyElection(output, expectedPath)
			if lastErr == nil {
				return nil
			}
		} else {
			lastErr = fmt.Errorf("inspect elected File Provider extension: %w", err)
		}
		select {
		case <-electionCtx.Done():
			return fmt.Errorf("CCPoolStatus: wait for exact File Provider election: %w", errors.Join(electionCtx.Err(), lastErr))
		case <-time.After(electionPoll):
		}
	}
}

func verifyElection(output []byte, expectedPath string) error {
	var enabled []string
	for _, line := range strings.Split(string(output), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "+") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 2 {
			return fmt.Errorf("CCPoolStatus: elected File Provider row has no exact path: %q", trimmed)
		}
		path := strings.TrimSpace(fields[len(fields)-1])
		if path == "" {
			return fmt.Errorf("CCPoolStatus: elected File Provider row has an empty path: %q", trimmed)
		}
		enabled = append(enabled, path)
	}
	if len(enabled) != 1 || enabled[0] != expectedPath {
		return fmt.Errorf("CCPoolStatus: elected File Provider paths %q differ from exact installed extension %q", enabled, expectedPath)
	}
	return nil
}

func ensureInstallDirectory(path string) error {
	parent := filepath.Dir(path)
	parentInfo, err := os.Lstat(parent)
	if err != nil {
		return fmt.Errorf("CCPoolStatus: inspect install parent %q: %w", parent, err)
	}
	if !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("CCPoolStatus: install parent %q is not a real directory", parent)
	}
	created := false
	if err := os.Mkdir(path, 0o700); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("CCPoolStatus: create install directory %q: %w", path, err)
		}
	} else {
		created = true
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("CCPoolStatus: inspect install directory %q: %w", path, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("CCPoolStatus: install path %q is not a real directory", path)
	}
	if info.Mode().Perm() != 0o700 {
		//nolint:gosec // A private directory requires the owner execute bit.
		if err := os.Chmod(path, 0o700); err != nil {
			return fmt.Errorf("CCPoolStatus: protect install directory %q: %w", path, err)
		}
		if err := daemon.SyncDir(path); err != nil {
			return fmt.Errorf("CCPoolStatus: persist install directory permissions: %w", err)
		}
	}
	if created {
		if err := daemon.SyncDir(parent); err != nil {
			return fmt.Errorf("CCPoolStatus: persist install directory: %w", err)
		}
	}
	return nil
}
