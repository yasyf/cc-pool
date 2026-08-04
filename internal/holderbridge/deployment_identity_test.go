package holderbridge

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestConsumerBuildForExecutableHashesExactBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ccp")
	payload := []byte("exact updater bytes")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o700); err != nil { //nolint:gosec // Executable fixture requires the owner execute bit.
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	want := consumerBuildDomain + hex.EncodeToString(digest[:])
	got, err := consumerBuildForExecutable(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("consumer build = %q, want %q", got, want)
	}
}

func TestConsumerBuildForExecutableRejectsNonExecutableInput(t *testing.T) {
	dir := t.TempDir()
	plain := filepath.Join(dir, "plain")
	if err := os.WriteFile(plain, []byte("not executable"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{
		"relative": "ccp", "directory": dir, "plain file": plain,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := consumerBuildForExecutable(path); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestCurrentConsumerBuildHashesTheResolvedTestExecutable(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	want, err := consumerBuildForExecutable(resolved)
	if err != nil {
		t.Fatal(err)
	}
	got, err := currentConsumerBuild()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("current consumer build = %q, want %q", got, want)
	}
}

func TestDeploymentIdentityUsesStartupCacheAndFailsClosed(t *testing.T) {
	originalBuild, originalErr := startupConsumerBuild, startupConsumerBuildErr
	t.Cleanup(func() { startupConsumerBuild, startupConsumerBuildErr = originalBuild, originalErr })

	startupConsumerBuild, startupConsumerBuildErr = "cached-build", nil
	build, err := DeploymentIdentity()
	if err != nil || build != "cached-build" {
		t.Fatalf("identity = (%q, %v)", build, err)
	}

	unavailable := errors.New("updater unavailable")
	startupConsumerBuild, startupConsumerBuildErr = "", unavailable
	build, err = DeploymentIdentity()
	if !errors.Is(err, unavailable) || build != "" {
		t.Fatalf("failed identity = (%q, %v)", build, err)
	}
}
