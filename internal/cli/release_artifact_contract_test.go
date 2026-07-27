package cli

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPureGoCLIDoesNotEmbedSwiftOwnedContainerIdentity(t *testing.T) {
	repository, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(t.TempDir(), "cc-pool")
	//nolint:gosec // The test builds one fixed repository target into t.TempDir.
	command := exec.CommandContext(t.Context(), "go", "build", "-trimpath", "-o", artifact, "./cmd/cc-pool")
	command.Dir = repository
	command.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build pure-Go CLI: %v\n%s", err, output)
	}
	payload, err := os.ReadFile(artifact) //nolint:gosec // The path is the exact t.TempDir output above.
	if err != nil {
		t.Fatal(err)
	}
	forbidden := []byte("SXKCTF23Q2.ccp")
	if bytes.Contains(payload, forbidden) {
		t.Fatalf("pure-Go CLI embeds signed-runtime container identity %q", forbidden)
	}
}

const (
	// releaseAppWorkflowCommit is an immutable Git commit, not a credential.
	//nolint:gosec // G101 cannot distinguish the pinned commit from a secret.
	releaseAppWorkflowCommit        = "41f8de6765b3b833ef333b0b98f5683f0e46685b"
	releaseActionCommit             = "19c3d5013032ad9c88f9a8f1170d1f366c19b8d9"
	releaseVerifyTagActionCommit    = "2281a3ea884422db190de44fad65ce9bc08b19c4"
	releaseStageDraftActionCommit   = "e4c3108e693681df1a3c666bae80e890bc44cf3e"
	releasePublishDraftActionCommit = "54e3e194bda69896894a82c17fcdb2822beefab5"
	releasePublishCommit            = "9525763796fce4d1042cf3393d9479f791908eaa"
)

func TestReleaseCLIFailsClosedBeforeArtifactPublication(t *testing.T) {
	release := readReleaseArtifactContract(t, ".github", "workflows", "release.yml")
	require := strings.Index(release, "Require CLI signing and notarization secrets")
	build := strings.Index(release, "Build universal CLI")
	create := strings.Index(release, "Stage and verify the complete draft release")
	if require < 0 || build < require || create < build {
		t.Fatal("CLI signing and notarization secrets are not required before artifact publication")
	}
	for _, secret := range []string{
		"MACOS_SIGN_P12", "MACOS_SIGN_PASSWORD", "MACOS_NOTARY_KEY",
		"MACOS_NOTARY_KEY_ID", "MACOS_NOTARY_ISSUER_ID",
	} {
		if !strings.Contains(release[require:build], `require_secret "$`+secret+`" `+secret) {
			t.Fatalf("CLI release does not fail closed on %s", secret)
		}
	}
	for _, required := range []string{
		"MACOS_CODESIGN_IDENTIFIER: com.yasyf.cc-pool",
		"codesign --verify --strict --verbose=2 dist/pure/cc-pool",
		"^Identifier=com.yasyf.cc-pool$",
		"^TeamIdentifier=${TEAM_ID}$",
		"flags=.*\\(runtime\\)",
		`identifier "com.yasyf.cc-pool"`,
		"application-groups",
		"SXKCTF23Q2.ccp",
		"Library/Group Containers",
	} {
		if !strings.Contains(release[build:create], required) {
			t.Fatalf("CLI release is missing final identity/notarization assertion %q", required)
		}
	}
	if strings.Contains(release[build:create], "spctl --assess --type execute --verbose=4 dist/pure/cc-pool") {
		t.Fatal("CLI release assesses a raw Mach-O binary as an application bundle")
	}
}

func TestReleasePublishesCLIAndApplicationAtomically(t *testing.T) {
	release := readReleaseArtifactContract(t, ".github", "workflows", "release.yml")
	releaseJob := strings.Index(release, "\n  release:")
	widgetJob := strings.Index(release, "\n  widget-test:")
	if releaseJob < 0 || widgetJob < releaseJob {
		t.Fatal("release job boundaries are missing")
	}
	job := release[releaseJob:widgetJob]

	stages := []string{
		"Download the verified staged CCPoolStatus application",
		"Build universal CLI",
		"Verify the complete staged stack before drafting",
		"Record the exact release asset manifest",
		"Stage and verify the complete draft release",
		"Publish the verified release",
	}
	previous := -1
	for _, stage := range stages {
		at := strings.Index(job, stage)
		if at <= previous {
			t.Fatalf("atomic release stage %q is missing or out of order", stage)
		}
		previous = at
	}
	for _, required := range []string{
		"permissions:\n      contents: write",
		"needs.release-app.outputs.artifact_name",
		`"dist/$CLI_ASSET"`,
		"dist/SHA256SUMS.txt",
		`"dist/staged-app/$APP_ASSET"`,
		`"dist/staged-app/$APP_ASSET.sha256"`,
		`> "$RUNNER_TEMP/cc-pool-release-assets"`,
		"actions/stage-draft-release@" + releaseStageDraftActionCommit,
		"actions/publish-draft-release@" + releasePublishDraftActionCommit,
		`release-id: ${{ steps.draft.outputs['release-id'] }}`,
		`manifest: ${{ runner.temp }}/cc-pool-release-assets`,
	} {
		if !strings.Contains(job, required) {
			t.Fatalf("atomic release is missing %q", required)
		}
	}
	for _, required := range []string{"group: cc-pool-release", "cancel-in-progress: false"} {
		if !strings.Contains(release, required) {
			t.Fatalf("release workflow is missing serialization contract %q", required)
		}
	}
	if got := strings.Count(release, "contents: write"); got != 1 {
		t.Fatalf("GitHub Release publishers = %d, want one", got)
	}
	if got := strings.Count(release, `/assets?per_page=100`); got != 1 {
		t.Fatalf("caller-owned release-ID asset listings = %d, want the tap verification only", got)
	}
	if !strings.Contains(release, "permissions:\n  contents: read") {
		t.Fatal("release workflow does not default non-owner jobs to read-only contents")
	}
	if !strings.Contains(release, "release-app.yml@"+releaseAppWorkflowCommit) {
		t.Fatal("release workflow is not pinned to the caller-owned staging contract")
	}
	appJob := strings.Index(release, "\n  release-app:")
	publishJob := strings.Index(release, "\n  publish-tap:")
	if appJob < 0 || publishJob < appJob || !strings.Contains(release[appJob:publishJob], "permissions:\n      contents: read") {
		t.Fatal("artifact-only application workflow retains release publication authority")
	}
	for _, forbidden := range []string{
		"softprops/action-gh-release",
		"attach-to-release: \"true\"",
		"releases/tags/${GITHUB_REF_NAME}",
		"uploads.github.com",
		"Create or reset the private draft GitHub Release",
	} {
		if strings.Contains(job, forbidden) {
			t.Fatalf("release job retains an independent publisher %q", forbidden)
		}
	}
}

func TestReleaseTapUsesExactVerifiedPublishedBytes(t *testing.T) {
	release := readReleaseArtifactContract(t, ".github", "workflows", "release.yml")
	publishJob := strings.Index(release, "\n  publish-tap:")
	if publishJob < 0 {
		t.Fatal("release has no post-verification tap transaction")
	}
	publish := release[publishJob:]
	for _, required := range []string{
		"needs: [release, release-app]",
		"needs.release.outputs.pure_sha256",
		"needs.release.outputs.release_id",
		"needs.release-app.outputs.asset_filename",
		"needs.release-app.outputs.asset_url",
		"needs.release-app.outputs.sha256",
		`repos/${GITHUB_REPOSITORY}/releases/${RELEASE_ID}/assets?per_page=100`,
		`https://api.github.com/repos/${GITHUB_REPOSITORY}/releases/assets/${asset_id}`,
		`read -r sidecar_sha sidecar_path < "$APP_ASSET.sha256"`,
		`[ "$sidecar_sha" = "$APP_SHA256" ]`,
		`[ "$(basename "$sidecar_path")" = "$APP_ASSET" ]`,
		`[ "$(shasum -a 256 "$APP_ASSET" | awk '{print $1}')" = "$APP_SHA256" ]`,
		"codesign --verify --strict --verbose=2 cli/cc-pool",
		"scripts/assert-signed-topology.sh",
		"xcrun stapler validate app/CCPoolStatus.app",
		"spctl --assess --type execute --verbose=4 app/CCPoolStatus.app",
		"__SHA_APP__=${{ needs.release-app.outputs.sha256 }}",
		"Publish the CLI formula to the tap",
		"homebrew-tap/.github/actions/publish@" + releasePublishCommit,
	} {
		if !strings.Contains(publish, required) {
			t.Fatalf("tap transaction is missing exact released-byte gate %q", required)
		}
	}
	if got := strings.Count(release, "homebrew-tap/.github/actions/publish@"); got != 1 {
		t.Fatalf("tap publishers = %d, want exactly one", got)
	}
	if strings.Contains(publish, "spctl --assess --type execute --verbose=4 cli/cc-pool") {
		t.Fatal("tap transaction assesses a raw Mach-O binary as an application bundle")
	}
	for _, forbidden := range []string{"gh release view", "gh release download", "releases/tags/"} {
		if strings.Contains(release, forbidden) {
			t.Fatalf("release uses tag-only draft or asset lookup %q", forbidden)
		}
	}
	for _, line := range strings.Split(release, "\n") {
		switch {
		case strings.Contains(line, "actions/verify-tag-on-main@"):
			if !strings.Contains(line, "@"+releaseVerifyTagActionCommit) {
				t.Fatalf("release uses a mixed or mutable tag-verification action reference: %s", line)
			}
		case strings.Contains(line, "yasyf/homebrew-tap/.github/workflows/release-app.yml@"):
			if !strings.Contains(line, "@"+releaseAppWorkflowCommit) {
				t.Fatalf("release-app uses a mixed or mutable workflow reference: %s", line)
			}
		case strings.Contains(line, "actions/stage-draft-release@"):
			if !strings.Contains(line, "@"+releaseStageDraftActionCommit) {
				t.Fatalf("release uses a mixed or mutable draft-stage action reference: %s", line)
			}
		case strings.Contains(line, "actions/publish-draft-release@"):
			if !strings.Contains(line, "@"+releasePublishDraftActionCommit) {
				t.Fatalf("release uses a mixed or mutable draft-publish action reference: %s", line)
			}
		case strings.Contains(line, "yasyf/homebrew-tap/.github/actions/publish@"):
			if !strings.Contains(line, "@"+releasePublishCommit) {
				t.Fatalf("release uses a mixed or mutable tap publish action reference: %s", line)
			}
		case strings.Contains(line, "yasyf/homebrew-tap/.github/actions/"):
			if !strings.Contains(line, "@"+releaseActionCommit) {
				t.Fatalf("release uses a mixed or mutable action reference: %s", line)
			}
		}
	}
}

func TestReleaseDoesNotPublishStandaloneStatusCask(t *testing.T) {
	release := readReleaseArtifactContract(t, ".github", "workflows", "release.yml")
	for _, forbidden := range []string{
		"Render the cask", ".github/cask/cc-pool-status", "delete-file: Casks/cc-pool-status.rb",
	} {
		if strings.Contains(release, forbidden) {
			t.Fatalf("release retains standalone status cask contract %q", forbidden)
		}
	}
}

func readReleaseArtifactContract(t *testing.T, components ...string) string {
	t.Helper()
	components = append([]string{"..", ".."}, components...)
	payload, err := os.ReadFile(filepath.Join(components...)) //nolint:gosec // Repository test fixtures only.
	if err != nil {
		t.Fatal(err)
	}
	return string(payload)
}
