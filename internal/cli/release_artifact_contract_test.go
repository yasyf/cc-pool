package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	releaseAppWorkflowPin = "8f422c652d836c40f9cc5a9d893d4120b26bc681"
	releaseActionPin      = "19c3d5013032ad9c88f9a8f1170d1f366c19b8d9"
)

func TestReleaseCLIFailsClosedBeforeArtifactPublication(t *testing.T) {
	release := readReleaseArtifactContract(t, ".github", "workflows", "release.yml")
	require := strings.Index(release, "Require CLI signing and notarization secrets")
	build := strings.Index(release, "Build universal CLI")
	create := strings.Index(release, "Create or reset the private draft GitHub Release")
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
		"Create or reset the private draft GitHub Release",
		"Upload the exact stack to the private draft",
		"Validate the private draft asset set and bytes",
		"Publish the complete draft release",
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
		`[ "$(jq -r '.draft' <<< "$release")" = true ]`,
		`gh api --paginate --slurp`,
		`https://uploads.github.com/repos/${GITHUB_REPOSITORY}/releases/${RELEASE_ID}/assets?name=${name}`,
		`"dist/$CLI_ASSET"`,
		"dist/SHA256SUMS.txt",
		`"dist/staged-app/$APP_ASSET"`,
		`"dist/staged-app/$APP_ASSET.sha256"`,
		`actual="$(jq -r '.assets[].name' <<< "$release" | LC_ALL=C sort)"`,
		`release="$(gh api "repos/${GITHUB_REPOSITORY}/releases/${RELEASE_ID}")"`,
		`https://api.github.com/repos/${GITHUB_REPOSITORY}/releases/assets/${asset_id}`,
		`if: ${{ steps.draft.outputs.already_public != 'true' }}`,
		`already_public=$already_public`,
		`[ "$ALREADY_PUBLIC" != true ] || expected_draft=false`,
		`newer stable release published during staging`,
		`-F draft=false`,
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
	if got := strings.Count(job, "if: ${{ steps.draft.outputs.already_public != 'true' }}"); got != 2 {
		t.Fatalf("draft-only mutations = %d, want upload and publish", got)
	}
	if got := strings.Count(job, "gh api --paginate --slurp"); got != 2 {
		t.Fatalf("stable-order checks = %d, want initial and final", got)
	}
	if !strings.Contains(release, "permissions:\n  contents: read") {
		t.Fatal("release workflow does not default non-owner jobs to read-only contents")
	}
	if !strings.Contains(release, "release-app.yml@"+releaseAppWorkflowPin) {
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
		"needs.release-app.outputs.asset_filename",
		"needs.release-app.outputs.asset_url",
		"needs.release-app.outputs.sha256",
		`gh release download "$GITHUB_REF_NAME"`,
		`read -r sidecar_sha sidecar_path < "$APP_ASSET.sha256"`,
		`[ "$sidecar_sha" = "$APP_SHA256" ]`,
		`[ "$(basename "$sidecar_path")" = "$APP_ASSET" ]`,
		`[ "$(shasum -a 256 "$APP_ASSET" | awk '{print $1}')" = "$APP_SHA256" ]`,
		"codesign --verify --strict --verbose=2 cli/cc-pool",
		"scripts/assert-signed-topology.sh",
		"xcrun stapler validate app/CCPoolStatus.app",
		"spctl --assess --type execute --verbose=4 app/CCPoolStatus.app",
		"Publish the CLI formula to the tap",
		"delete-file: Casks/cc-pool-status.rb",
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
	for _, line := range strings.Split(release, "\n") {
		switch {
		case strings.Contains(line, "yasyf/homebrew-tap/.github/workflows/release-app.yml@"):
			if !strings.Contains(line, "@"+releaseAppWorkflowPin) {
				t.Fatalf("release-app uses a mixed or mutable workflow reference: %s", line)
			}
		case strings.Contains(line, "yasyf/homebrew-tap/.github/actions/"):
			if !strings.Contains(line, "@"+releaseActionPin) {
				t.Fatalf("release uses a mixed or mutable action reference: %s", line)
			}
		}
	}
}

func TestReleaseDoesNotPublishStandaloneStatusCask(t *testing.T) {
	release := readReleaseArtifactContract(t, ".github", "workflows", "release.yml")
	if got := strings.Count(release, "delete-file: Casks/cc-pool-status.rb"); got != 1 {
		t.Fatalf("retired status cask deletions = %d, want exactly one", got)
	}
	for _, forbidden := range []string{"Render the cask", ".github/cask/cc-pool-status"} {
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
