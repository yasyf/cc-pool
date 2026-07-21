package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReleaseCLIFailsClosedBeforeArtifactPublication(t *testing.T) {
	release := readReleaseArtifactContract(t, ".github", "workflows", "release.yml")
	require := strings.Index(release, "Require CLI signing and notarization secrets")
	build := strings.Index(release, "Build universal CLI")
	create := strings.Index(release, "Create GitHub Release")
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
		"__ASSET_URL__=${{ needs.release-app.outputs.asset_url }}",
		"Publish formula and cask in one tap commit",
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
		if strings.Contains(line, "yasyf/homebrew-tap/") && strings.Contains(line, "@v") {
			t.Fatalf("release uses a mutable homebrew-tap workflow or action reference: %s", line)
		}
	}
}

func TestStatusCaskUsesVerifiedAssetURL(t *testing.T) {
	cask := readReleaseArtifactContract(t, ".github", "cask", "cc-pool-status.rb.tmpl")
	if !strings.Contains(cask, `url "__ASSET_URL__"`) {
		t.Fatal("status cask reconstructs an asset URL instead of using the verified release output")
	}
	if strings.Contains(cask, "/releases/download/") {
		t.Fatal("status cask retains a second release-asset URL derivation")
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
