package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeStopUninstallCommandUsesExactDaemonkitStopPath(t *testing.T) {
	want := errors.New("exact stop failure")
	called := 0
	swapVar(t, &stopHolder, func(context.Context) error {
		called++
		return want
	})
	cmd := newServiceCmd()
	cmd.SetArgs([]string{"runtime-stop-uninstall"})
	if err := cmd.ExecuteContext(t.Context()); !errors.Is(err, want) {
		t.Fatalf("runtime-stop-uninstall = %v, want %v", err, want)
	}
	if called != 1 {
		t.Fatalf("exact runtime stop calls = %d, want 1", called)
	}
}

func TestReleasePreservesGatekeeperAndPinsAppResourceDigestIntoFormula(t *testing.T) {
	release := readReleaseContract(t, ".github", "workflows", "release.yml")
	for _, required := range []string{
		"Require CLI signing and notarization secrets",
		"needs: [verify-tag-on-main, release-app, suite-pins, version]",
		"internal/version.StatusAppVersion=${{ needs.version.outputs.marketing }}",
		"__SHA_APP__=${{ needs.release-app.outputs.sha256 }}",
		"bash \"$GITHUB_WORKSPACE/scripts/assert-signed-topology.sh\"",
		"xcrun stapler validate app/CCPoolStatus.app",
		"spctl --assess --type execute --verbose=4 app/CCPoolStatus.app",
	} {
		if !strings.Contains(release, required) {
			t.Fatalf("release workflow is missing Gatekeeper contract %q", required)
		}
	}
	formula := readReleaseContract(t, ".github", "formula", "cc-pool.rb.tmpl")
	dependency := strings.Index(formula, "depends_on :macos")
	resource := strings.Index(formula, `resource "status_app" do`)
	if dependency < 0 || resource < 0 || dependency >= resource {
		t.Fatal("formula dependencies must precede resources")
	}
	for _, required := range []string{
		`resource "status_app" do`,
		"cc-pool-status-v#{version}-darwin.zip",
		`sha256 "__SHA_APP__"`,
		`libexec.install "CCPoolStatus.app"`,
		`ccp package install`,
	} {
		if !strings.Contains(formula, required) {
			t.Fatalf("formula is missing packaged application contract %q", required)
		}
	}
	withoutUserApp := strings.ReplaceAll(formula, "~/Applications/CCPoolStatus.app", "")
	for _, forbidden := range []string{"head do", "install_from_source", "/Applications/CCPoolStatus.app"} {
		if strings.Contains(withoutUserApp, forbidden) {
			t.Fatalf("formula retains forbidden package path %q", forbidden)
		}
	}
	if !strings.Contains(release, `assert-formula-service.sh" "$FORMULA"`) {
		t.Fatal("release does not validate the exact rendered formula ordering contract")
	}
}

func TestReleasePublishesOnlyFormulaAfterVerifiedApplication(t *testing.T) {
	release := readReleaseContract(t, ".github", "workflows", "release.yml")
	if !strings.Contains(release, "needs: [version, widget-test]") {
		t.Fatal("application release does not depend on its independent build gates")
	}
	if !strings.Contains(release, "needs: [release, release-app]") {
		t.Fatal("tap publication does not wait for both verified release artifacts")
	}
	if got := strings.Count(release, ".github/actions/publish@"); got != 1 {
		t.Fatalf("tap publication calls = %d, want one formula publication", got)
	}
	publishJob := strings.Index(release, "\n  publish-tap:")
	formula := strings.Index(release, "Render the formula into the atomic tap transaction")
	publish := strings.Index(release, "Publish the CLI formula to the tap")
	if publishJob < 0 || formula < publishJob || publish < formula {
		t.Fatal("formula is not rendered and published after the verified stack")
	}
	for _, forbidden := range []string{
		"Render the cask", ".github/cask/", "delete-file: Casks/cc-pool-status.rb",
	} {
		if strings.Contains(release, forbidden) {
			t.Fatalf("release retains standalone status cask contract %q", forbidden)
		}
	}
	if _, err := os.Lstat(filepath.Join("..", "..", ".github", "cask")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("standalone cask directory exists or could not be inspected: %v", err)
	}
}

func TestMacOSBootstrapDelegatesExactPackageDeliveryWithoutImplicitServiceMutation(t *testing.T) {
	installer := readReleaseContract(t, "scripts", "install.sh")
	for _, required := range []string{
		`VERSION="${1:-latest}"`,
		`brew install yasyf/tap/cc-pool`,
		`formula_prefix="$(brew --prefix yasyf/tap/cc-pool)"`,
		`ccp="$formula_prefix/bin/ccp"`,
		`"$ccp" package install`,
		`installed via Homebrew`,
	} {
		if !strings.Contains(installer, required) {
			t.Fatalf("macOS bootstrap is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"service install", "service uninstall", "/Applications/CCPoolStatus.app",
		"CC_POOL_BIN_DIR", "SHA256SUMS", "codesign", "ditto", ".local/libexec", "curl -fsSL --retry",
	} {
		if strings.Contains(installer, forbidden) {
			t.Fatalf("macOS bootstrap retains forbidden delivery operation %q", forbidden)
		}
	}
	ci := readReleaseContract(t, ".github", "workflows", "ci.yml")
	if !strings.Contains(ci, "run: scripts/install_test.sh") {
		t.Fatal("CI does not execute the direct installer harness")
	}
}

func TestWidgetBuildsFileProviderOnlyRuntime(t *testing.T) {
	project := readReleaseContract(t, "widget", "project.yml")
	for _, required := range []string{
		"go build -buildmode=c-archive",
		"./cmd/cc-pool-runtime-archive",
	} {
		if !strings.Contains(project, required) {
			t.Fatalf("widget project is missing %q", required)
		}
	}
	appMain := readReleaseContract(t, "widget", "Sources", "App", "main.swift")
	if !strings.Contains(appMain, "exit(CCPoolFuseKitWait())") {
		t.Fatal("fixed app does not exit after exact runtime terminal settlement")
	}
	for _, forbidden := range []string{
		"-tags fuse", "cc-pool-fuse-package",
		"fuset_source=", "fuset_target=", "license_source=", "license_target=",
		"lipo \"$fuset_source\"", "codesign --force --sign", "ThirdPartyLicenses/FUSE-T.txt",
	} {
		if strings.Contains(project, forbidden) {
			t.Fatalf("widget project duplicates FuseKit packaging mechanic %q", forbidden)
		}
	}

	assertions := readReleaseContract(t, ".github", "scripts", "assert-widget-app.sh")
	if strings.Contains(assertions, "cc-pool-fuse-package") || strings.Contains(assertions, "MACOS_SIGN_IDENTITY") {
		t.Fatal("widget assertion retains native FUSE packaging")
	}
	release := readReleaseContract(t, ".github", "workflows", "release.yml")
	if !strings.Contains(release, "go_version: 1.26.5") {
		t.Fatal("release workflow is missing the pinned Go version")
	}
	if strings.Contains(release, "fuse-t") || strings.Contains(release, "prebuild_brew_packages") {
		t.Fatal("release workflow retains a native FUSE dependency")
	}

	topology := readReleaseContract(t, "scripts", "assert-signed-topology.sh")
	for _, forbidden := range []string{
		"FUSE_T=", "FUSE_T_LICENSE", "FUSE_T_LICENSE_SHA256", "lipo \"$FUSE_T\"",
		"codesign --verify --strict \"$FUSE_T\"", "license_sha=",
	} {
		if strings.Contains(topology, forbidden) {
			t.Fatalf("signed-topology assertion duplicates FuseKit verification %q", forbidden)
		}
	}
	license := filepath.Join("..", "..", "widget", "Resources", "ThirdPartyLicenses", "FUSE-T.txt")
	if _, err := os.Stat(license); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("consumer-owned FUSE-T license still exists: %v", err)
	}

	push := readReleaseContract(t, "scripts", "vm", "push.sh")
	installAt := strings.Index(push, `log "installing $VMCTL_GUEST_APP"`)
	if strings.Contains(push, "cc-pool-fuse-package") || strings.Contains(push, "FUSE-T") {
		t.Fatal("VM push retains native FUSE packaging")
	}
	if installAt < 0 {
		t.Fatal("VM push no longer installs the signed app")
	}
}

func TestReleaseRejectsStandaloneHolderNamesAndSystemInstall(t *testing.T) {
	contracts := map[string]string{
		"formula":      readReleaseContract(t, ".github", "formula", "cc-pool.rb.tmpl"),
		"ci":           readReleaseContract(t, ".github", "workflows", "ci.yml"),
		"release":      readReleaseContract(t, ".github", "workflows", "release.yml"),
		"widget":       readReleaseContract(t, "widget", "project.yml"),
		"readme":       readReleaseContract(t, "README.md"),
		"architecture": readReleaseContract(t, "docs", "ARCHITECTURE.md"),
	}
	for name, contract := range contracts {
		lower := strings.ToLower(contract)
		for _, forbidden := range []string{
			"fusekitholder", "fusekit-holder", "holder.app", "cc-pool-holder-archive",
			"fusekit holder", "mount holder", "holder cask", "fuse overlays", "shared overlay",
		} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("%s retains standalone holder product name %q", name, forbidden)
			}
		}
		withoutUserApp := strings.ReplaceAll(contract, "~/Applications/CCPoolStatus.app", "")
		if strings.Contains(withoutUserApp, "/Applications/CCPoolStatus.app") {
			t.Fatalf("%s installs the status app in the system Applications directory", name)
		}
	}
}

func TestFileProviderOnlyRuntimeRejectsNativeAndLegacyPathResidue(t *testing.T) {
	contracts := map[string]struct {
		body      string
		forbidden []string
	}{
		"catalog authorization": {
			body:      readReleaseContract(t, "internal", "tenantfs", "authorization.go"),
			forbidden: []string{"cc-pool-mount", "RoleMount", "PresentationMount"},
		},
		"catalog materialization": {
			body:      readReleaseContract(t, "internal", "tenantfs", "authority_materializer.go"),
			forbidden: []string{"Mount: true"},
		},
		"pool paths": {
			body:      readReleaseContract(t, "internal", "pool", "paths.go"),
			forbidden: []string{"func AccountsDir(", "func AccountDir(", "func EnsureAccountsDir(", ".cc-pool/accounts", "holder runtime"},
		},
		"pool initialization": {
			body:      readReleaseContract(t, "internal", "pool", "account.go"),
			forbidden: []string{"EnsureAccountsDir"},
		},
		"user-facing runtime errors": {
			body: readReleaseContract(t, "internal", "daemon", "holderservice.go") +
				readReleaseContract(t, "internal", "tenantfs", "preparer.go") +
				readReleaseContract(t, "internal", "cli", "misc.go"),
			forbidden: []string{"holder runtime", "holder activation", "holder native presentation", "its overlay"},
		},
		"current documentation and fixtures": {
			body: readReleaseContract(t, "AGENTS.md") +
				readReleaseContract(t, ".claude", "fragments", "AGENTS.md", "cc-pool-guide.fragment.md") +
				readReleaseContract(t, "docs", "ARCHITECTURE.md") +
				readReleaseContract(t, "scripts", "sync-sim", "run.sh") +
				readReleaseContract(t, "widget", "Sources", "Shared", "Status.swift") +
				readReleaseContract(t, "widget", "Tests", "StatusProtocolTests.swift") +
				readReleaseContract(t, "widget", "Tests", "OutlookDisplayTests.swift"),
			forbidden: []string{".cc-pool/accounts", "shared overlay", "mount presentation"},
		},
	}
	for name, contract := range contracts {
		for _, forbidden := range contract.forbidden {
			if strings.Contains(contract.body, forbidden) {
				t.Fatalf("%s retains forbidden File Provider-only residue %q", name, forbidden)
			}
		}
	}
}

func TestSourceAuthorityRejectsLegacyFuseArtifactFilters(t *testing.T) {
	production := readReleaseContract(t, "internal", "overlay", "classify.go") +
		readReleaseContract(t, "internal", "tenantfs", "authority_policy.go")
	for _, forbidden := range []string{".fuse_hidden", ".nfs.", "sourceMutationArtifact"} {
		if strings.Contains(production, forbidden) {
			t.Fatalf("source authority retains legacy FUSE artifact filter %q", forbidden)
		}
	}
}

func TestTCCSnapshotCoversProtectedSurfacesAndRejectsDaemonRows(t *testing.T) {
	snapshot := readReleaseContract(t, "scripts", "vm", "tcc-snapshot.sh")
	for _, service := range []string{
		"kTCCServiceSystemPolicyAppData",
		"kTCCServiceSystemPolicyNetworkVolumes",
		"kTCCServiceSystemPolicyRemovableVolumes",
		"kTCCServiceSystemPolicyAllFiles",
		"kTCCServiceFileProviderDomain",
	} {
		if !strings.Contains(snapshot, service) {
			t.Fatalf("TCC snapshot is missing %s", service)
		}
	}
	for _, daemon := range []string{
		"com.yasyf.cc-pool",
		"com.yasyf.cc-pool.daemon",
		"client LIKE '%/cc-pool'",
	} {
		if !strings.Contains(snapshot, daemon) {
			t.Fatalf("TCC snapshot does not classify daemon identity %q", daemon)
		}
	}

	for _, scenario := range []string{
		"verify-signed-topology.sh",
		"verify-tcc-upgrade-reboot.sh",
	} {
		script := readReleaseContract(t, "scripts", "vm", "scenarios", scenario)
		if !strings.Contains(script, "grep -Fq '|daemon|'") {
			t.Fatalf("%s does not reject unsigned-daemon TCC rows", scenario)
		}
		if got := strings.Count(script, "[f]usekit-native-v1"); got != 1 {
			t.Fatalf("%s does not reject the removed native filesystem child", scenario)
		}
	}
}

func readReleaseContract(t *testing.T, components ...string) string {
	t.Helper()
	root, err := os.OpenRoot(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	payload, err := root.ReadFile(filepath.Join(components...))
	if err != nil {
		t.Fatal(err)
	}
	return string(payload)
}
