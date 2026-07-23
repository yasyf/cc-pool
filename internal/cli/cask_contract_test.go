package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHolderStopUninstallCommandUsesExactDaemonkitStopPath(t *testing.T) {
	want := errors.New("exact stop failure")
	called := 0
	swapVar(t, &stopHolder, func(context.Context) error {
		called++
		return want
	})
	cmd := newServiceCmd()
	cmd.SetArgs([]string{"holder-stop-uninstall"})
	if err := cmd.ExecuteContext(t.Context()); !errors.Is(err, want) {
		t.Fatalf("holder-stop-uninstall = %v, want %v", err, want)
	}
	if called != 1 {
		t.Fatalf("exact holder stop calls = %d, want 1", called)
	}
}

func TestStatusCaskRequiresExactHolderStopAndRejectsNameBasedKills(t *testing.T) {
	cask := readReleaseContract(t, ".github", "cask", "cc-pool-status.rb.tmpl")
	if strings.Count(cask, `args: ["service", "holder-stop-uninstall"]`) != 2 ||
		strings.Count(cask, "must_succeed: true") < 3 {
		t.Fatal("status cask does not fail closed through the exact holder stop hook")
	}
	lower := strings.ToLower(cask)
	for _, forbidden := range []string{"pkill", "pgrep", "killall", "osascript", "uninstall quit:"} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("status cask contains forbidden process control %q", forbidden)
		}
	}
}

func TestStatusCaskPinsFixedFileProviderApp(t *testing.T) {
	cask := readReleaseContract(t, ".github", "cask", "cc-pool-status.rb.tmpl")
	for _, required := range []string{
		"app \"__APP_NAME__.app\", target: \"/Applications/__APP_NAME__.app\"",
		"\"/Applications/__APP_NAME__.app\"",
	} {
		if !strings.Contains(cask, required) {
			t.Fatalf("status cask is missing %q", required)
		}
	}
	if strings.Contains(cask, "appdir") {
		t.Fatal("status cask permits a configurable application path")
	}
	if strings.Contains(strings.ToLower(cask), "fuse-t") {
		t.Fatal("status cask retains a native FUSE dependency")
	}
	if got := strings.Count(cask, "/Applications/__APP_NAME__.app"); got != 4 {
		t.Fatalf("fixed application path occurrences = %d, want 4", got)
	}
	register := strings.Index(cask, `args: ["-a", "/Applications/__APP_NAME__.app/Contents/PlugIns/CCPoolFileProvider.appex"], must_succeed: true`)
	elect := strings.Index(cask, `args: ["-e", "use", "-i", "com.yasyf.cc-pool.status.fileprovider"], must_succeed: true`)
	start := strings.Index(cask, `args: ["service", "install"]`)
	if register < 0 || elect < register || start < elect {
		t.Fatal("status cask does not fail-closed register and elect File Provider before daemon start")
	}
}

func TestReleasePublishesFormulaAndCaskOnlyAfterVerifiedApplication(t *testing.T) {
	release := readReleaseContract(t, ".github", "workflows", "release.yml")
	if !strings.Contains(release, "needs: [version, widget-test, release]") {
		t.Fatal("application release does not depend on the CLI artifact release")
	}
	if !strings.Contains(release, "needs: [release, release-app]") {
		t.Fatal("tap publication does not wait for both verified release artifacts")
	}
	if got := strings.Count(release, ".github/actions/publish@"); got != 1 {
		t.Fatalf("tap publication calls = %d, want one atomic formula+cask call", got)
	}
	publishJob := strings.Index(release, "\n  publish-tap:")
	formula := strings.Index(release, "Render the formula into the atomic tap transaction")
	cask := strings.Index(release, "Render the cask into the atomic tap transaction")
	publish := strings.Index(release, "Publish formula and cask in one tap commit")
	if publishJob < 0 || formula < publishJob || cask < formula || publish < cask {
		t.Fatal("formula and cask are not rendered and published after the verified stack")
	}
}

func TestWidgetBuildsFileProviderOnlyHolderWithoutNativeRuntime(t *testing.T) {
	project := readReleaseContract(t, "widget", "project.yml")
	for _, required := range []string{
		"go build -buildmode=c-archive",
		"./cmd/cc-pool-holder-archive",
	} {
		if !strings.Contains(project, required) {
			t.Fatalf("widget project is missing %q", required)
		}
	}
	appMain := readReleaseContract(t, "widget", "Sources", "App", "main.swift")
	if !strings.Contains(appMain, "exit(CCPoolFuseKitWait())") {
		t.Fatal("fixed app does not exit after exact holder terminal settlement")
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
