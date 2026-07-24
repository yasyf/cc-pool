package holderbridge

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/trust"
)

func TestRuntimePlanSpecPinsProductIdentityAndProtectedPolicy(t *testing.T) {
	const appPath = "/Users/test/Applications/CCPoolStatus.app"
	const runtimeDirectory = "/Users/test/.cc-pool/fusekit"
	const buildID = "v0.60.0"
	const requiredAppGroup = "ABCDE12345.ccp"
	spec := RuntimePlanSpec(appPath, runtimeDirectory, buildID, requiredAppGroup)
	application := spec.Application
	if application != Application(appPath) || application.BundleID != BundleID ||
		application.TeamID != TeamID || application.Broker != application.Runtime ||
		application.Runtime.ExecutableName != ExecutableName ||
		application.Runtime.SigningIdentifier != BundleID {
		t.Fatalf("application = %#v", application)
	}
	if spec.RuntimeDirectory != runtimeDirectory || spec.Native != nil ||
		spec.BuildID != buildID ||
		spec.Readiness != ReadinessContract() ||
		!spec.SourceCapable || spec.BrokerPolicy.RequiredAppGroup != requiredAppGroup ||
		spec.RuntimePolicy.RequiredAppGroup != requiredAppGroup {
		t.Fatalf("runtime plan spec = %#v", spec)
	}
}

func TestRuntimeTrustRequirementsPinEveryFixedRole(t *testing.T) {
	const requiredAppGroup = "ABCDE12345.ccp"
	requirements := RuntimeTrustRequirements(requiredAppGroup)
	for name, requirement := range map[string]trust.Requirement{
		"stop":      requirements.StopController,
		"receipt":   requirements.ReceiptController,
		"readiness": requirements.ReadinessController,
	} {
		if requirement.TeamID != TeamID || requirement.SigningIdentifier != BundleID ||
			requirement.RequiredAppGroup != "" || requirement.RequiredEntitlements != nil {
			t.Fatalf("%s controller requirement = %#v", name, requirement)
		}
	}
	extension := requirements.FileProviderExtension
	if extension.TeamID != TeamID || extension.SigningIdentifier != fileProviderBundleID ||
		extension.RequiredAppGroup != requiredAppGroup || extension.RequiredEntitlements != nil {
		t.Fatalf("File Provider extension requirement = %#v", extension)
	}
}

func TestOpaqueDeploymentDigestMatchesSignedSwiftPolicy(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join(
		"..", "..", "widget", "Sources", "FileProviderRuntime", "Configuration.swift",
	))
	if err != nil {
		t.Fatal(err)
	}
	const marker = `static let appGroupIdentifier = "`
	start := strings.Index(string(payload), marker)
	if start < 0 {
		t.Fatal("signed Swift configuration does not declare the App Group identifier")
	}
	value := string(payload)[start+len(marker):]
	end := strings.IndexByte(value, '"')
	if end <= 0 {
		t.Fatal("signed Swift configuration has an invalid App Group identifier")
	}
	requirement := trust.Requirement{
		TeamID: TeamID, SigningIdentifier: BundleID, RequiredAppGroup: value[:end],
	}
	digest, err := requirement.ValidationDigest()
	if err != nil {
		t.Fatal(err)
	}
	if digest != runtimePolicyDigest {
		t.Fatalf("opaque deployment digest = %x, signed Swift policy digest = %x", runtimePolicyDigest, digest)
	}
}

func TestSignedHolderUsesOnlyTheRuntimePlanReadinessBudget(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("..", "..", "cmd", "cc-pool-runtime-archive", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	for _, forbidden := range []string{"startTimeout", "readyTimeout", "shutdownTimeout", "15 * time.Second"} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("signed holder retains independent readiness deadline %q", forbidden)
		}
	}
	for _, required := range []string{
		"ReadinessContract().StartupTimeout()",
		"ReadinessContract().SettlementTimeout()",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("signed holder does not consume %q", required)
		}
	}
}

func TestSignedHolderDispatchesOnlyFuseKitChildren(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("..", "..", "cmd", "cc-pool-runtime-archive", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	drivers := strings.Index(source, "drivers, err := claudeDriverFactories()")
	child := strings.Index(source, "holder.RunChild")
	if strings.Contains(source, "StopControl"+"Child") || drivers < 0 || child < 0 || drivers >= child {
		t.Fatalf("signed FuseKit child dispatch order drivers=%d child=%d", drivers, child)
	}
}

func TestSignedAppDispatchesFuseKitChildrenBeforeBrokerInitialization(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("..", "..", "widget", "Sources", "App", "main.swift"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	importsEnd := strings.Index(source, "\n\n")
	if importsEnd < 0 {
		t.Fatal("signed app entrypoint has no import boundary")
	}
	entrypoint := source[importsEnd+2:]
	if !strings.HasPrefix(entrypoint, "let childStatus = CCPoolFuseKitDispatchChild()") {
		t.Fatal("signed app does not dispatch FuseKit child roles first")
	}
	stop := strings.Index(source, "CCPoolFuseKitDispatchChild()")
	broker := strings.Index(source, "CatalogBroker.runChildIfRequested")
	start := strings.Index(source, "CCPoolFuseKitStart(")
	if stop < 0 || broker < 0 || start < 0 || stop >= broker || broker >= start {
		t.Fatalf("signed app dispatch order stop=%d broker=%d start=%d", stop, broker, start)
	}
}
