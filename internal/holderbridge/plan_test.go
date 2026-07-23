package holderbridge

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimePlanSpecPinsProductIdentityAndProtectedPolicy(t *testing.T) {
	const appPath = "/Applications/CCPoolStatus.app"
	const runtimeDirectory = "/Users/test/.cc-pool/fusekit"
	const buildID = "v0.60.0"
	spec := RuntimePlanSpec(appPath, runtimeDirectory, buildID)
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
		!spec.SourceCapable || spec.BrokerPolicy.RequiredAppGroup != AppGroup ||
		spec.RuntimePolicy.RequiredAppGroup != AppGroup {
		t.Fatalf("runtime plan spec = %#v", spec)
	}
}

func TestSignedHolderUsesOnlyTheRuntimePlanReadinessBudget(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("..", "..", "cmd", "cc-pool-holder-archive", "main.go"))
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

func TestSignedHolderDispatchesStopControlBeforeOtherChildWork(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("..", "..", "cmd", "cc-pool-holder-archive", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	stop := strings.Index(source, "holder.RunStopControlChild")
	drivers := strings.Index(source, "drivers, err := claudeDriverFactories()")
	child := strings.Index(source, "holder.RunChild")
	if stop < 0 || drivers < 0 || child < 0 || stop >= drivers || drivers >= child {
		t.Fatalf("signed child dispatch order stop=%d drivers=%d child=%d", stop, drivers, child)
	}
}

func TestSignedAppDispatchesStopControlBeforeBrokerInitialization(t *testing.T) {
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
		t.Fatal("signed app does not dispatch the authenticated stop child first")
	}
	stop := strings.Index(source, "CCPoolFuseKitDispatchChild()")
	broker := strings.Index(source, "CatalogBroker.runChildIfRequested")
	start := strings.Index(source, "CCPoolFuseKitStart()")
	if stop < 0 || broker < 0 || start < 0 || stop >= broker || broker >= start {
		t.Fatalf("signed app dispatch order stop=%d broker=%d start=%d", stop, broker, start)
	}
}
