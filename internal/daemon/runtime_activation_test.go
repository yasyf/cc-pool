package daemon

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/trust"
)

func writeExecutableFixture(t *testing.T, dir, name string) string {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o700)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("fixture"); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	resolvedDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(resolvedDir, name)
}

func TestCurrentServiceExecutableRequiresExactAbsolutePath(t *testing.T) {
	root := t.TempDir()
	target := writeExecutableFixture(t, root, "ccp-v1")
	original := currentServiceExecutable
	currentServiceExecutable = func() (string, error) { return target, nil }
	t.Cleanup(func() { currentServiceExecutable = original })
	path, err := CurrentServiceExecutable()
	if err != nil {
		t.Fatal(err)
	}
	if path != target {
		t.Fatalf("CurrentServiceExecutable() = %q, want %q", path, target)
	}
	currentServiceExecutable = func() (string, error) { return "ccp", nil }
	if _, err := CurrentServiceExecutable(); err == nil {
		t.Fatal("CurrentServiceExecutable accepted a relative executable")
	}
}

func TestDaemonTrustPolicyAssignsDistinctLifecycleRoles(t *testing.T) {
	policy, err := daemonTrustPolicy()
	if err != nil {
		t.Fatal(err)
	}
	stop := trust.PeerRole(StopRoleID)
	receipt := trust.PeerRole(ReceiptRoleID)
	readiness := trust.PeerRole(ReadinessRoleID)
	if !policy.AllowsUnprotected() || !policy.AllowsStop(stop) ||
		!policy.AllowsReceipt(receipt) || !policy.AllowsReadiness(readiness) {
		t.Fatal("daemon trust policy omitted an exact lifecycle role")
	}
	if policy.AllowsStop(receipt) || policy.AllowsStop(readiness) ||
		policy.AllowsReceipt(stop) || policy.AllowsReadiness(stop) {
		t.Fatal("daemon trust policy conflated lifecycle role authority")
	}
	for _, role := range []trust.PeerRole{stop, receipt, readiness} {
		requirement, ok := policy.Requirement(role)
		if !ok || requirement.TeamID != ServiceTeamID || requirement.SigningIdentifier != ServiceRoleID {
			t.Fatalf("role %q requirement = %+v, present=%t", role, requirement, ok)
		}
	}
}
