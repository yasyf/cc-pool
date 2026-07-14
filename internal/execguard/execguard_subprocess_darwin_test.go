//go:build darwin

package execguard

import (
	"os"
	"os/exec"
	"syscall"
	"testing"
	"unsafe"
)

const iopolCmdGet = 1 // IOPOL_CMD_GET

// getMaterialize reads a scope's dataless-materialize policy via iopolicysys GET.
func getMaterialize(scope int32) (int32, syscall.Errno) {
	p := iopolParam{scope: scope, iotype: iopolTypeVFSMaterializeDatalessFiles}
	_, _, errno := syscall.Syscall(sysIopolicysys, iopolCmdGet, uintptr(unsafe.Pointer(&p)), 0)
	return p.policy, errno
}

// TestExecGuardInheritedByChild is the P8 end-to-end pin: the parent forces both
// process and thread materialize policy OFF, runs enable(), then fork+execs a child
// that verifies it inherited process=ON with thread=DEFAULT (the exact state a
// launched claude sees, so its dataless reads materialize).
func TestExecGuardInheritedByChild(t *testing.T) {
	if os.Getenv("EXECGUARD_CHILD") == "1" {
		proc, e1 := getMaterialize(iopolScopeProcess)
		thr, e2 := getMaterialize(iopolScopeThread)
		switch {
		case e1 != 0 || e2 != 0:
			os.Exit(3)
		case proc != iopolMaterializeDatalessFilesOn:
			os.Exit(4)
		case thr != iopolMaterializeDatalessFilesDefault:
			os.Exit(5)
		}
		os.Exit(0)
	}

	if err := setiopolicy(iopolTypeVFSMaterializeDatalessFiles, iopolScopeProcess, iopolMaterializeDatalessFilesOff); err != nil {
		t.Fatalf("force process OFF: %v", err)
	}
	if err := setiopolicy(iopolTypeVFSMaterializeDatalessFiles, iopolScopeThread, iopolMaterializeDatalessFilesOff); err != nil {
		t.Fatalf("force thread OFF: %v", err)
	}
	restore, err := enable()
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	//nolint:gosec // G204: re-execs this very test binary with a child flag
	cmd := exec.Command(os.Args[0], "-test.run=TestExecGuardInheritedByChild")
	cmd.Env = append(os.Environ(), "EXECGUARD_CHILD=1")
	out, cerr := cmd.CombinedOutput()
	if rerr := restore(); rerr != nil {
		t.Fatalf("restore: %v", rerr)
	}
	if cerr != nil {
		t.Fatalf("child did not observe process=ON thread=DEFAULT: %v\n%s", cerr, out)
	}
}
