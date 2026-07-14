//go:build darwin

package execguard

import (
	"bufio"
	"bytes"
	"io"
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

// TestEnableForSpawnRestoresParent is the G-X8 restore pin: EnableForSpawn turns the
// PROCESS scope ON; restore must return it to DEFAULT, and a no-op restore (the
// regression) leaves it ON. macOS resolves DEFAULT/OFF to the same process-scope GET
// (only ON reads back distinctly), so the post-restore check targets that resolved
// value, captured up front, rather than the raw DEFAULT sentinel.
func TestEnableForSpawnRestoresParent(t *testing.T) {
	if err := setiopolicy(iopolTypeVFSMaterializeDatalessFiles, iopolScopeProcess, iopolMaterializeDatalessFilesDefault); err != nil {
		t.Fatalf("set process DEFAULT baseline: %v", err)
	}
	reverted, e := getMaterialize(iopolScopeProcess)
	if e != 0 {
		t.Fatalf("GET DEFAULT baseline: errno %d", e)
	}
	if reverted == iopolMaterializeDatalessFilesOn {
		t.Fatalf("test premise broken: a DEFAULT process scope GETs as ON(%d)", iopolMaterializeDatalessFilesOn)
	}
	t.Cleanup(func() {
		_ = setiopolicy(iopolTypeVFSMaterializeDatalessFiles, iopolScopeProcess, iopolMaterializeDatalessFilesDefault)
	})

	restore, err := EnableForSpawn()
	if err != nil {
		t.Fatalf("EnableForSpawn: %v", err)
	}
	if p, e := getMaterialize(iopolScopeProcess); e != 0 || p != iopolMaterializeDatalessFilesOn {
		_ = restore()
		t.Fatalf("between EnableForSpawn and restore process scope = %d (errno %d), want ON(%d)", p, e, iopolMaterializeDatalessFilesOn)
	}
	if err := restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if p, e := getMaterialize(iopolScopeProcess); e != 0 || p != reverted {
		t.Fatalf("after restore process scope = %d (errno %d), want the DEFAULT-resolved %d — a no-op restore left it ON(%d)", p, e, reverted, iopolMaterializeDatalessFilesOn)
	}
}

// TestExecGuardChildSurvivesParentRestore proves the fork-inherited policy is the
// child's own: the parent EnableForSpawn + Start()s a child, restores its OWN process
// scope to DEFAULT, and only THEN releases the child to read — which must still see
// process=ON. The child blocks on stdin so its GET runs strictly after the parent
// reverted (Start-then-restore-then-child-GET, never CombinedOutput's fork-and-wait).
func TestExecGuardChildSurvivesParentRestore(t *testing.T) {
	if os.Getenv("EXECGUARD_CHILD_WAIT") == "1" {
		_, _ = bufio.NewReader(os.Stdin).ReadString('\n') // block until the parent has restored
		proc, e := getMaterialize(iopolScopeProcess)
		switch {
		case e != 0:
			os.Exit(3)
		case proc != iopolMaterializeDatalessFilesOn:
			os.Exit(4)
		}
		os.Exit(0)
	}

	if err := setiopolicy(iopolTypeVFSMaterializeDatalessFiles, iopolScopeProcess, iopolMaterializeDatalessFilesOff); err != nil {
		t.Fatalf("force process OFF baseline: %v", err)
	}
	t.Cleanup(func() {
		_ = setiopolicy(iopolTypeVFSMaterializeDatalessFiles, iopolScopeProcess, iopolMaterializeDatalessFilesDefault)
	})

	restore, err := EnableForSpawn()
	if err != nil {
		t.Fatalf("EnableForSpawn: %v", err)
	}
	//nolint:gosec // G204: re-execs this very test binary with a child flag
	cmd := exec.Command(os.Args[0], "-test.run=TestExecGuardChildSurvivesParentRestore")
	cmd.Env = append(os.Environ(), "EXECGUARD_CHILD_WAIT=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		_ = restore()
		t.Fatalf("stdin pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		_ = restore()
		t.Fatalf("start child: %v", err)
	}
	// Restore the PARENT before the child reads: the child's inherited ON must not
	// depend on the parent still holding it.
	if err := restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if p, e := getMaterialize(iopolScopeProcess); e != 0 || p == iopolMaterializeDatalessFilesOn {
		t.Fatalf("parent process scope after restore = %d (errno %d), want reverted from ON(%d) — a no-op restore left it ON", p, e, iopolMaterializeDatalessFilesOn)
	}
	_, _ = io.WriteString(stdin, "go\n")
	_ = stdin.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatalf("child did not observe process=ON after the parent restored: %v\n%s", err, stderr.String())
	}
}
