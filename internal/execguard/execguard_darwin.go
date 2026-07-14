// Package execguard turns ON macOS dataless-file materialization around a claude
// launch so a File Provider account's dataless files (settings.json, .claude.json)
// materialize on first read instead of returning empty — the exec-time backstop for
// the File Provider data plane (P6+P7 own the earlier gates).
//
// The policy is set through the iopolicysys syscall (SYS_iopolicysys = 322,
// IOPOL_CMD_SET), verified against xnu's bsd/kern/kern_resource.c and the macOS SDK
// <sys/resource.h>/<sys/syscall.h>. IOPOL_SCOPE_PROCESS is inherited across
// fork/exec (SDK header + man setiopolicy_np), so a child claude keeps it; the
// current thread's scope is reset to DEFAULT so a stray thread-level OFF cannot
// override the process ON.
package execguard

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"syscall"
	"time"
	"unsafe"
)

const (
	sysIopolicysys = 322 // SYS_iopolicysys
	iopolCmdSet    = 2   // IOPOL_CMD_SET (0x2; GET is 0x1 — see <sys/resource_private.h>)

	iopolTypeVFSMaterializeDatalessFiles = 3 // IOPOL_TYPE_VFS_MATERIALIZE_DATALESS_FILES
	iopolScopeProcess                    = 0 // IOPOL_SCOPE_PROCESS (inherited across fork/exec)
	iopolScopeThread                     = 1 // IOPOL_SCOPE_THREAD
	iopolMaterializeDatalessFilesDefault = 0 // IOPOL_MATERIALIZE_DATALESS_FILES_DEFAULT
	iopolMaterializeDatalessFilesOff     = 1 // IOPOL_MATERIALIZE_DATALESS_FILES_OFF
	iopolMaterializeDatalessFilesOn      = 2 // IOPOL_MATERIALIZE_DATALESS_FILES_ON
)

// materializeReadTimeout bounds the pre-exec settings.json read so a wedged domain
// cannot hang the launch forever.
const materializeReadTimeout = 10 * time.Second

// iopolParam mirrors xnu's struct _iopol_param_t { int iop_scope; int iop_iotype;
// int iop_policy; } — the copyin target of iopolicysys(IOPOL_CMD_SET, &param).
type iopolParam struct {
	scope  int32
	iotype int32
	policy int32
}

func setiopolicy(iotype, scope, policy int32) error {
	p := iopolParam{scope: scope, iotype: iotype, policy: policy}
	// #nosec G103 -- iopolicysys requires a pointer to this fixed kernel parameter struct.
	_, _, errno := syscall.Syscall(sysIopolicysys, iopolCmdSet, uintptr(unsafe.Pointer(&p)), 0)
	runtime.KeepAlive(&p)
	if errno != 0 {
		return errno
	}
	return nil
}

// enable pins the OS thread, turns process-scope dataless materialization ON, and
// resets the current thread's scope to DEFAULT. The returned restore reverts the
// process scope and unpins the thread.
func enable() (restore func() error, err error) {
	runtime.LockOSThread()
	if err := setiopolicy(iopolTypeVFSMaterializeDatalessFiles, iopolScopeProcess, iopolMaterializeDatalessFilesOn); err != nil {
		runtime.UnlockOSThread()
		return nil, fmt.Errorf("set process dataless-materialize policy on: %w", err)
	}
	if err := setiopolicy(iopolTypeVFSMaterializeDatalessFiles, iopolScopeThread, iopolMaterializeDatalessFilesDefault); err != nil {
		_ = setiopolicy(iopolTypeVFSMaterializeDatalessFiles, iopolScopeProcess, iopolMaterializeDatalessFilesDefault)
		runtime.UnlockOSThread()
		return nil, fmt.Errorf("reset thread dataless-materialize policy to default: %w", err)
	}
	return func() error {
		defer runtime.UnlockOSThread()
		if err := setiopolicy(iopolTypeVFSMaterializeDatalessFiles, iopolScopeProcess, iopolMaterializeDatalessFilesDefault); err != nil {
			return fmt.Errorf("restore process dataless-materialize policy: %w", err)
		}
		return nil
	}, nil
}

// PrimeForExec enables dataless-file materialization for the process (inherited by
// the exec'd child) and does one bounded complete read of settingsPath, proving it
// materialized before the exec. Any policy or read failure returns an error and the
// caller MUST abort the launch — a silent continue recreates the empty-file bug. It
// does not restore the policy: the process is about to be replaced by exec.
func PrimeForExec(settingsPath string) error {
	if _, err := enable(); err != nil {
		return err
	}
	return materializeRead(settingsPath, materializeReadTimeout)
}

// EnableForSpawn enables dataless-file materialization so a child spawned
// immediately after inherits it at fork. The returned restore reverts the process
// scope; a long-lived parent (the status TUI) MUST call it once the child's Start()
// has returned.
func EnableForSpawn() (restore func() error, err error) {
	return enable()
}

// materializeRead does one complete read of path, bounded by timeout so a wedged
// File Provider domain cannot hang the launch. The read runs in a goroutine on a
// fresh thread (thread scope DEFAULT), so the process-scope ON policy still forces
// materialization.
func materializeRead(path string, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- readFull(path) }()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("materialize read %s: timed out after %s (domain wedged?)", path, timeout)
	}
}

func readFull(path string) error {
	f, err := os.Open(path) //nolint:gosec // path is the selected account's own settings.json, not external input
	if err != nil {
		return fmt.Errorf("materialize read %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(io.Discard, f); err != nil {
		return fmt.Errorf("materialize read %s: %w", path, err)
	}
	return nil
}
