package procscan

import (
	"errors"
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

// Identity returns pid's kernel start-time identity.
func Identity(pid int) (Proc, error) {
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		if errors.Is(err, unix.ESRCH) || errors.Is(err, unix.ENOENT) {
			return Proc{}, fmt.Errorf("process %d does not exist", pid)
		}
		return Proc{}, fmt.Errorf("read process %d identity: %w", pid, err)
	}
	started := kp.Proc.P_starttime
	return Proc{PID: pid, StartedAt: time.Unix(started.Sec, int64(started.Usec)*1000)}, nil
}
