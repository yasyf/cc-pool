package daemon

import (
	"os"
	"os/exec"
	"syscall"
	"time"
)

// EnsureRunning returns true if the daemon is reachable, auto-spawning a
// detached `cc-pool daemon` and waiting up to timeout for its socket if it
// is not. A second instance is harmless: the daemon refuses to start if the
// socket is already owned. Used by the `select` hot path so it stays fast when
// the daemon is up and self-heals when it is not.
func (c *Client) EnsureRunning(timeout time.Duration) bool {
	if c.Available() {
		return true
	}
	exe, err := os.Executable()
	if err != nil {
		return false
	}
	//nolint:gosec // G204: exe is this binary's own resolved path; "daemon" is a fixed subcommand
	cmd := exec.Command(exe, "daemon")
	cmd.Stdin, cmd.Stdout, cmd.Stderr = nil, nil, nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} // detach from our session
	if err := cmd.Start(); err != nil {
		return false
	}
	_ = cmd.Process.Release()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.Available() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
