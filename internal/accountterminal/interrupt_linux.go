//go:build linux

package accountterminal

import "golang.org/x/sys/unix"

func enableTerminalInterrupt(fd int) error {
	state, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return err
	}
	state.Lflag |= unix.ISIG
	state.Cc[unix.VQUIT] = 0xff
	state.Cc[unix.VSUSP] = 0xff
	return unix.IoctlSetTermios(fd, unix.TCSETS, state)
}
