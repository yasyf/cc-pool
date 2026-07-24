//go:build darwin

package accountterminal

import "golang.org/x/sys/unix"

func enableTerminalInterrupt(fd int) error {
	state, err := unix.IoctlGetTermios(fd, unix.TIOCGETA)
	if err != nil {
		return err
	}
	state.Lflag |= unix.ISIG
	state.Cc[unix.VQUIT] = 0xff
	state.Cc[unix.VSUSP] = 0xff
	state.Cc[unix.VDSUSP] = 0xff
	return unix.IoctlSetTermios(fd, unix.TIOCSETA, state)
}
