//go:build !darwin

package cli

import "errors"

// errLeaseWatchUnsupported is the non-darwin stub verdict: cc-pool is macOS-only,
// so the pid-watch primitives exist only to keep cross-platform builds compiling.
var errLeaseWatchUnsupported = errors.New("session lease watching is only supported on macOS")

func realProcStartTime(int) (int64, error) { return 0, errLeaseWatchUnsupported }

func realRegisterProcExit(int) (procWaiter, error) { return nil, errLeaseWatchUnsupported }

func realSessionLeader() (int, error) { return 0, errLeaseWatchUnsupported }
