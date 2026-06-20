//go:build !darwin

package overlay

import "os"

// disableReadCache is a no-op off Darwin: F_NOCACHE is macOS-only, and the
// mount holder that runs the deep probe only ever runs on macOS/fuse-t. This
// stub keeps the untagged probe code compiling on every platform (the CI lint
// and vuln jobs build it on Linux) without changing behavior where it matters.
func disableReadCache(_ *os.File) error { return nil }
