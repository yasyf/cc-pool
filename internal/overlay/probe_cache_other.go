//go:build !darwin

package overlay

import "os"

// disableReadCache is a no-op off Darwin: F_NOCACHE is macOS-only.
func disableReadCache(_ *os.File) error { return nil }
