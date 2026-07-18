//go:build !darwin

package procscan

import "errors"

// Identity is unavailable because cc-pool session identity is macOS-specific.
func Identity(int) (Proc, error) {
	return Proc{}, errors.New("process identity is only supported on macOS")
}
