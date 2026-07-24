//go:build !darwin

// Package statuspackage installs the fixed signed application delivered with cc-pool.
package statuspackage

import (
	"context"
	"errors"
)

// Install reports that signed application packaging requires macOS.
func Install(context.Context) error {
	return errors.New("cc-pool package: signed application packaging requires macOS")
}

// Uninstall reports that signed application packaging requires macOS.
func Uninstall(context.Context) error {
	return errors.New("cc-pool package: signed application packaging requires macOS")
}
