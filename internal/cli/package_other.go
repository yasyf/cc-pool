//go:build !darwin

package cli

import (
	"context"
	"errors"
)

var (
	installPackage   = unsupportedPackage
	uninstallPackage = unsupportedPackage
	resetPackage     = unsupportedPackage
)

func unsupportedPackage(context.Context) error {
	return errors.New("cc-pool package: signed application packaging requires macOS")
}
