//go:build darwin

package cli

import "github.com/yasyf/cc-pool/internal/statuspackage"

var (
	installPackage   = statuspackage.Install
	uninstallPackage = statuspackage.Uninstall
	resetPackage     = statuspackage.Reset
)
