//go:build darwin

// Package statuspackage installs the fixed signed application delivered with cc-pool.
package statuspackage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/statusapp"
)

const packagedDirectory = "libexec"

type operations struct {
	packagedPath  func() (string, error)
	installedPath func() string
	apply         func(context.Context, string) error
	uninstall     func(context.Context) error
	reset         func(context.Context) error
}

var defaultOperations = operations{
	packagedPath: PackagedPath, installedPath: pool.WidgetAppPath,
	apply: func(ctx context.Context, candidate string) error {
		_, err := statusapp.ApplyPackagedApp(ctx, candidate)
		return err
	},
	uninstall: statusapp.UninstallPackagedApp,
	reset:     statusapp.ResetPackagedApp,
}

// PackagedPath returns the application resource beside the resolved cc-pool executable.
func PackagedPath() (string, error) {
	executable, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("cc-pool package: resolve executable: %w", err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return "", fmt.Errorf("cc-pool package: resolve executable links: %w", err)
	}
	if !filepath.IsAbs(executable) || filepath.Clean(executable) != executable {
		return "", errors.New("cc-pool package: executable is not an exact absolute path")
	}
	prefix := filepath.Dir(filepath.Dir(executable))
	return filepath.Join(prefix, packagedDirectory, "CCPoolStatus.app"), nil
}

// Install applies the exact delivered application candidate through daemonkit.
func Install(ctx context.Context) error {
	return install(ctx, defaultOperations)
}

func install(ctx context.Context, ops operations) error {
	source, err := ops.packagedPath()
	if err != nil {
		return err
	}
	target := ops.installedPath()
	if source == target {
		return errors.New("cc-pool package: packaged and installed application paths are identical")
	}
	if err := ensureRealDirectory(filepath.Dir(target)); err != nil {
		return err
	}
	if err := ops.apply(ctx, source); err != nil {
		return fmt.Errorf("cc-pool package: apply signed application: %w", err)
	}
	return nil
}

// Uninstall removes the controller-sealed application through daemonkit.
func Uninstall(ctx context.Context) error {
	return uninstall(ctx, defaultOperations)
}

func uninstall(ctx context.Context, ops operations) error {
	if err := ops.uninstall(ctx); err != nil {
		return fmt.Errorf("cc-pool package: uninstall signed application: %w", err)
	}
	return nil
}

// Reset retires the installed application's agents through daemonkit, leaving
// its installed bytes in place.
func Reset(ctx context.Context) error {
	return reset(ctx, defaultOperations)
}

func reset(ctx context.Context, ops operations) error {
	if err := ops.reset(ctx); err != nil {
		return fmt.Errorf("cc-pool package: reset signed application: %w", err)
	}
	return nil
}

func ensureRealDirectory(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("cc-pool package: application directory is not an exact absolute path")
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("cc-pool package: create application directory: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("cc-pool package: inspect application directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("cc-pool package: application directory is not a real directory")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("cc-pool package: resolve application directory: %w", err)
	}
	if resolved != path {
		return errors.New("cc-pool package: application directory is not a canonical real path")
	}
	return nil
}
