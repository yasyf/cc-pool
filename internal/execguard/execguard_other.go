//go:build !darwin

package execguard

import (
	"context"
	"fmt"
	"runtime"
)

// PrimeForExec is macOS-only (File Provider dataless materialization); it errors
// cleanly elsewhere. cc-pool is macOS-only, so no launch reaches it.
func PrimeForExec(settingsPath string) error {
	return fmt.Errorf("dataless-file materialization is only available on darwin (GOOS=%s)", runtime.GOOS)
}

// PrimeForExecContext is the context-aware form of PrimeForExec.
func PrimeForExecContext(ctx context.Context, settingsPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return PrimeForExec(settingsPath)
}

// EnableForSpawn is macOS-only; it errors cleanly elsewhere.
func EnableForSpawn() (restore func() error, err error) {
	return nil, fmt.Errorf("dataless-file materialization is only available on darwin (GOOS=%s)", runtime.GOOS)
}
