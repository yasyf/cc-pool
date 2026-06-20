package service

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// BrewLabel is the launchd label Homebrew assigns the cc-pool service.
const BrewLabel = "homebrew.mxcl." + FormulaName

// brewServices runs `brew services <action> cc-pool`, streaming brew's
// output to the user.
func brewServices(action string) error {
	//nolint:gosec // G204: a fixed `brew services <action> <FormulaName>` invocation; action is a cc-pool constant
	cmd := exec.Command("brew", "services", action, FormulaName)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}

// BrewStart starts the daemon via brew services (installs the user agent).
func BrewStart() error { return brewServices("start") }

// BrewKickstart ensures the brew-managed daemon is actually running.
// `brew services start` only bootstraps the job; on a stop/start bootout race it
// can leave the job loaded-but-never-running (RunAtLoad fires only at
// bootstrap), so we kick it explicitly. Plain `kickstart` (no `-k`) starts a
// loaded-but-not-running job and is a no-op when it is already running — so the
// daemon `brew services start` just launched is kept rather than killed and
// cold-started a second time. Version skew is handled separately (the caller
// stops the old service first; the daemon evicts a stale socket holder on
// startup), so a forceful restart is not needed here.
func BrewKickstart() error {
	target := domainTarget() + "/" + BrewLabel
	if out, err := launchctl("kickstart", target); err != nil {
		return fmt.Errorf("launchctl kickstart %s: %w: %s", target, err, out)
	}
	return nil
}

// BrewStop stops and unloads the brew-managed agent.
func BrewStop() error { return brewServices("stop") }

// BrewInfo returns `brew services info cc-pool` output for status display.
func BrewInfo() (string, error) {
	out, err := exec.Command("brew", "services", "info", FormulaName).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}
