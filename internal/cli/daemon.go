package cli

import (
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/fusekit/proc"
)

// reexecStable and daemonRun are seams so the daemon RunE is testable without a
// real self-exec: the fork-storm hazard class demands the real syscall.Exec stay
// unreachable from test binaries, so it lives here — not in daemon.Run, the
// launchd entry point.
var (
	reexecStable = proc.ReexecStable
	daemonRun    = daemon.Run
)

func newDaemonCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "daemon",
		Short:  "Run the background daemon used by the LaunchAgent",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			reexecFromStableBin(cmd.ErrOrStderr())
			return daemonRun(cmd.Context())
		},
	}
}

// reexecFromStableBin re-execs the daemon from pool.StableBinDir() so the macOS
// TCC app-group-container grant — keyed by resolved executable path — survives
// the per-version keg paths a `brew upgrade` churns through. Loud-log-and-
// continue: a failure here is never fatal to the daemon; it only means the grant
// stays keyed to this build's per-version path and macOS may re-prompt after
// upgrades. ReexecStable is a no-op once the process already runs from the stable
// path, so the surviving process reaches daemonRun.
func reexecFromStableBin(errOut io.Writer) {
	dir := pool.StableBinDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		warn(errOut, "daemon self-exec skipped: cannot create the stable exec dir %s (%v); TCC app-group grants stay keyed to this build's per-version path and macOS may re-prompt after upgrades", dir, err)
		return
	}
	if err := reexecStable(dir, "cc-pool"); err != nil {
		warn(errOut, "daemon self-exec from %s failed (%v); TCC app-group grants stay keyed to this build's per-version path and macOS may re-prompt after upgrades", dir, err)
	}
}
