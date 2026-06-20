package cli

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
)

func newSelectCmd() *cobra.Command {
	var (
		noDaemon bool
		wait     bool
		account  int
		fresh    time.Duration
	)
	cmd := &cobra.Command{
		Use:   "select",
		Short: "Print the config dir of the emptiest account",
		Long: `select scores every account and prints only the chosen account's config dir
to stdout, so it composes as:

    CLAUDE_CODE_PLUGIN_CACHE_DIR="$HOME/.claude/plugins" CLAUDE_CODE_DEBUG_LOGS_DIR="$HOME/.claude/debug" CLAUDE_CONFIG_DIR=$(ccp select) claude

(Prefer ` + "`ccp run`" + `, which sets these vars itself. The plugin var keeps the
session writing canonical ~/.claude plugin paths into the shared plugin state;
the debug var keeps DEBUG=1's verbose per-session log off the fuse-t mirror.)

Diagnostics go to stderr. With the daemon running, select reads its cached
scores; otherwise it samples usage live.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				if err := requireInit(m); err != nil {
					return err
				}
				// Best-effort: an unreadable cwd just disables stickiness.
				cwd, _ := os.Getwd()
				req := selectReq{wait: wait, fresh: fresh, noDaemon: noDaemon, cwd: cwd}
				if cmd.Flags().Changed("account") {
					req.account = &account
				}
				dir, line, err := resolveSelection(cmd, m, req)
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), dir)
				announceLine(cmd, line)
				return nil
			})
		},
	}
	cmd.Flags().BoolVar(&noDaemon, "no-daemon", false, "do not use the daemon; sample usage live")
	cmd.Flags().BoolVar(&wait, "wait", false, "wait for an account with headroom instead of failing or using an exhausted one")
	cmd.Flags().IntVar(&account, "account", 0, "force a specific account id")
	cmd.Flags().DurationVar(&fresh, "fresh", pool.DefaultFreshFor, "reuse cached usage newer than this (live mode)")
	return cmd
}

// selectReq carries the per-invocation knobs that differ between `ccp run` and
// `ccp select`; the pipeline (forced → daemon → live) is identical.
type selectReq struct {
	account  *int          // forced pick (CCP_ACCOUNT / --account); nil = auto-score
	wait     bool          // wait for availability instead of failing (--wait)
	fresh    time.Duration // live-mode cache window (--fresh)
	noDaemon bool          // skip the daemon, sample live (--no-daemon)
	cwd      string        // keys select stickiness; empty disables it
	// pid marks the pick with a session checkout. `ccp run` passes its own
	// pid — exec replaces it with claude in place, so the row tracks the real
	// session and feeds the sticky activity rules. `ccp select` leaves it 0:
	// the process printing the dir exits before claude starts.
	pid int
}

// resolveSelection runs the full account-selection pipeline shared by `ccp run`
// and `ccp select` and returns the chosen config dir plus a formatted diagnostic
// line. A forced account resolves directly; otherwise it takes the daemon's
// reserved pick, falling back to live scoring (optionally waiting) when the
// daemon is unreachable or every account is busy. Picks it makes itself
// (forced/live) get an overlay sync + preflight refresh; a daemon-served pick is
// already prepared by the daemon (its poller syncs overlays and it preflights its
// own pick). Only warnings reach stderr — the caller owns dir output and printing
// the diagnostic line.
func resolveSelection(cmd *cobra.Command, m *pool.Manager, req selectReq) (dir, line string, err error) {
	// Forced account: resolve directly, no scoring (hence no headroom to report).
	if req.account != nil {
		a, err := m.Store.GetAccount(*req.account)
		if err != nil {
			return "", "", err
		}
		// Route the pick through the daemon when one is up at-version: its
		// forced path holds gates a client cannot see — an overlay conversion
		// mid-flight on the dir, mount state, reservations — and refusing
		// there beats landing a claude in a dir being remade under it. With
		// no daemon, no conversion can be running either (conversions are
		// daemon-only), so the local path below is safe.
		viaDaemon := false
		if !req.noDaemon {
			cl := daemon.NewClient()
			if cl.EnsureRunning(2*time.Second) && daemonAt(version.String()) {
				if resp, ok := cl.Select(req.account, req.pid, false, req.cwd, false); ok {
					if !resp.OK {
						return "", "", errors.New(resp.Error)
					}
					viaDaemon = true // daemon recorded the session, sticky, and reservation
				}
			}
		}
		if !viaDaemon {
			_ = m.RecordSticky(req.cwd, a.ID, time.Now()) // anchor future selects here
			if req.pid > 0 {
				// Best-effort bookkeeping, like RecordSticky: the session row
				// feeds the sticky activity rules.
				_, _ = m.Store.OpenSession(a.ID, req.pid, a.ConfigDir, req.cwd, time.Now())
			}
		}
		dir, err := prepareAccount(cmd, m, a)
		// Forced pick: no scoring, so no usage to report (hasUsage=false → bare name).
		return dir, selectionLine(accountName(a.Label), false, false, 0, 0), err
	}

	// Fast path: the daemon's cached, reserved pick. EnsureRunning keeps a daemon
	// alive to adopt any token claude rotates after we exec away; a version-skewed
	// daemon scores with stale logic, so ignore it until status/add/init restarts
	// it onto the current binary.
	if !req.noDaemon {
		cl := daemon.NewClient()
		if cl.EnsureRunning(2*time.Second) && daemonAt(version.String()) {
			// req.pid is `ccp run`'s own pid (the future claude pid, see
			// selectReq) or 0 for `ccp select`, whose pid-0 picks lean on
			// procscan to attribute the live process. We still reserve
			// (anti-thundering-herd). --wait refuses exhausted fallback picks
			// (it would discard them, and the daemon must not commit their
			// sticky/reservation side effects).
			if resp, ok := cl.Select(nil, req.pid, false, req.cwd, req.wait); ok {
				switch daemonSelectOutcome(resp, req.wait) {
				case outcomePicked:
					if resp.ExhaustedFallback {
						warnExhaustedFallback(cmd, daemonAccountName(m, resp.SelectedID), resp.ExtraEnabled, derefTime(resp.SoonestReset))
					}
					warnPinHeld(cmd, m, resp.PinHeldAccount, resp.SelectedID)
					mergeDaemonPick(cmd, m, resp.SelectedID)
					return resp.Dir, daemonSelectionLine(m, resp), nil
				case outcomeError:
					return "", "", errors.New(resp.Error)
				case outcomeWait:
					if resp.SoonestReset != nil {
						step(cmd.ErrOrStderr(), "All accounts are busy; waiting until %s.", humanizeReset(*resp.SoonestReset))
					}
					// fall through to the live waiting loop
				case outcomeFail:
					if resp.MountsNotReady {
						return "", "", pool.ErrMountsNotReady
					}
					return "", "", pool.ErrNoneAvailable
				}
			}
		}
	}

	// Live path (no daemon, or waiting): sample + score synchronously. Select
	// records stickiness (and the session row) itself.
	opts := pool.SelectOptions{Live: true, FreshFor: req.fresh, Cwd: req.cwd, PID: req.pid, NoFallback: req.wait}
	for {
		sr, err := m.Select(cmd.Context(), opts)
		if errors.Is(err, pool.ErrNoneAvailable) {
			if !req.wait {
				step(cmd.ErrOrStderr(), "No account is available right now; all are exhausted or rate-limited.")
				return "", "", err
			}
			reset, ok := sr.SoonestReset()
			d := 15 * time.Second
			if ok {
				step(cmd.ErrOrStderr(), "All accounts are exhausted or rate-limited; soonest reset at %s.", humanizeReset(reset))
				if until := time.Until(reset); until > 0 && until < d {
					d = until
				}
			}
			select {
			case <-cmd.Context().Done():
				return "", "", cmd.Context().Err()
			case <-time.After(d):
				continue
			}
		}
		if err != nil {
			return "", "", err
		}
		if sr.ExhaustedFallback {
			warnExhaustedFallback(cmd, accountName(sr.Best.Label), sr.ExtraEnabled, sr.Result.ExhaustedUntil)
		}
		warnPinHeld(cmd, m, sr.PinHeldAccount, &sr.Best.ID)
		dir, err := prepareAccount(cmd, m, sr.Best)
		return dir, liveSelectionLine(sr), err
	}
}

// warnPinHeld prints the bypass notice for a held manual pin: the user
// explicitly pinned this directory, so silently launching elsewhere would
// violate their expectation. No notice when the free ranking landed on the
// pinned account anyway — the pin was honored in effect.
func warnPinHeld(cmd *cobra.Command, m *pool.Manager, held, selected *int) {
	if held == nil || (selected != nil && *selected == *held) {
		return
	}
	warn(cmd.ErrOrStderr(), "manual pin to %s is rate-limited or out of headroom — using %s; pin kept",
		daemonAccountName(m, held), daemonAccountName(m, selected))
}

// selectOutcome classifies a daemon select reply for resolveSelection.
type selectOutcome int

const (
	outcomePicked selectOutcome = iota // use resp.Dir
	outcomeWait                        // none available, caller waits via the live loop
	outcomeFail                        // none available, caller errors
	outcomeError                       // a real daemon error
)

// daemonSelectOutcome classifies a daemon select reply. NoneAvailable is
// checked before Error — the daemon sets both for a none-available result, and
// matching on Error first made the --wait arm unreachable. The first arm is a
// backstop: a --wait request sends NoFallback so the daemon should never offer
// an exhausted fallback pick, but if one arrives anyway the caller's explicit
// choice of waiting over billing extra-usage credits must win.
func daemonSelectOutcome(resp *daemon.Response, wait bool) selectOutcome {
	switch {
	case resp.OK && resp.Dir != "" && resp.ExhaustedFallback && wait:
		return outcomeWait
	case resp.OK && resp.Dir != "":
		return outcomePicked
	case resp.NoneAvailable && wait:
		return outcomeWait
	case resp.NoneAvailable:
		return outcomeFail
	case resp.Error != "":
		return outcomeError
	case wait:
		return outcomeWait
	default:
		return outcomeFail
	}
}

// warnExhaustedFallback prints the loud warning for an all-exhausted fallback
// pick: the launched session bills pay-as-you-go credits (extra usage enabled)
// or just rate-limits until the account recovers. recoversAt is the binding
// reset — the latest among the pegged windows, not necessarily the 5h one.
// Always written to stderr — this is the one selection outcome that can
// silently cost money.
func warnExhaustedFallback(cmd *cobra.Command, name string, extraEnabled bool, recoversAt time.Time) {
	until := ""
	if !recoversAt.IsZero() {
		until = fmt.Sprintf(" (resets at %s)", humanizeReset(recoversAt))
	}
	if extraEnabled {
		warn(cmd.ErrOrStderr(), "all accounts have exhausted their plan limits — using %s; this WILL bill extra-usage credits%s", name, until)
		return
	}
	warn(cmd.ErrOrStderr(), "all accounts have exhausted their plan limits — using %s; it will be rate-limited until its window resets%s", name, until)
}

// derefTime returns the pointed-to time, or the zero time for nil.
func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// prepareAccount re-asserts the account's overlay, merges the base's shareable
// .claude.json settings, and preflight-refreshes its token — the daemonless
// equivalent of what the daemon does for its own picks — then returns the
// config dir. Warnings go to stderr; the caller prints the success line.
func prepareAccount(cmd *cobra.Command, m *pool.Manager, a store.Account) (string, error) {
	if err := m.SyncOverlay(a); err != nil {
		warn(cmd.ErrOrStderr(), "couldn't sync this account's settings: %v", err)
	}
	mergeLaunchSettings(cmd, m, a)
	if err := m.PreflightRefresh(cmd.Context(), a); err != nil {
		if errors.Is(err, pool.ErrNeedsLogin) {
			warn(cmd.ErrOrStderr(), "%s needs to log in again; run `ccp add` or `claude /login`", accountName(a.Label))
		} else {
			warn(cmd.ErrOrStderr(), "%v", err)
		}
	}
	return a.ConfigDir, nil
}

// mergeLaunchSettings propagates ~/.claude.json's shareable settings into the
// account about to be launched, warning and continuing on failure — the same
// warn-and-launch contract as SyncOverlay and PreflightRefresh: a malformed
// ~/.claude.json must not brick every pooled launch.
func mergeLaunchSettings(cmd *cobra.Command, m *pool.Manager, a store.Account) {
	if _, err := m.MergeBaseClaudeJSON(a); err != nil {
		warn(cmd.ErrOrStderr(), "couldn't propagate shared settings from ~/.claude.json: %v", err)
	}
}

// mergeDaemonPick runs the launch-settings merge for a daemon-served pick,
// which returns before prepareAccount runs. A nil or unknown SelectedID warns
// and skips, degrading like daemonAccountName.
func mergeDaemonPick(cmd *cobra.Command, m *pool.Manager, id *int) {
	if id == nil {
		warn(cmd.ErrOrStderr(), "daemon pick carried no account id; skipping the shared-settings merge")
		return
	}
	a, err := m.Store.GetAccount(*id)
	if err != nil {
		warn(cmd.ErrOrStderr(), "couldn't load account %d for the shared-settings merge: %v", *id, err)
		return
	}
	mergeLaunchSettings(cmd, m, a)
}

// announceLine prints the selection diagnostic to stderr, but only when stdout is
// an interactive terminal — captured/piped callers ($(ccp select)) get the bare
// dir on stdout and nothing else.
func announceLine(cmd *cobra.Command, line string) {
	if !stdoutIsTTY() {
		return
	}
	step(cmd.ErrOrStderr(), "%s", line)
}

// selectionLine formats the selection diagnostic: "Selected <name>", or
// "Reusing <name> (pinned)" for a sticky pick, plus raw 5h/7d percent-used when
// known. The account name is accented and the usage figures health-tinted.
func selectionLine(name string, sticky, hasUsage bool, used5, used7 float64) string {
	verb := "Selected"
	styledName := bestStyle.Render(name)
	if sticky {
		verb = "Reusing"
		styledName += dimStyle.Render(" (pinned)")
	}
	return fmt.Sprintf("%s %s%s", verb, styledName, usageSuffix(hasUsage, used5, used7))
}

// daemonSelectionLine builds the diagnostic from a daemon select reply: the name
// resolved from SelectedID (degrading to "account") plus its raw 5h/7d usage.
func daemonSelectionLine(m *pool.Manager, resp *daemon.Response) string {
	return selectionLine(daemonAccountName(m, resp.SelectedID), resp.Sticky, resp.HasUsage, 100-resp.Remaining5h, 100-resp.Remaining7d)
}

// liveSelectionLine builds the diagnostic from a live scoring result.
func liveSelectionLine(sr *pool.SelectResult) string {
	return selectionLine(accountName(sr.Best.Label), sr.Sticky, sr.HasUsage, sr.Util5h, sr.Util7d)
}
