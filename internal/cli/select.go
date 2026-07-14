package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/version"
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
				// Unreadable cwd just disables stickiness.
				cwd, _ := os.Getwd()
				req := selectReq{wait: wait, fresh: fresh, noDaemon: noDaemon, cwd: cwd}
				if cmd.Flags().Changed("account") {
					req.account = &account
				}
				selection, err := resolveSelectionTxn(cmd, m, req)
				if err != nil {
					return err
				}
				defer selection.Abort()
				// select prints the dir for the invoking shell to run claude, so the
				// session lease must outlive ccp: a detached agent holds it until the
				// terminal's session leader exits. Block on the agent's acquired+probed
				// handshake BEFORE printing anything — a failure hands out nothing and
				// exits non-zero rather than launching claude onto an unprotected dir.
				if err := commitSelectionWithLease(cmd.Context(), selection); err != nil {
					return err
				}
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), selection.dir)
				announceLine(cmd, selection.line)
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

func commitSelectionWithLease(ctx context.Context, selection *selectionTxn) error {
	if ctx == nil {
		ctx = context.Background()
	}
	agent, err := spawnLeaseAgent(selection.acct)
	if err != nil {
		return fmt.Errorf("couldn't hold the session lease for %s: %w", accountName(selection.acct.Label), err)
	}
	if err := selection.Commit(ctx); err != nil {
		commitErr := fmt.Errorf("commit selection: %w", err)
		if agent == nil {
			return commitErr
		}
		if stopErr := agent.Stop(); stopErr != nil {
			return errors.Join(commitErr, stopErr)
		}
		return commitErr
	}
	return nil
}

type selectReq struct {
	account  *int          // forced pick (CCP_ACCOUNT / --account); nil = auto-score
	wait     bool          // wait for availability instead of failing (--wait)
	fresh    time.Duration // live-mode cache window (--fresh)
	noDaemon bool          // skip the daemon, sample live (--no-daemon)
	cwd      string        // keys select stickiness; empty disables it
	// pid tags the session row: `ccp run` passes its own (exec replaces it in
	// place), `ccp select` passes 0 so procscan attributes the live process.
	pid        int
	excludeIDs []int
	ctx        context.Context
}

func (r selectReq) context(cmd *cobra.Command) context.Context {
	if r.ctx != nil {
		return r.ctx
	}
	if ctx := cmd.Context(); ctx != nil {
		return ctx
	}
	return context.Background()
}

type selectionTxn struct {
	acct   store.Account
	dir    string
	line   string
	commit func(context.Context) error
	abort  func()
	done   bool
}

func (s *selectionTxn) Commit(ctx context.Context) error {
	if s.done {
		return errors.New("selection transaction already finished")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.commit(ctx); err != nil {
		return err
	}
	s.done = true
	return nil
}

func (s *selectionTxn) Abort() {
	if s == nil || s.done {
		return
	}
	s.abort()
	s.done = true
}

// resolveSelection runs the shared `ccp run`/`ccp select` pipeline (forced →
// daemon → live), returning the selected account so callers can take its session
// lease; the caller owns stdout and the diagnostic line.
func resolveSelection(cmd *cobra.Command, m *pool.Manager, req selectReq) (acct store.Account, dir, line string, err error) {
	selection, err := resolveSelectionTxn(cmd, m, req)
	if err != nil {
		return store.Account{}, "", "", err
	}
	defer selection.Abort()
	if err := selection.Commit(req.context(cmd)); err != nil {
		return store.Account{}, "", "", fmt.Errorf("commit selection: %w", err)
	}
	return selection.acct, selection.dir, selection.line, nil
}

func resolveSelectionTxn(cmd *cobra.Command, m *pool.Manager, req selectReq) (*selectionTxn, error) {
	ctx := req.context(cmd)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.account != nil {
		a, err := m.Store.GetAccount(*req.account)
		if err != nil {
			return nil, err
		}
		for _, id := range req.excludeIDs {
			if id == a.ID {
				return nil, fmt.Errorf("forced account %d is excluded", a.ID)
			}
		}
		// An at-version daemon holds gates a client can't see (overlay conversion,
		// mounts, reservations); prefer it over launching claude into a dir being
		// remade. Daemonless, conversions can't run, so local prep is safe.
		if !req.noDaemon {
			cl := daemon.NewClient()
			if cl.EnsureRunning(daemonEnsureTimeout(ctx)) && daemonClientAt(ctx, cl, version.String()) {
				if resp, ok := cl.Select(ctx, req.account, req.pid, false, req.cwd, false, req.excludeIDs); ok {
					if !resp.OK {
						return nil, errors.New(resp.Error)
					}
					if err := ctx.Err(); err != nil {
						abortDaemonSelection(cl, resp.ReservationToken)
						return nil, err
					}
					picked, err := validateDaemonSelection(m, resp, &a)
					if err != nil {
						abortDaemonSelection(cl, resp.ReservationToken)
						return nil, err
					}
					if resp.ReservationToken == "" {
						return nil, fmt.Errorf("invalid daemon selection: empty reservation token for id %d", picked.ID)
					}
					dir, err := prepareAccount(ctx, cmd, m, picked)
					if err != nil {
						abortDaemonSelection(cl, resp.ReservationToken)
						return nil, err
					}
					return &selectionTxn{
						acct: picked, dir: dir, line: selectionLine(accountName(picked.Label), false, false, 0, 0, "", 0),
						commit: func(ctx context.Context) error { return cl.CommitSelection(ctx, resp.ReservationToken) },
						abort:  func() { abortDaemonSelection(cl, resp.ReservationToken) },
					}, nil
				}
			}
		}
		dir, err := prepareAccount(ctx, cmd, m, a)
		if err != nil {
			return nil, err
		}
		return &selectionTxn{
			acct: a, dir: dir, line: selectionLine(accountName(a.Label), false, false, 0, 0, "", 0),
			commit: func(context.Context) error {
				return m.Store.CommitSelection(a.ID, req.pid, a.ConfigDir, req.cwd, time.Now(), true)
			},
			abort: func() {},
		}, nil
	}

	// EnsureRunning so a daemon outlives the exec to adopt tokens claude
	// rotates; a version-skewed daemon (stale scoring) is ignored until
	// status/add/init restarts it.
	if !req.noDaemon {
		cl := daemon.NewClient()
		if cl.EnsureRunning(daemonEnsureTimeout(ctx)) && daemonClientAt(ctx, cl, version.String()) {
			// Reserve even pid-0 picks (anti-thundering-herd). --wait sends
			// NoFallback: the daemon must not commit sticky/reservation side
			// effects for a pick the client would discard.
			if resp, ok := cl.Select(ctx, nil, req.pid, false, req.cwd, req.wait, req.excludeIDs); ok {
				if err := ctx.Err(); err != nil {
					if resp.OK {
						abortDaemonSelection(cl, resp.ReservationToken)
					}
					return nil, err
				}
				switch daemonSelectOutcome(resp, req.wait) {
				case outcomePicked:
					a, err := validateDaemonSelection(m, resp, nil)
					if err != nil {
						abortDaemonSelection(cl, resp.ReservationToken)
						return nil, err
					}
					if resp.ReservationToken == "" {
						return nil, fmt.Errorf("invalid daemon selection: empty reservation token for id %d", a.ID)
					}
					if resp.ExhaustedFallback {
						warnExhaustedFallback(cmd, daemonAccountName(m, resp.SelectedID), resp.ExtraEnabled, derefTime(resp.SoonestReset))
					}
					warnPinHeld(cmd, m, resp.PinHeldAccount, resp.SelectedID)
					mergeLaunchSettings(cmd, m, a)
					return &selectionTxn{
						acct: a, dir: a.ConfigDir, line: daemonSelectionLine(m, resp),
						commit: func(ctx context.Context) error { return cl.CommitSelection(ctx, resp.ReservationToken) },
						abort:  func() { abortDaemonSelection(cl, resp.ReservationToken) },
					}, nil
				case outcomeError:
					if resp.OK {
						abortDaemonSelection(cl, resp.ReservationToken)
					}
					return nil, errors.New(resp.Error)
				case outcomeWait:
					if resp.OK {
						abortDaemonSelection(cl, resp.ReservationToken)
					}
					if resp.SoonestReset != nil {
						step(cmd.ErrOrStderr(), "All accounts are busy; waiting until %s.", humanizeReset(*resp.SoonestReset))
					}
				case outcomeFail:
					if resp.OK {
						abortDaemonSelection(cl, resp.ReservationToken)
					}
					if resp.MountsNotReady {
						return nil, pool.ErrMountsNotReady
					}
					return nil, pool.ErrNoneAvailable
				}
			}
		}
	}

	// Live selection defers stickiness and the session row to the caller's commit.
	opts := pool.SelectOptions{Live: true, FreshFor: req.fresh, Cwd: req.cwd, PID: req.pid, NoFallback: req.wait, ExcludeIDs: req.excludeIDs, DeferCommit: true}
	for {
		sr, err := m.Select(req.context(cmd), opts)
		if errors.Is(err, pool.ErrNoneAvailable) {
			if !req.wait {
				step(cmd.ErrOrStderr(), "No account is available right now; all are exhausted or rate-limited.")
				return nil, err
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
			case <-req.context(cmd).Done():
				return nil, req.context(cmd).Err()
			case <-time.After(d):
				continue
			}
		}
		if err != nil {
			return nil, err
		}
		if sr.ExhaustedFallback {
			warnExhaustedFallback(cmd, accountName(sr.Best.Label), sr.ExtraEnabled, sr.Result.ExhaustedUntil)
		}
		warnPinHeld(cmd, m, sr.PinHeldAccount, &sr.Best.ID)
		dir, err := prepareAccount(ctx, cmd, m, sr.Best)
		if err != nil {
			return nil, err
		}
		return &selectionTxn{
			acct: sr.Best, dir: dir, line: liveSelectionLine(sr),
			commit: func(context.Context) error { return m.CommitSelection(sr, req.cwd, req.pid) },
			abort:  func() {},
		}, nil
	}
}

func daemonEnsureTimeout(ctx context.Context) time.Duration {
	const limit = 2 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < limit {
			if remaining <= 0 {
				return time.Nanosecond
			}
			return remaining
		}
	}
	return limit
}

func daemonClientAt(ctx context.Context, cl *daemon.Client, wantVersion string) bool {
	resp, err := cl.HealthContext(ctx)
	return err == nil && resp.OK && resp.Version == wantVersion
}

func abortDaemonSelection(cl *daemon.Client, token string) {
	if token == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = cl.AbortSelection(ctx, token)
}

func validateDaemonSelection(m *pool.Manager, resp *daemon.Response, forced *store.Account) (store.Account, error) {
	expectedDir := "<unknown>"
	if forced != nil {
		expectedDir = forced.ConfigDir
	}
	if resp.SelectedID == nil {
		return store.Account{}, fmt.Errorf("invalid daemon selection: id <nil>, expected dir %q, returned dir %q", expectedDir, resp.Dir)
	}
	id := *resp.SelectedID
	a, err := m.Store.GetAccount(id)
	if err != nil {
		return store.Account{}, fmt.Errorf("invalid daemon selection: id %d, expected dir %q, returned dir %q: %w", id, expectedDir, resp.Dir, err)
	}
	if resp.Dir != a.ConfigDir {
		return store.Account{}, fmt.Errorf("invalid daemon selection: id %d, expected dir %q, returned dir %q", id, a.ConfigDir, resp.Dir)
	}
	if forced != nil && id != forced.ID {
		return store.Account{}, fmt.Errorf("invalid daemon selection: id %d does not match forced account %d, expected dir %q, returned dir %q", id, forced.ID, forced.ConfigDir, resp.Dir)
	}
	return a, nil
}

func warnPinHeld(cmd *cobra.Command, m *pool.Manager, held, selected *int) {
	if held == nil || (selected != nil && *selected == *held) {
		return
	}
	warn(cmd.ErrOrStderr(), "manual pin to %s is rate-limited or out of headroom — using %s; pin kept",
		daemonAccountName(m, held), daemonAccountName(m, selected))
}

type selectOutcome int

const (
	outcomePicked selectOutcome = iota // use resp.Dir
	outcomeWait                        // none available, caller waits via the live loop
	outcomeFail                        // none available, caller errors
	outcomeError                       // a real daemon error
)

// daemonSelectOutcome classifies a daemon select reply: check NoneAvailable
// before Error (the daemon sets both); an exhausted fallback arriving despite
// NoFallback must wait, not bill credits.
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

// warnExhaustedFallback flags the one outcome that can silently cost money.
// recoversAt is the binding reset: the latest pegged window, not necessarily 5h.
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

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// prepareAccount is the daemonless equivalent of the daemon's own pick prep.
func prepareAccount(ctx context.Context, cmd *cobra.Command, m *pool.Manager, a store.Account) (string, error) {
	if err := m.SyncOverlayContext(ctx, a); err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		warn(cmd.ErrOrStderr(), "couldn't sync this account's settings: %v", err)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	mergeLaunchSettings(cmd, m, a)
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := m.PreflightRefresh(ctx, a); err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		warnPreflight(cmd.ErrOrStderr(), a, err)
	}
	return a.ConfigDir, nil
}

// warnPreflight translates a non-fatal preflight-refresh error into operator
// guidance on stderr.
func warnPreflight(w io.Writer, a store.Account, err error) {
	switch {
	case errors.Is(err, pool.ErrNeedsLogin):
		warn(w, "%s needs to log in again; run `ccp login %d`", accountName(a.Label), a.ID)
	case errors.Is(err, pool.ErrUnrefreshable):
		warn(w, "%s's token is expiring and this machine holds a synced copy it can't refresh — the origin rotates it, or run `ccp login %d` here", accountName(a.Label), a.ID)
	default:
		warn(w, "%v", err)
	}
}

// mergeLaunchSettings warns rather than fails: a malformed ~/.claude.json must
// not brick every pooled launch.
func mergeLaunchSettings(cmd *cobra.Command, m *pool.Manager, a store.Account) {
	if _, err := m.MergeBaseClaudeJSON(a); err != nil {
		warn(cmd.ErrOrStderr(), "couldn't propagate shared settings from ~/.claude.json: %v", err)
	}
}

// announceLine prints the diagnostic to stderr only when stdout is a TTY:
// captured $(ccp select) callers get the bare dir and nothing else.
func announceLine(cmd *cobra.Command, line string) {
	if !stdoutIsTTY() {
		return
	}
	step(cmd.ErrOrStderr(), "%s", line)
}

func selectionLine(name string, sticky, hasUsage bool, used5, used7 float64, scopedModel string, scopedUsed float64) string {
	verb := "Selected"
	styledName := bestStyle.Render(name)
	if sticky {
		verb = "Reusing"
		styledName += dimStyle.Render(" (pinned)")
	}
	return fmt.Sprintf("%s %s%s", verb, styledName, usageSuffix(hasUsage, used5, used7, scopedModel, scopedUsed))
}

func daemonSelectionLine(m *pool.Manager, resp *daemon.Response) string {
	return selectionLine(daemonAccountName(m, resp.SelectedID), resp.Sticky, resp.HasUsage, 100-resp.Remaining5h, 100-resp.Remaining7d, resp.Scoped7dModel, resp.Scoped7dUtil)
}

func liveSelectionLine(sr *pool.SelectResult) string {
	return selectionLine(accountName(sr.Best.Label), sr.Sticky, sr.HasUsage, sr.Util5h, sr.Util7d, sr.Scoped7dModel, sr.Scoped7dUtil)
}
