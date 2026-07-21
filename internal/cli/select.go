package cli

import (
	"context"
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
		wait    bool
		account int
	)
	cmd := &cobra.Command{
		Use:   "select",
		Short: "Print the config dir of the emptiest account",
		Long: `select asks the exact running daemon to prepare and inspect the best account,
then prints its config directory. It creates no session, reservation, sticky pin,
or launch ownership. Use ` + "`ccp run`" + ` to launch Claude Code.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(cmd.Context(), func(m *pool.Manager) error {
				if err := requireInit(m); err != nil {
					return err
				}
				// Unreadable cwd just disables stickiness.
				cwd, _ := os.Getwd()
				req := selectReq{wait: wait, cwd: cwd}
				if cmd.Flags().Changed("account") {
					req.account = &account
				}
				selection, err := resolveSelectionTxn(commandContext(cmd), cmd, m, req)
				if err != nil {
					return err
				}
				defer selection.Abort()
				// A raw select is inspect-only; its commit is intentionally a no-op.
				if err := selection.Commit(cmd.Context()); err != nil {
					return fmt.Errorf("commit selection: %w", err)
				}
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), selection.dir)
				announceLine(cmd, selection.line)
				return nil
			})
		},
	}
	cmd.Flags().BoolVar(&wait, "wait", false, "wait for an account with headroom instead of failing or using an exhausted one")
	cmd.Flags().IntVar(&account, "account", 0, "force a specific account id")
	return cmd
}

type selectReq struct {
	account *int   // forced pick (CCP_ACCOUNT / --account); nil = auto-score
	wait    bool   // wait for availability instead of failing (--wait)
	cwd     string // keys select stickiness; empty disables it
	// process tags only `ccp run`: exec replaces this exact kernel identity in
	// place. Raw select carries no process and is intrinsically inspect-only.
	process    store.ProcessIdentity
	excludeIDs []int
}

func commandContext(cmd *cobra.Command) context.Context {
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
	close  func()
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
	if s.close != nil {
		s.close()
	}
	return nil
}

func (s *selectionTxn) Abort() {
	if s == nil || s.done {
		return
	}
	s.abort()
	s.done = true
	if s.close != nil {
		s.close()
	}
}

// resolveSelection runs the shared daemon-owned `ccp run`/`ccp select` pipeline.
func resolveSelection(cmd *cobra.Command, m *pool.Manager, req selectReq) (acct store.Account, dir, line string, err error) {
	ctx := commandContext(cmd)
	selection, err := resolveSelectionTxn(ctx, cmd, m, req)
	if err != nil {
		return store.Account{}, "", "", err
	}
	defer selection.Abort()
	if err := selection.Commit(ctx); err != nil {
		return store.Account{}, "", "", fmt.Errorf("commit selection: %w", err)
	}
	return selection.acct, selection.dir, selection.line, nil
}

func resolveSelectionTxn(ctx context.Context, cmd *cobra.Command, m *pool.Manager, req selectReq) (*selectionTxn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var forced *store.Account
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
		forced = &a
	}

	cl := daemon.NewClient()
	keepClient := false
	defer func() {
		if !keepClient {
			_ = cl.Close()
		}
	}()
	health, err := cl.HealthContext(ctx)
	if errors.Is(err, daemon.ErrDaemonUnavailable) {
		health, err = startSelectionDaemon(ctx, cmd, cl)
	}
	if err != nil {
		return nil, fmt.Errorf("require daemon: %w", err)
	}
	if !health.OK {
		return nil, fmt.Errorf("require daemon: %s", health.Error)
	}
	if health.Version != version.String() {
		return nil, fmt.Errorf("require exact daemon version: daemon=%s client=%s", health.Version, version.String())
	}
	for {
		resp, err := cl.Select(ctx, req.account, req.process, req.cwd, req.wait, req.excludeIDs)
		if err != nil {
			return nil, fmt.Errorf("select through daemon: %w", err)
		}
		if err := ctx.Err(); err != nil {
			abortDaemonSelection(ctx, cl, resp.ReservationToken)
			return nil, err
		}
		switch daemonSelectOutcome(resp, req.wait) {
		case outcomePicked:
			a, err := validateDaemonSelection(m, resp, forced)
			if err != nil {
				abortDaemonSelection(ctx, cl, resp.ReservationToken)
				return nil, err
			}
			launch := req.process.PID > 0
			if launch && resp.ReservationToken == "" {
				return nil, fmt.Errorf("invalid daemon launch selection: empty reservation token for id %d", a.ID)
			}
			if !launch && resp.ReservationToken != "" {
				abortDaemonSelection(ctx, cl, resp.ReservationToken)
				return nil, fmt.Errorf("invalid daemon inspection selection: unexpected reservation token for id %d", a.ID)
			}
			if resp.ExhaustedFallback {
				warnExhaustedFallback(cmd, daemonAccountName(m, resp.SelectedID), resp.ExtraEnabled, derefTime(resp.SoonestReset))
			}
			warnPinHeld(cmd, m, resp.PinHeldAccount, resp.SelectedID)
			commit := func(context.Context) error { return nil }
			abort := func() {}
			if launch {
				commit = func(ctx context.Context) error { return cl.CommitSelection(ctx, resp.ReservationToken) }
				abort = func() { abortDaemonSelection(ctx, cl, resp.ReservationToken) }
			}
			keepClient = true
			return &selectionTxn{
				acct: a, dir: resp.Dir, line: daemonSelectionLine(m, resp), commit: commit, abort: abort,
				close: func() { _ = cl.Close() },
			}, nil
		case outcomeError:
			abortDaemonSelection(ctx, cl, resp.ReservationToken)
			return nil, errors.New(resp.Error)
		case outcomeFail:
			abortDaemonSelection(ctx, cl, resp.ReservationToken)
			return nil, pool.ErrNoneAvailable
		case outcomeWait:
			abortDaemonSelection(ctx, cl, resp.ReservationToken)
			if resp.SoonestReset != nil {
				step(cmd.ErrOrStderr(), "All accounts are busy; waiting until %s.", humanizeReset(*resp.SoonestReset))
			}
		}
		timer := time.NewTimer(15 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func startSelectionDaemon(ctx context.Context, cmd *cobra.Command, cl *daemon.Client) (*daemon.Response, error) {
	want := version.String()
	step(cmd.OutOrStdout(), "Starting the cc-pool daemon…")
	if err := installSelectionDaemon(ctx, cmd); err != nil {
		health, healthErr := cl.HealthContext(ctx)
		if healthErr == nil && health.OK && health.Version == want {
			return health, nil
		}
		warn(cmd.ErrOrStderr(),
			"couldn't start the daemon: %v; run `ccp service install` from a GUI session to enable background polling", err)
		return health, healthErr
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var health *daemon.Response
	var err error
	for {
		health, err = cl.HealthContext(waitCtx)
		if err == nil && health.OK && health.Version == want {
			return health, nil
		}
		select {
		case <-waitCtx.Done():
			warn(cmd.ErrOrStderr(), "the daemon isn't responding yet; check `ccp service status`")
			return health, err
		case <-ticker.C:
		}
	}
}

func installSelectionDaemon(ctx context.Context, cmd *cobra.Command) error {
	if err := installDaemonService(ctx); err != nil {
		return err
	}
	success(cmd.OutOrStdout(), "Installed and started the daemon.")
	return nil
}

func abortDaemonSelection(ctx context.Context, cl *daemon.Client, token string) {
	if token == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	_ = cl.AbortSelection(ctx, token)
}

func validateDaemonSelection(m *pool.Manager, resp *daemon.Response, forced *store.Account) (store.Account, error) {
	expectedDir := "<unknown>"
	if forced != nil {
		expectedDir = pool.AccountPresentationDir(forced.ID)
	}
	if resp.SelectedID == nil {
		return store.Account{}, fmt.Errorf("invalid daemon selection: id <nil>, expected dir %q, returned dir %q", expectedDir, resp.Dir)
	}
	id := *resp.SelectedID
	a, err := m.Store.GetAccount(id)
	if err != nil {
		return store.Account{}, fmt.Errorf("invalid daemon selection: id %d, expected dir %q, returned dir %q: %w", id, expectedDir, resp.Dir, err)
	}
	presentationDir := pool.AccountPresentationDir(a.ID)
	if resp.Dir != presentationDir {
		return store.Account{}, fmt.Errorf("invalid daemon selection: id %d, expected dir %q, returned dir %q", id, presentationDir, resp.Dir)
	}
	if forced != nil && id != forced.ID {
		return store.Account{}, fmt.Errorf("invalid daemon selection: id %d does not match forced account %d, expected dir %q, returned dir %q", id, forced.ID, expectedDir, resp.Dir)
	}
	if resp.AccountInstanceID != a.InstanceID || resp.AccountGeneration != a.Generation {
		return store.Account{}, fmt.Errorf("invalid daemon selection: account %d identity %s/%d, current %s/%d",
			id, resp.AccountInstanceID, resp.AccountGeneration, a.InstanceID, a.Generation)
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

// announceLine prints the inspection diagnostic to stderr only when stdout is a TTY.
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
