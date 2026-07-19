package cli

import (
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"github.com/charmbracelet/huh"
	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

type addOptions struct {
	label   string
	runNow  bool
	autoYes bool
	count   int
	noAlias bool
}

// errAddSkipped: the user declined to add an already-pooled subscription; not a failure.
var errAddSkipped = errors.New("add skipped")

func newAddCmd() *cobra.Command {
	var opts addOptions
	cmd := &cobra.Command{
		Use:   "add",
		Short: "Add a Claude subscription to the pool",
		Long: `add pools a Claude subscription. It sets up a new account, logs it in, and
records it so ccp can route sessions to it.

Each account logs in with its own ` + "`claude auth login`" + `, so it gets its own token
chain. Plain claude stays logged in and untouched.

On a fresh machine, add also sets up the pool and starts the background daemon,
so you do not need a separate ` + "`ccp init`" + `.

Run it without flags to add accounts one at a time; it offers to add another
after each. Use --count to add a set number, or -y to add one and log in right
away.

When the login prompt can't run here (a non-interactive shell), add prints a
command to run elsewhere and holds the new account's overlay with a session lease
tied to THIS terminal. Keep this terminal open until the login finishes: closing
it releases the lease even if the login is still going in another terminal.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				return runAdd(cmd, m, opts)
			})
		},
	}
	cmd.Flags().StringVar(&opts.label, "label", "", "name for the first account")
	cmd.Flags().BoolVar(&opts.runNow, "run-login", false, "log in immediately instead of asking how")
	cmd.Flags().BoolVarP(&opts.autoYes, "yes", "y", false, "add one account and log in right away")
	cmd.Flags().IntVar(&opts.count, "count", 0, "add exactly N accounts, no continue prompt")
	cmd.Flags().BoolVar(&opts.noAlias, "no-alias", false, "don't add a `claude` shell alias")
	return cmd
}

// runAdd serves both `ccp add` and bare `ccp` on an empty pool (zero-value opts).
func runAdd(cmd *cobra.Command, m *pool.Manager, opts addOptions) error {
	if err := ensureReady(cmd, m); err != nil {
		return err
	}
	var added []store.Account
	for i := 0; ; i++ {
		if h := accountHeader(i+1, opts); h != "" && isTTY() {
			step(cmd.OutOrStdout(), "\n%s", h)
		}
		lbl := ""
		if i == 0 {
			lbl = opts.label
		}
		acct, err := addOne(cmd, m, lbl, opts)
		if err != nil {
			if errors.Is(err, errAddSkipped) {
				break
			}
			if len(added) == 0 {
				return err
			}
			warn(cmd.ErrOrStderr(), "stopping after %s: %v", plural(len(added), "account"), err)
			break
		}
		added = append(added, *acct)
		if !addAnother(cmd, len(added), opts.count, opts.autoYes) {
			break
		}
	}
	summarizeAdds(cmd, m, added)
	if len(added) > 0 {
		offerAlias(cmd, opts)
	}
	return nil
}

func ensureReady(cmd *cobra.Command, m *pool.Manager) error {
	ok, err := m.Initialized()
	if err != nil {
		return err
	}
	if !ok {
		res, err := m.Init()
		if err != nil {
			return err
		}
		success(cmd.OutOrStdout(), "Set up cc-pool on this machine.")
		reportOverlayChoice(cmd, res)
	}
	ensureDaemon(cmd)
	return nil
}

func addOne(cmd *cobra.Command, m *pool.Manager, label string, opts addOptions) (*store.Account, error) {
	out := cmd.OutOrStdout()
	var pending *pool.PendingAdd
	if err := withSpinner(out, "preparing the account…", func() error {
		var e error
		pending, e = m.PrepareAdd(cmd.Context())
		return e
	}); err != nil {
		diagnoseFPAddFailure(cmd, m, err)
		return nil, err
	}
	if pending.FallbackReason != "" {
		warn(cmd.ErrOrStderr(), "fuse overlay unavailable (%s); using symlinks", pending.FallbackReason)
	}
	noteFuseFirstMount(out, pending.OverlayKind)

	if err := loginFlow(cmd, pending, opts); err != nil {
		releaseKeptAdd(cmd, m, pending)
		return nil, err
	}
	// Decorative acknowledgment; identity-read failures stay silent, and the
	// non-TTY contract (script-parsed output) is unchanged.
	if isTTY() {
		if id, err := pool.AccountIdentity(pending.OverlayKind, pending.ConfigDir); err == nil {
			if id.EmailAddress != "" {
				success(out, "Logged in as %s.", id.EmailAddress)
			} else {
				success(out, "Logged in.")
			}
		}
	}

	if checkDuplicate(cmd, m, pending, opts) {
		note(out, "Skipped; didn't add a duplicate.")
		if aerr := m.AbandonAdd(cmd.Context(), pending); aerr != nil {
			warn(cmd.ErrOrStderr(), "cleanup failed: %v", aerr)
		}
		return nil, errAddSkipped
	}

	prompt := label == "" && isTTY() && !opts.autoYes
	label = defaultLabel(label, pending.OverlayKind, pending.ConfigDir)
	if prompt {
		_ = huh.NewInput().
			Title("Name for this account (optional)").
			Placeholder("e.g. Work, or an email").
			Value(&label).
			WithTheme(ccpTheme()).
			Run()
	}

	acct, err := m.FinalizeAdd(cmd.Context(), pending, label)
	if err != nil {
		if acct != nil {
			// Row and credential are live; only the best-effort usage check failed.
			warn(cmd.ErrOrStderr(), "added the account, but its first usage check failed; run `ccp doctor` to retry")
		} else {
			fail(cmd.ErrOrStderr(), "couldn't finish adding the account: %v", err)
			if shouldAbandon(cmd) {
				if aerr := m.AbandonAdd(cmd.Context(), pending); aerr != nil {
					warn(cmd.ErrOrStderr(), "cleanup failed: %v", aerr)
				} else {
					step(out, "Rolled back the account.")
				}
			} else {
				releaseKeptAdd(cmd, m, pending)
			}
			return nil, err
		}
	}
	// Explicit add intent: publish past any tombstone so a re-add survives a
	// peer's earlier removal; the account is live locally either way.
	if perr := syncPublishAccount(cmd, m, acct.ID); perr != nil {
		warn(cmd.ErrOrStderr(), "added the account, but couldn't publish it to the sync registry: %v — peer hosts won't see it; check `ccp sync status`", perr)
	}
	name := acct.Label
	if name == "" {
		name = "the account"
	}
	success(out, "Added %s.", name)
	return acct, nil
}

// releaseKeptAdd frees a kept-dir attempt's index reservation so rerunning
// `ccp add` resumes it on the same index (SeedKeptExisting adopts the login);
// without it a retry would count up until the daemon's TTL sweep.
func releaseKeptAdd(cmd *cobra.Command, m *pool.Manager, p *pool.PendingAdd) {
	if err := m.ReleaseAdd(p); err != nil {
		warn(cmd.ErrOrStderr(), "couldn't release the account's index reservation: %v", err)
	}
}

// noteFuseFirstMount warns about macOS's one-time volume-access grant prompts on an account's first fuse mount.
func noteFuseFirstMount(out io.Writer, backend fkoverlay.Backend) {
	if !pool.CanHostFuse() || !backend.IsFuse() {
		return
	}
	en := backend.Enablement()
	if !en.Needed {
		return
	}
	note(out, "macOS will show one-time grant prompts for both the fusekit-holder app and cc-pool itself (%s) — click Allow on each.", en.Pane)
}

// defaultLabel's email-derived prefill is decorative — identity-read failures
// stay silent.
func defaultLabel(explicit string, backend fkoverlay.Backend, configDir string) string {
	if explicit != "" {
		return explicit
	}
	id, err := pool.AccountIdentity(backend, configDir)
	if err != nil {
		return ""
	}
	return pool.LabelForEmail(id.EmailAddress)
}

// checkDuplicate reports whether to skip an already-pooled subscription.
// Non-interactive runs keep it (never block automation); no identity, no check.
func checkDuplicate(cmd *cobra.Command, m *pool.Manager, p *pool.PendingAdd, opts addOptions) bool {
	id, err := pool.AccountIdentity(p.OverlayKind, p.ConfigDir)
	if err != nil {
		return false
	}
	dup, err := m.DuplicateIdentity(*id)
	if err != nil || dup == nil {
		return false
	}
	who := id.EmailAddress
	if who == "" {
		who = "that subscription"
	}
	if opts.autoYes || opts.count > 0 || !isTTY() {
		warn(cmd.ErrOrStderr(), "%s is already in the pool; adding it again", who)
		return false
	}
	warn(cmd.ErrOrStderr(), "%s is already in the pool.", who)
	keep := false
	_ = huh.NewConfirm().
		Title("Add it again anyway?").
		Value(&keep).
		WithTheme(ccpTheme()).
		Run()
	return !keep
}

func loginFlow(cmd *cobra.Command, pending *pool.PendingAdd, opts addOptions) error {
	out := cmd.OutOrStdout()
	// Hold the pending account's session lease across the whole login interaction so
	// holder retirement/recovery never tears down its fresh mount while claude writes
	// credentials; the supervising ccp parent releases it on return. Probe first —
	// never log in through a dead or wedged mirror.
	h, err := acquireAndProbePendingLease(pending)
	if err != nil {
		return err
	}
	defer func() { _ = h.Close() }()
	doRun := opts.runNow || opts.autoYes
	if isTTY() && !doRun {
		choice := "run"
		_ = huh.NewSelect[string]().
			Title("How do you want to log in?").
			Options(
				huh.NewOption("Log in now, in this terminal", "run"),
				huh.NewOption("I'll log in from another terminal", "manual"),
			).
			Value(&choice).
			WithTheme(ccpTheme()).
			Run()
		doRun = choice == "run"
	}

	if doRun {
		if err := runWatchedLogin(cmd.Context(), cmd, pending); err != nil {
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				return err
			}
			// A nonzero claude exit isn't fatal; finalize decides.
			warn(cmd.ErrOrStderr(), "the login command exited with an error: %v", err)
		}
		return nil
	}

	if !isTTY() {
		return errors.New("non-interactive add requires --run; detached login lease agents were removed")
	}
	step(out, "\nRun this in another terminal, finish the login, then come back:\n\n    %s\n", pending.LoginCommand)
	if pending.ClaudeJSONSeed == pool.SeedKeptExisting {
		// Polling can't tell a reused dir's pre-existing login from a fresh one.
		cont := true
		_ = huh.NewConfirm().
			Title("Press enter when the login is done").
			Value(&cont).
			WithTheme(ccpTheme()).
			Run()
		return nil
	}
	return waitForLogin(cmd.Context(), out, pending.OverlayKind, pending.ConfigDir)
}

func addAnother(cmd *cobra.Command, done, count int, autoYes bool) bool {
	if count > 0 {
		return done < count
	}
	if autoYes || !isTTY() {
		return false
	}
	// Default yes only after the first add — most people pool 2+ subscriptions.
	again := done == 1
	if err := huh.NewConfirm().Title("Add another account?").Value(&again).WithTheme(ccpTheme()).Run(); err != nil {
		warn(cmd.ErrOrStderr(), "prompt failed: %v", err)
		return false
	}
	return again
}

func summarizeAdds(cmd *cobra.Command, m *pool.Manager, added []store.Account) {
	if len(added) == 0 {
		return
	}
	total := len(added)
	if all, err := m.Store.ListAccounts(); err == nil {
		total = len(all)
	}
	if s := addedSummary(added); s != "" && isTTY() {
		step(cmd.OutOrStdout(), "\n%s Your pool now has %s.", s, plural(total, "account"))
		return
	}
	step(cmd.OutOrStdout(), "\nYour pool now has %s.", plural(total, "account"))
}

// accountHeader returns the wizard section header for the nth add, or "" when
// a single add is guaranteed (-y, --count 1) — a lone section needs no frame.
func accountHeader(n int, opts addOptions) string {
	if opts.count == 1 || (opts.autoYes && opts.count == 0) {
		return ""
	}
	if opts.count > 1 {
		return hdrStyle.Render(fmt.Sprintf("Account %d of %d", n, opts.count))
	}
	return hdrStyle.Render(fmt.Sprintf("Account %d", n))
}

// addedSummary names the accounts a multi-add landed; "" for a single add —
// its success line already named it.
func addedSummary(added []store.Account) string {
	if len(added) < 2 {
		return ""
	}
	names := make([]string, len(added))
	for i, a := range added {
		names[i] = a.Label
		if names[i] == "" {
			names[i] = "an unnamed account"
		}
	}
	return fmt.Sprintf("Added %s.", strings.Join(names[:len(names)-1], ", ")+" and "+names[len(names)-1])
}

func shouldAbandon(_ *cobra.Command) bool {
	if !isTTY() {
		return false
	}
	abandon := true
	_ = huh.NewConfirm().
		Title("Roll back this incomplete account?").
		Value(&abandon).
		WithTheme(ccpTheme()).
		Run()
	return abandon
}
