package cli

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/huh"
	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

type addOptions struct {
	label   string
	autoYes bool
	count   int
	noAlias bool
}

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
after each. Use --count to add a set number, or -y to add one. The terminal is
attached to a daemon-supervised login process; the CLI never opens credential
stores or owns the login child.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(cmd.Context(), func(m *pool.Manager) error {
				return runAdd(cmd, m, opts)
			})
		},
	}
	cmd.Flags().StringVar(&opts.label, "label", "", "name for the first account")
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
	cl, err := requireDaemon(m, "account and credential mutation runs inside the daemon")
	if err != nil {
		return err
	}
	defer func() { _ = cl.Close() }()
	var added []store.Account
	for i := 0; ; i++ {
		if h := accountHeader(i+1, opts); h != "" && isTTY() {
			step(cmd.OutOrStdout(), "\n%s", h)
		}
		lbl := ""
		if i == 0 {
			lbl = opts.label
		}
		acct, err := addOne(cmd, m, cl, lbl)
		if err != nil {
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
		_, err := m.Init()
		if err != nil {
			return err
		}
		success(cmd.OutOrStdout(), "Set up cc-pool on this machine.")
	}
	ensureDaemon(cmd)
	return nil
}

func addOne(cmd *cobra.Command, m *pool.Manager, cl *daemon.Client, label string) (*store.Account, error) {
	out := cmd.OutOrStdout()
	loginURL := &terminalURLAction{}
	result, err := cl.AccountMutationTerminal(cmd.Context(), daemon.AccountMutationRequest{
		Kind: daemon.AccountMutationAdd, Action: daemon.AccountMutationStartOrAttach,
		Label: label,
	}, os.Stdin, out, loginURL.observe)
	loginURL.annotate(out)
	if err != nil {
		fail(cmd.ErrOrStderr(), "couldn't finish adding the account: %v", err)
		return nil, err
	}
	if !result.Completed || result.State != daemon.AccountMutationCompleted {
		return nil, errors.New("daemon did not commit the account mutation")
	}
	committed, err := m.Store.GetAccount(result.AccountID)
	if err != nil {
		return nil, err
	}
	acct := &committed
	name := acct.Label
	if name == "" {
		name = "the account"
	}
	success(out, "Added %s.", name)
	return acct, nil
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
