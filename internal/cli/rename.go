package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

type renameOptions struct {
	auto  bool
	force bool
}

func newRenameCmd() *cobra.Command {
	var opts renameOptions
	cmd := &cobra.Command{
		Use:   "rename <account> <new-label>",
		Short: "Rename an account",
		Long: `rename changes an account's label:

    ccp rename 3 Work
    ccp rename acct-03 Work

With --auto it instead derives a friendly name from each account's logged-in
email — consumer providers keep the username ("yasyfm@gmail.com" → "yasyfm"),
org domains become the org name ("rebecca.fang@ucsf.edu" → "UCSF") — and
applies it to every account whose label is empty or still the raw email;
--force overwrites custom labels too. Labels are display-only and need not be
unique: status and list always disambiguate by acct-NN.`,
		// Args deliberately ArbitraryArgs: arity depends on --auto, so RunE validates.
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return withManager(cmd.Context(), func(m *pool.Manager) error {
				if err := requireInit(m); err != nil {
					return err
				}
				return runRename(cmd, m, args, opts)
			})
		},
	}
	cmd.Flags().BoolVar(&opts.auto, "auto", false, "re-derive labels from each account's email")
	cmd.Flags().BoolVar(&opts.force, "force", false, "with --auto, overwrite custom labels too")
	return cmd
}

func runRename(cmd *cobra.Command, m *pool.Manager, args []string, opts renameOptions) error {
	if opts.force && !opts.auto {
		return fmt.Errorf("--force only applies to --auto")
	}
	if opts.auto {
		return renameAuto(cmd, m, args, opts.force)
	}
	if len(args) != 2 {
		return fmt.Errorf("usage: ccp rename <account> <new-label>, or ccp rename --auto [account...]")
	}
	return renameManual(cmd, m, args[0], args[1])
}

func renameManual(cmd *cobra.Command, m *pool.Manager, ref, label string) error {
	if label == "" {
		return fmt.Errorf("new label must not be empty")
	}
	id, err := parseAccountRef(ref)
	if err != nil {
		return err
	}
	old, err := m.Store.GetAccount(id)
	if err != nil {
		return err
	}
	if err := m.Store.SetAccountLabel(id, label); err != nil {
		return err
	}
	syncRecordLabel(cmd, m, old)
	success(cmd.OutOrStdout(), "Renamed acct-%02d: %s → %s.", id, accountName(old.Label), label)
	return nil
}

func renameAuto(cmd *cobra.Command, m *pool.Manager, refs []string, force bool) error {
	accts, err := autoTargets(m, refs)
	if err != nil {
		return err
	}
	if len(accts) == 0 {
		step(cmd.ErrOrStderr(), "No accounts yet. Run `ccp add` to add one.")
		return nil
	}
	client, err := requireDaemon(m, "automatic rename requires the account daemon")
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	out := cmd.OutOrStdout()
	for _, a := range accts {
		ident, err := client.AccountIdentity(cmd.Context(), a.ID)
		if err != nil {
			note(out, "acct-%02d: no readable identity; skipped", a.ID)
			continue
		}
		if ident.EmailAddress == "" {
			note(out, "acct-%02d: no email on its login; skipped", a.ID)
			continue
		}
		derived := pool.LabelForEmail(ident.EmailAddress)
		switch {
		case derived == a.Label:
			note(out, "acct-%02d: already %s", a.ID, a.Label)
		case a.Label != "" && a.Label != ident.EmailAddress && !force:
			note(out, "acct-%02d: kept %q (use --force to overwrite)", a.ID, a.Label)
		default:
			if err := m.Store.SetAccountLabel(a.ID, derived); err != nil {
				return err
			}
			syncRecordLabel(cmd, m, a)
			success(out, "acct-%02d: %s → %s", a.ID, accountName(a.Label), derived)
		}
	}
	return nil
}

// autoTargets resolves --auto's account set up front: an unknown reference
// fails before anything is renamed.
func autoTargets(m *pool.Manager, refs []string) ([]store.Account, error) {
	if len(refs) == 0 {
		return m.Store.ListAccounts()
	}
	accts := make([]store.Account, 0, len(refs))
	for _, ref := range refs {
		id, err := parseAccountRef(ref)
		if err != nil {
			return nil, err
		}
		a, err := m.Store.GetAccount(id)
		if err != nil {
			return nil, err
		}
		accts = append(accts, a)
	}
	return accts, nil
}

// parseAccountRef accepts a numeric id ("3") or the acct-NN spelling `ccp list` prints.
func parseAccountRef(s string) (int, error) {
	id, err := strconv.Atoi(strings.TrimPrefix(s, "acct-"))
	if err != nil || id < 1 {
		return 0, fmt.Errorf("%q is not a valid account id", s)
	}
	return id, nil
}
