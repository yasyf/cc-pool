package cli

import (
	"errors"
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/keychain"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"golang.org/x/term"
)

func newLoginCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "login <account>",
		Short: "Re-login a pool account whose token was revoked",
		Long: "login runs `claude /login` for one pool account, attached to your\n" +
			"terminal, so you can re-authenticate an account the daemon flagged as\n" +
			"needing login (its refresh token was revoked or cleared). Complete the\n" +
			"login and exit claude; cc-pool re-asserts its Keychain ACL over the new\n" +
			"credential and clears the needs-login flag.\n\n" +
			"    ccp login 3\n" +
			"    ccp login acct-03",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withManager(func(m *pool.Manager) error {
				if err := requireInit(m); err != nil {
					return err
				}
				return runRelogin(cmd, m, args[0])
			})
		},
	}
}

// runRelogin runs `claude /login` for an existing account, then verifies a
// usable credential landed, re-asserts our Keychain ACL over it, and clears the
// needs-login flag. It uses the unwatched flow (the user exits claude when done)
// because the account already holds a stale identity — an identity probe would
// fire immediately, so completion can only be confirmed by the credential the
// login leaves behind.
func runRelogin(cmd *cobra.Command, m *pool.Manager, ref string) error {
	id, err := parseAccountRef(ref)
	if err != nil {
		return err
	}
	a, err := m.Store.GetAccount(id)
	if err != nil {
		return err
	}
	bin, err := exec.LookPath("claude")
	if err != nil {
		return fmt.Errorf("`claude` not found on PATH: %w", err)
	}

	out := cmd.OutOrStdout()
	note(out, "Logging in %s — complete the login, then exit claude.", accountName(a.Label))

	fd := int(os.Stdin.Fd())
	state, _ := term.GetState(fd) // nil on non-TTY; restore is nil-safe
	c := exec.Command(bin, "/login")
	c.Env = execEnv(os.Environ(), a.ConfigDir)
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := c.Start(); err != nil {
		return fmt.Errorf("start claude /login: %w", err)
	}
	procExit := make(chan error, 1)
	go func() { procExit <- c.Wait() }()
	<-procExit
	restoreTerminal(out, fd, state)

	// A real login leaves a fresh, usable credential in whichever backend the
	// account uses. If it didn't, the daemon will re-flag on its next poll, so
	// we refuse to clear the flag here.
	cred, err := reloginCred(a)
	if err != nil || !cred.HasRefreshToken() || cred.Expired() {
		return fmt.Errorf("login left no usable credential for %s; run `ccp login %d` again", accountName(a.Label), a.ID)
	}
	// Re-assert our `security`-trusted ACL over the freshly written item (a
	// no-op rewrite for the plaintext-file backend).
	if err := m.AdoptRotatedToken(cmd.Context(), a); err != nil {
		return fmt.Errorf("re-assert credential for %s: %w", accountName(a.Label), err)
	}
	if _, err := m.Store.ClearNeedsLogin(a.ID); err != nil {
		return fmt.Errorf("clear needs-login for %s: %w", accountName(a.Label), err)
	}
	success(out, "%s re-logged in.", accountName(a.Label))
	return nil
}

// reloginCred reads the account's credential from whichever backend holds it —
// the Keychain first, then the plaintext file claude writes when the Keychain is
// unavailable.
func reloginCred(a store.Account) (*keychain.Credential, error) {
	cred, err := keychain.Read(a.KeychainService, a.KeychainAccount)
	if err == nil {
		return cred, nil
	}
	if !errors.Is(err, keychain.ErrNotFound) {
		return nil, err
	}
	return keychain.ReadFileCredential(a.ConfigDir)
}
