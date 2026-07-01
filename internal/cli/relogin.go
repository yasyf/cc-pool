package cli

import (
	"context"
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
			"login; cc-pool closes claude once it lands (or exit claude yourself), then\n" +
			"re-asserts its Keychain ACL over the new credential and clears the\n" +
			"needs-login flag.\n\n" +
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

func runRelogin(cmd *cobra.Command, m *pool.Manager, ref string) error {
	id, err := parseAccountRef(ref)
	if err != nil {
		return err
	}
	a, err := m.Store.GetAccount(id)
	if err != nil {
		return err
	}
	c, err := loginCommand(a.ConfigDir)
	if err != nil {
		return err
	}

	out := cmd.OutOrStdout()
	note(out, "Logging in %s — complete the login; cc-pool closes claude once it lands (or exit claude yourself).", accountName(a.Label))

	fd := int(os.Stdin.Fd())
	state, _ := term.GetState(fd) // nil on non-TTY; restore is nil-safe
	read := func() (*keychain.Credential, error) { return reloginCred(a) }
	baseline := ""
	if cred, err := read(); err == nil {
		baseline = cred.ClaudeAiOauth.AccessToken
	}
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, os.Stdout, os.Stderr
	outcome, werr := watchAndClose(cmd.Context(), c, newReloginProbe(read, baseline))
	restoreTerminal(out, fd, state)
	if outcome == awaitCanceled {
		return werr
	}
	if err := cmd.Context().Err(); err != nil {
		return err
	}
	if err := finishRelogin(cmd.Context(), m, a); err != nil {
		return err
	}
	success(out, "%s re-logged in.", accountName(a.Label))
	return nil
}

// loginCommand builds the `claude /login` command for an account's config dir;
// the caller owns the child's stdio.
func loginCommand(configDir string) (*exec.Cmd, error) {
	bin, err := exec.LookPath("claude")
	if err != nil {
		return nil, fmt.Errorf("`claude` not found on PATH: %w", err)
	}
	//nolint:gosec // G204: bin is the resolved claude executable path; "/login" is a fixed argument
	c := exec.Command(bin, "/login")
	c.Env = execEnv(os.Environ(), configDir)
	return c, nil
}

func finishRelogin(ctx context.Context, m *pool.Manager, a store.Account) error {
	// Fail closed: an unusable credential means the login didn't land; the
	// daemon would re-flag on its next poll anyway.
	cred, err := reloginCred(a)
	if err != nil || !cred.HasRefreshToken() || cred.Expired() {
		return fmt.Errorf("login left no usable credential for %s; run `ccp login %d` again", accountName(a.Label), a.ID)
	}
	// Re-assert our `security`-trusted ACL; a no-op rewrite for the plaintext-file backend.
	if err := m.AdoptRotatedToken(ctx, a); err != nil {
		return fmt.Errorf("re-assert credential for %s: %w", accountName(a.Label), err)
	}
	if _, err := m.Store.ClearNeedsLogin(a.ID); err != nil {
		return fmt.Errorf("clear needs-login for %s: %w", accountName(a.Label), err)
	}
	return nil
}

type credReader func() (*keychain.Credential, error)

// newReloginProbe fires when a fresh, usable credential's access token differs
// from the baseline read just before claude started — the re-login completion
// signal.
//
// SAFETY: no identity gate (unlike newIdentityProbe) is needed — claude adopts
// the global credential only into a FRESH CLAUDE_CONFIG_DIR, so a re-login dir's
// only credential change is the user's own login.
func newReloginProbe(read credReader, baseline string) func() (bool, error) {
	return func() (bool, error) {
		cred, err := read()
		if err != nil {
			// A transient backend-read failure (security(1) hiccup, locked keychain, not
			// yet written) is not a signal — keep waiting; the interactive user bounds the wait.
			return false, nil
		}
		return cred.HasRefreshToken() && !cred.Expired() &&
			cred.ClaudeAiOauth.AccessToken != baseline, nil
	}
}

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
