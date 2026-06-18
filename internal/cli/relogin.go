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

// runRelogin runs `claude /login` for an existing account, watching for a fresh
// usable credential to replace the account's stale one; when one lands it closes
// claude (the user may also exit it manually), then verifies the credential,
// re-asserts our Keychain ACL over it, and clears the needs-login flag. The
// account already holds a stale identity, so an identity probe would fire
// immediately — completion is keyed on the credential's access token changing to
// a fresh, usable one (see newReloginProbe).
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

// loginCommand builds the `claude /login` command for an account's config dir,
// with the credential-isolating env from execEnv. The caller owns the child's
// stdio: runRelogin attaches it to the terminal directly, while the status TUI
// hands it to a watched tea.Exec spawn. Shared so both paths spawn an identical
// login.
func loginCommand(configDir string) (*exec.Cmd, error) {
	bin, err := exec.LookPath("claude")
	if err != nil {
		return nil, fmt.Errorf("`claude` not found on PATH: %w", err)
	}
	c := exec.Command(bin, "/login")
	c.Env = execEnv(os.Environ(), configDir)
	return c, nil
}

// finishRelogin verifies a `claude /login` left a usable credential for the
// account, re-asserts our Keychain ACL over it, and clears the needs-login flag.
// Shared by the `ccp login` command and the status TUI's re-login action.
func finishRelogin(ctx context.Context, m *pool.Manager, a store.Account) error {
	// A real login leaves a fresh, usable credential in whichever backend the
	// account uses. If it didn't, the daemon will re-flag on its next poll, so
	// we refuse to clear the flag here.
	cred, err := reloginCred(a)
	if err != nil || !cred.HasRefreshToken() || cred.Expired() {
		return fmt.Errorf("login left no usable credential for %s; run `ccp login %d` again", accountName(a.Label), a.ID)
	}
	// Re-assert our `security`-trusted ACL over the freshly written item (a
	// no-op rewrite for the plaintext-file backend).
	if err := m.AdoptRotatedToken(ctx, a); err != nil {
		return fmt.Errorf("re-assert credential for %s: %w", accountName(a.Label), err)
	}
	if _, err := m.Store.ClearNeedsLogin(a.ID); err != nil {
		return fmt.Errorf("clear needs-login for %s: %w", accountName(a.Label), err)
	}
	return nil
}

// credReader reads an account's live credential; injectable for tests.
type credReader func() (*keychain.Credential, error)

// newReloginProbe reports that a fresh, usable credential has replaced the
// account's pre-login one — the signal that an interactive re-login completed.
// Identity presence can't be the signal (the account already carries its prior
// identity), so completion is keyed on the credential changing to a usable one.
// baseline is the access token read just before claude started.
//
// SAFETY: keying on the credential alone — with NO identity gate — is safe here.
// Claude's startup "adoption" copies the global session's credential only into a
// FRESH CLAUDE_CONFIG_DIR (the add flow's concern, which is exactly why
// newIdentityProbe keys on identity, not credential). A re-login dir is NON-fresh:
// it already holds its own (revoked) credential and identity, so claude does not
// overwrite that existing credential with the global one at startup. The only
// credential change during a re-login is therefore the user's own login.
func newReloginProbe(read credReader, baseline string) func() (bool, error) {
	return func() (bool, error) {
		cred, err := read()
		if err != nil {
			// A transient backend read (a security(1) spawn hiccup, a momentarily
			// locked keychain, or simply "not written yet") tells us nothing — keep
			// waiting rather than abort the watch and force-close the live login.
			// The interactive user bounds the wait: they finish the login (the
			// probe then fires) or exit claude themselves (awaitExited).
			return false, nil
		}
		return cred.HasRefreshToken() && !cred.Expired() &&
			cred.ClaudeAiOauth.AccessToken != baseline, nil
	}
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
