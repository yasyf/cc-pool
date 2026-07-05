package cli

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"golang.org/x/term"
)

// finishReloginGrace bounds the post-exit credential re-probe: a user quitting
// claude within a poll tick of the write must not read as a failed login. A
// var so tests shrink it.
var finishReloginGrace = 2 * time.Second

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

	out := cmd.OutOrStdout()
	cleared, err := shortCircuitRelogin(cmd.Context(), m, a)
	if err != nil {
		return err
	}
	if cleared {
		// Same hygiene as the interactive path below: the cross-session login that
		// makes the short-circuit fire is exactly what strands a stale copy on the
		// other backend.
		if err := m.DropDivergentCopy(cmd.Context(), a); err != nil {
			note(out, "couldn't remove a stale credential copy on the other backend: %v — run `ccp doctor`.", err)
		}
		success(out, "%s already has a valid credential — cleared needs-login. Run `ccp login %d` again to switch subscriptions.", accountName(a.Label), a.ID)
		return nil
	}

	c, err := loginCommand(a.ConfigDir)
	if err != nil {
		return err
	}

	note(out, "Logging in %s — complete the login; cc-pool closes claude once it lands (or exit claude yourself).", accountName(a.Label))

	fd := int(os.Stdin.Fd())
	state, _ := term.GetState(fd) // nil on non-TTY; restore is nil-safe
	read := func() (*creds.Credential, error) {
		cred, _, rerr := m.ReadCredential(a)
		return cred, rerr
	}
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
	// A cross-session re-login (e.g. an SSH login of a Keychain-backed account
	// lands the fresh credential in the file backend) can leave a stale copy on
	// the other backend. Resolution already prefers the fresher copy, so this is
	// hygiene, not correctness — a failure (or an unreachable headless Keychain)
	// must not fail the completed login.
	if err := m.DropDivergentCopy(cmd.Context(), a); err != nil {
		note(out, "couldn't remove a stale credential copy on the other backend: %v — run `ccp doctor`.", err)
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

// forcedRefreshHorizon makes EnsureFreshToken treat any expiry as imminent, so
// the short-circuit always exercises the refresh chain.
const forcedRefreshHorizon = time.Duration(math.MaxInt64)

// shortCircuitRelogin clears a stale needs-login flag when a login that already
// landed left a live credential. Liveness is proven by a forced refresh through
// the daemon-proven rotate-and-persist path — never by the access token, which
// can carry hours of life on a revoked refresh chain (the daemon flags on
// proactive refresh failure). Anything unproven reports false so the
// interactive login proceeds.
func shortCircuitRelogin(ctx context.Context, m *pool.Manager, a store.Account) (bool, error) {
	h, err := m.Store.GetAuthHealth(a.ID)
	if err != nil {
		return false, fmt.Errorf("auth health for %s: %w", accountName(a.Label), err)
	}
	if !h.NeedsLogin {
		return false, nil
	}
	// No AdoptRotatedToken here: the refresh's own persist already wrote the
	// credential under our ACL — only claude-written credentials (via /login)
	// need re-asserting.
	_, refreshed, err := m.EnsureFreshToken(ctx, a, forcedRefreshHorizon, true)
	if err != nil || !refreshed {
		return false, nil
	}
	if _, err := m.Store.ClearNeedsLogin(a.ID); err != nil {
		return false, fmt.Errorf("clear needs-login for %s: %w", accountName(a.Label), err)
	}
	return true, nil
}

// awaitUsableCred re-probes read until a usable credential lands or grace
// expires — claude's exit can beat its credential write by a poll tick. An
// ErrUnavailable read is unknown state and fails immediately.
func awaitUsableCred(ctx context.Context, read credReader, grace, interval time.Duration) (*creds.Credential, error) {
	deadline := time.Now().Add(grace)
	for {
		cred, err := read()
		switch {
		case errors.Is(err, creds.ErrUnavailable):
			return nil, err
		case err == nil && cred.HasRefreshToken() && !cred.Expired():
			return cred, nil
		}
		if time.Now().After(deadline) {
			return cred, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}

func finishRelogin(ctx context.Context, m *pool.Manager, a store.Account) error {
	// Fail closed: an unusable credential means the login didn't land; the
	// daemon would re-flag on its next poll anyway. A read failure keeps its
	// cause: an unsearchable Keychain (creds.ErrUnavailable) is unknown state,
	// not a failed login.
	read := func() (*creds.Credential, error) {
		cred, _, err := m.ReadCredential(a)
		return cred, err
	}
	cred, err := awaitUsableCred(ctx, read, finishReloginGrace, loginPollInterval)
	if err != nil {
		return fmt.Errorf("read %s's credential after login: %w — run `ccp login %d` again", accountName(a.Label), err, a.ID)
	}
	if !cred.HasRefreshToken() || cred.Expired() {
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

type credReader func() (*creds.Credential, error)

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
