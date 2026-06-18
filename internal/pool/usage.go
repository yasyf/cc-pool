package pool

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-pool/internal/keychain"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
)

// RefreshLeadTime is how close to expiry an idle account's token is refreshed
// preemptively (the spec's "<10 min" pre-flight window).
const RefreshLeadTime = 10 * time.Minute

// ErrNeedsLogin indicates the stored refresh token is gone/revoked and the
// account must be re-logged-in interactively.
var ErrNeedsLogin = errors.New("account needs re-login (refresh token missing or revoked)")

// EnsureFreshToken returns the account's credential, refreshing it if the access
// token expires within `within` and allowRefresh is true. A successful refresh
// is written back to all of the account's services and logged. allowRefresh
// should be false for accounts with a live session (that session owns refresh).
func (m *Manager) EnsureFreshToken(ctx context.Context, a store.Account, within time.Duration, allowRefresh bool) (*keychain.Credential, bool, error) {
	release, err := m.lockAccount(ctx, a.ID)
	if err != nil {
		return nil, false, err
	}
	defer release()
	cred, _, refreshed, err := m.ensureFreshToken(ctx, a, within, allowRefresh)
	return cred, refreshed, err
}

// readCred returns a's credential from whichever backend currently holds it —
// the Keychain first (claude's own preference whenever it is reachable), else
// the plaintext $CONFIG_DIR/.credentials.json file claude writes when the
// Keychain is unavailable (a headless SSH session) — plus that source, so a
// refresh writes the new token back to the same place rather than splitting an
// account across backends.
func (m *Manager) readCred(a store.Account) (*keychain.Credential, keychain.Source, error) {
	cred, err := m.Keychain.Read(a.KeychainService, a.KeychainAccount)
	if err == nil {
		return cred, keychain.SourceKeychain, nil
	}
	if !errors.Is(err, keychain.ErrNotFound) {
		return nil, keychain.SourceKeychain, err
	}
	fcred, ferr := keychain.ReadFileCredential(a.ConfigDir)
	if ferr != nil {
		return nil, keychain.SourceKeychain, ferr
	}
	return fcred, keychain.SourceFile, nil
}

// writeCred persists cred to src — the backend readCred resolved — so refresh
// never moves an account between the Keychain and its plaintext file.
func (m *Manager) writeCred(a store.Account, src keychain.Source, cred *keychain.Credential) error {
	if src == keychain.SourceFile {
		if err := keychain.WriteFileCredential(a.ConfigDir, cred); err != nil {
			return fmt.Errorf("write credential to %s: %w", keychain.FileCredentialPath(a.ConfigDir), err)
		}
		return nil
	}
	if err := m.Keychain.Write(a.KeychainService, a.KeychainAccount, cred); err != nil {
		return fmt.Errorf("write credential to %q: %w", a.KeychainService, err)
	}
	return nil
}

// ensureFreshToken is EnsureFreshToken's body; split out so SampleUsage can
// compose it with fetchUsage's 401-retry under ONE continuous critical section
// (sync.Mutex is not reentrant). Caller must hold the per-account lock
// (lockAccount). The credential is (re-)read here, inside the lock, so a waiter
// that wins the lock after a peer rotated the token sees the fresh blob and
// returns it without a second refresh POST.
func (m *Manager) ensureFreshToken(ctx context.Context, a store.Account, within time.Duration, allowRefresh bool) (*keychain.Credential, keychain.Source, bool, error) {
	cred, src, err := m.readCred(a)
	if err != nil {
		return nil, src, false, err
	}
	if !cred.ExpiresWithin(within) || !allowRefresh {
		return cred, src, false, nil
	}
	if !cred.HasRefreshToken() {
		return cred, src, false, ErrNeedsLogin
	}
	refreshed, err := m.refresh(ctx, a, src, cred)
	if err != nil {
		_ = m.Store.LogRefresh(a.ID, false, err.Error())
		var re *oauth.RefreshError
		if errors.As(err, &re) && re.Revoked() {
			return cred, src, false, ErrNeedsLogin
		}
		// Transient: fall back to the (stale) credential we have.
		return cred, src, false, err
	}
	_ = m.Store.LogRefresh(a.ID, true, "")
	return refreshed, src, true, nil
}

// refresh performs the OAuth refresh and persists the new blob, preserving the
// non-token fields from the prior credential. Caller must hold the per-account
// lock (lockAccount).
// Every account runs its own token chain, created by its own `claude /login`, so
// refreshing a pool account's credential never affects plain claude.
func (m *Manager) refresh(ctx context.Context, a store.Account, src keychain.Source, prev *keychain.Credential) (*keychain.Credential, error) {
	tr, err := m.OAuth.Refresh(ctx, fmt.Sprintf("acct-%d", a.ID), prev.ClaudeAiOauth.RefreshToken)
	if err != nil {
		return nil, err
	}
	next := &keychain.Credential{ClaudeAiOauth: prev.ClaudeAiOauth}
	next.ClaudeAiOauth.AccessToken = tr.AccessToken
	if tr.RefreshToken != "" { // rotated
		next.ClaudeAiOauth.RefreshToken = tr.RefreshToken
	}
	next.ClaudeAiOauth.ExpiresAt = tr.Expiry(time.Now()).UnixMilli()
	if err := m.writeCred(a, src, next); err != nil {
		return nil, err
	}
	return next, nil
}

// AdoptRotatedToken re-reads an account's credential from its backend (where a
// live claude session may have rotated it) and writes it straight back. For a
// Keychain item this re-asserts our `security`-trusted ACL over the rotated
// item; for the plaintext file backend it is a harmless rewrite (no ACL).
// Used by the daemon on session check-in.
func (m *Manager) AdoptRotatedToken(ctx context.Context, a store.Account) error {
	release, err := m.lockAccount(ctx, a.ID)
	if err != nil {
		return err
	}
	defer release()
	cred, src, err := m.readCred(a)
	if err != nil {
		return err
	}
	return m.writeCred(a, src, cred)
}

// SampleOpts controls how SampleUsage may recover a 401.
//
// AllowRefresh permits the normal idle-account refresh: the account has no live
// session, so it owns its own token chain and pre-flight + 401 refresh are safe.
//
// AllowBusyRefresh permits the guarded busy-account refresh: a session is live
// but its access token has expired (a long-parked session refreshes only on API
// traffic), so usage sampling would 401 forever. The daemon sets this ONLY when
// the busy state is a procscan-visible session (never a bare reservation, whose
// claude is mid-launch and about to read the Keychain) AND it has already seen a
// consecutive 401, giving any waking session a full poll to refresh first. Even
// then fetchUsage refreshes only when the token is provably expired and a fresh
// Keychain re-read still yields that same expired credential — proof no session
// rotated it underneath us.
type SampleOpts struct {
	AllowRefresh     bool
	AllowBusyRefresh bool
}

// SampleUsage fetches the account's usage windows, recovering a 401 per opts,
// and records a usage_sample. Returns the usage and whether the account is
// currently rate-limited.
func (m *Manager) SampleUsage(ctx context.Context, a store.Account, opts SampleOpts) (*oauth.Usage, bool, error) {
	usage, rateLimited, err := m.sampleUsage(ctx, a, opts)
	if err != nil {
		return nil, rateLimited, err
	}
	m.recordSample(a.ID, usage, rateLimited)
	return usage, rateLimited, nil
}

// sampleUsage holds acctLock for the full credential span: the pre-flight
// refresh AND fetchUsage's 401-retry refresh must form one atomic cycle, or
// another goroutine could rotate the token between them and the retry would
// re-POST a consumed single-use refresh token.
func (m *Manager) sampleUsage(ctx context.Context, a store.Account, opts SampleOpts) (*oauth.Usage, bool, error) {
	release, err := m.lockAccount(ctx, a.ID)
	if err != nil {
		return nil, false, err
	}
	defer release()
	cred, src, _, freshErr := m.ensureFreshToken(ctx, a, RefreshLeadTime, opts.AllowRefresh)
	if freshErr != nil && !errors.Is(freshErr, ErrNeedsLogin) && cred == nil {
		// Transient refresh failure with no usable token to fall back on.
		return nil, false, freshErr
	}
	usage, rateLimited, err := m.fetchUsage(ctx, a, src, cred, opts)
	// A confirmed revocation from the pre-flight refresh (invalid_grant) must not be
	// masked by a usage-endpoint 429 or a plain transient 401: surface it so the daemon
	// flags needs-login. fetchUsage's rung-1 re-read still gets first crack at a
	// session-rotated token — a clean recovery (no error, not rate-limited) suppresses it.
	if errors.Is(freshErr, ErrNeedsLogin) && (err != nil || rateLimited) {
		return nil, false, freshErr
	}
	return usage, rateLimited, err
}

// sameTokens reports whether two credentials carry byte-identical access AND
// refresh tokens — the test for "no session rotated the chain underneath us".
func sameTokens(a, b *keychain.Credential) bool {
	return a.ClaudeAiOauth.AccessToken == b.ClaudeAiOauth.AccessToken &&
		a.ClaudeAiOauth.RefreshToken == b.ClaudeAiOauth.RefreshToken
}

// fetchUsage fetches usage and, on a 401, walks the recovery ladder:
//
//  1. re-read the Keychain (a live session may have rotated the token since our
//     pre-flight read) and retry once with the new access token — a pure read,
//     it never spends a refresh token;
//  2. with no refresh token, the account is definitively signed out → ErrNeedsLogin;
//  3. refresh+retry when permitted — always for an idle account (AllowRefresh),
//     or for a busy account under the strict busy guard (AllowBusyRefresh AND the
//     token is provably expired AND a fresh re-read still yields that same
//     credential). A Revoked refresh that finds the credential changed on re-read
//     means a session won the race (transient); unchanged means truly revoked
//     (ErrNeedsLogin).
//
// Caller must hold the per-account lock (lockAccount).
func (m *Manager) fetchUsage(ctx context.Context, a store.Account, src keychain.Source, cred *keychain.Credential, opts SampleOpts) (*oauth.Usage, bool, error) {
	usage, err := m.OAuth.Usage(ctx, cred.ClaudeAiOauth.AccessToken)
	if err == nil {
		return usage, false, nil
	}
	var ue *oauth.UsageError
	if !errors.As(err, &ue) {
		return nil, false, err
	}
	if ue.RateLimited() {
		return &oauth.Usage{}, true, nil
	}
	if !ue.Unauthorized() {
		return nil, false, err
	}

	// Rung 1: re-read and retry with a session-rotated token (pure read).
	if reread, _, rerr := m.readCred(a); rerr == nil && reread.ClaudeAiOauth.AccessToken != cred.ClaudeAiOauth.AccessToken {
		if usage, err2 := m.OAuth.Usage(ctx, reread.ClaudeAiOauth.AccessToken); err2 == nil {
			return usage, false, nil
		}
		cred = reread // the rotated token also 401s; refresh from it below
	}

	// Rung 2: no refresh token → definitively signed out.
	if !cred.HasRefreshToken() {
		return nil, false, fmt.Errorf("%w: %v", ErrNeedsLogin, err)
	}

	// Rung 3: may we refresh? Idle accounts always may; a busy account only
	// under the guard — provably expired and unchanged on a fresh re-read.
	mayRefresh := opts.AllowRefresh
	if !mayRefresh && opts.AllowBusyRefresh && cred.Expired() {
		if reread, _, rerr := m.readCred(a); rerr == nil && sameTokens(reread, cred) {
			mayRefresh = true
		}
	}
	if !mayRefresh {
		return nil, false, err
	}

	refreshed, rfErr := m.refresh(ctx, a, src, cred)
	if rfErr != nil {
		_ = m.Store.LogRefresh(a.ID, false, rfErr.Error())
		var re *oauth.RefreshError
		if errors.As(rfErr, &re) && re.Revoked() {
			// Revoked: a session that rotated the chain in the meantime leaves a
			// different credential on disk (transient); an unchanged credential
			// is a genuine server-side revocation (re-login required).
			if reread, _, rerr := m.readCred(a); rerr == nil && !sameTokens(reread, cred) {
				return nil, false, err
			}
			return nil, false, fmt.Errorf("%w: %v", ErrNeedsLogin, rfErr)
		}
		return nil, false, err
	}
	_ = m.Store.LogRefresh(a.ID, true, "")
	if usage, err2 := m.OAuth.Usage(ctx, refreshed.ClaudeAiOauth.AccessToken); err2 == nil {
		return usage, false, nil
	}
	return nil, false, err
}

// recordSample persists a usage sample (utilization stored as 0..100 percent).
func (m *Manager) recordSample(accountID int, u *oauth.Usage, rateLimited bool) {
	s := store.UsageSample{
		AccountID:    accountID,
		TS:           time.Now(),
		Util5h:       u.FiveHour.Used(),
		Util7d:       u.SevenDay.Used(),
		Resets5h:     u.FiveHour.ResetsAt,
		Resets7d:     u.SevenDay.ResetsAt,
		RateLimited:  rateLimited,
		ExtraEnabled: u.ExtraUsage.IsEnabled,
		ExtraUsed:    u.ExtraUsage.UsedCredits,
		ExtraLimit:   u.ExtraUsage.MonthlyLimit,
	}
	// Best-effort: a failed insert self-heals on the next poll and surfaces as
	// the account going stale, so it is intentionally not escalated here.
	_ = m.Store.InsertUsageSample(s)
}
