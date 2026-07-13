package pool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
)

// RefreshLeadTime is how close to expiry an idle account's token is preemptively refreshed.
const RefreshLeadTime = 10 * time.Minute

// ErrNeedsLogin indicates the stored refresh token is gone/revoked and the
// account must be re-logged-in interactively.
var ErrNeedsLogin = errors.New("account needs re-login (refresh token missing or revoked)")

// ErrUnrefreshable indicates an expired (or server-rejected) synced
// credential: only the origin host holds the refresh token, so this host can
// only wait for the origin's rotation to sync over, or mint its own chain via
// `ccp login`.
var ErrUnrefreshable = errors.New("credential expired and holds no refresh token; this host cannot refresh it")

// ErrCredentialChangedUnderfoot aborts a write-back when a concurrent writer
// (usually `claude /login`) minted a newer credential we must not clobber.
var ErrCredentialChangedUnderfoot = errors.New("stored credential changed under us before write-back")

// ErrCredentialUnverifiable aborts a write-back when the pre-write re-read
// failed for anything but a proven-empty slot (absent, or a tombstone):
// writing over an unverifiable state could destroy a credential we cannot see.
var ErrCredentialUnverifiable = errors.New("stored credential unreadable before write-back")

// EnsureFreshToken returns the account's credential, refreshing it when the access
// token expires within `within` and allowRefresh is true. allowRefresh must be
// false for an account with a live session (that session owns refresh).
func (m *Manager) EnsureFreshToken(ctx context.Context, a store.Account, within time.Duration, allowRefresh bool) (*creds.Credential, bool, error) {
	release, err := m.lockAccount(ctx, a.ID)
	if err != nil {
		return nil, false, err
	}
	defer release()
	cred, _, refreshed, err := m.ensureFreshToken(ctx, a, within, allowRefresh)
	return cred, refreshed, err
}

// ReadCredential resolves a's credential from whichever backend holds it, reading
// every candidate store (the Keychain claude prefers, then the plaintext
// .credentials.json). On drift ownership then freshness decides (see credOutranks) and
// its Source is returned. When every store misses, creds.ErrUnavailable outranks
// creds.ErrNotFound. See ccn doc 935d323.
func (m *Manager) ReadCredential(a store.Account) (*creds.Credential, creds.Source, error) {
	probes, win, err := m.probeCredentialStores(a)
	if err != nil {
		return nil, creds.SourceKeychain, err
	}
	if win != nil {
		return win.cred, win.store.Source(), nil
	}
	for _, p := range probes {
		if errors.Is(p.err, creds.ErrUnavailable) {
			return nil, creds.SourceKeychain, p.err
		}
	}
	return nil, creds.SourceKeychain, fmt.Errorf("no credential in the Keychain or credential file: %w", creds.ErrNotFound)
}

// writeCred upserts cred on src, then fires OnCredWrite; hook errors are
// logged and swallowed — the hook may never fail a refresh.
func (m *Manager) writeCred(a store.Account, src creds.Source, cred *creds.Credential) error {
	s := m.Creds.Store(a, src)
	if err := s.Write(cred); err != nil {
		return fmt.Errorf("write credential to %s: %w", s, err)
	}
	if m.OnCredWrite != nil {
		if err := m.OnCredWrite(a, cred); err != nil {
			log.Printf("acct-%d OnCredWrite hook: %v", a.ID, err)
		}
	}
	return nil
}

// writeCredCAS writes next only when the backend still holds prev (both
// tokens) or, for a nil prev, is still the empty slot the caller decided
// over. Any divergence — including the credential vanishing under a non-nil
// prev (a concurrent logout) — aborts with ErrCredentialChangedUnderfoot;
// unverifiable re-reads with ErrCredentialUnverifiable. Caller must hold the
// account lock.
func (m *Manager) writeCredCAS(a store.Account, src creds.Source, prev, next *creds.Credential) error {
	s := m.Creds.Store(a, src)
	cur, err := s.Read()
	switch {
	case errors.Is(err, creds.ErrNotFound), errors.Is(err, creds.ErrNoTokens):
		if prev != nil {
			return fmt.Errorf("%w: %s (credential deleted or tombstoned since the read)", ErrCredentialChangedUnderfoot, s)
		}
	case err != nil:
		return fmt.Errorf("%w: %s: %w", ErrCredentialUnverifiable, s, err)
	case prev == nil, !sameTokens(cur, prev):
		return fmt.Errorf("%w: %s (a concurrent writer owns the newer credential)", ErrCredentialChangedUnderfoot, s)
	}
	// Residual re-read→write TOCTOU vs an in-session claude (separate .oauth_refresh.lock); deferred by design — ccn task 4ed1146.
	return m.writeCred(a, src, next)
}

// ensureFreshToken requires the caller hold the per-account lock and is itself
// lock-free so SampleUsage composes it with fetchUsage's 401-retry in one critical
// section (sync.Mutex is not reentrant). Re-reading the credential under the lock lets
// a waiter that lost the race skip a redundant refresh POST.
func (m *Manager) ensureFreshToken(ctx context.Context, a store.Account, within time.Duration, allowRefresh bool) (*creds.Credential, creds.Source, bool, error) {
	cred, src, err := m.ReadCredential(a)
	if err != nil {
		if errors.Is(err, creds.ErrNoTokens) {
			return nil, src, false, fmt.Errorf("%w: %w", ErrNeedsLogin, err)
		}
		return nil, src, false, err
	}
	if !cred.ExpiresWithin(within) || !allowRefresh {
		return cred, src, false, nil
	}
	if cred.Synced() {
		// Nothing to refresh: the lead window is meaningless — the token
		// serves until actually expired.
		if !cred.Expired() {
			return cred, src, false, nil
		}
		return cred, src, false, ErrUnrefreshable
	}
	refreshed, err := m.refresh(ctx, a, src, cred)
	if err != nil {
		_ = m.Store.LogRefresh(a.ID, false, err.Error())
		var re *oauth.RefreshError
		if errors.As(err, &re) && re.Revoked() {
			m.stripSpentRefreshToken(a, src, cred, re)
			return cred, src, false, ErrNeedsLogin
		}
		// Transient: fall back to the stale credential.
		return cred, src, false, err
	}
	_ = m.Store.LogRefresh(a.ID, true, "")
	return refreshed, src, true, nil
}

// stripSpentRefreshToken demotes a server-confirmed dead chain to a
// refresh-token-free blob, which is pull-healable where an owned one never is:
// an access token stays servable until expiry; a refresh-only blob becomes a
// tombstone (ErrNoTokens → needs-login). Only invalid_grant strips — a plain
// 401 may be transient. Best-effort: the CAS aborts if a concurrent
// login/rotation landed, and needs-login covers any failure.
func (m *Manager) stripSpentRefreshToken(a store.Account, src creds.Source, cred *creds.Credential, re *oauth.RefreshError) {
	if !re.InvalidGrant() {
		return
	}
	if err := m.writeCredCAS(a, src, cred, cred.Strip()); err != nil {
		log.Printf("acct-%d strip spent refresh token: %v", a.ID, err)
	}
}

// refresh performs the OAuth refresh and persists the new blob, preserving the prior
// credential's non-token fields. Caller must hold the per-account lock. Each account
// runs its own token chain, so refreshing a pool account never touches plain claude.
func (m *Manager) refresh(ctx context.Context, a store.Account, src creds.Source, prev *creds.Credential) (*creds.Credential, error) {
	tr, err := m.OAuth.Refresh(ctx, fmt.Sprintf("acct-%d", a.ID), prev.ClaudeAiOauth.RefreshToken)
	if err != nil {
		return nil, err
	}
	next := &creds.Credential{ClaudeAiOauth: prev.ClaudeAiOauth}
	next.ClaudeAiOauth.AccessToken = tr.AccessToken
	if tr.RefreshToken != "" { // rotated
		next.ClaudeAiOauth.RefreshToken = tr.RefreshToken
	}
	next.ClaudeAiOauth.ExpiresAt = tr.Expiry(time.Now()).UnixMilli()
	if err := m.writeCredCAS(a, src, prev, next); err != nil {
		return nil, err
	}
	return next, nil
}

// AdoptRotatedToken re-reads an account's credential (a live session may have rotated
// it) and writes it back to re-assert our `security`-trusted ACL over the rotated
// Keychain item; on the file backend it is a harmless no-ACL rewrite. The write-back
// is CAS-guarded against a concurrent `claude /login`.
func (m *Manager) AdoptRotatedToken(ctx context.Context, a store.Account) error {
	release, err := m.lockAccount(ctx, a.ID)
	if err != nil {
		return err
	}
	defer release()
	cred, src, err := m.ReadCredential(a)
	if err != nil {
		return err
	}
	return m.writeCredCAS(a, src, cred, cred)
}

// SampleOpts controls how SampleUsage may recover a 401. AllowRefresh permits the
// normal idle-account refresh (the account owns its token chain). AllowBusyRefresh
// permits the guarded refresh for a live session with an expired token; fetchUsage
// gates it so it never re-POSTs a refresh token the session may rotate.
type SampleOpts struct {
	AllowRefresh     bool
	AllowBusyRefresh bool
}

// SampleUsage fetches the account's usage windows (recovering a 401 per opts),
// records a usage_sample, and reports whether the account is rate-limited along
// with the server's Retry-After hint from a 429 (0 when absent or not a 429).
func (m *Manager) SampleUsage(ctx context.Context, a store.Account, opts SampleOpts) (*oauth.Usage, bool, time.Duration, error) {
	usage, rateLimited, retryAfter, err := m.sampleUsage(ctx, a, opts)
	if err != nil {
		return nil, rateLimited, retryAfter, err
	}
	m.recordSample(a.ID, usage, rateLimited)
	return usage, rateLimited, retryAfter, nil
}

// sampleUsage holds acctLock across the whole credential span so the pre-flight refresh
// and fetchUsage's 401-retry form one atomic cycle; else a peer could rotate the token
// between them and the retry would re-POST a consumed single-use refresh token.
func (m *Manager) sampleUsage(ctx context.Context, a store.Account, opts SampleOpts) (*oauth.Usage, bool, time.Duration, error) {
	release, err := m.lockAccount(ctx, a.ID)
	if err != nil {
		return nil, false, 0, err
	}
	defer release()
	cred, src, _, freshErr := m.ensureFreshToken(ctx, a, RefreshLeadTime, opts.AllowRefresh)
	if cred == nil {
		return nil, false, 0, freshErr
	}
	// An expired synced token cannot be refreshed on this host, so a grace-window
	// 200 from the usage endpoint must not mask it: propagate ErrUnrefreshable so
	// the daemon flags awaiting-origin. cred is non-nil here (the expired synced
	// blob), so this never re-treads the nil-cred panic path.
	if errors.Is(freshErr, ErrUnrefreshable) {
		return nil, false, 0, freshErr
	}
	usage, rateLimited, retryAfter, err := m.fetchUsage(ctx, a, src, cred, opts)
	// A confirmed pre-flight revocation must not be masked by a usage-endpoint 429 or
	// transient 401; a clean fetchUsage recovery suppresses it (a session may have rotated).
	if errors.Is(freshErr, ErrNeedsLogin) && (err != nil || rateLimited) {
		return nil, false, 0, freshErr
	}
	return usage, rateLimited, retryAfter, err
}

// sameTokens reports whether both access and refresh tokens match — no session rotated
// the chain.
func sameTokens(a, b *creds.Credential) bool {
	return a.ClaudeAiOauth.AccessToken == b.ClaudeAiOauth.AccessToken &&
		a.ClaudeAiOauth.RefreshToken == b.ClaudeAiOauth.RefreshToken
}

// fetchUsage fetches usage and, on a 401, walks a recovery ladder: re-read for a
// session-rotated token, else signed-out (ErrNeedsLogin), else refresh+retry when
// permitted. Caller must hold the per-account lock.
func (m *Manager) fetchUsage(ctx context.Context, a store.Account, src creds.Source, cred *creds.Credential, opts SampleOpts) (*oauth.Usage, bool, time.Duration, error) {
	usage, err := m.OAuth.Usage(ctx, cred.ClaudeAiOauth.AccessToken)
	if err == nil {
		return usage, false, 0, nil
	}
	var ue *oauth.UsageError
	if !errors.As(err, &ue) {
		return nil, false, 0, err
	}
	if ue.RateLimited() {
		return &oauth.Usage{}, true, ue.RetryAfter, nil
	}
	if !ue.Unauthorized() {
		return nil, false, 0, err
	}

	if reread, _, rerr := m.ReadCredential(a); rerr == nil && reread.ClaudeAiOauth.AccessToken != cred.ClaudeAiOauth.AccessToken {
		if usage, err2 := m.OAuth.Usage(ctx, reread.ClaudeAiOauth.AccessToken); err2 == nil {
			return usage, false, 0, nil
		}
		cred = reread // the rotated token also 401s; refresh from it below
	}

	if !cred.HasRefreshToken() {
		return nil, false, 0, fmt.Errorf("%w: %w", ErrUnrefreshable, err)
	}

	// A busy account may refresh only under the guard: provably expired and unchanged
	// on a fresh re-read.
	mayRefresh := opts.AllowRefresh
	if !mayRefresh && opts.AllowBusyRefresh && cred.Expired() {
		if reread, _, rerr := m.ReadCredential(a); rerr == nil && sameTokens(reread, cred) {
			mayRefresh = true
		}
	}
	if !mayRefresh {
		return nil, false, 0, err
	}

	refreshed, rfErr := m.refresh(ctx, a, src, cred)
	if rfErr != nil {
		_ = m.Store.LogRefresh(a.ID, false, rfErr.Error())
		var re *oauth.RefreshError
		if errors.As(rfErr, &re) && re.Revoked() {
			// Revoked: a differing on-disk credential means a session rotated the chain
			// (transient); unchanged means genuine server-side revocation.
			if reread, _, rerr := m.ReadCredential(a); rerr == nil && !sameTokens(reread, cred) {
				return nil, false, 0, err
			}
			m.stripSpentRefreshToken(a, src, cred, re)
			return nil, false, 0, fmt.Errorf("%w: %w", ErrNeedsLogin, rfErr)
		}
		return nil, false, 0, err
	}
	_ = m.Store.LogRefresh(a.ID, true, "")
	if usage, err2 := m.OAuth.Usage(ctx, refreshed.ClaudeAiOauth.AccessToken); err2 == nil {
		return usage, false, 0, nil
	}
	return nil, false, 0, err
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
	// Collapse the per-model weekly limits to the single binding (max-util) bucket;
	// oauth keeps the full slice, only the binding one is carried downstream.
	if sw, ok := u.BindingScoped(); ok {
		s.Scoped7dModel = sw.ModelName
		s.Scoped7dUtil = sw.Used()
		s.Scoped7dResets = sw.ResetsAt
	}
	// Best-effort: a failed insert self-heals on the next poll (account goes stale),
	// so the error is intentionally not escalated.
	_ = m.Store.InsertUsageSample(s)
}
