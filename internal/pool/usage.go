package pool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

var (
	errCredentialDeterministicNeedsLogin = errors.New("credential refresh deterministically requires login")
	errCredentialDeterministicNoTokens   = errors.New("credential refresh deterministically produced no tokens")
	errCredentialCleanupPending          = errors.New("credential refresh cleanup remains pending")
)

// EnsureFreshToken returns the account's credential, refreshing it when the access
// token expires within `within` and allowRefresh is true. allowRefresh must be
// false for an account with a live session (that session owns refresh).
func (m *Manager) EnsureFreshToken(ctx context.Context, a store.Account, within time.Duration, allowRefresh bool) (*creds.Credential, bool, error) {
	result, err := m.ensureFreshTokenOperation(ctx, a, within, allowRefresh)
	return result.Credential, result.Refreshed, err
}

func (m *Manager) ensureFreshTokenOperation(
	ctx context.Context,
	a store.Account,
	within time.Duration,
	allowRefresh bool,
) (freshTokenResult, error) {
	result, err := runCredentialOperation(
		ctx,
		m,
		a,
		store.CredentialOperationEnsureFresh,
		freshCredentialOperationCodec(),
		func(ctx context.Context, boundary *credentialOperationBoundary) (freshTokenResult, error) {
			credential, source, refreshed, err := m.ensureFreshToken(
				ctx,
				a,
				within,
				allowRefresh,
				boundary,
			)
			return freshTokenResult{
				Credential: credential, Source: source, Refreshed: refreshed,
				RefreshAttempted: boundary.crossed,
			}, err
		},
		within.String(),
		fmt.Sprintf("%t", allowRefresh),
	)
	return result, err
}

type freshTokenResult struct {
	Credential       *creds.Credential
	Source           creds.Source
	Refreshed        bool
	RefreshAttempted bool
}

// ReadCredential resolves a's credential from whichever backend holds it, reading
// every candidate store (the Keychain claude prefers, then the plaintext
// .credentials.json). On drift ownership then freshness decides (see credOutranks) and
// its Source is returned. When every store misses, creds.ErrUnavailable outranks
// creds.ErrNotFound. See ccn doc 935d323.
func (m *Manager) ReadCredential(
	ctx context.Context,
	a store.Account,
) (*creds.Credential, creds.Source, error) {
	probes, win, err := m.probeCredentialStores(ctx, a)
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

// writeObservedCredential stages publication before crossing the journal fence,
// then delegates the authoritative compare-and-swap to the refresh-lock worker.
func (m *Manager) writeObservedCredential(
	ctx context.Context,
	a store.Account,
	src creds.Source,
	prev, next *creds.Credential,
	boundary *credentialOperationBoundary,
) error {
	if boundary == nil {
		return errors.New("credential mutation boundary is required")
	}
	if m.credentialCAS == nil {
		return errors.New("credential CAS worker is unavailable")
	}
	s := m.Creds.Store(a, src)
	cur, err := s.Read(ctx)
	switch creds.ClassifyRead(err) {
	case creds.ReadEmpty:
		if prev != nil {
			return fmt.Errorf("%w: %s (credential deleted or tombstoned since the read)", ErrCredentialChangedUnderfoot, s)
		}
	case creds.ReadUnsearchable, creds.ReadFatal:
		return fmt.Errorf("%w: %s: %w", ErrCredentialUnverifiable, s, err)
	case creds.ReadPresent:
		if prev == nil || !sameTokens(cur, prev) {
			return fmt.Errorf("%w: %s (a concurrent writer owns the newer credential)", ErrCredentialChangedUnderfoot, s)
		}
	}
	if err := boundary.recordCredentialWrite(next); err != nil {
		return err
	}
	if err := boundary.Cross(ctx); err != nil {
		return err
	}
	if _, err := m.credentialCAS(ctx, a, boundary.expected, credentialCASMutation{
		Target: src, Credential: next,
	}); err != nil {
		if errors.Is(err, errCredentialCASConflict) {
			return ErrCredentialChangedUnderfoot
		}
		return err
	}
	return nil
}

// ensureFreshToken runs inside one durable credential operation. Re-reading the
// credential after admission lets a coalesced caller skip a redundant refresh POST.
func (m *Manager) ensureFreshToken(
	ctx context.Context,
	a store.Account,
	within time.Duration,
	allowRefresh bool,
	boundary *credentialOperationBoundary,
) (*creds.Credential, creds.Source, bool, error) {
	cred, src, err := m.ReadCredential(ctx, a)
	if err != nil {
		if errors.Is(err, creds.ErrNoTokens) || errors.Is(err, creds.ErrNotFound) {
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
	refreshed, err := m.refresh(ctx, a, src, boundary)
	if err != nil {
		category, digest := classifyRefreshOutcome(err)
		_ = m.Store.LogRefresh(a.ID, category, digest)
		var re *oauth.RefreshError
		if errors.As(err, &re) && re.InvalidGrant() {
			stripErr := m.stripSpentRefreshToken(ctx, a, src, cred, re, boundary)
			if stripErr != nil {
				if current, currentSource, ok := m.concurrentCredentialRotation(ctx, a, cred); ok {
					return current, currentSource, false, nil
				}
				return cred, src, false, errors.Join(
					err, stripErr, deterministicNeedsLoginError(cred.Strip()),
					errCredentialCleanupPending,
				)
			}
			return cred, src, false, deterministicNeedsLoginError(cred.Strip())
		}
		// Transient: fall back to the stale credential.
		return cred, src, false, err
	}
	_ = m.Store.LogRefresh(a.ID, store.RefreshSucceeded, [32]byte{})
	return refreshed, src, true, nil
}

func (m *Manager) concurrentCredentialRotation(
	ctx context.Context,
	a store.Account,
	expected *creds.Credential,
) (*creds.Credential, creds.Source, bool) {
	current, source, err := m.ReadCredential(ctx, a)
	if err != nil || sameTokens(current, expected) {
		return nil, source, false
	}
	return current, source, true
}

func classifyRefreshOutcome(err error) (store.RefreshCategory, [32]byte) {
	digest := sha256.Sum256([]byte(err.Error()))
	switch {
	case errors.Is(err, context.Canceled):
		return store.RefreshCanceled, digest
	case errors.Is(err, oauth.ErrNetwork):
		return store.RefreshNetwork, digest
	}
	var refreshErr *oauth.RefreshError
	if !errors.As(err, &refreshErr) {
		return store.RefreshInternal, digest
	}
	switch {
	case refreshErr.InvalidGrant():
		return store.RefreshInvalidGrant, digest
	case refreshErr.Status >= 500:
		return store.RefreshServer, digest
	default:
		return store.RefreshRejected, digest
	}
}

// stripSpentRefreshToken demotes a server-confirmed dead chain to a
// refresh-token-free blob, which is pull-healable where an owned one never is:
// an access token stays servable until expiry; a refresh-only blob becomes a
// tombstone (ErrNoTokens → needs-login). Only invalid_grant strips — a plain
// 401 may be transient. Best-effort: the CAS aborts if a concurrent
// login/rotation landed, and needs-login covers any failure.
func (m *Manager) stripSpentRefreshToken(
	ctx context.Context,
	a store.Account,
	src creds.Source,
	cred *creds.Credential,
	re *oauth.RefreshError,
	boundary *credentialOperationBoundary,
) error {
	if !re.InvalidGrant() {
		return nil
	}
	if err := m.writeObservedCredential(ctx, a, src, cred, cred.Strip(), boundary); err != nil {
		log.Printf("acct-%d strip spent refresh token: %v", a.ID, err)
		return err
	}
	return nil
}

func deterministicNeedsLoginError(credential *creds.Credential) error {
	if credential.ClaudeAiOauth.AccessToken == "" && !credential.HasRefreshToken() {
		return errors.Join(
			ErrNeedsLogin, creds.ErrNoTokens, errCredentialDeterministicNoTokens,
		)
	}
	return errors.Join(ErrNeedsLogin, errCredentialDeterministicNeedsLogin)
}

// refresh performs the OAuth refresh and persists the new blob, preserving the prior
// credential's non-token fields. Each account runs its own token chain, so
// refreshing a pool account never touches plain claude.
func (m *Manager) refresh(
	ctx context.Context,
	a store.Account,
	src creds.Source,
	boundary *credentialOperationBoundary,
) (*creds.Credential, error) {
	if m.credentialCAS == nil {
		return nil, errors.New("credential CAS worker is unavailable")
	}
	if err := boundary.Cross(ctx); err != nil {
		return nil, err
	}
	proof, err := m.credentialCAS(ctx, a, boundary.expected, credentialCASMutation{
		Target: src, Refresh: true,
	})
	if err != nil {
		if errors.Is(err, errCredentialCASConflict) {
			return nil, ErrCredentialChangedUnderfoot
		}
		return nil, err
	}
	if proof.Credential == nil {
		return nil, errors.New("credential refresh worker returned no credential")
	}
	if err := boundary.recordCredentialWrite(proof.Credential); err != nil {
		return nil, err
	}
	return proof.Credential, nil
}

// AdoptRotatedToken re-reads an account's credential (a live session may have rotated
// it) and writes it back to re-assert our `security`-trusted ACL over the rotated
// Keychain item; on the file backend it is a harmless no-ACL rewrite. The write-back
// is CAS-guarded against a concurrent `claude /login`.
func (m *Manager) AdoptRotatedToken(ctx context.Context, a store.Account) error {
	if err := m.requireCredentialMutationAllowed(a); err != nil {
		return err
	}
	_, source, err := m.ReadCredential(ctx, a)
	if err != nil {
		return err
	}
	_, err = runCredentialOperation(
		ctx,
		m,
		a,
		store.CredentialOperationAdoptRotated,
		unitCredentialOperationCodec(credentialTarget(source)),
		func(ctx context.Context, boundary *credentialOperationBoundary) (struct{}, error) {
			return struct{}{}, m.adoptRotatedToken(ctx, a, source, boundary)
		},
	)
	return err
}

func (m *Manager) adoptRotatedToken(
	ctx context.Context,
	a store.Account,
	expectedSource creds.Source,
	boundary *credentialOperationBoundary,
) error {
	credential, source, err := m.ReadCredential(ctx, a)
	if err != nil || source != expectedSource {
		return errors.Join(ErrCredentialChangedUnderfoot, err)
	}
	return m.writeObservedCredential(
		ctx, a, source, credential, credential, boundary,
	)
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

// ProbeUsageReachability performs one read-only usage request with the stored
// access token. It never refreshes or records a sample; outage recovery uses it
// when durable credential evidence returned without any network I/O.
func (m *Manager) ProbeUsageReachability(
	ctx context.Context,
	a store.Account,
) (probed, rateLimited bool, retryAfter time.Duration, err error) {
	credential, _, readErr := m.ReadCredential(ctx, a)
	if readErr != nil || credential == nil || credential.ClaudeAiOauth.AccessToken == "" || m.OAuth == nil {
		return false, false, 0, readErr
	}
	_, err = m.OAuth.Usage(ctx, credential.ClaudeAiOauth.AccessToken)
	var usageErr *oauth.UsageError
	if errors.As(err, &usageErr) && usageErr.RateLimited() {
		return true, true, usageErr.RetryAfter, nil
	}
	return true, false, 0, err
}

// sampleUsage serializes only credential refresh/mutation. The usage request is
// ordinary network I/O and never occupies the account's durable credential lane.
func (m *Manager) sampleUsage(ctx context.Context, a store.Account, opts SampleOpts) (*oauth.Usage, bool, time.Duration, error) {
	fresh, freshErr := m.ensureFreshTokenOperation(ctx, a, RefreshLeadTime, opts.AllowRefresh)
	cred, src := fresh.Credential, fresh.Source
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
	fetchOpts := opts
	if fresh.RefreshAttempted {
		fetchOpts.AllowRefresh = false
		fetchOpts.AllowBusyRefresh = false
	}
	usage, rateLimited, retryAfter, err := m.fetchUsage(ctx, a, src, cred, fetchOpts)
	if errors.Is(freshErr, ErrCredentialOperationQuarantined) {
		return nil, false, 0, freshErr
	}
	replayedLiveProbe := errors.Is(freshErr, ErrCredentialOperationReplayed) &&
		(err != nil || rateLimited)
	if replayedLiveProbe {
		freshErr = errors.Join(freshErr, err, ErrCredentialOperationLiveProbe)
	}
	// A confirmed pre-flight revocation must not be masked by a usage-endpoint 429 or
	// transient 401; a clean fetchUsage recovery suppresses it (a session may have rotated).
	if errors.Is(freshErr, ErrNeedsLogin) && (err != nil || rateLimited) {
		if replayedLiveProbe {
			return nil, rateLimited, retryAfter, freshErr
		}
		return nil, false, 0, freshErr
	}
	if freshErr != nil && (err != nil || rateLimited) {
		return nil, false, 0, errors.Join(freshErr, err)
	}
	if err != nil && replayedLiveProbe {
		err = errors.Join(
			err, ErrCredentialOperationReplayed, ErrCredentialOperationLiveProbe,
		)
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
// permitted. Usage I/O stays outside the durable lane; only the guarded refresh
// enters a credential operation.
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

	if reread, _, rerr := m.ReadCredential(ctx, a); rerr == nil && reread.ClaudeAiOauth.AccessToken != cred.ClaudeAiOauth.AccessToken {
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
		if reread, _, rerr := m.ReadCredential(ctx, a); rerr == nil && sameTokens(reread, cred) {
			mayRefresh = true
		}
	}
	if !mayRefresh {
		return nil, false, 0, err
	}

	fresh, rfErr := m.refreshCurrentCredentialOperation(ctx, a, src, cred)
	if rfErr != nil {
		if errors.Is(rfErr, ErrNeedsLogin) {
			return nil, false, 0, rfErr
		}
		return nil, false, 0, err
	}
	if fresh.Credential == nil {
		return nil, false, 0, err
	}
	if usage, err2 := m.OAuth.Usage(ctx, fresh.Credential.ClaudeAiOauth.AccessToken); err2 == nil {
		return usage, false, 0, nil
	}
	return nil, false, 0, err
}

func (m *Manager) refreshCurrentCredentialOperation(
	ctx context.Context,
	a store.Account,
	source creds.Source,
	expected *creds.Credential,
) (freshTokenResult, error) {
	fingerprint := sha256.Sum256([]byte(expected.ClaudeAiOauth.AccessToken + "\x00" + expected.ClaudeAiOauth.RefreshToken))
	return runCredentialOperation(
		ctx,
		m,
		a,
		store.CredentialOperationRefreshCurrent,
		freshCredentialOperationCodec(),
		func(ctx context.Context, boundary *credentialOperationBoundary) (result freshTokenResult, resultErr error) {
			defer func() { result.RefreshAttempted = boundary.crossed }()
			current, currentSource, err := m.ReadCredential(ctx, a)
			if err != nil {
				return freshTokenResult{}, err
			}
			if !sameTokens(current, expected) {
				return freshTokenResult{Credential: current, Source: currentSource}, nil
			}
			refreshed, err := m.refresh(ctx, a, source, boundary)
			if err == nil {
				_ = m.Store.LogRefresh(a.ID, store.RefreshSucceeded, [32]byte{})
				return freshTokenResult{Credential: refreshed, Source: source, Refreshed: true}, nil
			}
			category, digest := classifyRefreshOutcome(err)
			_ = m.Store.LogRefresh(a.ID, category, digest)
			var refreshErr *oauth.RefreshError
			if errors.As(err, &refreshErr) && refreshErr.InvalidGrant() {
				if reread, rereadSource, readErr := m.ReadCredential(ctx, a); readErr == nil && !sameTokens(reread, current) {
					return freshTokenResult{Credential: reread, Source: rereadSource}, nil
				}
				stripErr := m.stripSpentRefreshToken(ctx, a, source, current, refreshErr, boundary)
				if stripErr != nil {
					if reread, rereadSource, ok := m.concurrentCredentialRotation(ctx, a, current); ok {
						return freshTokenResult{Credential: reread, Source: rereadSource}, nil
					}
					return freshTokenResult{Credential: current, Source: source, RefreshAttempted: true}, errors.Join(
						err, stripErr, deterministicNeedsLoginError(current.Strip()),
						errCredentialCleanupPending,
					)
				}
				return freshTokenResult{Credential: current, Source: source, RefreshAttempted: true}, errors.Join(
					err, deterministicNeedsLoginError(current.Strip()),
				)
			}
			return freshTokenResult{Credential: current, Source: source, RefreshAttempted: true}, err
		},
		hex.EncodeToString(fingerprint[:]),
	)
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
