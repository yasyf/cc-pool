package hostsync

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

// ErrMaterializeNoEnvelope is a retryable materialization failure: no peer
// held the credential envelope; the half-built account is rolled back first.
var ErrMaterializeNoEnvelope = errors.New("hostsync: no credential envelope available from peers")

// PullCredential fetches uuid's credential envelope from its chain origin,
// falling back to the other peers; a nil credential with a nil error means no envelope.
type PullCredential func(ctx context.Context, uuid string, chain ChainStamp, peers []string) (*creds.Credential, error)

// MaterializeResult reports what Materialize did for one peer-added account.
type MaterializeResult struct {
	// UUID is the account this pass acted on.
	UUID string
	// AccountID is the new pool row's account index, or 0 when Deferred.
	AccountID int
	// FileFallback is true when the credential landed in the plaintext file
	// store because the login Keychain was unsearchable.
	FileFallback bool
	// Bootstrapped is true when a minimal private .claude.json had to be
	// written first (no ~/.claude.json to seed from).
	Bootstrapped bool
	// Deferred is true when v carried no oauthAccount yet: nothing was created,
	// and a later scan-publish backfill supplies the identity.
	Deferred bool
}

// Materialize creates the local account for a peer-added entry without any
// interactive login. An entry with no oauthAccount defers; failures after
// PrepareAdd roll the dir and reservation back via AbandonAdd, except retained
// or unprovable slot state and rejected envelopes, which only release the
// reservation.
func (s *Service) Materialize(ctx context.Context, v AccountValue, peers []string, pull PullCredential, manifestPath string) (MaterializeResult, error) {
	if v.UUID == "" {
		return MaterializeResult{}, fmt.Errorf("hostsync: Materialize requires a UUID")
	}
	// An unpublished identity would inject null and spin the abandon/retry loop; defer.
	if emptyOAuth(v.OAuthAccount) {
		return MaterializeResult{UUID: v.UUID, Deferred: true}, nil
	}
	if s.Preparer == nil {
		return MaterializeResult{}, errors.New("hostsync: account preparer is required")
	}

	reservation, err := s.M.ReserveAdd()
	if err != nil {
		return MaterializeResult{}, err
	}
	p, err := s.M.PrepareReservedAdd(ctx, reservation)
	if err != nil {
		return MaterializeResult{}, fmt.Errorf("materialize %s: prepare add: %w", v.UUID, err)
	}

	// Every failure past PrepareAdd must roll the dir and reservation back —
	// except a kept dir's retained login state, which release preserves.
	abandon := func(cause error) (MaterializeResult, error) {
		if aerr := s.M.AbandonAdd(ctx, p); aerr != nil {
			return MaterializeResult{}, errors.Join(cause, fmt.Errorf("roll back %s: %w", v.UUID, aerr))
		}
		return MaterializeResult{}, cause
	}
	release := func(cause error) (MaterializeResult, error) {
		if rerr := s.M.ReleaseAdd(p); rerr != nil {
			return MaterializeResult{}, errors.Join(cause, fmt.Errorf("release %s: %w", v.UUID, rerr))
		}
		return MaterializeResult{}, cause
	}
	// Rollback after a failed pull is decided by what's in the slot, not the
	// pull-error class: a concurrent `ccp add` login may have landed an owned
	// credential mid-pull, which AbandonAdd would delete. Only a provably
	// empty slot is torn down; a retained or unprovable one is released intact.
	abandonUnlessRetained := func(cause error) (MaterializeResult, error) {
		retained, err := s.slotRetainsCredential(ctx, p)
		switch {
		case err != nil:
			return release(errors.Join(cause, err))
		case retained:
			return release(cause)
		}
		return abandon(cause)
	}

	// A kept dir may retain a usable credential from an interrupted `ccp add`
	// (ReleaseAdd keeps login state); materializing over it would destroy an
	// owned chain, so abort before touching the dir.
	if p.ClaudeJSONSeed == pool.SeedKeptExisting {
		retained, err := s.slotRetainsCredential(ctx, p)
		if err != nil {
			return release(fmt.Errorf("materialize %s: %w", v.UUID, err))
		}
		if retained {
			return release(fmt.Errorf("materialize %s: %s retains a credential from an interrupted add; resume it with `ccp add` or remove the dir", v.UUID, p.ConfigDir))
		}
	}

	// The backing worker seeded the minimal onboarding document when no source existed.
	bootstrapped := p.ClaudeJSONSeed == pool.SeedNoSource

	if err := s.M.WriteIdentity(ctx, p.Reservation.ID, p.ConfigDir, v.OAuthAccount); err != nil {
		return abandon(fmt.Errorf("materialize %s: write identity: %w", v.UUID, err))
	}

	env, err := pull(ctx, v.UUID, v.Chain, peers)
	switch {
	// A rejected envelope (the puller's per-peer sentinels included) must
	// never AbandonAdd: that deletes any retained slot credentials — release
	// keeps them intact.
	case errors.Is(err, pool.ErrEnvelopeCarriesSecret), errors.Is(err, pool.ErrEnvelopeNoAccessToken):
		return release(fmt.Errorf("materialize %s: %w", v.UUID, err))
	case err != nil:
		return abandonUnlessRetained(fmt.Errorf("materialize %s: %w", v.UUID, errors.Join(ErrMaterializeNoEnvelope, err)))
	case env == nil:
		return abandonUnlessRetained(fmt.Errorf("materialize %s: %w", v.UUID, ErrMaterializeNoEnvelope))
	case env.HasRefreshToken():
		return release(fmt.Errorf("materialize %s: %w", v.UUID, pool.ErrEnvelopeCarriesSecret))
	case !env.Synced():
		return release(fmt.Errorf("materialize %s: %w", v.UUID, pool.ErrEnvelopeNoAccessToken))
	}

	fileFallback, err := s.installEnvelope(ctx, p, env)
	switch {
	// A credential landed (or a backend became unprovable) under the pull:
	// AbandonAdd would delete it — release keeps it.
	case errors.Is(err, pool.ErrCredentialChangedUnderfoot), errors.Is(err, pool.ErrCredentialUnverifiable):
		return release(fmt.Errorf("materialize %s: install credential: %w", v.UUID, err))
	case err != nil:
		return abandon(fmt.Errorf("materialize %s: install credential: %w", v.UUID, err))
	}

	acct, err := s.M.FinalizeAdd(ctx, p, v.Label)
	if acct == nil {
		// A hard finalize failure leaves no row: roll the dir + reservation back.
		return abandon(fmt.Errorf("materialize %s: finalize: %w", v.UUID, err))
	}
	if err != nil {
		// The row landed; only best-effort usage validation failed — keep the account.
		s.logf("hostsync: materialized %s (acct-%d) but usage validation failed: %v", v.UUID, acct.ID, err)
	}

	if err := s.M.Store.SetAccountUUID(acct.ID, v.UUID); err != nil {
		return MaterializeResult{}, fmt.Errorf("materialize %s: backfill account uuid on acct-%d: %w", v.UUID, acct.ID, err)
	}
	committed, err := s.M.Store.GetAccount(acct.ID)
	if err != nil {
		return MaterializeResult{}, fmt.Errorf("materialize %s: read committed acct-%d: %w", v.UUID, acct.ID, err)
	}
	if err := s.Preparer.PrepareAccount(ctx, committed); err != nil {
		return MaterializeResult{}, fmt.Errorf("materialize %s: prepare acct-%d tenant: %w", v.UUID, acct.ID, err)
	}

	// The new stamp dir is not watched yet; the nudge re-reads the manifest.
	s.NudgeSynckitd(ctx, manifestPath)

	return MaterializeResult{
		UUID:         v.UUID,
		AccountID:    acct.ID,
		FileFallback: fileFallback,
		Bootstrapped: bootstrapped,
	}, nil
}

// slotAccount is the pending slot's credential coordinates; Discover adopts
// whatever account label a prior login wrote, and an absent or unsearchable
// Keychain keeps the default label (the store probes rule on it separately).
func (s *Service) slotAccount(
	ctx context.Context,
	p *pool.PendingAdd,
) (store.Account, error) {
	acct := store.Account{ID: p.Reservation.ID, InstanceID: p.Reservation.InstanceID, Generation: p.Reservation.Generation, ConfigDir: p.ConfigDir, KeychainService: p.KeychainService, KeychainAccount: creds.AccountLabel()}
	account, err := s.M.Creds.Discover(ctx, p.KeychainService)
	switch {
	case err == nil:
		acct.KeychainAccount = account
	case !errors.Is(err, creds.ErrNotFound) && !errors.Is(err, creds.ErrUnavailable):
		return store.Account{}, fmt.Errorf("probe credential slot for %s: %w", p.ConfigDir, err)
	}
	return acct, nil
}

// slotRetainsCredential reports whether a kept dir's credential slot still
// holds a credential (tombstones excluded); an unprovable backend fails closed.
func (s *Service) slotRetainsCredential(
	ctx context.Context,
	p *pool.PendingAdd,
) (bool, error) {
	acct, err := s.slotAccount(ctx, p)
	if err != nil {
		return false, err
	}
	for _, st := range s.M.Creds.Stores(acct) {
		_, err := st.Read(ctx)
		switch creds.ClassifyRead(err) {
		case creds.ReadEmpty:
		case creds.ReadUnsearchable, creds.ReadFatal:
			return false, fmt.Errorf("probe retained credential in %s: %w", st, err)
		case creds.ReadPresent:
			return true, nil
		}
	}
	return false, nil
}

// installEnvelope writes the pulled stripped credential to the Keychain,
// falling back to the file store when the login keychain is unsearchable (the
// returned bool flags the fallback), under the same owned-precedence guard as
// pool.InstallSyncedCredential — ccn note e30f860.
func (s *Service) installEnvelope(ctx context.Context, p *pool.PendingAdd, env *creds.Credential) (bool, error) {
	switch {
	case env.HasRefreshToken():
		return false, fmt.Errorf("install credential envelope: %w", pool.ErrEnvelopeCarriesSecret)
	case !env.Synced():
		return false, fmt.Errorf("install credential envelope: %w", pool.ErrEnvelopeNoAccessToken)
	}
	// PendingAdd is the durable exclusive reservation for this not-yet-created
	// account. No account mutex or flock spans the credential I/O.
	acct, err := s.slotAccount(ctx, p)
	if err != nil {
		return false, err
	}
	kc := s.M.Creds.Store(acct, creds.SourceKeychain)
	file := s.M.Creds.Store(acct, creds.SourceFile)
	target, fileFallback := kc, false
	_, kcErr := kc.Read(ctx)
	switch creds.ClassifyRead(kcErr) {
	case creds.ReadEmpty:
		// Provably empty (or a tombstone): install there.
	case creds.ReadUnsearchable:
		target, fileFallback = file, true
	case creds.ReadFatal:
		return false, fmt.Errorf("%w: %s: %w", pool.ErrCredentialUnverifiable, kc, kcErr)
	case creds.ReadPresent:
		return false, fmt.Errorf("%w: %s holds a credential", pool.ErrCredentialChangedUnderfoot, kc)
	}
	_, fErr := file.Read(ctx)
	switch creds.ClassifyRead(fErr) {
	case creds.ReadEmpty:
	case creds.ReadUnsearchable, creds.ReadFatal:
		return false, fmt.Errorf("%w: %s: %w", pool.ErrCredentialUnverifiable, file, fErr)
	case creds.ReadPresent:
		return false, fmt.Errorf("%w: %s holds a credential", pool.ErrCredentialChangedUnderfoot, file)
	}
	if err := target.Write(ctx, env); err != nil {
		return false, fmt.Errorf("write credential to %s: %w", target, err)
	}
	return fileFallback, nil
}
