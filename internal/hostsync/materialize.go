package hostsync

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/state"
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
// slot state and rejected envelopes, which only release the reservation.
func (s *Service) Materialize(ctx context.Context, v AccountValue, peers []string, pull PullCredential, manifestPath string) (MaterializeResult, error) {
	if v.UUID == "" {
		return MaterializeResult{}, fmt.Errorf("hostsync: Materialize requires a UUID")
	}
	// An unpublished identity would inject null and spin the abandon/retry loop; defer.
	if emptyOAuth(v.OAuthAccount) {
		return MaterializeResult{UUID: v.UUID, Deferred: true}, nil
	}

	p, err := s.M.PrepareAdd(ctx)
	if err != nil {
		return MaterializeResult{}, fmt.Errorf("materialize %s: prepare add: %w", v.UUID, err)
	}

	// Every failure past PrepareAdd must roll the dir and reservation back —
	// except a kept dir's retained login state, which release preserves.
	abandon := func(cause error) (MaterializeResult, error) {
		if aerr := s.M.AbandonAdd(p); aerr != nil {
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

	// A kept dir may retain a usable credential from an interrupted `ccp add`
	// (ReleaseAdd keeps login state); materializing over it would destroy an
	// owned chain, so abort before touching the dir.
	if p.ClaudeJSONSeed == pool.SeedKeptExisting {
		retained, err := s.slotRetainsCredential(p)
		if err != nil {
			return release(fmt.Errorf("materialize %s: %w", v.UUID, err))
		}
		if retained {
			return release(fmt.Errorf("materialize %s: %s retains a credential from an interrupted add; resume it with `ccp add` or remove the dir", v.UUID, p.ConfigDir))
		}
	}

	bootstrapped := false
	if p.ClaudeJSONSeed == pool.SeedNoSource {
		// No seed document exists; WriteIdentity would ENOENT forever without this bootstrap.
		if err := state.AtomicWrite(privateClaudeJSON(p.OverlayKind, p.ConfigDir), []byte("{}"), 0o600); err != nil {
			return abandon(fmt.Errorf("materialize %s: bootstrap private .claude.json: %w", v.UUID, err))
		}
		bootstrapped = true
	}

	if err := pool.WriteIdentity(p.OverlayKind, p.ConfigDir, v.OAuthAccount); err != nil {
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
		return abandon(fmt.Errorf("materialize %s: %w", v.UUID, errors.Join(ErrMaterializeNoEnvelope, err)))
	case env == nil:
		return abandon(fmt.Errorf("materialize %s: %w", v.UUID, ErrMaterializeNoEnvelope))
	case env.HasRefreshToken():
		return release(fmt.Errorf("materialize %s: %w", v.UUID, pool.ErrEnvelopeCarriesSecret))
	case !env.Synced():
		return release(fmt.Errorf("materialize %s: %w", v.UUID, pool.ErrEnvelopeNoAccessToken))
	}

	fileFallback, err := s.installEnvelope(p, env)
	if err != nil {
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

	// The new stamp dir is not watched yet; the nudge re-reads the manifest.
	s.NudgeSynckitd(ctx, manifestPath)

	return MaterializeResult{
		UUID:         v.UUID,
		AccountID:    acct.ID,
		FileFallback: fileFallback,
		Bootstrapped: bootstrapped,
	}, nil
}

// slotRetainsCredential reports whether a kept dir's credential slot still
// holds a credential (tombstones excluded); an unprovable backend fails closed.
func (s *Service) slotRetainsCredential(p *pool.PendingAdd) (bool, error) {
	acct := store.Account{ConfigDir: p.ConfigDir, KeychainService: p.KeychainService, KeychainAccount: creds.AccountLabel()}
	account, err := s.M.Creds.Discover(p.KeychainService)
	switch {
	case err == nil:
		acct.KeychainAccount = account
	case !errors.Is(err, creds.ErrNotFound):
		return false, fmt.Errorf("probe retained credential for %s: %w", p.ConfigDir, err)
	}
	for _, st := range s.M.Creds.Stores(acct) {
		_, err := st.Read()
		switch {
		case errors.Is(err, creds.ErrNotFound), errors.Is(err, creds.ErrNoTokens):
		case err != nil:
			return false, fmt.Errorf("probe retained credential in %s: %w", st, err)
		default:
			return true, nil
		}
	}
	return false, nil
}

// installEnvelope writes the pulled stripped credential to the Keychain,
// falling back to the file store when the login keychain is unsearchable (the
// returned bool flags the fallback). It writes directly — no row exists yet
// for OnCredWrite.
func (s *Service) installEnvelope(p *pool.PendingAdd, env *creds.Credential) (bool, error) {
	switch {
	case env.HasRefreshToken():
		return false, fmt.Errorf("install credential envelope: %w", pool.ErrEnvelopeCarriesSecret)
	case !env.Synced():
		return false, fmt.Errorf("install credential envelope: %w", pool.ErrEnvelopeNoAccessToken)
	}
	acct := store.Account{
		ConfigDir:       p.ConfigDir,
		KeychainService: p.KeychainService,
		KeychainAccount: creds.AccountLabel(),
	}
	kc := s.M.Creds.Store(acct, creds.SourceKeychain)
	_, probeErr := kc.Read()
	target := kc
	fileFallback := false
	switch {
	case probeErr == nil, errors.Is(probeErr, creds.ErrNotFound):
		// Keychain reachable (item already present or provably absent): install there.
	case errors.Is(probeErr, creds.ErrUnavailable):
		target = s.M.Creds.Store(acct, creds.SourceFile)
		fileFallback = true
	default:
		return false, fmt.Errorf("probe keychain %s: %w", p.KeychainService, probeErr)
	}
	if err := target.Write(env); err != nil {
		return false, fmt.Errorf("write credential to %s: %w", target, err)
	}
	return fileFallback, nil
}

// privateClaudeJSON is the account's private .claude.json path — the same
// private-root math as pool.WriteIdentity and pool.AccountIdentity.
func privateClaudeJSON(backend fkoverlay.Backend, configDir string) string {
	priv := configDir
	if backend != fkoverlay.BackendSymlink {
		priv = fkoverlay.FusePrivateRoot(configDir)
	}
	return filepath.Join(priv, ".claude.json")
}
