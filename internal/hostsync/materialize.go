package hostsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

// MaterializeResult reports what Materialize did for one peer-added account.
type MaterializeResult struct {
	// UUID is the account this pass acted on.
	UUID string
	// AccountID is the new pool row's account index, or 0 when Deferred.
	AccountID int
	// Bootstrapped is true when a minimal private .claude.json had to be
	// written first (no ~/.claude.json to seed from).
	Bootstrapped bool
	// Deferred is true when v carried no oauthAccount yet: nothing was created,
	// and a later origin publication must supply the identity.
	Deferred bool
}

// Materialize creates the local account for a peer-added entry without any
// interactive login. An entry with no oauthAccount defers; failures after
// PrepareAdd roll the dir and reservation back via AbandonAdd, except retained
// or unprovable slot state and rejected envelopes, which only release the
// reservation.
func (s *Service) Materialize(ctx context.Context, v AccountValue, credential *creds.Credential, manifestPath string) (MaterializeResult, error) {
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
	if err := validateMaterializeIdentity(v); err != nil {
		return MaterializeResult{}, err
	}
	if credential == nil {
		return MaterializeResult{}, ErrCredentialMaterialUnavailable
	}
	if credential.HasRefreshToken() {
		return MaterializeResult{}, pool.ErrEnvelopeCarriesSecret
	}
	if !credential.Synced() {
		return MaterializeResult{}, pool.ErrEnvelopeNoAccessToken
	}
	if err := rejectExistingExternalUUID(ctx, s.M, v.UUID); err != nil {
		return MaterializeResult{}, err
	}

	reservation, err := s.M.ReserveAdd()
	if err != nil {
		return MaterializeResult{}, err
	}
	releaseReservation := func(cause error) (MaterializeResult, error) {
		if rerr := s.M.Store.ReleaseAccountIndex(reservation); rerr != nil {
			return MaterializeResult{}, errors.Join(cause, fmt.Errorf("release %s: %w", v.UUID, rerr))
		}
		return MaterializeResult{}, cause
	}
	presentation, err := s.Preparer.PrepareReservedAccount(ctx, reservation, v.Label)
	if err != nil {
		return releaseReservation(fmt.Errorf("materialize %s: prepare presentation: %w", v.UUID, err))
	}
	p, err := s.M.PrepareReservedSyncedAdd(ctx, reservation, presentation)
	if err != nil {
		return releaseReservation(fmt.Errorf("materialize %s: prepare add: %w", v.UUID, err))
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
	// A promotion race may leave an owned credential in the slot. Only a
	// provably empty slot is torn down; retained or unprovable state is released.
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

	retained, err := s.slotRetainsCredential(ctx, p)
	if err != nil && !errors.Is(err, creds.ErrUnavailable) {
		return release(fmt.Errorf("materialize %s: recheck credential slot: %w", v.UUID, err))
	}
	if retained {
		return release(fmt.Errorf("materialize %s: %w", v.UUID, pool.ErrCredentialChangedUnderfoot))
	}

	if err := rejectExistingExternalUUID(ctx, s.M, v.UUID); err != nil {
		return abandon(err)
	}
	prospective := store.Account{
		ID: p.Reservation.ID, InstanceID: p.Reservation.InstanceID,
		Generation: p.Reservation.Generation, ConfigDir: p.ConfigDir,
	}
	freshProof, err := s.Preparer.RefreshPreparedAccount(ctx, prospective, p.PresentationProof)
	if err != nil {
		return abandon(fmt.Errorf("materialize %s: revalidate presentation before promotion: %w", v.UUID, err))
	}
	p.PresentationProof = freshProof
	acct, err := s.promotePreparedSyncedAdd(ctx, p, v.Label, v.UUID)
	if err != nil {
		promotionErr := fmt.Errorf("materialize %s: publish awaiting-origin row: %w", v.UUID, err)
		resolved, committed, resolveErr := s.resolvePreparedSyncedAdd(p, v.Label, v.UUID)
		if resolveErr != nil {
			return MaterializeResult{}, errors.Join(promotionErr, fmt.Errorf("resolve promotion: %w", resolveErr))
		}
		if !committed {
			return abandonUnlessRetained(promotionErr)
		}
		acct = resolved
	}
	durable := MaterializeResult{UUID: v.UUID, AccountID: acct.ID, Bootstrapped: bootstrapped}
	installed, err := s.M.InstallSyncedCredential(ctx, *acct, credential)
	if err != nil {
		return durable, fmt.Errorf("materialize %s: install access-only credential: %w", v.UUID, err)
	}
	if !installed {
		return durable, fmt.Errorf("materialize %s: access-only credential did not land", v.UUID)
	}
	if _, err := s.AdmitSyncedAccount(ctx, *acct, creds.AccessHash(credential)); err != nil {
		return durable, fmt.Errorf("materialize %s: admit synced account: %w", v.UUID, err)
	}

	// The new stamp dir is not watched yet; the nudge re-reads the manifest.
	s.NudgeSynckitd(ctx, manifestPath)

	return durable, nil
}

func (s *Service) promotePreparedSyncedAdd(
	ctx context.Context,
	pending *pool.PendingAdd,
	label string,
	accountUUID string,
) (*store.Account, error) {
	if s.promoteSyncedAdd != nil {
		return s.promoteSyncedAdd(ctx, pending, label, accountUUID)
	}
	return s.M.PromoteSyncedAdd(ctx, pending, label, accountUUID)
}

func (s *Service) resolvePreparedSyncedAdd(
	pending *pool.PendingAdd,
	label string,
	accountUUID string,
) (*store.Account, bool, error) {
	if s.resolvePromotedSyncedAdd != nil {
		return s.resolvePromotedSyncedAdd(pending, label, accountUUID)
	}
	return s.M.ResolvePromotedSyncedAdd(pending, label, accountUUID)
}

// AdmitSyncedAccount verifies credential and presentation freshness before
// atomically refreshing proof and clearing awaiting-origin admission state.
func (s *Service) AdmitSyncedAccount(
	ctx context.Context,
	account store.Account,
	expectedAccessHash string,
) (bool, error) {
	health, err := s.M.Store.GetAuthHealth(account.ID)
	if err != nil {
		return false, err
	}
	if !health.NeedsLogin {
		return false, nil
	}
	if health.Kind != store.AuthKindAwaitingOrigin {
		return false, errors.New("hostsync: account is not awaiting its origin credential")
	}
	presentation, err := s.M.Store.AccountPresentation(account.ID)
	if err != nil {
		return false, fmt.Errorf("read presentation proof: %w", err)
	}
	if s.Preparer == nil {
		return false, errors.New("hostsync: account preparer is required")
	}
	freshProof, err := s.Preparer.RefreshPreparedAccount(ctx, account, presentation.Proof)
	if err != nil {
		return false, fmt.Errorf("revalidate presentation before admission: %w", err)
	}
	credential, _, err := s.M.ReadCredential(ctx, account)
	if err != nil {
		return false, fmt.Errorf("read installed credential: %w", err)
	}
	if credential.HasRefreshToken() || !credential.Synced() ||
		(expectedAccessHash != "" && creds.AccessHash(credential) != expectedAccessHash) {
		return false, errors.New("installed credential violates origin policy")
	}
	if _, _, _, err := s.M.SampleUsage(ctx, account, pool.SampleOpts{AllowRefresh: false}); err != nil {
		return false, fmt.Errorf("validate access-only credential: %w", err)
	}
	return s.M.AdmitSyncedCredential(
		ctx, account, presentation.Proof, freshProof, expectedAccessHash,
	)
}

func validateMaterializeIdentity(value AccountValue) error {
	var identity struct {
		AccountUUID string `json:"accountUuid"`
	}
	if err := json.Unmarshal(value.OAuthAccount, &identity); err != nil {
		return fmt.Errorf("hostsync: decode oauthAccount for %s: %w", value.UUID, err)
	}
	if identity.AccountUUID == "" || identity.AccountUUID != value.UUID {
		return fmt.Errorf(
			"hostsync: oauthAccount UUID mismatch: registry %q, identity %q",
			value.UUID, identity.AccountUUID,
		)
	}
	return nil
}

func rejectExistingExternalUUID(ctx context.Context, manager *pool.Manager, uuid string) error {
	rows, err := scanLocalAccounts(ctx, manager)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.uuid == uuid {
			return fmt.Errorf(
				"%w: %q already exists on acct-%02d",
				store.ErrDuplicateAccountUUID, uuid, row.acct.ID,
			)
		}
	}
	return nil
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
	st := s.M.Creds.Store(acct, creds.SourceKeychain)
	_, err = st.Read(ctx)
	switch creds.ClassifyRead(err) {
	case creds.ReadEmpty:
		return false, nil
	case creds.ReadUnsearchable, creds.ReadFatal:
		return false, fmt.Errorf("probe retained credential in %s: %w", st, err)
	case creds.ReadPresent:
		return true, nil
	}
	panic("unreachable")
}
