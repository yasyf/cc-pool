package hostsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

const materializeRetirementTimeout = 30 * time.Second

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
// interactive login. An entry with no oauthAccount defers. Post-reservation
// failures retire the exact tenant before cleanup; ambiguous state retains the
// reservation and its execution identity.
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
	retireReservation := func(cause error) (pool.PendingAddRetirementProof, error) {
		retireCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), materializeRetirementTimeout)
		defer cancel()
		proof, retireErr := s.Preparer.AbortReservedAccount(retireCtx, reservation)
		if retireErr != nil {
			return pool.PendingAddRetirementProof{}, errors.Join(
				cause, fmt.Errorf("retire reserved presentation for %s: %w", v.UUID, retireErr),
			)
		}
		return proof, nil
	}
	finalizeUnprepared := func(cause error) (MaterializeResult, error) {
		proof, retireErr := retireReservation(cause)
		if retireErr != nil {
			return MaterializeResult{}, retireErr
		}
		if releaseErr := s.M.FinalizeUnpreparedAdd(reservation, proof); releaseErr != nil {
			return MaterializeResult{}, errors.Join(cause, fmt.Errorf("release unprepared %s: %w", v.UUID, releaseErr))
		}
		return MaterializeResult{}, cause
	}
	abandonPrepared := func(cause error) (MaterializeResult, error) {
		proof, retireErr := retireReservation(cause)
		if retireErr != nil {
			return MaterializeResult{}, retireErr
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), materializeRetirementTimeout)
		defer cancel()
		if abandonErr := s.M.AbandonPreparedAdd(cleanupCtx, reservation, proof); abandonErr != nil {
			return MaterializeResult{}, errors.Join(cause, fmt.Errorf("roll back %s: %w", v.UUID, abandonErr))
		}
		return MaterializeResult{}, cause
	}
	presentation, err := s.Preparer.PrepareReservedAccount(ctx, reservation, v.Label)
	if err != nil {
		return finalizeUnprepared(fmt.Errorf("materialize %s: prepare presentation: %w", v.UUID, err))
	}
	p, err := s.M.PrepareReservedSyncedAdd(ctx, reservation, presentation)
	if err != nil {
		return abandonPrepared(fmt.Errorf("materialize %s: prepare add: %w", v.UUID, err))
	}

	// Every failure past preparation retires the tenant before cleanup.
	abandon := func(cause error) (MaterializeResult, error) {
		proof, retireErr := retireReservation(cause)
		if retireErr != nil {
			return MaterializeResult{}, retireErr
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), materializeRetirementTimeout)
		defer cancel()
		if aerr := s.M.AbandonAdd(cleanupCtx, p, proof); aerr != nil {
			return MaterializeResult{}, errors.Join(cause, fmt.Errorf("roll back %s: %w", v.UUID, aerr))
		}
		return MaterializeResult{}, cause
	}
	retain := func(cause error) (MaterializeResult, error) {
		if _, retireErr := retireReservation(cause); retireErr != nil {
			return MaterializeResult{}, retireErr
		}
		return MaterializeResult{}, cause
	}
	// A promotion race may leave an owned credential in the slot. Only a
	// provably empty slot is torn down; retained or unprovable state is kept.
	abandonUnlessRetained := func(cause error) (MaterializeResult, error) {
		retained, err := s.slotRetainsCredential(ctx, p)
		switch {
		case err != nil:
			return retain(errors.Join(cause, err))
		case retained:
			return retain(cause)
		}
		return abandon(cause)
	}

	// An interrupted add may retain a usable credential. Materializing over it
	// would destroy an owned chain, so abort before touching the dir.
	if p.ClaudeJSONSeed == pool.SeedKeptExisting {
		retained, err := s.slotRetainsCredential(ctx, p)
		if err != nil {
			return retain(fmt.Errorf("materialize %s: %w", v.UUID, err))
		}
		if retained {
			return retain(fmt.Errorf("materialize %s: %s retains a credential from an interrupted add; resume it with `ccp add` or remove the dir", v.UUID, p.ConfigDir))
		}
	}

	// The backing worker seeded the minimal onboarding document when no source existed.
	bootstrapped := p.ClaudeJSONSeed == pool.SeedNoSource

	if err := s.M.WriteIdentity(ctx, p.Reservation.ID, p.ConfigDir, v.OAuthAccount); err != nil {
		return abandon(fmt.Errorf("materialize %s: write identity: %w", v.UUID, err))
	}

	retained, err := s.slotRetainsCredential(ctx, p)
	if err != nil && !errors.Is(err, creds.ErrUnavailable) {
		return retain(fmt.Errorf("materialize %s: recheck credential slot: %w", v.UUID, err))
	}
	if retained {
		return retain(fmt.Errorf("materialize %s: %w", v.UUID, pool.ErrCredentialChangedUnderfoot))
	}

	if err := rejectExistingExternalUUID(ctx, s.M, v.UUID); err != nil {
		return abandon(err)
	}
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

// AdmitSyncedAccount verifies the credential before clearing awaiting-origin admission state.
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
		return false, fmt.Errorf("read presentation identity: %w", err)
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
		ctx, account, presentation.Identity, presentation.Identity, expectedAccessHash,
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

// slotAccount is the pending slot's exact credential identity.
func (s *Service) slotAccount(
	ctx context.Context,
	p *pool.PendingAdd,
) (store.Account, error) {
	acct := store.Account{ID: p.Reservation.ID, InstanceID: p.Reservation.InstanceID, Generation: p.Reservation.Generation, ConfigDir: p.ConfigDir, KeychainService: p.KeychainService, KeychainAccount: creds.AccountLabel()}
	account, err := s.M.DiscoverCredentialAccount(ctx, acct, p.PublicPath)
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
	state, err := s.M.CredentialExternalStateAt(ctx, acct, p.PublicPath)
	if err != nil {
		return false, fmt.Errorf("probe retained credential in %s: %w", p.ConfigDir, err)
	}
	switch state.Keychain.State {
	case store.CredentialSlotEmpty:
		return false, nil
	case store.CredentialSlotUnsearchable:
		return false, fmt.Errorf(
			"probe retained credential in %s: %w",
			p.ConfigDir, errors.Join(pool.ErrCredentialUnverifiable, creds.ErrUnavailable),
		)
	case store.CredentialSlotUnreadable:
		return false, fmt.Errorf(
			"probe retained credential in %s: %w",
			p.ConfigDir, pool.ErrCredentialUnverifiable,
		)
	case store.CredentialSlotPresent:
		return true, nil
	}
	panic("unreachable")
}
