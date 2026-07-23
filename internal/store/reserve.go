package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// PendingAccountReservation is the exact prospective account identity reserved
// before any add-side external I/O.
type PendingAccountReservation struct {
	ID         int
	InstanceID string
	Generation uint64
	Owner      proc.Record
	CreatedAt  time.Time
}

// ErrSyncedPromotionAmbiguous means durable state cannot prove either an exact
// promotion or an untouched reservation safe to abandon.
var ErrSyncedPromotionAmbiguous = errors.New("synced account promotion state is ambiguous")

// ReserveAccountIndex atomically allocates the smallest unused account index
// (>= 1, gap-filling): the free-index computation and insert are one SQL
// statement, so two concurrent callers can never be handed the same index.
func (s *Store) ReserveAccountIndex(owner proc.Record) (PendingAccountReservation, error) {
	if err := owner.Validate(); err != nil {
		return PendingAccountReservation{}, err
	}
	instanceID, err := NewAccountInstanceID()
	if err != nil {
		return PendingAccountReservation{}, err
	}
	createdAt := s.now()
	ownerRecord, err := encodeCredentialOwner(owner)
	if err != nil {
		return PendingAccountReservation{}, err
	}
	var createdAtUnix int64
	var reservation PendingAccountReservation
	err = s.db.QueryRow(`
		INSERT INTO pending_adds(id,instance_id,generation,owner_record,created_at)
		SELECT MIN(candidate), ?, 1, ?, ?
		FROM (SELECT 1 AS candidate
		      UNION ALL
		      SELECT id+1 FROM (SELECT id FROM accounts UNION SELECT id FROM pending_adds))
		WHERE candidate NOT IN (SELECT id FROM accounts UNION SELECT id FROM pending_adds)
		RETURNING id,instance_id,generation,created_at`,
		instanceID, ownerRecord, createdAt.UnixNano()).Scan(
		&reservation.ID, &reservation.InstanceID, &reservation.Generation, &createdAtUnix,
	)
	if err != nil {
		return PendingAccountReservation{}, fmt.Errorf("reserve account index: %w", err)
	}
	reservation.Owner = owner
	reservation.CreatedAt = time.Unix(0, createdAtUnix)
	return reservation, nil
}

// ReleaseAccountIndex frees exactly the fenced reservation.
func (s *Store) ReleaseAccountIndex(reservation PendingAccountReservation) error {
	if err := validatePendingReservationFence(reservation); err != nil {
		return err
	}
	result, err := s.db.Exec(
		`DELETE FROM pending_adds WHERE id=? AND instance_id=? AND generation=? AND owner_record=?`,
		reservation.ID, reservation.InstanceID, reservation.Generation,
		mustEncodeCredentialOwner(reservation.Owner),
	)
	if err != nil {
		return fmt.Errorf("release account index %d: %w", reservation.ID, err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("release account index %d: reservation fence changed", reservation.ID)
	}
	return nil
}

// consumeReservation spends a reservation inside an exact promotion transaction.
func consumeReservation(e rowExecer, reservation PendingAccountReservation) error {
	if err := validatePendingReservationFence(reservation); err != nil {
		return err
	}
	res, err := e.Exec(
		`DELETE FROM pending_adds WHERE id=? AND instance_id=? AND generation=? AND owner_record=?`,
		reservation.ID, reservation.InstanceID, reservation.Generation,
		mustEncodeCredentialOwner(reservation.Owner),
	)
	if err != nil {
		return fmt.Errorf("consume account index %d: %w", reservation.ID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("consume account index %d: %w", reservation.ID, err)
	}
	if n != 1 {
		return fmt.Errorf("consume account index %d: reservation gone or fence changed", reservation.ID)
	}
	return nil
}

// PromoteReservedSyncedAccount atomically publishes a non-origin row, its
// complete FuseKit presentation proof, and awaiting-origin health state.
func (s *Store) PromoteReservedSyncedAccount(
	reservation PendingAccountReservation,
	a Account,
	proof PresentationPreparationProof,
) error {
	if a.AccountUUID == "" {
		return errors.New("promote synced account: external UUID is required")
	}
	return s.promoteReservedAccount(reservation, a, true, &proof)
}

func (s *Store) promoteReservedAccount(
	reservation PendingAccountReservation,
	a Account,
	awaitingOrigin bool,
	presentationProof *PresentationPreparationProof,
) error {
	if err := validatePendingReservationFence(reservation); err != nil {
		return err
	}
	if a.ID != reservation.ID || a.InstanceID != reservation.InstanceID ||
		a.Generation != reservation.Generation {
		return fmt.Errorf("promote account %d: reserved identity changed", a.ID)
	}
	if awaitingOrigin {
		if presentationProof == nil {
			return errors.New("promote synced account: presentation proof is required")
		}
		if err := validatePresentationPreparationProofForAccount(
			*presentationProof, a.InstanceID, a.Generation, a.ConfigDir,
		); err != nil {
			return fmt.Errorf("promote synced account: %w", err)
		}
	}
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("promote account %d: %w", a.ID, err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := consumeReservation(tx, reservation); err != nil {
		if awaitingOrigin && presentationProof != nil {
			replayed, replayErr := exactSyncedPromotion(tx, a, *presentationProof)
			if replayErr != nil {
				return errors.Join(err, replayErr)
			}
			if replayed {
				return tx.Commit()
			}
		}
		return err
	}
	if a.AccountUUID != "" {
		var duplicateID int
		err := tx.QueryRow(
			`SELECT id FROM accounts WHERE account_uuid=? AND deleted_at IS NULL LIMIT 1`,
			a.AccountUUID,
		).Scan(&duplicateID)
		if err == nil {
			return fmt.Errorf("%w: %q already belongs to account %d", ErrDuplicateAccountUUID, a.AccountUUID, duplicateID)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	created := a.CreatedAt
	if created.IsZero() {
		created = s.now()
	}
	result, err := tx.Exec(
		`INSERT INTO accounts(id,instance_id,generation,config_dir,keychain_service,keychain_account,label,account_uuid,created_at)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		a.ID, a.InstanceID, a.Generation, a.ConfigDir, a.KeychainService,
		a.KeychainAccount, a.Label, a.AccountUUID, created.Unix(),
	)
	if err != nil {
		return fmt.Errorf("promote account %d: %w", a.ID, err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		if err != nil {
			return fmt.Errorf("promote account %d: %w", a.ID, err)
		}
		return fmt.Errorf("promote account %d: inserted %d rows", a.ID, rows)
	}
	if awaitingOrigin {
		if err := upsertAccountPresentation(tx, AccountPresentation{
			AccountID: a.ID, AccountInstanceID: a.InstanceID,
			AccountGeneration: a.Generation, Proof: *presentationProof, ObservedAt: s.now(),
		}); err != nil {
			return fmt.Errorf("promote synced account %d presentation: %w", a.ID, err)
		}
		digest := DigestReason("host-sync: awaiting origin credential")
		if _, err := tx.Exec(
			`INSERT INTO auth_health(account_id,needs_login,since,reason,digest,kind,gen)
			 VALUES(?,1,?,'awaiting_origin',?,'awaiting_origin',1)`,
			a.ID, s.now().Unix(), digest[:],
		); err != nil {
			return fmt.Errorf("promote synced account %d auth health: %w", a.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("promote account %d: %w", a.ID, err)
	}
	return nil
}

func exactSyncedPromotion(
	tx *sql.Tx,
	expected Account,
	proof PresentationPreparationProof,
) (bool, error) {
	current, err := presentationAccount(tx, expected.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !samePresentationAccount(current, expected) || current.Label != expected.Label ||
		current.AccountUUID != expected.AccountUUID {
		return false, nil
	}
	presentation, err := accountPresentation(tx, expected.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if presentation.AccountInstanceID != expected.InstanceID ||
		presentation.AccountGeneration != expected.Generation || presentation.Proof != proof {
		return false, nil
	}
	var needsLogin, gen int64
	var reason, kind string
	var digest []byte
	if err := tx.QueryRow(
		`SELECT needs_login,reason,digest,kind,gen FROM auth_health WHERE account_id=?`,
		expected.ID,
	).Scan(&needsLogin, &reason, &digest, &kind, &gen); errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	wantDigest := DigestReason("host-sync: awaiting origin credential")
	var reservationRows int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM pending_adds WHERE id=?`, expected.ID).Scan(&reservationRows); err != nil {
		return false, err
	}
	return reservationRows == 0 && needsLogin == 1 && reason == string(AuthReasonAwaitingOrigin) &&
		kind == string(AuthKindAwaitingOrigin) && gen == 1 &&
		bytes.Equal(digest, wantDigest[:]), nil
}

// ResolveReservedSyncedPromotion classifies an interrupted promotion from one
// consistent snapshot. Only an exact untouched reservation is safe to abandon.
func (s *Store) ResolveReservedSyncedPromotion(
	reservation PendingAccountReservation,
	expected Account,
	proof PresentationPreparationProof,
) (Account, bool, error) {
	if err := validatePendingReservationFence(reservation); err != nil {
		return Account{}, false, err
	}
	if expected.ID != reservation.ID || expected.InstanceID != reservation.InstanceID ||
		expected.Generation != reservation.Generation {
		return Account{}, false, ErrSyncedPromotionAmbiguous
	}
	if err := validatePresentationPreparationProofForAccount(
		proof, expected.InstanceID, expected.Generation, expected.ConfigDir,
	); err != nil {
		return Account{}, false, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return Account{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	replayed, err := exactSyncedPromotion(tx, expected, proof)
	if err != nil {
		return Account{}, false, err
	}
	if replayed {
		account, err := presentationAccount(tx, expected.ID)
		return account, err == nil, err
	}
	var partial int
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM accounts WHERE id=?)
		 OR EXISTS(SELECT 1 FROM account_presentations WHERE account_id=?)
		 OR EXISTS(SELECT 1 FROM auth_health WHERE account_id=?)
		 OR EXISTS(SELECT 1 FROM synced_credential_admissions WHERE account_id=?)
		 OR EXISTS(SELECT 1 FROM account_mutations WHERE account_id=?)
		 OR EXISTS(SELECT 1 FROM credential_operations WHERE account_id=?)
		 OR EXISTS(SELECT 1 FROM credential_quarantines WHERE account_id=?)
		 OR EXISTS(SELECT 1 FROM account_removals WHERE account_id=?)`,
		expected.ID, expected.ID, expected.ID, expected.ID,
		expected.ID, expected.ID, expected.ID, expected.ID,
	).Scan(&partial); err != nil {
		return Account{}, false, err
	}
	if partial != 0 {
		return Account{}, false, ErrSyncedPromotionAmbiguous
	}
	current, err := pendingAccountReservationByID(tx, reservation.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return Account{}, false, ErrSyncedPromotionAmbiguous
	}
	if err != nil {
		return Account{}, false, err
	}
	if !samePendingAccountReservation(current, reservation) {
		return Account{}, false, ErrSyncedPromotionAmbiguous
	}
	return Account{}, false, nil
}

// PendingAddReservationsOwnedBy returns one bounded stable account-id page.
func (s *Store) PendingAddReservationsOwnedBy(
	owner proc.Record,
	afterAccountID, limit int,
) (reservations []PendingAccountReservation, more bool, err error) {
	if err := owner.Validate(); err != nil {
		return nil, false, err
	}
	if afterAccountID < 0 || limit <= 0 || limit > CredentialOperationPageLimit {
		return nil, false, errors.New("pending add page is invalid")
	}
	rows, err := s.db.Query(
		`SELECT id,instance_id,generation,owner_record,created_at FROM pending_adds
		 WHERE owner_record=? AND id>?
		 AND NOT EXISTS (SELECT 1 FROM account_mutations WHERE account_id=pending_adds.id AND account_instance_id=pending_adds.instance_id AND account_generation=pending_adds.generation)
		 AND NOT EXISTS (SELECT 1 FROM account_mutation_receipts WHERE account_id=pending_adds.id AND account_instance_id=pending_adds.instance_id AND account_generation=pending_adds.generation)
		 AND NOT EXISTS (SELECT 1 FROM account_removals WHERE account_id=pending_adds.id AND account_instance_id=pending_adds.instance_id AND account_generation=pending_adds.generation)
		 AND NOT EXISTS (SELECT 1 FROM credential_quarantines WHERE account_id=pending_adds.id AND account_instance_id=pending_adds.instance_id AND account_generation=pending_adds.generation)
		 ORDER BY id LIMIT ?`,
		mustEncodeCredentialOwner(owner), afterAccountID, limit+1,
	)
	if err != nil {
		return nil, false, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	reservations = make([]PendingAccountReservation, 0, limit)
	for rows.Next() {
		if len(reservations) == limit {
			more = true
			break
		}
		reservation, err := scanPendingAccountReservation(rows)
		if err != nil {
			return nil, false, err
		}
		reservations = append(reservations, reservation)
	}
	return reservations, more, rows.Err()
}

// ReleaseRetiredPendingAdd releases only an exact owner-fenced reservation
// with no durable mutation, receipt, removal, or quarantine evidence.
func (s *Store) ReleaseRetiredPendingAdd(
	ctx context.Context,
	reservation PendingAccountReservation,
	newOwner proc.Record,
	receipt proc.ReapReceipt,
	verifier ProcessRetirementVerifier,
) error {
	if err := verifyProcessRetirement(ctx, reservation.Owner, newOwner, receipt, verifier); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	stored, err := pendingAccountReservationByID(tx, reservation.ID)
	if err != nil {
		return err
	}
	if !samePendingAccountReservation(stored, reservation) {
		return ErrAccountGenerationChanged
	}
	var evidence int
	if err := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM account_mutations WHERE account_id=? AND account_instance_id=? AND account_generation=?)
		 OR EXISTS(SELECT 1 FROM account_mutation_receipts WHERE account_id=? AND account_instance_id=? AND account_generation=?)
		 OR EXISTS(SELECT 1 FROM account_removals WHERE account_id=? AND account_instance_id=? AND account_generation=?)
		 OR EXISTS(SELECT 1 FROM credential_quarantines WHERE account_id=? AND account_instance_id=? AND account_generation=?)`,
		reservation.ID, reservation.InstanceID, reservation.Generation,
		reservation.ID, reservation.InstanceID, reservation.Generation,
		reservation.ID, reservation.InstanceID, reservation.Generation,
		reservation.ID, reservation.InstanceID, reservation.Generation,
	).Scan(&evidence); err != nil {
		return err
	}
	if evidence != 0 {
		return ErrCredentialOperationEvidenceActive
	}
	if err := consumeReservation(tx, reservation); err != nil {
		return err
	}
	return tx.Commit()
}

// PendingAddIndexes lists every live account-index reservation, ascending. It
// is the daemon orphan reap's mid-add guard: an account whose exact add mutation
// has not committed remains reserved here, so it is never mistaken for an orphan.
func (s *Store) PendingAddIndexes() ([]int, error) {
	rows, err := s.db.Query(`SELECT id FROM pending_adds ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list pending adds: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("list pending adds: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list pending adds: %w", err)
	}
	return ids, nil
}

type pendingAccountReservationQueryer interface {
	QueryRow(query string, args ...any) *sql.Row
}

func pendingAccountReservationByID(
	queryer pendingAccountReservationQueryer,
	accountID int,
) (PendingAccountReservation, error) {
	return scanPendingAccountReservation(queryer.QueryRow(
		`SELECT id,instance_id,generation,owner_record,created_at FROM pending_adds WHERE id=?`,
		accountID,
	))
}

func scanPendingAccountReservation(row interface{ Scan(...any) error }) (PendingAccountReservation, error) {
	var reservation PendingAccountReservation
	var owner []byte
	var createdAt int64
	if err := row.Scan(
		&reservation.ID, &reservation.InstanceID, &reservation.Generation, &owner, &createdAt,
	); err != nil {
		return PendingAccountReservation{}, err
	}
	if err := decodeCredentialOwner(owner, &reservation.Owner); err != nil {
		return PendingAccountReservation{}, err
	}
	reservation.CreatedAt = time.Unix(0, createdAt)
	if reservation.ID <= 0 || validateAccountInstanceID(reservation.InstanceID) != nil ||
		reservation.Generation != 1 || reservation.CreatedAt.IsZero() {
		return PendingAccountReservation{}, errors.New("pending add reservation is corrupt")
	}
	return reservation, nil
}

func samePendingAccountReservation(left, right PendingAccountReservation) bool {
	return left.ID == right.ID && left.InstanceID == right.InstanceID &&
		left.Generation == right.Generation && sameCredentialOwner(left.Owner, right.Owner) &&
		left.CreatedAt.Equal(right.CreatedAt)
}

func validatePendingReservationFence(reservation PendingAccountReservation) error {
	if reservation.ID <= 0 || validateAccountInstanceID(reservation.InstanceID) != nil ||
		reservation.Generation != 1 {
		return errors.New("pending add reservation fence is invalid")
	}
	if err := reservation.Owner.Validate(); err != nil {
		return errors.New("pending add reservation owner is invalid")
	}
	return nil
}
