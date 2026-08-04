package pool

import (
	"context"
	"log"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
)

const (
	credentialRecoveryLanePage = 64
	pendingAddRecoveryTimeout  = 30 * time.Second
)

// ClaimForeignLanes claims every credential lane and pending-add reservation a
// prior daemon generation left behind — any era's owner bytes included. The
// rows themselves are the durable work list: each claim is one per-row epoch
// CAS echoing the row's stored owner bytes, so a crash mid-claim re-presents
// the remainder at the next boot. Serve's flock guarantees every foreign owner
// is dead or fully drained before this runs; it belongs in Start, before the
// scheduler and before business admission opens. Foreign account mutations are
// TakeoverRetiredAccountMutationPage's leg, since their state-machine re-entry
// is daemon-owned.
func (m *Manager) ClaimForeignLanes(ctx context.Context) error {
	owner, err := m.MutationOwner()
	if err != nil {
		return err
	}
	if err := m.claimForeignCredentialOperations(ctx, owner); err != nil {
		return err
	}
	return m.claimForeignPendingAdds(ctx, owner)
}

func (m *Manager) claimForeignCredentialOperations(
	ctx context.Context,
	owner store.OwnerRecord,
) error {
	afterAccountID := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		operations, more, err := m.Store.CredentialOperationsNotOwnedBy(
			owner, afterAccountID, credentialRecoveryLanePage,
		)
		if err != nil {
			return err
		}
		for _, operation := range operations {
			if err := m.recoverCredentialOperation(ctx, operation); err != nil {
				log.Printf(
					"credential recovery deferred: account=%d token=%s: %v",
					operation.AccountID, operation.Token, err,
				)
			}
			afterAccountID = operation.AccountID
		}
		if !more {
			return nil
		}
	}
}

func (m *Manager) claimForeignPendingAdds(
	ctx context.Context,
	owner store.OwnerRecord,
) error {
	afterAccountID := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		pending, more, err := m.Store.PendingAddReservationsNotOwnedBy(
			owner, afterAccountID, credentialRecoveryLanePage,
		)
		if err != nil {
			return err
		}
		for _, reservation := range pending {
			afterAccountID = reservation.ID
			if m.RetirePendingAdd == nil {
				log.Printf(
					"pending add recovery deferred: account=%d: retirement proof is unavailable",
					reservation.ID,
				)
				continue
			}
			cleanupCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx), pendingAddRecoveryTimeout,
			)
			proof, err := m.RetirePendingAdd(cleanupCtx, reservation)
			if err == nil {
				err = m.AbandonReservedAdd(cleanupCtx, reservation, proof)
			}
			cancel()
			if err != nil {
				log.Printf("pending add recovery deferred: account=%d: %v", reservation.ID, err)
			}
		}
		if !more {
			return nil
		}
	}
}

// TakeoverRetiredAccountMutationPage claims one bounded page of foreign
// account mutations for daemon-side recovery: each row's stored owner bytes
// are echoed into the epoch CAS, so a delivered mutation is never re-delivered
// and a crash mid-page re-presents the remainder as still foreign.
func (m *Manager) TakeoverRetiredAccountMutationPage(
	ctx context.Context,
) ([]store.AccountMutation, bool, error) {
	owner, err := m.MutationOwner()
	if err != nil {
		return nil, false, err
	}
	mutations, more, err := m.Store.AccountMutationsNotOwnedBy(
		owner, 0, credentialRecoveryLanePage,
	)
	if err != nil {
		return nil, more, err
	}
	taken := make([]store.AccountMutation, 0, len(mutations))
	for _, mutation := range mutations {
		recovered, err := m.Store.TakeoverAccountMutation(ctx, mutation.Fence(), owner)
		if err != nil {
			return taken, true, err
		}
		taken = append(taken, recovered)
	}
	return taken, more, nil
}
