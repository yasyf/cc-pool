package pool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
)

// ErrCredentialRecoveryPending refuses an account whose claimed lane failed
// its post-takeover settlement; each later access retries the settlement and
// lifts the fence on success.
var ErrCredentialRecoveryPending = errors.New("credential operation recovery is pending")

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
				var stranded strandedRecoveryError
				if errors.As(err, &stranded) {
					m.strandCredentialRecovery(operation.AccountID, stranded.token, stranded.cause)
				}
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

type strandedCredentialRecovery struct {
	token string
	cause error
	retry chan struct{}
}

type strandedRecoveryError struct {
	token string
	cause error
}

func (e strandedRecoveryError) Error() string { return e.cause.Error() }

func (e strandedRecoveryError) Unwrap() error { return e.cause }

func (m *Manager) strandCredentialRecovery(accountID int, token string, cause error) {
	m.credentialMu.Lock()
	defer m.credentialMu.Unlock()
	if m.strandedRecovery == nil {
		m.strandedRecovery = make(map[int]strandedCredentialRecovery)
	}
	m.strandedRecovery[accountID] = strandedCredentialRecovery{token: token, cause: cause}
}

// StrandedCredentialRecoveryStatus names one fenced account: a claimed lane
// whose post-takeover settlement failed and awaits a successful retry.
type StrandedCredentialRecoveryStatus struct {
	AccountID int
	Token     string
	Cause     string
}

// StrandedCredentialRecoveries reports every account currently fenced from
// credential admission, oldest account first, for the status and doctor
// surfaces — a silently fenced account is otherwise visible only to a caller
// who trips over ErrCredentialRecoveryPending.
func (m *Manager) StrandedCredentialRecoveries() []StrandedCredentialRecoveryStatus {
	m.credentialMu.Lock()
	defer m.credentialMu.Unlock()
	fenced := make([]StrandedCredentialRecoveryStatus, 0, len(m.strandedRecovery))
	for accountID, strand := range m.strandedRecovery {
		fenced = append(fenced, StrandedCredentialRecoveryStatus{
			AccountID: accountID, Token: strand.token, Cause: strand.cause.Error(),
		})
	}
	sort.Slice(fenced, func(i, j int) bool { return fenced[i].AccountID < fenced[j].AccountID })
	return fenced
}

func (m *Manager) strandedTokenAccount(token string) (int, bool) {
	m.credentialMu.Lock()
	defer m.credentialMu.Unlock()
	for accountID, strand := range m.strandedRecovery {
		if strand.token == token {
			return accountID, true
		}
	}
	return 0, false
}

// retryStrandedCredentialRecovery re-attempts a fenced account's failed
// settlement; cleared reports that a strand existed and is now resolved, so
// callers holding pre-retry evidence re-derive it. The settlement is
// single-flight per account: the fence's many doors mean callers arrive here
// concurrently from paths sharing no lock, and two settlements racing the same
// token can write a spurious durable quarantine. Joiners park on the winner's
// retry channel and report its outcome.
func (m *Manager) retryStrandedCredentialRecovery(ctx context.Context, accountID int) (cleared bool, err error) {
	joined := false
	for {
		m.credentialMu.Lock()
		strand, stranded := m.strandedRecovery[accountID]
		if !stranded {
			m.credentialMu.Unlock()
			return joined, nil
		}
		if strand.retry == nil {
			if joined {
				m.credentialMu.Unlock()
				return false, fmt.Errorf(
					"%w: account %d: %w", ErrCredentialRecoveryPending, accountID, strand.cause,
				)
			}
			retry := make(chan struct{})
			strand.retry = retry
			m.strandedRecovery[accountID] = strand
			m.credentialMu.Unlock()
			cleared, err := m.settleStrandedCredentialRecovery(ctx, accountID, strand.token)
			m.credentialMu.Lock()
			if current, ok := m.strandedRecovery[accountID]; ok && current.token == strand.token {
				current.retry = nil
				m.strandedRecovery[accountID] = current
			}
			m.credentialMu.Unlock()
			close(retry)
			return cleared, err
		}
		retry := strand.retry
		m.credentialMu.Unlock()
		joined = true
		select {
		case <-retry:
		case <-ctx.Done():
			return false, fmt.Errorf(
				"%w: account %d: %w", ErrCredentialRecoveryPending, accountID, ctx.Err(),
			)
		}
	}
}

func (m *Manager) settleStrandedCredentialRecovery(
	ctx context.Context,
	accountID int,
	token string,
) (cleared bool, err error) {
	pending := func(err error) error {
		return fmt.Errorf("%w: account %d: %w", ErrCredentialRecoveryPending, accountID, err)
	}
	operation, err := m.Store.CredentialOperationByToken(token)
	if errors.Is(err, sql.ErrNoRows) {
		m.unstrandCredentialRecoveryToken(accountID, token)
		return true, nil
	}
	if err != nil {
		return false, pending(err)
	}
	account, err := m.credentialOperationAccount(operation)
	if err != nil {
		return false, pending(err)
	}
	if !credentialOperationAccountCurrent(account, operation) {
		return false, pending(store.ErrAccountGenerationChanged)
	}
	if err := m.settleClaimedCredentialOperation(ctx, account, operation); err != nil {
		var stranded strandedRecoveryError
		if errors.As(err, &stranded) {
			m.restrandCredentialRecoveryToken(accountID, stranded.token, stranded.cause)
		}
		return false, pending(err)
	}
	m.unstrandCredentialRecoveryToken(accountID, token)
	return true, nil
}

func (m *Manager) unstrandCredentialRecoveryToken(accountID int, token string) {
	m.credentialMu.Lock()
	defer m.credentialMu.Unlock()
	if current, ok := m.strandedRecovery[accountID]; ok && current.token == token {
		delete(m.strandedRecovery, accountID)
	}
}

func (m *Manager) restrandCredentialRecoveryToken(accountID int, token string, cause error) {
	m.credentialMu.Lock()
	defer m.credentialMu.Unlock()
	current, ok := m.strandedRecovery[accountID]
	if !ok || current.token != token {
		return
	}
	current.cause = cause
	m.strandedRecovery[accountID] = current
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
