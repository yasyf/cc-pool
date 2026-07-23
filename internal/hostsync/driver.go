package hostsync

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/converge"
	"github.com/yasyf/synckit/cregistry"
)

// The converge.Outcome values a cc-pool account reconcile reports. A single
// item runs at most one install; when it also relabels, the install outcome wins.
const (
	// OutcomeNoop means the item needed no action (removals belong to the teardown pass).
	OutcomeNoop converge.Outcome = "noop"
	// OutcomeUnchanged means the local account already matched the registry entry.
	OutcomeUnchanged converge.Outcome = "unchanged"
	// OutcomeMaterialized means a peer-added account was created locally this pass.
	OutcomeMaterialized converge.Outcome = "materialized"
	// OutcomeDeferred means the item could not complete this pass; a later tick retries.
	OutcomeDeferred converge.Outcome = "deferred"
	// OutcomeLabeled means the local label was updated to the registry's LWW label.
	OutcomeLabeled converge.Outcome = "labeled"
	// OutcomeCredInstalled means a fresher credential was pulled from a peer and installed.
	OutcomeCredInstalled converge.Outcome = "cred-installed"
)

// DriverStore is the slice of *store.Store the driver reads and writes; tests fake it.
type DriverStore interface {
	// GetAccountByUUID resolves the local row whose Claude accountUuid is uuid.
	GetAccountByUUID(uuid string) (store.Account, bool, error)
	// SetAccountUUID tags a local row with its Claude accountUuid.
	SetAccountUUID(id int, uuid string) error
	// SetAccountLabel updates a local row's label.
	SetAccountLabel(id int, label string) error
}

// CredentialManager is the slice of *pool.Manager the fresher-credential path
// drives; tests fake it.
type CredentialManager interface {
	// ReadCredential returns the account's current credential and its store, or
	// creds.ErrNotFound when the account holds none.
	ReadCredential(context.Context, store.Account) (*creds.Credential, creds.Source, error)
	// InstallSyncedCredential installs a pulled credential under the durable account lane
	// when it wins the owned-precedence/freshness re-check; reports whether it landed.
	InstallSyncedCredential(ctx context.Context, a store.Account, cred *creds.Credential) (bool, error)
}

// LocalIndex maps each local account's Claude accountUuid to its store row id,
// for LoadRegistry's account_uuid backfill; tests inject a fake.
type LocalIndex func(ctx context.Context) (map[string]int, error)

// AccountMaterializer creates the local pool account for a peer-added entry
// missing locally; tests inject a fake.
type AccountMaterializer func(ctx context.Context, v AccountValue, peers []string) (MaterializeResult, error)

// SyncedAdmitter revalidates one persisted presentation before admission.
type SyncedAdmitter func(context.Context, store.Account, string) (bool, error)

// FresherPuller pulls a fresher credential for uuid from its chain origin,
// falling back to the other peers; ErrNoPeerCredential is the deferred outcome.
type FresherPuller func(ctx context.Context, uuid string, chain ChainStamp, localExpiresAt int64, peers []string) (*creds.Credential, error)

// DriverDeps are the injected seams the Driver reconciles through.
type DriverDeps struct {
	// Store resolves, tags, and relabels local account rows.
	Store DriverStore
	// Cred reads the local credential and installs a fresher pulled one.
	Cred CredentialManager
	// LocalIndex enumerates local (uuid -> row id) for the LoadRegistry backfill.
	LocalIndex LocalIndex
	// Materialize creates a local account for a peer-added entry missing locally.
	Materialize AccountMaterializer
	// Admit revalidates a synced account's persisted presentation before selection.
	Admit SyncedAdmitter
	// Pull fetches a strictly-fresher credential for an existing local account.
	Pull FresherPuller
}

// Driver is cc-pool's converge.Driver[AccountValue]. converge.Reconcile calls
// LoadRegistry once, SaveRegistry once, then Reconcile per present item — all
// under the registry flock, so SaveRegistry writes lock-free.
type Driver struct {
	svc  *Service
	deps DriverDeps

	mu   sync.Mutex
	base map[string]string // uuid -> on-disk fingerprint captured by LoadRegistry
}

// NewDriver builds the account Driver over svc and the injected seams.
func NewDriver(svc *Service, deps DriverDeps) *Driver {
	return &Driver{svc: svc, deps: deps, base: map[string]string{}}
}

// LoadRegistry loads the registry, snapshots per-entry fingerprints, backfills
// local uuids, and folds local accounts in via ScanPublish (in memory only).
func (d *Driver) LoadRegistry(ctx context.Context) (cregistry.Registry[AccountValue], error) {
	reg, err := d.svc.Registry.Load()
	if err != nil {
		return nil, err
	}
	d.snapshot(reg)
	if err := d.backfillUUIDs(ctx); err != nil {
		return nil, err
	}
	if _, err := d.svc.ScanPublish(ctx, reg); err != nil {
		return nil, err
	}
	return reg, nil
}

// SaveRegistry persists the merged registry and touches the stamp of every
// entry whose fingerprint changed since LoadRegistry, so peers hear onward; an
// unchanged pass rewrites and touches nothing.
func (d *Driver) SaveRegistry(_ context.Context, reg cregistry.Registry[AccountValue]) error {
	if err := d.svc.Registry.Save(reg); err != nil {
		return err
	}
	base := d.baseline()
	for id, entry := range reg {
		if Fingerprint(entry) == base[id] {
			continue
		}
		if err := d.svc.TouchStamp(id); err != nil {
			return fmt.Errorf("touch stamp %s: %w", id, err)
		}
	}
	return nil
}

// Reconcile resolves one present entry to its local account: materialize when
// missing, else apply the LWW label and install a fresher credential; a
// tombstone here is a defensive noop.
func (d *Driver) Reconcile(ctx context.Context, id string, entry cregistry.Entry[AccountValue], peers []string, _ string) (converge.Outcome, error) {
	if !entry.Present() {
		return OutcomeNoop, nil
	}
	v := entry.Value
	if v.UUID != id {
		// The registry is keyed by account UUID; an entry whose value UUID
		// disagrees with its key is a cross-account injection (pull/install the
		// wrong account's credential into the key's local row). Never act on it.
		d.svc.logf("hostsync: reconcile SKIPPED key %s: value UUID %q disagrees with the key — refusing a cross-account install", id, v.UUID)
		return OutcomeNoop, nil
	}
	a, ok, err := d.deps.Store.GetAccountByUUID(id)
	if err != nil {
		return "", fmt.Errorf("resolve account %s: %w", id, err)
	}
	if !ok {
		return d.materialize(ctx, v, peers)
	}
	return d.reconcileLocal(ctx, a, v, peers)
}

// backfillUUIDs tags local rows the store has not yet stamped with their uuid,
// so Reconcile unifies with the existing account instead of materializing a duplicate.
func (d *Driver) backfillUUIDs(ctx context.Context) error {
	idx, err := d.deps.LocalIndex(ctx)
	if err != nil {
		return fmt.Errorf("index local accounts: %w", err)
	}
	for uuid, id := range idx {
		_, ok, err := d.deps.Store.GetAccountByUUID(uuid)
		if err != nil {
			return fmt.Errorf("resolve local account %s: %w", uuid, err)
		}
		if ok {
			continue
		}
		if err := d.deps.Store.SetAccountUUID(id, uuid); err != nil {
			return fmt.Errorf("backfill uuid on acct-%d: %w", id, err)
		}
		d.svc.logf("hostsync: backfilled acct-%d uuid=%s", id, uuid)
	}
	return nil
}

// materialize creates a missing local account for v, deferring when the
// identity is not yet scan-published or no peer holds the envelope.
func (d *Driver) materialize(ctx context.Context, v AccountValue, peers []string) (converge.Outcome, error) {
	if emptyOAuth(v.OAuthAccount) {
		return OutcomeDeferred, nil
	}
	res, err := d.deps.Materialize(ctx, v, peers)
	if err != nil {
		if errors.Is(err, ErrMaterializeNoEnvelope) {
			return OutcomeDeferred, nil
		}
		return "", fmt.Errorf("materialize %s: %w", v.UUID, err)
	}
	if res.Deferred {
		return OutcomeDeferred, nil
	}
	d.svc.logf("hostsync: materialized %s as acct-%d (bootstrapped=%v)",
		v.UUID, res.AccountID, res.Bootstrapped)
	return OutcomeMaterialized, nil
}

// reconcileLocal applies the LWW label, then pulls a credential only when the
// local one is unowned and the registry chain is strictly fresher; the
// definitive re-check runs in InstallSyncedCredential.
func (d *Driver) reconcileLocal(ctx context.Context, a store.Account, v AccountValue, peers []string) (converge.Outcome, error) {
	outcome := OutcomeUnchanged
	if v.Label != a.Label {
		if err := d.deps.Store.SetAccountLabel(a.ID, v.Label); err != nil {
			return "", fmt.Errorf("apply label to acct-%d: %w", a.ID, err)
		}
		d.svc.logf("hostsync: acct-%d label %q -> %q", a.ID, a.Label, v.Label)
		outcome = OutcomeLabeled
	}

	localExp, localHash, owned, err := d.localChain(ctx, a)
	if errors.Is(err, creds.ErrUnavailable) {
		return OutcomeDeferred, nil
	}
	if err != nil {
		return "", err
	}
	if owned {
		// Owned blobs are never replaced by sync; only their origin refreshes them.
		return outcome, nil
	}
	if v.Chain.Hash == localHash {
		if d.deps.Admit != nil {
			if _, err := d.deps.Admit(ctx, a, v.Chain.Hash); err != nil {
				return "", fmt.Errorf("admit synced acct-%d: %w", a.ID, err)
			}
		}
		return outcome, nil
	}
	// Strictly-later expiry, same ordering as InstallSyncedCredential's guard.
	// Forward origin clock skew (a rollback child stamped earlier) is benign:
	// the peer keeps the still-valid parent AT until expiry, then re-pulls.
	if v.Chain.ExpiresAt <= localExp {
		return outcome, nil
	}
	// Never install under a busy account — a concurrent `ccp login`'s claude
	// subprocess owns the slot (the writeCredCAS TOCTOU window, ccn 4ed1146).
	// Same fail-closed discipline as teardownBusy: a nil seam reads busy.
	busy, reason, err := d.installBusy(ctx, v.UUID)
	if err != nil {
		return "", fmt.Errorf("busy check for %s: %w", v.UUID, err)
	}
	if busy {
		d.svc.logf("hostsync: acct-%d credential install deferred: %s", a.ID, reason)
		return OutcomeDeferred, nil
	}

	installed, deferred, err := d.pullAndInstall(ctx, a, v, localExp, peers)
	switch {
	case err != nil:
		return "", err
	case installed:
		if d.deps.Admit != nil {
			if _, err := d.deps.Admit(ctx, a, v.Chain.Hash); err != nil {
				return "", fmt.Errorf("admit synced acct-%d: %w", a.ID, err)
			}
		}
		return OutcomeCredInstalled, nil
	case deferred:
		return OutcomeDeferred, nil
	default:
		return outcome, nil
	}
}

// installBusy reports whether uuid is locally held (live session, select
// reservation, or in-flight convert); a nil Sessions seam reads busy.
func (d *Driver) installBusy(ctx context.Context, uuid string) (bool, string, error) {
	if d.svc.Sessions == nil {
		return true, "no sessions seam wired", nil
	}
	return d.svc.Sessions.Busy(ctx, uuid)
}

// pullAndInstall pulls the fresher chain and installs it; ErrNoPeerCredential
// and creds.ErrUnavailable are deferred, not failures — the next tick retries.
func (d *Driver) pullAndInstall(ctx context.Context, a store.Account, v AccountValue, localExp int64, peers []string) (installed, deferred bool, err error) {
	cred, err := d.deps.Pull(ctx, v.UUID, v.Chain, localExp, peers)
	switch {
	case errors.Is(err, ErrNoPeerCredential):
		return false, true, nil
	case err != nil:
		return false, false, fmt.Errorf("pull credential for %s: %w", v.UUID, err)
	case cred == nil:
		return false, true, nil
	}
	ok, err := d.deps.Cred.InstallSyncedCredential(ctx, a, cred)
	if err != nil {
		if errors.Is(err, creds.ErrUnavailable) {
			return false, true, nil
		}
		return false, false, fmt.Errorf("install credential for acct-%d: %w", a.ID, err)
	}
	return ok, false, nil
}

// localChain returns a's credential expiry (Unix ms), AccessHash, and whether
// it is owned; absent and tombstoned both read (0, "", false) — pullable.
func (d *Driver) localChain(
	ctx context.Context,
	a store.Account,
) (int64, string, bool, error) {
	cred, _, err := d.deps.Cred.ReadCredential(ctx, a)
	switch {
	case errors.Is(err, creds.ErrNotFound), errors.Is(err, creds.ErrNoTokens):
		return 0, "", false, nil
	case err != nil:
		return 0, "", false, fmt.Errorf("read acct-%d credential: %w", a.ID, err)
	}
	return cred.ClaudeAiOauth.ExpiresAt, creds.AccessHash(cred), cred.HasRefreshToken(), nil
}

// snapshot records each on-disk entry's fingerprint, before the scan fold so a
// local fold counts as a change worth notifying.
func (d *Driver) snapshot(reg cregistry.Registry[AccountValue]) {
	base := make(map[string]string, len(reg))
	for id, entry := range reg {
		base[id] = Fingerprint(entry)
	}
	d.mu.Lock()
	d.base = base
	d.mu.Unlock()
}

// baseline returns the fingerprints LoadRegistry recorded for this pass.
func (d *Driver) baseline() map[string]string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.base
}

// Driver satisfies converge.Driver for the account registry.
var _ converge.Driver[AccountValue] = (*Driver)(nil)
