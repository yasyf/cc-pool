package hostsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/synckit/converge"
	"github.com/yasyf/synckit/cregistry"
	"github.com/yasyf/synckit/syncservice"
)

// stampDirPerm and stampFilePerm keep the stamp tree private — it names account UUIDs.
const (
	stampDirPerm  = 0o700
	stampFilePerm = 0o600
)

// LocalAccount is the narrow view of a pool account the publish and scan hooks
// consume, injected through Service.Locals.
type LocalAccount struct {
	// UUID is the account's stable identity and the registry key.
	UUID string
	// Email is the account's login email.
	Email string
	// Label is the account's current user-assigned label.
	Label string
	// OAuthAccount is Claude's opaque oauthAccount object; empty until a local
	// scan reads it.
	OAuthAccount json.RawMessage
	// Chain is the account's current secretless chain stamp.
	Chain ChainStamp
}

// Mesh resolves this host's identity and the peer hosts a converge pass pulls from.
type Mesh interface {
	// Resolve returns this host's name and the peer hosts to pull from.
	Resolve(ctx context.Context) (self string, peers []string, err error)
}

// Sessions reports whether a live local session or in-flight convert holds an
// account, so a converge pass defers acting on it.
type Sessions interface {
	// Busy reports whether uuid is held locally and a human-readable reason.
	Busy(ctx context.Context, uuid string) (busy bool, reason string, err error)
}

// AccountRemoval completes external teardown after its durable removal intent
// has been installed.
type AccountRemoval interface {
	Finish(context.Context) error
}

// AccountRemover installs one account's durable removal intent. The
// returned removal owns FuseKit tenant/domain teardown and private-state deletion.
type AccountRemover interface {
	BeginAccountRemoval(id int, deleteCredential bool) (AccountRemoval, error)
}

// Service owns the convergent account registry and its write hooks: every
// mutation is load-modify-save under the flock, then a stamp touch that notifies peers.
type Service struct {
	// M is the pool manager the materializer and cred-pull drive.
	M *pool.Manager
	// Registry is the on-disk convergent registry plus its flock.
	Registry *RegistryFile
	// StampDir is the per-account fsnotify stamp tree (StampDir/<uuid>/stamp).
	StampDir string
	// Log receives advisory diagnostics; nil discards them.
	Log *log.Logger
	// Now supplies the stamp clock; nil means time.Now.
	Now func() time.Time
	// Locals enumerates this host's local accounts for the scan-publish fold.
	Locals func(ctx context.Context) ([]LocalAccount, error)
	// Run executes external commands in disposable process groups; it is required.
	Run func(ctx context.Context, name string, args ...string) error

	// Mesh resolves this host and its peers for a converge pass.
	Mesh Mesh
	// Sessions reports local liveness so busy items defer.
	Sessions Sessions
	// Remover owns the durable FuseKit-first account removal lifecycle.
	Remover AccountRemover
	// Status is the process-lifetime peer up/down tracker; one per Service.
	Status *converge.PeerStatus
	// Driver is the converge.Driver the reconcile pass drives; the daemon wires
	// NewDriver, tests inject a fake.
	Driver converge.Driver[AccountValue]
	// Fetcher reads each peer's registry for the pull-merge.
	Fetcher converge.Fetcher[AccountValue]
}

// now returns the injected clock, or the wall clock when none is injected.
func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// stamp is the current registry add/remove timestamp.
func (s *Service) stamp() cregistry.Micros {
	return cregistry.UnixMicros(s.now())
}

// forceStamp stamps an explicit local mutation strictly past the entry's Added
// and Removed so it always lands, even under peer clock skew — see ccn 10bf17d.
func (s *Service) forceStamp(entry cregistry.Entry[AccountValue]) cregistry.Micros {
	at := s.stamp()
	for _, floor := range [...]cregistry.Micros{entry.Added, entry.Removed} {
		if at <= floor {
			at = floor + 1
		}
	}
	return at
}

// bumpStamp stamps an automatic field update one past the entry's own stamps —
// never the wall clock, so it can't cancel an unmerged removal — see ccn 10bf17d.
func (s *Service) bumpStamp(entry cregistry.Entry[AccountValue]) cregistry.Micros {
	at := entry.Added
	if entry.Removed > at {
		at = entry.Removed
	}
	return at + 1
}

// logf writes an advisory line when a logger is attached.
func (s *Service) logf(format string, args ...any) {
	if s.Log != nil {
		s.Log.Printf(format, args...)
	}
}

// TouchStamp writes the account's stamp file under StampDir/<uuid>/ so the
// host's synckitd fsnotify watch fires; it always writes, so a re-touch notifies too.
func (s *Service) TouchStamp(uuid string) error {
	dir := filepath.Join(s.StampDir, uuid)
	if err := os.MkdirAll(dir, stampDirPerm); err != nil {
		return fmt.Errorf("create stamp dir %s: %w", dir, err)
	}
	path := filepath.Join(dir, "stamp")
	payload := strconv.FormatInt(s.now().UnixNano(), 10)
	if err := os.WriteFile(path, []byte(payload), stampFilePerm); err != nil {
		return fmt.Errorf("write stamp %s: %w", path, err)
	}
	return nil
}

// mutate runs a single-account registry edit under the flock, saving and
// touching the stamp only when the entry changed; reports whether it did.
func (s *Service) mutate(ctx context.Context, uuid string, mut func(Registry) error) (bool, error) {
	var changed bool
	err := s.Registry.Update(ctx, func(reg Registry) error {
		before := Fingerprint(reg[uuid])
		if err := mut(reg); err != nil {
			return err
		}
		changed = Fingerprint(reg[uuid]) != before
		return nil
	})
	if err != nil {
		return false, err
	}
	if changed {
		if err := s.TouchStamp(uuid); err != nil {
			return true, err
		}
	}
	return changed, nil
}

// PublishAccount force-stamps v into the registry: an explicit add/relogin
// that lands past any tombstone or skewed add. It resurrects tombstones by
// design, so bulk callers (enable backfill, scans) MUST use ScanPublish.
// A non-zero chain must name its origin — an origin-less chain has no host to
// pull from and would misclassify as owned everywhere; a zero chain (identity
// only) is fine.
func (s *Service) PublishAccount(ctx context.Context, v AccountValue) error {
	if v.UUID == "" {
		return fmt.Errorf("hostsync: PublishAccount requires a UUID")
	}
	if v.Chain.Origin == "" && v.Chain != (ChainStamp{}) {
		return fmt.Errorf("hostsync: PublishAccount for %s: chain stamp names no origin", v.UUID)
	}
	_, err := s.mutate(ctx, v.UUID, func(reg Registry) error {
		reg.Add(v.UUID, v, s.forceStamp(reg[v.UUID]))
		return nil
	})
	return err
}

// RecordRemoval tombstones uuid (an absent id still records the tombstone),
// force-stamped so the remove lands under peer clock skew — see ccn 10bf17d.
func (s *Service) RecordRemoval(ctx context.Context, uuid string) error {
	_, err := s.mutate(ctx, uuid, func(reg Registry) error {
		reg.Remove(uuid, s.forceStamp(reg[uuid]))
		return nil
	})
	return err
}

// RecordLabel re-adds uuid with label under a forced stamp so a local rename
// always lands; unknown or removed accounts fail loud rather than resurrect.
func (s *Service) RecordLabel(ctx context.Context, uuid, label string) error {
	_, err := s.mutate(ctx, uuid, func(reg Registry) error {
		entry, ok := reg[uuid]
		if !ok || !entry.Present() {
			return fmt.Errorf("hostsync: RecordLabel for unknown account %s", uuid)
		}
		v := entry.Value
		v.Label = label
		reg.Add(uuid, v, s.forceStamp(entry))
		return nil
	})
	return err
}

// NoteCredWrite records chain only when it expires strictly later than the
// registry's; staler chains and absent or tombstoned accounts are no-ops.
func (s *Service) NoteCredWrite(ctx context.Context, uuid string, chain ChainStamp) error {
	_, err := s.mutate(ctx, uuid, func(reg Registry) error {
		entry, ok := reg[uuid]
		if !ok || !entry.Present() {
			return nil
		}
		if chain.ExpiresAt <= entry.Value.Chain.ExpiresAt {
			return nil
		}
		v := entry.Value
		v.Chain = chain
		reg.Add(uuid, v, s.bumpStamp(entry))
		return nil
	})
	return err
}

// ScanPublish folds this host's local accounts into reg in place (no I/O, no
// stamp touches) and reports whether anything changed; it never resurrects a
// tombstone, never overwrites a peer-set oauthAccount, and never creates an
// entry for an account this host doesn't own (a zero chain yields to peers).
func (s *Service) ScanPublish(ctx context.Context, reg Registry) (bool, error) {
	locals, err := s.Locals(ctx)
	if err != nil {
		return false, fmt.Errorf("list local accounts: %w", err)
	}
	changed := false
	for _, l := range locals {
		entry, ok := reg[l.UUID]
		switch {
		case !ok && l.Chain.Origin == "":
			// A synced-only account (zero chain — this host doesn't own it) with
			// no registry entry is a cold start: seeding a fresh zero-chain entry
			// would beat the origin's live chain in the whole-value LWW merge.
			// The origin's entry arrives by merge instead.
			continue
		case !ok:
			reg.Add(l.UUID, AccountValue(l), s.forceStamp(entry))
			changed = true
		case !entry.Present():
			// Tombstoned: never resurrect from a local scan.
			continue
		default:
			v := entry.Value
			dirty := false
			if l.Chain.ExpiresAt > entry.Value.Chain.ExpiresAt {
				v.Chain = l.Chain
				dirty = true
			}
			// Fill-if-empty only: never clobber a peer-set oauthAccount.
			if emptyOAuth(entry.Value.OAuthAccount) && !emptyOAuth(l.OAuthAccount) {
				v.OAuthAccount = l.OAuthAccount
				dirty = true
			}
			if dirty {
				reg.Add(l.UUID, v, s.bumpStamp(entry))
				changed = true
			}
		}
	}
	return changed, nil
}

// emptyOAuth reports whether a raw oauthAccount is unset or the JSON literal null.
func emptyOAuth(raw json.RawMessage) bool {
	return len(raw) == 0 || string(raw) == "null"
}

// NudgeSynckitd best-effort asks the local synckitd to re-read cc-pool's
// manifest; a missing or erroring synckitd is logged and swallowed.
func (s *Service) NudgeSynckitd(ctx context.Context, manifestPath string) {
	if err := s.run(ctx, "synckitd", "register", manifestPath); err != nil {
		s.logf("hostsync: synckitd register nudge failed (advisory): %v", err)
	}
}

// run executes through the required disposable process-group runner.
func (s *Service) run(ctx context.Context, name string, args ...string) error {
	if s.Run == nil {
		return errors.New("hostsync: disposable task runner is required")
	}
	return s.Run(ctx, name, args...)
}

// Converge runs one convergence pass — pull peers (skipping origin, the
// anti-echo), merge, reconcile each present entry, then the teardown pass —
// and reports what changed. Per-peer and per-item failures never abort a pass.
func (s *Service) Converge(ctx context.Context, origin string) (syncservice.ReconcileResult, error) {
	if s.Mesh == nil {
		return syncservice.ReconcileResult{}, fmt.Errorf("hostsync: Converge requires a Mesh")
	}
	_, peers, err := s.Mesh.Resolve(ctx)
	if err != nil {
		return syncservice.ReconcileResult{}, fmt.Errorf("hostsync: resolve mesh: %w", err)
	}
	results, err := converge.Reconcile(ctx, s.Registry.WithLock, s.Driver, s.Fetcher, s.Status, peers, origin)
	if err != nil {
		return syncservice.ReconcileResult{}, fmt.Errorf("hostsync: converge: %w", err)
	}
	converged := 0
	for _, r := range results {
		if r.Err != nil {
			s.logf("hostsync: reconcile %s: %v", r.ID, r.Err)
			continue
		}
		if convergedOutcome(r.Outcome) {
			converged++
		}
	}
	tornDown, skippedBusy, err := s.teardown(ctx)
	if err != nil {
		return syncservice.ReconcileResult{}, err
	}
	return syncservice.ReconcileResult{Converged: converged + tornDown, SkippedBusy: skippedBusy}, nil
}

// convergedOutcome reports whether an outcome changed local state, so the
// Converged count reflects real work only.
func convergedOutcome(o converge.Outcome) bool {
	switch o {
	case OutcomeMaterialized, OutcomeLabeled, OutcomeCredInstalled:
		return true
	default:
		return false
	}
}

// teardown removes every locally-materialized account a peer has tombstoned.
// Busy, unprovably idle, ambiguous, and per-item failures all defer
// to a later pass; only a registry load failure is fatal.
func (s *Service) teardown(ctx context.Context) (tornDown, skippedBusy int, err error) {
	reg, err := s.Registry.Load()
	if err != nil {
		return 0, 0, fmt.Errorf("hostsync: teardown load registry: %w", err)
	}
	for uuid, entry := range reg {
		if entry.Present() {
			continue
		}
		rows, err := s.M.Store.AccountsByUUID(uuid)
		if err != nil {
			s.logf("hostsync: teardown resolve %s: %v — deferred to a later pass", uuid, err)
			continue
		}
		if len(rows) == 0 {
			continue
		}
		if len(rows) > 1 {
			// A tombstone must never serially destroy every row sharing a uuid — see ccn 10bf17d.
			s.logf("hostsync: teardown of %s deferred: %d local accounts share the uuid — refusing an ambiguous teardown", uuid, len(rows))
			skippedBusy++
			continue
		}
		a := rows[0]
		if s.teardownBusy(ctx, a.ID, uuid) {
			skippedBusy++
			continue
		}
		if s.Remover == nil {
			s.logf("hostsync: teardown of acct-%d (%s) deferred: no lifecycle remover wired", a.ID, uuid)
			skippedBusy++
			continue
		}
		var (
			removal    AccountRemoval
			removalErr error
			readded    bool
		)
		// The registry lock is the linearization boundary with re-add writers.
		// Install the generation-fenced durable removal intent while the registry
		// snapshot remains fenced. Selection and mutation activation observe that
		// intent transactionally; no daemon-local claim crosses worker I/O.
		recheckErr := s.Registry.WithLock(ctx, func() error {
			cur, err := s.Registry.Load()
			if err != nil {
				return err
			}
			if cur[uuid].Present() {
				readded = true
				return nil
			}
			removal, removalErr = s.Remover.BeginAccountRemoval(a.ID, true)
			return nil
		})
		if recheckErr != nil {
			s.logf("hostsync: teardown of acct-%d (%s) deferred: registry fence: %v", a.ID, uuid, recheckErr)
			continue
		}
		if readded {
			s.logf("hostsync: teardown of acct-%d (%s) cancelled: re-added since the pass snapshot", a.ID, uuid)
			continue
		}
		if removalErr != nil {
			s.logf("hostsync: begin teardown of acct-%d (%s): %v — deferred to a later pass", a.ID, uuid, removalErr)
			continue
		}
		if removal == nil {
			s.logf("hostsync: begin teardown of acct-%d (%s) returned no durable removal — deferred to a later pass", a.ID, uuid)
			continue
		}
		if removalErr = removal.Finish(ctx); removalErr != nil {
			s.logf("hostsync: finish teardown of acct-%d (%s): %v — durable intent will resume", a.ID, uuid, removalErr)
			continue
		}
		s.logf("hostsync: tore down acct-%d (%s) per peer tombstone", a.ID, uuid)
		tornDown++
	}
	return tornDown, skippedBusy, nil
}

// teardownBusy reports whether uuid must defer; it fails CLOSED — a nil
// Sessions seam or a Busy error both read busy.
func (s *Service) teardownBusy(ctx context.Context, id int, uuid string) bool {
	if s.Sessions == nil {
		s.logf("hostsync: teardown of acct-%d (%s) deferred: no sessions seam wired", id, uuid)
		return true
	}
	busy, reason, err := s.Sessions.Busy(ctx, uuid)
	if err != nil {
		s.logf("hostsync: teardown of acct-%d (%s) deferred: busy check: %v", id, uuid, err)
		return true
	}
	if busy {
		s.logf("hostsync: teardown of acct-%d (%s) deferred: %s", id, uuid, reason)
	}
	return busy
}
