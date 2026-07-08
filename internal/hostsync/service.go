package hostsync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/synckit/converge"
	"github.com/yasyf/synckit/cregistry"
	"github.com/yasyf/synckit/syncservice"
)

// stampDirPerm and stampFilePerm keep the fsnotify stamp tree private — it names
// account UUIDs, so 0700/0600 even though it carries no secrets.
const (
	stampDirPerm  = 0o700
	stampFilePerm = 0o600
)

// LocalAccount is the narrow view of a pool account the publish and scan hooks
// need: its stable identity and current chain, with no dependency on the
// *pool.Manager account-enumeration API (which lands in P2). It is a first-class
// seam, not a shim: the Manager-backed adapter that produces these is wired in P2
// through Service.Locals, and tests inject a fake.
type LocalAccount struct {
	// UUID is the account's stable identity and the registry key.
	UUID string
	// Email is the account's login email.
	Email string
	// Label is the account's current user-assigned label.
	Label string
	// OAuthAccount is Claude's opaque oauthAccount object for the account, folded
	// into the registry so a peer's materializer can write it verbatim. It is empty
	// until a local scan reads it; ScanPublish sets it on a new add and backfills it
	// fill-if-empty onto a present entry that has none yet.
	OAuthAccount json.RawMessage
	// Chain is the account's current secretless chain stamp.
	Chain ChainStamp
}

// Mesh resolves this host's identity and the peer hosts a converge pass pulls
// from. Declared for the P2 converge/driver wiring; the P1 publish hooks never
// call it.
type Mesh interface {
	// Resolve returns this host's name and the peer hosts to pull from.
	Resolve(ctx context.Context) (self string, peers []string, err error)
}

// Sessions reports whether a local live Claude session (or an in-flight convert)
// holds an account, so a converge pass can mark the item busy and defer acting on
// it. Declared for P2; unused by the P1 hooks.
type Sessions interface {
	// Busy reports whether uuid is held locally and a human-readable reason.
	Busy(ctx context.Context, uuid string) (busy bool, reason string, err error)
}

// Claims is the daemon's convert-claim seam a converge teardown acquires before
// removing a locally-materialized account; *daemon.Server satisfies it in P2 via
// its beginConvert/endConvert methods. Declared for P2; unused by the P1 hooks.
type Claims interface {
	// TryClaim reserves uuid for a teardown, returning a release func and whether
	// the claim succeeded (false when a live session already holds it).
	TryClaim(uuid string) (release func(), ok bool)
}

// Service owns cc-pool's convergent account registry and the write hooks that
// mutate it: every mutation is a load-modify-save under the registry flock, then
// a stamp touch so the host's synckitd fsnotify watch notifies peers. It carries
// no secrets — the registry is metadata plus a chain stamp only.
type Service struct {
	// M is the pool manager the P2 materializer and cred-pull drive; unused by the
	// P1 publish hooks.
	M *pool.Manager
	// Registry is the on-disk convergent registry plus its flock.
	Registry *RegistryFile
	// StampDir is the root of the per-account fsnotify stamp tree
	// (StampDir/<uuid>/stamp); a touch here fires synckitd's watch.
	StampDir string
	// Log receives advisory diagnostics; nil discards them.
	Log *log.Logger
	// Now supplies the wall clock feeding cregistry stamps; nil means time.Now.
	// Injected so tests drive a deterministic clock.
	Now func() time.Time
	// Locals enumerates this host's local pool accounts for the scan-publish fold.
	// The Manager-backed adapter is wired in P2; tests inject a fake.
	Locals func(ctx context.Context) ([]LocalAccount, error)
	// Run execs an external command; nil means os/exec. Injected so tests never
	// spawn a real synckitd.
	Run func(ctx context.Context, name string, args ...string) error

	// The remaining fields are declared for the P2 converge/driver wiring and are
	// unexercised by the P1 publish hooks.

	// Mesh resolves this host and its peers for a converge pass.
	Mesh Mesh
	// DialPeer opens a typed sync client to a peer for the pull fetch.
	DialPeer func(peer string) *syncservice.Client
	// Sessions reports local liveness so busy items defer.
	Sessions Sessions
	// Claims reserves an account for a teardown.
	Claims Claims
	// Status is the process-lifetime peer up/down tracker converge.Reconcile
	// threads through; one per Service.
	Status *converge.PeerStatus
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

// forceStamp returns the add stamp a local mutation must use so its
// cregistry.Registry.Add actually lands. Add is monotone — it adopts the new
// value only when the stamp is strictly newer than the entry's current Added —
// so a bare wall-clock stamp silently no-ops whenever the entry was last stamped
// by a peer whose clock ran ahead of ours (mesh clock skew). Every local mutation
// here has already passed its own precondition (present account, fresher chain,
// owned lease, …) and so MUST take effect; flooring the stamp strictly past both
// the entry's Added and Removed guarantees the add lands and, over a tombstone,
// flips it Present. Per-entry stamps stay strictly monotone, so cross-host
// concurrency still resolves by the LWW max-join in cregistry.Merge — forcing
// only breaks a local-vs-self skew tie, never a genuine cross-host order.
func (s *Service) forceStamp(entry cregistry.Entry[AccountValue]) cregistry.Micros {
	at := s.stamp()
	for _, floor := range [...]cregistry.Micros{entry.Added, entry.Removed} {
		if at <= floor {
			at = floor + 1
		}
	}
	return at
}

// logf writes an advisory line when a logger is attached.
func (s *Service) logf(format string, args ...any) {
	if s.Log != nil {
		s.Log.Printf(format, args...)
	}
}

// TouchStamp writes the account's stamp file under StampDir/<uuid>/, creating the
// directory on demand, so the host's synckitd fsnotify watch fires and notifies
// peers. It always writes (the payload is the touch time), so even a same-content
// re-touch produces a write event; callers skip it when nothing changed.
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

// mutate runs a single-account registry edit under the flock: load, apply mut,
// and — only if mut actually changed that account's entry — save and touch the
// stamp so synckitd notifies peers. A mut that changes nothing leaves the file
// (its bytes, inode, and mtime) and the stamp untouched. It reports whether
// anything changed.
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

// PublishAccount publishes v into the registry. It is an explicit re-add whose
// forced stamp (see forceStamp) lands past both a prior tombstone AND a prior
// present add, so a re-login publish always applies: it flips a skewed-clock
// tombstone back to Present and overrides a present entry whose add was
// peer-stamped ahead of the local clock. Touches the account stamp on any change.
func (s *Service) PublishAccount(ctx context.Context, v AccountValue) error {
	if v.UUID == "" {
		return fmt.Errorf("hostsync: PublishAccount requires a UUID")
	}
	_, err := s.mutate(ctx, v.UUID, func(reg Registry) error {
		reg.Add(v.UUID, v, s.forceStamp(reg[v.UUID]))
		return nil
	})
	return err
}

// RecordRemoval tombstones uuid at the current stamp. Removing an id absent from
// the registry still records the tombstone (cregistry.Remove's property), so a
// peer that has the account learns of the removal.
func (s *Service) RecordRemoval(ctx context.Context, uuid string) error {
	_, err := s.mutate(ctx, uuid, func(reg Registry) error {
		reg.Remove(uuid, s.stamp())
		return nil
	})
	return err
}

// RecordLabel re-adds uuid with label so a later local rename always lands (via
// forceStamp), preserving every other value field; cross-host concurrency still
// resolves by the registry's LWW max-join. It fails loud for an account not
// present in the registry — a rename of an unknown or removed account is a caller
// bug, and re-adding would resurrect a tombstone.
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

// ClaimHolder sets host as uuid's chain holder — the one host allowed to
// preemptively refresh the chain — preserving every other value field. The claim
// is forced to land (see forceStamp), so a skewed-ahead add can never leave the
// registry naming a stale holder and let two hosts refresh one single-use chain.
// It fails loud for an account not present in the registry.
func (s *Service) ClaimHolder(ctx context.Context, uuid, host string) error {
	_, err := s.mutate(ctx, uuid, func(reg Registry) error {
		entry, ok := reg[uuid]
		if !ok || !entry.Present() {
			return fmt.Errorf("hostsync: ClaimHolder for unknown account %s", uuid)
		}
		v := entry.Value
		v.Chain.Holder = host
		reg.Add(uuid, v, s.forceStamp(entry))
		return nil
	})
	return err
}

// RenewLease sets uuid's refresh lease to host until the given Unix-millis
// expiry, preserving every other value field. It fails loud for an account not
// present in the registry.
func (s *Service) RenewLease(ctx context.Context, uuid, host string, until int64) error {
	_, err := s.mutate(ctx, uuid, func(reg Registry) error {
		entry, ok := reg[uuid]
		if !ok || !entry.Present() {
			return fmt.Errorf("hostsync: RenewLease for unknown account %s", uuid)
		}
		v := entry.Value
		v.Lease = &Lease{Host: host, Until: until}
		reg.Add(uuid, v, s.forceStamp(entry))
		return nil
	})
	return err
}

// ReleaseLease clears uuid's lease only when host owns it. A lease held by a
// different host, an unleased account, or an absent account is left untouched (a
// no-op, so no save and no stamp touch): a host can only release its own lease.
func (s *Service) ReleaseLease(ctx context.Context, uuid, host string) error {
	_, err := s.mutate(ctx, uuid, func(reg Registry) error {
		entry, ok := reg[uuid]
		if !ok || !entry.Present() {
			return nil
		}
		if entry.Value.Lease == nil || entry.Value.Lease.Host != host {
			return nil
		}
		v := entry.Value
		v.Lease = nil
		reg.Add(uuid, v, s.forceStamp(entry))
		return nil
	})
	return err
}

// NoteCredWrite updates uuid's chain stamp only when chain is strictly fresher
// (a later server-issued ExpiresAt) than what the registry already carries. Equal
// or staler chains are a no-op — no save, no stamp touch — the guard that stops
// the merge-install ping-pong where two hosts trade the same chain forever. An
// absent or tombstoned account is a no-op: a stray local refresh must never
// resurrect a removed account. The freshness verdict is skew-immune (ExpiresAt),
// so once it passes the add is forced to land (see forceStamp): a holder's
// rotation can never silently vanish behind a skewed-ahead add and leave the
// registry advertising the dead chain.
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
		reg.Add(uuid, v, s.forceStamp(entry))
		return nil
	})
	return err
}

// ScanPublish folds this host's local accounts (from Locals) into reg: it adds an
// account the registry has never seen, refreshes an existing present account's
// chain when the local chain is strictly fresher, and backfills a present
// account's oauthAccount when the registry has none yet — but it NEVER resurrects
// a tombstone and NEVER overwrites a non-empty peer-set oauthAccount. It mutates
// reg in place and reports whether anything changed; it performs no I/O and
// touches no stamps (the P2 driver persists the merged registry and notifies).
func (s *Service) ScanPublish(ctx context.Context, reg Registry) (bool, error) {
	locals, err := s.Locals(ctx)
	if err != nil {
		return false, fmt.Errorf("list local accounts: %w", err)
	}
	changed := false
	for _, l := range locals {
		entry, ok := reg[l.UUID]
		switch {
		case !ok:
			reg.Add(l.UUID, AccountValue{
				UUID:         l.UUID,
				Email:        l.Email,
				Label:        l.Label,
				OAuthAccount: l.OAuthAccount,
				Chain:        l.Chain,
			}, s.forceStamp(entry))
			changed = true
		case !entry.Present():
			// Tombstoned: never resurrect from a local scan.
			continue
		default:
			v := entry.Value
			dirty := false
			// Chain: the freshness verdict is ExpiresAt-based and skew-immune, so
			// move it only when strictly fresher.
			if l.Chain.ExpiresAt > entry.Value.Chain.ExpiresAt {
				v.Chain = l.Chain
				dirty = true
			}
			// oauthAccount: fill-if-empty only — backfill a scan-published account
			// that still has none, but never clobber a value a peer already set.
			if emptyOAuth(entry.Value.OAuthAccount) && !emptyOAuth(l.OAuthAccount) {
				v.OAuthAccount = l.OAuthAccount
				dirty = true
			}
			if dirty {
				reg.Add(l.UUID, v, s.forceStamp(entry))
				changed = true
			}
		}
	}
	return changed, nil
}

// emptyOAuth reports whether a raw oauthAccount is absent — unset or the JSON
// literal null (its round-tripped form) — and so eligible for a fill-if-empty
// backfill.
func emptyOAuth(raw json.RawMessage) bool {
	return len(raw) == 0 || string(raw) == "null"
}

// NudgeSynckitd best-effort nudges the local synckitd to re-read cc-pool's
// manifest by exec'ing `synckitd register <manifestPath>`. It never fails the
// caller: a missing or erroring synckitd is logged and swallowed, because the
// nudge is advisory — the 900s reconcile tick is the floor.
func (s *Service) NudgeSynckitd(ctx context.Context, manifestPath string) {
	if err := s.run(ctx, "synckitd", "register", manifestPath); err != nil {
		s.logf("hostsync: synckitd register nudge failed (advisory): %v", err)
	}
}

// run execs name with args through the injected runner, or os/exec when none is
// injected.
func (s *Service) run(ctx context.Context, name string, args ...string) error {
	if s.Run != nil {
		return s.Run(ctx, name, args...)
	}
	return exec.CommandContext(ctx, name, args...).Run()
}
