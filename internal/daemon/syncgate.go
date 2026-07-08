package daemon

import (
	"context"
	"hash/fnv"
	"log"
	"time"

	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

// Lease and takeover tuning; vars so tests shrink them.
var (
	// holderLeaseDuration is how long the select-claimed refresh lease runs
	// before the scheduler must renew it.
	holderLeaseDuration = 45 * time.Minute
	// leaseRenewUnder renews a busy account's own lease once less than this remains.
	leaseRenewUnder = 20 * time.Minute
	// takeoverStaleAfter must exceed the 900s synckitd reconcile tick plus a
	// converge pass, or a takeover double-spends a live holder's chain — see ccn 10bf17d.
	takeoverStaleAfter = 35 * time.Minute
	// takeoverJitterSpan bounds the deterministic per-host offset added to
	// takeoverStaleAfter so two hosts never take over on the same tick.
	takeoverJitterSpan = basePollInterval
)

// registryService is the slice of the hostsync Service API the refresh gate
// and lease lifecycle consume; *hostsync.Service satisfies it, tests fake it.
type registryService interface {
	ClaimHolder(ctx context.Context, uuid, host string) error
	RenewLease(ctx context.Context, uuid, host string, until int64) error
	ReleaseLease(ctx context.Context, uuid, host string) error
}

// syncGate gates preemptive refreshes to the chain holder, runs the
// dead-holder takeover, and carries the select/checkin lease lifecycle; a nil
// *syncGate disables sync entirely.
type syncGate struct {
	svc registryService
	// load returns the current registry snapshot; wired to RegistryFile.Load.
	load func() (hostsync.Registry, error)
	// self is this host's name as published in the registry.
	self string
	// enabled reports whether sync is on, consulted per call; nil means always on.
	enabled func() bool
	// now supplies the clock; tests inject a fake.
	now func() time.Time
	log *log.Logger
}

// newSyncGate wires the gate to a live hostsync service; enabled may be nil.
func newSyncGate(svc *hostsync.Service, self string, enabled func() bool, logger *log.Logger) *syncGate {
	return &syncGate{
		svc:     svc,
		load:    svc.Registry.Load,
		self:    self,
		enabled: enabled,
		now:     time.Now,
		log:     logger,
	}
}

// active reports whether the gate participates at all; safe on a nil receiver.
func (g *syncGate) active() bool {
	return g != nil && (g.enabled == nil || g.enabled())
}

func (g *syncGate) logf(format string, args ...any) {
	if g.log != nil {
		g.log.Printf(format, args...)
	}
}

// lookup returns uuid's present entry; a load failure reads as absent — fail
// open, the sync layer must never take down single-host refresh.
func (g *syncGate) lookup(uuid string) (hostsync.AccountValue, bool) {
	reg, err := g.load()
	if err != nil {
		g.logf("hostsync registry load: %v", err)
		return hostsync.AccountValue{}, false
	}
	e, ok := reg[uuid]
	if !ok || !e.Present() {
		return hostsync.AccountValue{}, false
	}
	return e.Value, true
}

// mayRefresh reports whether this host may refresh a's chain: sync off, no
// uuid, no present entry, or this host is the holder; a non-holder may still
// take over a dead holder's chain (tryTakeover).
func (g *syncGate) mayRefresh(ctx context.Context, a store.Account) bool {
	if !g.active() {
		return true
	}
	if a.AccountUUID == "" {
		return true
	}
	v, ok := g.lookup(a.AccountUUID)
	if !ok {
		return true
	}
	if v.Chain.Holder == g.self {
		return true
	}
	return g.tryTakeover(ctx, a, v)
}

// tryTakeover claims a dead holder's chain — expired, no live peer lease, and
// rotation stale past the jittered threshold. The claim publishes FIRST; a
// failed claim refuses the refresh.
func (g *syncGate) tryTakeover(ctx context.Context, a store.Account, v hostsync.AccountValue) bool {
	nowMS := g.now().UnixMilli()
	if v.Chain.ExpiresAt > nowMS {
		return false
	}
	if l := v.Lease; l != nil && l.Host != g.self && l.Until > nowMS {
		return false
	}
	stale := time.Duration(nowMS-v.Chain.RotatedAt) * time.Millisecond
	if stale <= takeoverStaleAfter+hostJitter(g.self) {
		return false
	}
	if err := g.svc.ClaimHolder(ctx, a.AccountUUID, g.self); err != nil {
		g.logf("acct-%02d holder takeover claim: %v", a.ID, err)
		return false
	}
	g.logf("acct-%02d took over dead holder %q (chain expired, rotation stale %s)",
		a.ID, v.Chain.Holder, stale.Round(time.Second))
	return true
}

// hostJitter derives a bounded deterministic offset from the host name, so
// takeover thresholds differ per host without any coordination.
func hostJitter(host string) time.Duration {
	if takeoverJitterSpan <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(host))
	return time.Duration(h.Sum64() % uint64(takeoverJitterSpan))
}

// claimForSelect claims holdership plus a lease before the select's async
// preflight refresh runs; failures are logged only — sync never fails a select.
func (g *syncGate) claimForSelect(ctx context.Context, a store.Account) {
	if !g.active() || a.AccountUUID == "" {
		return
	}
	if err := g.svc.ClaimHolder(ctx, a.AccountUUID, g.self); err != nil {
		g.logf("acct-%02d sync holder claim: %v", a.ID, err)
		return
	}
	until := g.now().Add(holderLeaseDuration).UnixMilli()
	if err := g.svc.RenewLease(ctx, a.AccountUUID, g.self, until); err != nil {
		g.logf("acct-%02d sync lease: %v", a.ID, err)
	}
}

// renewWhileBusy renews this host's lease on a busy account once less than
// leaseRenewUnder remains; a live peer lease is never clobbered.
func (g *syncGate) renewWhileBusy(ctx context.Context, a store.Account) {
	if !g.active() || a.AccountUUID == "" {
		return
	}
	v, ok := g.lookup(a.AccountUUID)
	if !ok {
		return
	}
	nowMS := g.now().UnixMilli()
	if l := v.Lease; l != nil {
		if l.Host != g.self && l.Until > nowMS {
			return
		}
		if l.Host == g.self && l.Until-nowMS >= leaseRenewUnder.Milliseconds() {
			return
		}
	}
	until := g.now().Add(holderLeaseDuration).UnixMilli()
	if err := g.svc.RenewLease(ctx, a.AccountUUID, g.self, until); err != nil {
		g.logf("acct-%02d sync lease renew: %v", a.ID, err)
	}
}

// releaseLease releases this host's lease on a; releasing under a peer's live
// lease is a safe no-op.
func (g *syncGate) releaseLease(ctx context.Context, a store.Account) {
	if !g.active() || a.AccountUUID == "" {
		return
	}
	if err := g.svc.ReleaseLease(ctx, a.AccountUUID, g.self); err != nil {
		g.logf("acct-%02d sync lease release: %v", a.ID, err)
	}
}

// peerLeaseCounts marks accounts holding a live peer lease as one extra active
// session for ranking — penalize, never exclude; a load failure skips the penalty.
func (g *syncGate) peerLeaseCounts(snaps []pool.Snapshot) map[int]int {
	if !g.active() {
		return nil
	}
	reg, err := g.load()
	if err != nil {
		g.logf("hostsync registry load for ranking: %v", err)
		return nil
	}
	nowMS := g.now().UnixMilli()
	out := map[int]int{}
	for _, sn := range snaps {
		uuid := sn.Account.AccountUUID
		if uuid == "" {
			continue
		}
		e, ok := reg[uuid]
		if !ok || !e.Present() {
			continue
		}
		if l := e.Value.Lease; l != nil && l.Host != g.self && l.Until > nowMS {
			out[sn.Account.ID] = 1
		}
	}
	return out
}
