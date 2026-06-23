// Package daemon implements the background user-LaunchAgent: a usage poller,
// idle-only credential refresher, score cache, and unix-socket server. The CLI
// hot paths (select/status) talk to it over a 0600 unix socket using the
// newline-delimited JSON protocol defined here.
package daemon

import (
	"time"

	"github.com/yasyf/cc-pool/internal/forecast"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/fusekit/version"
)

// ProtocolVersion is bumped on incompatible wire changes.
const ProtocolVersion = 1

// Op is a request operation.
type Op string

// Recognized request operations.
const (
	OpSelect   Op = "select"   // pick the best account; optionally mark a checkout
	OpStatus   Op = "status"   // return scored status for all accounts
	OpCheckin  Op = "checkin"  // release a checkout and adopt a rotated token
	OpHealth   Op = "health"   // liveness + version probe
	OpShutdown Op = "shutdown" // step down gracefully and release the socket
	OpMigrate  Op = "migrate"  // convert accounts between overlay providers
)

// Request is one client request (one JSON object per line).
type Request struct {
	Proto   int    `json:"proto"`
	Op      Op     `json:"op"`
	Account *int   `json:"account,omitempty"` // force a specific account (select)
	PID     int    `json:"pid,omitempty"`     // launching pid (select checkout / checkin)
	NoMark  bool   `json:"no_mark,omitempty"` // select without recording a checkout
	Cwd     string `json:"cwd,omitempty"`     // caller's working directory, keys select stickiness
	// NoFallback: report none-available instead of a least-bad exhausted pick.
	// Set by --wait clients, which would discard the pick (and its sticky
	// rewrite, reservation, and preflight side effects) to keep waiting.
	NoFallback bool `json:"no_fallback,omitempty"`
	// To: target overlay kind for migrate ("fuse" or "symlink"). The daemon is
	// the only process that can perform conversions — the mounts live in the
	// detached holder, but the daemon owns the select reservations and poll
	// claims the conversion gates on.
	To string `json:"to,omitempty"`
	// Force: migrate even when accounts have live sessions (the user vouches
	// they are idle). Reservations still refuse — a reserved account has a
	// claude launching into it right now.
	Force bool `json:"force,omitempty"`
}

// MigrationOutcome classifies one account's migrate result.
type MigrationOutcome string

// Possible per-account migrate outcomes.
const (
	MigrationDone    MigrationOutcome = "done"    // converted
	MigrationAlready MigrationOutcome = "already" // was already the target kind
	MigrationBusy    MigrationOutcome = "busy"    // live session or reservation; re-run later
	MigrationFailed  MigrationOutcome = "failed"  // conversion errored (detail says why)
)

// MigrationResult is one account's outcome in a migrate response.
type MigrationResult struct {
	ID      int              `json:"id"`
	Label   string           `json:"label,omitempty"`
	From    string           `json:"from,omitempty"`
	To      string           `json:"to,omitempty"`
	Outcome MigrationOutcome `json:"outcome"`
	Detail  string           `json:"detail,omitempty"` // busy reason / failure text
}

// HolderStatus is the daemon's cached view of the detached mount holder,
// included in status responses. Additive: a pre-step-6 daemon omits it and an
// old client ignores it, so ProtocolVersion stays 1.
type HolderStatus struct {
	// Version is the holder's reported build version; "" means the holder was
	// unreachable at the daemon's last refresh.
	Version string `json:"version"`
	// Mounts counts the live mirrors in the holder's last List.
	Mounts int `json:"mounts"`
	// WedgedMounts counts the partial-wedge mirrors in the daemon's own
	// deep-probe verdict: mounts that answer shallow metadata stats but hang
	// bulk reads. Supervision remounts them automatically; status and doctor
	// surface them so already-wedged sessions get relaunched. Additive.
	WedgedMounts int `json:"wedged_mounts,omitempty"`
	// Skewed means a reachable holder runs a different build than the daemon.
	Skewed bool `json:"skewed"`
	// TCCError carries the latest mount-blocked-pending-TCC guidance (the
	// macOS volume-access grant walkthrough); "" when no mount is blocked.
	TCCError string `json:"tcc_error,omitempty"`
	// SpawnError is the daemon's latest failed attempt to spawn a mount
	// holder; "" when the last spawn succeeded or none was needed. Additive.
	SpawnError string `json:"spawn_error,omitempty"`
}

// AccountStatus is the per-account view returned by status/select.
type AccountStatus struct {
	ID             int       `json:"id"`
	ConfigDir      string    `json:"config_dir"`
	Label          string    `json:"label"`
	OverlayKind    string    `json:"overlay_kind"`
	Score          float64   `json:"score"`
	Remaining5h    float64   `json:"remaining_5h"`
	Remaining7d    float64   `json:"remaining_7d"`
	ActiveSessions int       `json:"active_sessions"`
	RateLimited    bool      `json:"rate_limited"`
	Exhausted      bool      `json:"exhausted,omitempty"`   // a window is pegged with its reset pending
	NeedsLogin     bool      `json:"needs_login,omitempty"` // refresh token gone/revoked; run `ccp login N`
	HasUsage       bool      `json:"has_usage"`             // false only if the account was never sampled
	Stale          bool      `json:"stale"`
	Resets5h       time.Time `json:"resets_5h"`
	Resets7d       time.Time `json:"resets_7d"`
	SampleAge      string    `json:"sample_age"`
	// Forecast fields, computed from recent usage history at snapshot time.
	// All are omitted when no projection is possible (idle, stale,
	// rate-limited, exhausted, or too little burn history); the widget
	// decodes them as optionals.
	Burn5hPerHour float64 `json:"burn_5h_per_hour,omitempty"` // %/hr drain
	// Burn7dPerHour is the gated display drain of the 7d window, %/hr.
	Burn7dPerHour float64 `json:"burn_7d_per_hour,omitempty"`
	// Projected5hAtReset is the projected REMAINING percent at Resets5h,
	// clamped to 0..100 (matching the remaining_5h convention).
	Projected5hAtReset float64 `json:"projected_5h_at_reset,omitempty"`
	// Depleted5hAt is when remaining hits 0 at the current burn; omitted
	// when a reset refills the window first.
	Depleted5hAt time.Time `json:"depleted_5h_at,omitzero"`
	// Extra-usage (pay-as-you-go overage) state, for status display.
	ExtraEnabled bool    `json:"extra_enabled,omitempty"`
	ExtraUsed    float64 `json:"extra_used,omitempty"`  // currency cents
	ExtraLimit   float64 `json:"extra_limit,omitempty"` // currency cents
	// Components is the per-term score breakdown, so status can explain the score.
	Components score.Components `json:"components"`
}

// StatusSnapshot is the on-disk mirror of the status op, written atomically to
// pool.StatusSnapshotPath() after every completed poll so out-of-process
// readers (the Notification Center widget) can render status without the
// socket. Accounts reuses the wire AccountStatus verbatim; Proto is bumped in
// lockstep with the socket protocol.
type StatusSnapshot struct {
	Proto       int             `json:"proto"`
	Version     string          `json:"version"`
	GeneratedAt time.Time       `json:"generated_at"`
	Accounts    []AccountStatus `json:"accounts"`
	// Pool is the pool-wide rollup behind the widget's headline and mascot;
	// nil (key absent) when no account has ever been sampled, which the
	// widget models as an optional.
	Pool *PoolOutlook `json:"pool,omitempty"`
}

// PoolOutlook is the wire form of the forecast pool rollup: mean remaining
// capacity, summed and net burn, projected dry-out, and the alarm mood. Mood
// is computed here, daemon-side, so the widget mascot and any CLI rendering
// always agree.
type PoolOutlook struct {
	Remaining5hPct float64 `json:"remaining_5h_pct"`
	Remaining7dPct float64 `json:"remaining_7d_pct"`
	Burn5hPerHour  float64 `json:"burn_5h_per_hour,omitempty"`
	// NetBurn5hPerHour is forecast.Pool.NetBurnPerHour: the projected drop of
	// Remaining5hPct over the next hour, points/hr, crediting refills inside
	// that hour. Deliberately NOT omitempty: a balanced pool's net is exactly
	// 0, and the widget treats an absent key as "daemon predates the field"
	// and falls back to the gross burn — omitting 0 would caption a balanced
	// pool with its gross rate.
	NetBurn5hPerHour float64 `json:"net_burn_5h_per_hour"`
	// Pace5h and Pace7d are the gross burn divided by the pool's regeneration
	// rate for each window (forecast.Pool.Pace5h/Pace7d): <1 is sustainable
	// indefinitely, 1 is exactly break-even, >1 means a wall is coming.
	// Deliberately NOT omitempty: 0 is a real value (an idle pool sustains
	// forever), and the widget treats an absent key as "daemon predates the
	// field" and re-derives pace locally — omitting 0 would flip an idle pool
	// to that skew path.
	Pace5h float64       `json:"pace_5h"`
	Pace7d float64       `json:"pace_7d"`
	DryAt  time.Time     `json:"dry_at,omitzero"`
	Mood   forecast.Mood `json:"mood"`
}

// NewStatusSnapshot stamps accounts with the protocol version, build version,
// and generation time, and rolls up the pool-wide outlook. GeneratedAt is
// truncated to whole seconds: Go would otherwise emit RFC3339Nano, whose
// fractional part trips plain ISO-8601 decoders (the widget's Swift
// JSONDecoder among them).
func NewStatusSnapshot(accounts []AccountStatus, now time.Time) StatusSnapshot {
	if accounts == nil {
		// A nil slice reaches here when an empty pool round-trips the socket
		// (Response.Accounts is omitempty, so a zero-length reply decodes as
		// nil). The snapshot schema pins "accounts": [] — never null, which
		// the widget's non-optional array would refuse to decode.
		accounts = []AccountStatus{}
	}
	snap := StatusSnapshot{
		Proto:       ProtocolVersion,
		Version:     version.String(),
		GeneratedAt: now.Truncate(time.Second),
		Accounts:    accounts,
	}
	pa := make([]forecast.PoolAccount, 0, len(accounts))
	for _, a := range accounts {
		pa = append(pa, forecast.PoolAccount{
			HasUsage:      a.HasUsage,
			RateLimited:   a.RateLimited,
			Remaining5h:   a.Remaining5h,
			Remaining7d:   a.Remaining7d,
			Burn5hPerHour: a.Burn5hPerHour,
			Burn7dPerHour: a.Burn7dPerHour,
			Resets5h:      a.Resets5h,
		})
	}
	if p, ok := forecast.PoolOf(pa, now); ok {
		snap.Pool = &PoolOutlook{
			Remaining5hPct:   p.Remaining5h,
			Remaining7dPct:   p.Remaining7d,
			Burn5hPerHour:    p.BurnPerHour,
			NetBurn5hPerHour: p.NetBurnPerHour,
			Pace5h:           p.Pace5h,
			Pace7d:           p.Pace7d,
			DryAt:            p.DryAt,
			Mood:             p.Mood,
		}
	}
	return snap
}

// Response is one server reply (one JSON object per line).
type Response struct {
	Proto       int     `json:"proto"`
	OK          bool    `json:"ok"`
	Error       string  `json:"error,omitempty"`
	Dir         string  `json:"dir,omitempty"` // select: chosen config dir
	SelectedID  *int    `json:"selected_id,omitempty"`
	Sticky      bool    `json:"sticky,omitempty"`       // select honored a sticky record
	Remaining5h float64 `json:"remaining_5h,omitempty"` // select: raw 5h remaining (100−used) of the pick
	Remaining7d float64 `json:"remaining_7d,omitempty"` // select: raw 7d remaining (100−used) of the pick
	HasUsage    bool    `json:"has_usage,omitempty"`    // select: false if the pick was never sampled
	// ExhaustedFallback: every account was exhausted and the pick is the
	// least-bad one — the client must warn that it bills credits or rate-limits.
	ExhaustedFallback bool `json:"exhausted_fallback,omitempty"`
	// ExtraEnabled: the pick has overage billing enabled (fallback warning).
	ExtraEnabled bool `json:"extra_enabled,omitempty"`
	// PinHeldAccount: the cwd has a manual pin to this account, but it could
	// not serve (rate-limited, exhausted, or below the sticky headroom floor).
	// The pin was kept; the client must surface the bypass when the pick
	// differs from it.
	PinHeldAccount *int `json:"pin_held_account,omitempty"`
	// NoneAvailable: select found no servable account (all rate-limited or the
	// pool is empty) — a structured signal so clients don't match error strings.
	NoneAvailable bool `json:"none_available,omitempty"`
	// MountsNotReady: the none-available verdict is a mount-layer fact, not a
	// capacity one — every account has headroom but none has a mounted, healthy
	// mirror (all migrating, unmounted, or the holder is mid-replacement). Lets
	// the client surface that instead of the misleading "exhausted/rate-limited".
	MountsNotReady bool              `json:"mounts_not_ready,omitempty"`
	Accounts       []AccountStatus   `json:"accounts,omitempty"`   // status
	Holder         *HolderStatus     `json:"holder,omitempty"`     // status: mount-holder cache
	Version        string            `json:"version,omitempty"`    // health
	Migrations     []MigrationResult `json:"migrations,omitempty"` // migrate
	SoonestReset   *time.Time        `json:"soonest_reset,omitempty"`
}
