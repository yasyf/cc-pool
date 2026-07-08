// Package daemon implements the background user-LaunchAgent: a usage poller,
// idle-only credential refresher, score cache, and unix-socket server. The CLI
// hot paths (select/status) talk to it over a 0600 unix socket using the
// newline-delimited JSON protocol defined here.
package daemon

import (
	"time"

	"github.com/yasyf/cc-pool/internal/forecast"
	"github.com/yasyf/cc-pool/internal/score"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/version"
)

// ProtocolVersion is bumped on incompatible wire changes.
const ProtocolVersion = 1

// Op is a request operation.
type Op string

const (
	// OpSelect picks the best account; optionally marks a checkout.
	OpSelect Op = "select"
	// OpStatus returns scored status for all accounts.
	OpStatus Op = "status"
	// OpCheckin releases a checkout and adopts a rotated token.
	OpCheckin Op = "checkin"
	// OpHealth is the liveness + version probe.
	OpHealth Op = "health"
	// OpShutdown steps down gracefully and releases the socket.
	OpShutdown Op = "shutdown"
	// OpMigrate converts accounts between overlay providers.
	OpMigrate Op = "migrate"
	// OpCredMove moves account credentials between backends.
	OpCredMove Op = "credmove"
	// OpFPRepair re-registers wedged File Provider domains.
	OpFPRepair Op = "fprepair"
)

// Request is one client request (one JSON object per line).
type Request struct {
	Proto   int    `json:"proto"`
	Op      Op     `json:"op"`
	Account *int   `json:"account,omitempty"` // force a specific account (select)
	PID     int    `json:"pid,omitempty"`     // launching pid (select checkout / checkin)
	NoMark  bool   `json:"no_mark,omitempty"` // select without recording a checkout
	Cwd     string `json:"cwd,omitempty"`     // caller's working directory, keys select stickiness
	// NoFallback: report none-available instead of a least-bad exhausted pick;
	// discarding a pick doesn't undo its sticky/reservation/preflight side effects.
	NoFallback bool `json:"no_fallback,omitempty"`
	// To: target overlay kind for migrate ("fuse" or "symlink") or credential
	// backend for credmove ("keychain" or "file"). Only the daemon converts or
	// moves: it owns the reservations and poll claims those ops gate on.
	To string `json:"to,omitempty"`
	// Force: migrate despite live sessions. Reservations still refuse — a
	// reserved account has a claude launching into it right now. Ignored by
	// credmove: moving a credential under a live session forks its
	// refresh-token chain, so that gate has no override.
	Force bool `json:"force,omitempty"`
	// Retreat: on fprepair, retreat the target File Provider domain(s) to the
	// symlink floor instead of re-registering. This is the ONLY path that reaches
	// the (now automatic-retreat-removed) File-Provider→symlink conversion; the
	// heal breaker parks a wedged-but-controllable domain rather than retreating it.
	Retreat bool `json:"retreat,omitempty"`
}

// MigrationOutcome classifies one account's migrate or credmove result.
type MigrationOutcome string

const (
	// MigrationDone means the account converted.
	MigrationDone MigrationOutcome = "done"
	// MigrationAlready means the account was already the target kind.
	MigrationAlready MigrationOutcome = "already"
	// MigrationBusy means a live session or reservation blocked it; re-run later.
	MigrationBusy MigrationOutcome = "busy"
	// MigrationFailed means the conversion errored (detail says why).
	MigrationFailed MigrationOutcome = "failed"
)

// MigrationResult is one account's outcome in a migrate or credmove response.
// From/To carry overlay kinds for migrate and credential backend names
// ("keychain"/"file") for credmove; From is empty when the move never probed
// the source (busy/failed).
type MigrationResult struct {
	ID      int              `json:"id"`
	Label   string           `json:"label,omitempty"`
	From    string           `json:"from,omitempty"`
	To      string           `json:"to,omitempty"`
	Outcome MigrationOutcome `json:"outcome"`
	Detail  string           `json:"detail,omitempty"` // busy reason / failure text
}

// FPRepairOutcome classifies one domain's `ccp fp repair` result.
type FPRepairOutcome string

const (
	// FPRepairRepaired means the domain re-registered; the next probe verifies it.
	FPRepairRepaired FPRepairOutcome = "repaired"
	// FPRepairRetreated means File Provider cannot serve here; the account fell back to symlink.
	FPRepairRetreated FPRepairOutcome = "retreated"
	// FPRepairBusy means the domain is held by a pending select; retry.
	FPRepairBusy FPRepairOutcome = "busy"
	// FPRepairFailed means the re-register errored (detail says why).
	FPRepairFailed FPRepairOutcome = "failed"
)

// FPRepairResult is one account's `ccp fp repair` outcome.
type FPRepairResult struct {
	ID      int             `json:"id"`
	Label   string          `json:"label,omitempty"`
	Outcome FPRepairOutcome `json:"outcome"`
	Detail  string          `json:"detail,omitempty"` // failure text / retreat reason
}

// FPDomainState is the daemon's cached verdict for one wedged File Provider
// domain: its control ops answer but its reads hang — the wedge cc-pool's
// control-plane Health cannot see. Surfaced by status so `ccp doctor` renders it
// (and its recovery progress) without re-probing. Additive; status only.
type FPDomainState struct {
	ID        int    `json:"id"`
	Label     string `json:"label,omitempty"`
	ConfigDir string `json:"config_dir"`
	// RecoveryAttempts is how many recovery-ladder attempts the daemon has spent
	// on this domain so far.
	RecoveryAttempts int `json:"recovery_attempts,omitempty"`
	// BreakerTripped: the recovery ladder exhausted its attempts and parked the
	// domain — automated recovery is done; a manual `ccp fp repair` (or a
	// fileproviderd restart) is needed.
	BreakerTripped bool `json:"breaker_tripped,omitempty"`
}

// LedgerState is one self-heal ledger row on the status wire: the composed
// observability view over both ledger stores (the Server-owned store and the
// holder cache's fuse verdict rows — composed at snapshot time, never merged).
// Parked is computed against the row's policy. Additive; status only.
type LedgerState struct {
	Policy   string    `json:"policy"`
	Resource string    `json:"resource"`
	Strikes  int       `json:"strikes,omitempty"`
	Faulted  bool      `json:"faulted,omitempty"`
	Attempts int       `json:"attempts,omitempty"`
	AltHits  int       `json:"alt_hits,omitempty"`
	Parked   bool      `json:"parked,omitempty"`
	NextDue  time.Time `json:"next_due,omitzero"`
	LastErr  string    `json:"last_err,omitempty"`
	LastAt   time.Time `json:"last_at,omitzero"`
}

// HolderStatus is the daemon's cached view of the detached mount holder.
type HolderStatus struct {
	// Version is the holder's reported build version; "" means the holder was
	// unreachable at the daemon's last refresh.
	Version string `json:"version"`
	// Mounts counts the live mirrors in the holder's last List.
	Mounts int `json:"mounts"`
	// WedgedMounts counts mirrors the daemon's deep probe found wedged: shallow
	// metadata stats answer but bulk reads hang.
	WedgedMounts int `json:"wedged_mounts,omitempty"`
	// TCCError carries the latest mount-blocked-pending-TCC guidance (the
	// macOS volume-access grant walkthrough); "" when no mount is blocked.
	TCCError string `json:"tcc_error,omitempty"`
	// TCCBlockedBackend is the fuse backend whose one-time macOS grant the
	// blocked mount needs; "" when no mount is TCC-blocked.
	TCCBlockedBackend fkoverlay.Backend `json:"tcc_blocked_backend,omitempty"`
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
	HasUsage       bool      `json:"has_usage"`             // false when there is no known-good sample (never sampled, or only 429 placeholders)
	Stale          bool      `json:"stale"`
	Resets5h       time.Time `json:"resets_5h"`
	Resets7d       time.Time `json:"resets_7d"`
	SampleAge      string    `json:"sample_age"`
	// Forecast fields; all omitted when no projection is possible, so the
	// widget decodes them as optionals.
	Burn5hPerHour float64 `json:"burn_5h_per_hour,omitempty"` // %/hr drain
	// Burn7dPerHour is the gated display drain of the 7d window, %/hr.
	Burn7dPerHour float64 `json:"burn_7d_per_hour,omitempty"`
	// Projected5hAtReset is the projected REMAINING percent at Resets5h,
	// clamped to 0..100 (matching the remaining_5h convention).
	Projected5hAtReset float64 `json:"projected_5h_at_reset,omitempty"`
	// Depleted5hAt is when remaining hits 0 at the current burn; omitted
	// when a reset refills the window first.
	Depleted5hAt time.Time `json:"depleted_5h_at,omitzero"`
	// Extra-usage (pay-as-you-go overage) state.
	ExtraEnabled bool    `json:"extra_enabled,omitempty"`
	ExtraUsed    float64 `json:"extra_used,omitempty"`  // currency cents
	ExtraLimit   float64 `json:"extra_limit,omitempty"` // currency cents
	// Scoped7dUtil/Scoped7dResets/Scoped7dModel carry the account's binding
	// model-scoped weekly bucket (e.g. the Fable 5 weekly cap). The presence
	// signal is Scoped7dModel non-empty, so omitempty on a 0 util is safe.
	Scoped7dUtil   float64   `json:"scoped_7d_util,omitempty"`  // percent used 0..100 of the scoped weekly bucket
	Scoped7dResets time.Time `json:"scoped_7d_resets,omitzero"` // when the scoped weekly bucket resets
	Scoped7dModel  string    `json:"scoped_7d_model,omitempty"` // API display name of the scoped model ("" when none)
	// WeeklyExhausted reports that a weekly window — aggregate or model-scoped —
	// is pegged with its reset pending; feeds the daemon-side pool mood.
	WeeklyExhausted bool `json:"weekly_exhausted,omitempty"`
	// Components is the per-term score breakdown.
	Components score.Components `json:"components"`
}

// StatusSnapshot is the on-disk mirror of the status op, written atomically to
// pool.StatusSnapshotPath() after every poll so the widget can render without
// the socket. Proto is bumped in lockstep with the socket protocol.
type StatusSnapshot struct {
	Proto       int             `json:"proto"`
	Version     string          `json:"version"`
	GeneratedAt time.Time       `json:"generated_at"`
	Accounts    []AccountStatus `json:"accounts"`
	// Pool is nil (key absent — the widget decodes it as optional) when no
	// account has a known-good sample (never sampled, or only 429 placeholders).
	Pool *PoolOutlook `json:"pool,omitempty"`
	// Ledgers mirrors the status op's composed self-heal ledger block so doctor
	// and the widget can read it with the daemon down. Additive.
	Ledgers []LedgerState `json:"ledgers,omitempty"`
}

// PoolOutlook is the wire form of the forecast pool rollup. Mood is computed
// daemon-side so the widget mascot and CLI rendering always agree.
type PoolOutlook struct {
	Remaining5hPct float64 `json:"remaining_5h_pct"`
	Remaining7dPct float64 `json:"remaining_7d_pct"`
	Burn5hPerHour  float64 `json:"burn_5h_per_hour,omitempty"`
	// NetBurn5hPerHour is deliberately NOT omitempty: 0 is a real value, and
	// the widget falls back to gross burn on an absent key.
	NetBurn5hPerHour float64 `json:"net_burn_5h_per_hour"`
	// Pace5h and Pace7d are deliberately NOT omitempty: 0 is a real value
	// (idle pool), and the widget re-derives pace locally on an absent key.
	Pace5h float64       `json:"pace_5h"`
	Pace7d float64       `json:"pace_7d"`
	DryAt  time.Time     `json:"dry_at,omitzero"`
	Mood   forecast.Mood `json:"mood"`
}

// NewStatusSnapshot builds the stamped snapshot plus pool rollup. GeneratedAt
// is truncated to whole seconds: RFC3339Nano's fractional part trips plain
// ISO-8601 decoders (the widget's Swift JSONDecoder among them).
func NewStatusSnapshot(accounts []AccountStatus, now time.Time) StatusSnapshot {
	if accounts == nil {
		// The snapshot pins "accounts": [] — never null, which the widget's
		// non-optional array refuses to decode.
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
			HasUsage:        a.HasUsage,
			RateLimited:     a.RateLimited,
			WeeklyExhausted: a.WeeklyExhausted,
			Remaining5h:     a.Remaining5h,
			Remaining7d:     a.Remaining7d,
			Burn5hPerHour:   a.Burn5hPerHour,
			Burn7dPerHour:   a.Burn7dPerHour,
			Resets5h:        a.Resets5h,
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
	HasUsage    bool    `json:"has_usage,omitempty"`    // select: false when the pick has no known-good sample (never sampled, or only 429 placeholders)
	// ExhaustedFallback: every account was exhausted and the pick is the
	// least-bad one — the client must warn that it bills credits or rate-limits.
	ExhaustedFallback bool `json:"exhausted_fallback,omitempty"`
	// ExtraEnabled: the pick has overage billing enabled (fallback warning).
	ExtraEnabled bool `json:"extra_enabled,omitempty"`
	// Scoped7dUtil/Scoped7dModel describe the pick's binding model-scoped weekly
	// bucket for the select announce line; Scoped7dModel is "" when the pick has
	// none (the presence signal, so omitempty on a 0 util is safe).
	Scoped7dUtil  float64 `json:"scoped_7d_util,omitempty"`
	Scoped7dModel string  `json:"scoped_7d_model,omitempty"`
	// PinHeldAccount: the cwd's manual pin could not serve (rate-limited,
	// exhausted, or below the sticky headroom floor). The pin was kept; the
	// client must surface the bypass.
	PinHeldAccount *int `json:"pin_held_account,omitempty"`
	// NoneAvailable: select found no servable account (all rate-limited or the
	// pool is empty) — a structured signal so clients don't match error strings.
	NoneAvailable bool `json:"none_available,omitempty"`
	// MountsNotReady: the none-available verdict is a mount-layer fact — every
	// account has headroom but none has a mounted, healthy mirror — so clients
	// don't misreport it as exhausted/rate-limited.
	MountsNotReady bool            `json:"mounts_not_ready,omitempty"`
	Accounts       []AccountStatus `json:"accounts,omitempty"` // status
	Holder         *HolderStatus   `json:"holder,omitempty"`   // status: mount-holder cache
	// ContentHealth joins the daemon content source's recorded read and
	// write-through failures for the computed files (merged .claude.json,
	// injected settings.json) it serves over the holder and File Provider
	// bridges, "; "-separated; "" when every domain's content is healthy.
	// Status only.
	ContentHealth string `json:"content_health,omitempty"`
	// FPConsentPending: the daemon's File Provider bridge bind has not
	// completed while the daemon is alive — the signature of the app-group-
	// container TCC consent (keyed on the binary's code-signing identity: the
	// release build's stable dotted identifier makes it one-time, and only
	// identity churn — e.g. an unsigned dev build — re-prompts; launchd never
	// surfaces the prompt). Approve it, then restart the daemon. Additive;
	// status only.
	FPConsentPending bool `json:"fp_consent_pending,omitempty"`
	// FPWedged lists File Provider domains the daemon's data-plane probe found
	// wedged (control ops answer, reads hang). Additive; status only.
	FPWedged []FPDomainState `json:"fp_wedged,omitempty"`
	// Ledgers is the composed self-heal ledger block: every live ledger row from
	// both stores (Server-owned and holder-cache), sorted by policy then
	// resource. Additive; status only.
	Ledgers    []LedgerState     `json:"ledgers,omitempty"`
	Version    string            `json:"version,omitempty"`    // health
	Migrations []MigrationResult `json:"migrations,omitempty"` // migrate/credmove
	// FPRepairs carries per-account `ccp fp repair` outcomes.
	FPRepairs    []FPRepairResult `json:"fp_repairs,omitempty"`
	SoonestReset *time.Time       `json:"soonest_reset,omitempty"`
}
