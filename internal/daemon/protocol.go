// Package daemon implements the background user-LaunchAgent: a usage poller,
// idle-only credential refresher, score cache, and persistent control server.
package daemon

import (
	"time"

	"github.com/yasyf/cc-pool/internal/forecast"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/version"
)

// SnapshotVersion is the exact on-disk status format accepted by the widget.
// Daemon transport identity is owned by daemonkit and deliberately not coupled
// to this derived snapshot.
const SnapshotVersion = 1

// DaemonHealthSchema is the sole hard-cut runtime-health schema.
const DaemonHealthSchema uint16 = 1

//go:generate go run ./wirebuildgen -protocol protocol.go -output wirebuild_gen.go

// ServiceRoleID is the exact request-daemon role shared by launchd and lifecycle admission.
const ServiceRoleID = "com.yasyf.cc-pool"

// StopRoleID is the controller-launched one-shot daemon settlement role.
const StopRoleID = "com.yasyf.cc-pool.stop-control"

// Op is a request operation.
type Op string

const (
	// OpHealth returns the exact ready daemon build through daemonkit's immutable observation route.
	OpHealth Op = "cc-pool.runtime.health"
	// OpSelect prepares an inspection for PID 0 or reserves a tracked launch for PID > 0.
	OpSelect Op = "select"
	// OpSelectCommit commits a provisional selection immediately before launch.
	OpSelectCommit Op = "select-commit"
	// OpSelectAbort releases a provisional selection without side effects.
	OpSelectAbort Op = "select-abort"
	// OpStatus returns scored status for all accounts.
	OpStatus Op = "status"
	// OpAccountRemove durably deprovisions and destroys one account.
	OpAccountRemove Op = "account-remove"
	// OpAccountIdentity reads worker-validated identity metadata for one account.
	OpAccountIdentity Op = "account-identity"
	// OpAccountHealth verifies one account's backing identity and credential stores.
	OpAccountHealth Op = "account-health"
	// OpAccountMutation starts or attaches to one daemon-owned credential workflow.
	OpAccountMutation Op = "account-mutation"
	// OpAccountMutationAck acknowledges one replayed terminal workflow receipt.
	OpAccountMutationAck Op = "account-mutation-ack"
)

// AccountMutationKind names one daemon-only account/credential workflow.
type AccountMutationKind string

const (
	// AccountMutationAdd creates a new account through the daemon-owned workflow.
	AccountMutationAdd AccountMutationKind = "add"
	// AccountMutationRelogin replaces one existing account credential.
	AccountMutationRelogin AccountMutationKind = "relogin"
)

// AccountMutationAction is the only client-controlled workflow input. The
// daemon alone advances reserve/apply/publish/compensate states.
type AccountMutationAction string

const (
	// AccountMutationStartOrAttach starts a workflow or attaches to its exact operation.
	AccountMutationStartOrAttach AccountMutationAction = "start-or-attach"
	// AccountMutationProvideInput supplies terminal input under the daemon-issued fence.
	AccountMutationProvideInput AccountMutationAction = "provide-input"
	// AccountMutationCancel cancels the fenced workflow.
	AccountMutationCancel AccountMutationAction = "cancel"
)

// AccountMutationRequest carries workflow intent plus the exact daemon-issued
// fence required after StartOrAttach. It cannot select an internal mutation transition.
type AccountMutationRequest struct {
	Kind           AccountMutationKind   `json:"kind"`
	Action         AccountMutationAction `json:"action"`
	AccountID      int                   `json:"account_id,omitempty"`
	Label          string                `json:"label,omitempty"`
	Fence          AccountMutationFence  `json:"fence,omitzero"`
	TerminalCursor *uint64               `json:"terminal_cursor,omitempty"`
}

// AccountMutationFence prevents publication across account/removal/credential drift.
type AccountMutationFence struct {
	CanonicalOperationID [32]byte `json:"canonical_operation_id,omitempty"`
	AccountInstanceID    string   `json:"account_instance_id,omitempty"`
	AccountGeneration    uint64   `json:"account_generation,omitempty"`
	RegistrySequence     uint64   `json:"registry_sequence,omitempty"`
	CredentialDigest     [32]byte `json:"credential_digest,omitempty"`
}

// AccountMutationResult is an exact replayable stage result.
type AccountMutationResult struct {
	OperationID [32]byte             `json:"operation_id"`
	Kind        AccountMutationKind  `json:"kind"`
	State       AccountMutationState `json:"state"`
	AccountID   int                  `json:"account_id,omitempty"`
	ConfigDir   string               `json:"config_dir,omitempty"`
	Label       string               `json:"label,omitempty"`
	Fence       AccountMutationFence `json:"fence,omitzero"`
	Completed   bool                 `json:"completed,omitempty"`
}

// AccountIdentityResult is the minimal worker-validated account identity projection.
type AccountIdentityResult struct {
	AccountID    int    `json:"account_id"`
	AccountUUID  string `json:"account_uuid"`
	EmailAddress string `json:"email_address,omitempty"`
}

// AccountHealthResult proves the daemon completed every account health check.
type AccountHealthResult struct {
	AccountID int `json:"account_id"`
}

// AccountMutationState is a closed daemon-owned externally observable state.
type AccountMutationState string

const (
	// AccountMutationAwaitingInput means the terminal is waiting for client input.
	AccountMutationAwaitingInput AccountMutationState = "awaiting-input"
	// AccountMutationApplying means the daemon is applying the credential mutation.
	AccountMutationApplying AccountMutationState = "applying"
	// AccountMutationCompleted means the mutation and publication committed.
	AccountMutationCompleted AccountMutationState = "completed"
	// AccountMutationCancelled means the workflow was cancelled without publication.
	AccountMutationCancelled AccountMutationState = "cancelled"
	// AccountMutationSuperseded means a newer account generation invalidated the workflow.
	AccountMutationSuperseded AccountMutationState = "superseded"
	// AccountMutationQuarantined means the workflow stopped after an ambiguous credential boundary.
	AccountMutationQuarantined AccountMutationState = "quarantined"
)

// Request is one typed daemon operation payload. Op is carried by the daemonkit
// frame route and is never encoded into the payload.
type Request struct {
	Op               Op     `json:"-"`
	Account          *int   `json:"account,omitempty"`            // force a select account / identify a checkin account
	PID              int    `json:"pid,omitempty"`                // launching pid for select activation
	ProcessStartedAt int64  `json:"process_started_at,omitempty"` // launching pid start time, Unix microseconds
	Cwd              string `json:"cwd,omitempty"`                // caller's working directory, keys select stickiness
	// NoFallback: report none-available instead of a least-bad exhausted pick;
	// no provisional selection is created when no account can serve.
	NoFallback bool `json:"no_fallback,omitempty"`
	// ExcludeIDs removes account-local preparation failures from a retry ranking.
	ExcludeIDs []int `json:"exclude_ids,omitempty"`
	// ReservationToken identifies a provisional select for commit or abort.
	ReservationToken string `json:"reservation_token,omitempty"`
	// DeleteCredential controls whether account removal destroys its Keychain item.
	DeleteCredential bool `json:"delete_credential,omitempty"`
	// Mutation is required only for OpAccountMutation.
	Mutation        *AccountMutationRequest `json:"mutation,omitempty"`
	MutationReceipt *[32]byte               `json:"mutation_receipt,omitempty"`
}

// HealthRequest selects the exact v1 immutable daemon-health schema.
type HealthRequest struct {
	Schema uint16 `json:"schema"`
}

// HealthResponse is one exact daemon process-generation lifecycle snapshot.
type HealthResponse struct {
	Schema             uint16            `json:"schema"`
	RuntimeBuild       string            `json:"runtime_build"`
	RuntimeProtocol    int               `json:"runtime_protocol"`
	ProcessGeneration  string            `json:"process_generation"`
	PID                int               `json:"pid"`
	State              RuntimeState      `json:"state"`
	Draining           bool              `json:"draining"`
	Busy               bool              `json:"busy"`
	Ready              bool              `json:"ready"`
	ActiveReservations int               `json:"active_reservations"`
	ActiveSessions     int               `json:"active_sessions"`
	ExclusiveClaims    int               `json:"exclusive_claims"`
	Bootstrap          BootstrapProgress `json:"bootstrap"`
}

// BootstrapProgress is the exact current-generation desired-tenant bootstrap state.
type BootstrapProgress struct {
	Generation     string             `json:"generation"`
	Total          int                `json:"total"`
	Settled        int                `json:"settled"`
	Quarantined    int                `json:"quarantined"`
	Terminal       bool               `json:"terminal"`
	Failures       []BootstrapFailure `json:"failures"`
	LastProgressAt time.Time          `json:"last_progress_at"`
}

// BootstrapFailure is one currently terminal account bootstrap failure.
type BootstrapFailure struct {
	AccountID int    `json:"account_id,omitempty"`
	Error     string `json:"error"`
}

// RuntimeState is the exact v1 daemon-health state enum.
type RuntimeState string

const (
	// RuntimeStateHealthy means the runtime is fully operational.
	RuntimeStateHealthy RuntimeState = "healthy"
	// RuntimeStateDegraded means the runtime remains available with reduced capability.
	RuntimeStateDegraded RuntimeState = "degraded"
	// RuntimeStateFailed means the runtime cannot safely serve work.
	RuntimeStateFailed RuntimeState = "failed"
)

// LedgerState is one daemon-owned auth or rate-limit gate row on the status wire.
type LedgerState struct {
	Policy   string    `json:"policy"`
	Resource string    `json:"resource"`
	Strikes  int       `json:"strikes,omitempty"`
	Faulted  bool      `json:"faulted,omitempty"`
	Attempts int       `json:"attempts,omitempty"`
	NextDue  time.Time `json:"next_due,omitzero"`
	LastErr  string    `json:"last_err,omitempty"`
	LastAt   time.Time `json:"last_at,omitzero"`
}

// ScoreComponents is the exact v1 wire projection of the scoring breakdown.
type ScoreComponents struct {
	Eff5                        float64
	Eff7                        float64
	RawRemaining5h              float64
	RawRemaining7d              float64
	Remaining5h                 float64
	Remaining7d                 float64
	SessionPenalty              float64
	RateLimitPenalty            float64
	NeedsLoginPenalty           float64
	CredentialQuarantinePenalty float64
	StalePenalty                float64
	Barrier5h                   float64
	Barrier7d                   float64
	RunwayPenalty               float64
}

// ScoreComponentsFromDomain converts the scoring model into its exact wire projection.
func ScoreComponentsFromDomain(components score.Components) ScoreComponents {
	return ScoreComponents{
		Eff5: components.Eff5, Eff7: components.Eff7,
		RawRemaining5h: components.RawRemaining5h, RawRemaining7d: components.RawRemaining7d,
		Remaining5h: components.Remaining5h, Remaining7d: components.Remaining7d,
		SessionPenalty: components.SessionPenalty, RateLimitPenalty: components.RateLimitPenalty,
		NeedsLoginPenalty:           components.NeedsLoginPenalty,
		CredentialQuarantinePenalty: components.CredentialQuarantinePenalty,
		StalePenalty:                components.StalePenalty, Barrier5h: components.Barrier5h,
		Barrier7d: components.Barrier7d, RunwayPenalty: components.RunwayPenalty,
	}
}

// ScoreComponentsToDomain converts the exact wire projection into the scoring model.
func ScoreComponentsToDomain(components ScoreComponents) score.Components {
	return score.Components{
		Eff5: components.Eff5, Eff7: components.Eff7,
		RawRemaining5h: components.RawRemaining5h, RawRemaining7d: components.RawRemaining7d,
		Remaining5h: components.Remaining5h, Remaining7d: components.Remaining7d,
		SessionPenalty: components.SessionPenalty, RateLimitPenalty: components.RateLimitPenalty,
		NeedsLoginPenalty:           components.NeedsLoginPenalty,
		CredentialQuarantinePenalty: components.CredentialQuarantinePenalty,
		StalePenalty:                components.StalePenalty, Barrier5h: components.Barrier5h,
		Barrier7d: components.Barrier7d, RunwayPenalty: components.RunwayPenalty,
	}
}

// AccountStatus is the per-account view returned by status/select.
type AccountStatus struct {
	ID                    int       `json:"id"`
	ConfigDir             string    `json:"config_dir"`
	Label                 string    `json:"label"`
	Score                 float64   `json:"score"`
	Remaining5h           float64   `json:"remaining_5h"`
	Remaining7d           float64   `json:"remaining_7d"`
	ActiveSessions        int       `json:"active_sessions"`
	RateLimited           bool      `json:"rate_limited"`
	Exhausted             bool      `json:"exhausted,omitempty"`   // a window is pegged with its reset pending
	NeedsLogin            bool      `json:"needs_login,omitempty"` // refresh token gone/revoked; run `ccp login N`
	CredentialQuarantined bool      `json:"credential_quarantined,omitempty"`
	AwaitingOrigin        bool      `json:"awaiting_origin,omitempty"` // synced peer copy expired; recovers on origin rotation or a local `ccp login`
	HasUsage              bool      `json:"has_usage"`                 // false when there is no known-good sample (never sampled, or only 429 placeholders)
	Stale                 bool      `json:"stale"`
	Resets5h              time.Time `json:"resets_5h"`
	Resets7d              time.Time `json:"resets_7d"`
	SampleAge             string    `json:"sample_age"`
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
	Components ScoreComponents `json:"components"`
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

// PoolMood is the exact v1 pool-health mood enum.
type PoolMood string

const (
	// PoolMoodChill is the calmest pool state.
	PoolMoodChill PoolMood = "chill"
	// PoolMoodEasy indicates ample headroom.
	PoolMoodEasy PoolMood = "easy"
	// PoolMoodUneasy indicates tightening headroom.
	PoolMoodUneasy PoolMood = "uneasy"
	// PoolMoodWorried indicates near-term pressure.
	PoolMoodWorried PoolMood = "worried"
	// PoolMoodAlarmed indicates severe pressure.
	PoolMoodAlarmed PoolMood = "alarmed"
	// PoolMoodPanic is the most severe pool state.
	PoolMoodPanic PoolMood = "panic"
)

func poolMoodFromForecast(mood forecast.Mood) PoolMood {
	switch mood {
	case forecast.MoodChill:
		return PoolMoodChill
	case forecast.MoodEasy:
		return PoolMoodEasy
	case forecast.MoodUneasy:
		return PoolMoodUneasy
	case forecast.MoodWorried:
		return PoolMoodWorried
	case forecast.MoodAlarmed:
		return PoolMoodAlarmed
	case forecast.MoodPanic:
		return PoolMoodPanic
	default:
		panic("daemon: unknown forecast mood " + string(mood))
	}
}

// PoolOutlook is the wire form of the forecast pool rollup. Mood is computed
// daemon-side so the widget mascot and CLI rendering always agree.
type PoolOutlook struct {
	Remaining5hPct float64 `json:"remaining_5h_pct"`
	// Remaining7dPct is the pool's mean EFFECTIVE weekly remaining — aggregate
	// weekly headroom min-folded with each account's model-scoped bucket. The wire
	// key and type are unchanged; only its meaning tightens.
	Remaining7dPct float64 `json:"remaining_7d_pct"`
	Burn5hPerHour  float64 `json:"burn_5h_per_hour,omitempty"`
	// NetBurn5hPerHour is required because zero is a real idle-pool value.
	NetBurn5hPerHour float64 `json:"net_burn_5h_per_hour"`
	// Pace5h and Pace7d are required because zero is a real idle-pool value.
	Pace5h float64   `json:"pace_5h"`
	Pace7d float64   `json:"pace_7d"`
	DryAt  time.Time `json:"dry_at,omitzero"`
	Mood   PoolMood  `json:"mood"`
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
		Proto:       SnapshotVersion,
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
			HasScoped7d:     a.Scoped7dModel != "",
			Scoped7dUtil:    a.Scoped7dUtil,
			Scoped7dResets:  a.Scoped7dResets,
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
			Mood:             poolMoodFromForecast(p.Mood),
		}
	}
	return snap
}

// Response is one daemon operation result.
type Response struct {
	OK                bool    `json:"ok"`
	Error             string  `json:"error,omitempty"`
	Dir               string  `json:"dir,omitempty"` // select: chosen config dir
	SelectedID        *int    `json:"selected_id,omitempty"`
	ReservationToken  string  `json:"reservation_token,omitempty"`
	AccountInstanceID string  `json:"account_instance_id,omitempty"`
	AccountGeneration uint64  `json:"account_generation,omitempty"`
	Sticky            bool    `json:"sticky,omitempty"`       // select honored a sticky record
	Remaining5h       float64 `json:"remaining_5h,omitempty"` // select: raw 5h remaining (100−used) of the pick
	Remaining7d       float64 `json:"remaining_7d,omitempty"` // select: raw 7d remaining (100−used) of the pick
	HasUsage          bool    `json:"has_usage,omitempty"`    // select: false when the pick has no known-good sample (never sampled, or only 429 placeholders)
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
	NoneAvailable bool            `json:"none_available,omitempty"`
	Accounts      []AccountStatus `json:"accounts,omitempty"` // status
	// Ledgers is the daemon self-heal ledger block, sorted by policy then resource.
	Ledgers         []LedgerState          `json:"ledgers,omitempty"`
	Version         string                 `json:"version,omitempty"` // health
	AccountIdentity *AccountIdentityResult `json:"account_identity,omitempty"`
	AccountHealth   *AccountHealthResult   `json:"account_health,omitempty"`
	AccountMutation *AccountMutationResult `json:"account_mutation,omitempty"`
	SoonestReset    *time.Time             `json:"soonest_reset,omitempty"`
}
