package store

import (
	"crypto/sha256"
	"time"
)

// FileProviderLeaseReceipt is the byte-exact FuseKit receipt retained for one session.
type FileProviderLeaseReceipt []byte

// SessionLeaseState is the durable cross-store settlement state.
type SessionLeaseState string

const (
	SessionLeasePending  SessionLeaseState = "pending"
	SessionLeaseActive   SessionLeaseState = "active"
	SessionLeaseReleased SessionLeaseState = "released"
)

// Account is one pool account. ID is the account index (>= 1).
type Account struct {
	ID              int
	InstanceID      string
	Generation      uint64
	ConfigDir       string // exact string exported as CLAUDE_CONFIG_DIR
	KeychainService string
	KeychainAccount string
	Label           string // human note, e.g. an email or alias
	AccountUUID     string // immutable Claude accountUuid
	CreatedAt       time.Time
}

// AccountRemoval is one crash-recoverable destructive lifecycle intent.
type AccountRemoval struct {
	AccountID         int
	AccountInstanceID string
	AccountGeneration uint64
	RegistrySequence  uint64
	DeleteCredential  bool
	CreatedAt         time.Time
}

// UsageSample is one poll of an account's quota windows. Utilization fields are
// 0..100 percent-used; Extra* mirrors the API's extra_usage overage block and is
// display-only (scoring ignores it).
type UsageSample struct {
	AccountID    int
	TS           time.Time
	Util5h       float64
	Util7d       float64
	Resets5h     time.Time
	Resets7d     time.Time
	RateLimited  bool
	ExtraEnabled bool
	ExtraUsed    float64 // overage credits consumed this month (currency cents)
	ExtraLimit   float64 // overage credit cap (currency cents)
	// Scoped7dUtil is the model-scoped weekly bucket's utilization (0..100
	// percent-used); meaningful only when Scoped7dModel is non-empty.
	Scoped7dUtil float64
	// Scoped7dResets is when the model-scoped weekly bucket resets; zero when
	// the bucket is absent.
	Scoped7dResets time.Time
	// Scoped7dModel is the API's display name for the model-scoped weekly
	// bucket (e.g. "Fable"). Empty means the response carried no such bucket.
	Scoped7dModel string
}

// Session is a checkout of an account to a live claude process.
type Session struct {
	ID                int64
	SelectionToken    string
	AccountID         int
	AccountInstanceID string
	AccountGeneration uint64
	PID               int
	ProcessStartedAt  time.Time
	ConfigDir         string
	Cwd               string // launch working directory; "" when unknown (never matches a pin)
	StartedAt         time.Time
	LeaseState        SessionLeaseState
	FileProviderLease FileProviderLeaseReceipt
	LeaseExpiresAt    time.Time
	// LeaseRenewalExpiresAt is the durable exact renewal request awaiting settlement.
	LeaseRenewalExpiresAt *time.Time
	// LastSeenAt is when a reconcile scan last observed the pid alive; nil
	// when never observed. Dead rows are closed at this time, not at reap
	// time, so an observer gap cannot fabricate a recent session end.
	LastSeenAt *time.Time
	EndedAt    *time.Time
}

// ProcessIdentity is the kernel identity of the process that owns a session.
// PID alone is not an identity because the kernel reuses it.
type ProcessIdentity struct {
	PID       int
	StartedAt time.Time
}

// SelectionActivation is the compare-and-swap payload that turns a short
// reservation into sticky/session state.
type SelectionActivation struct {
	Token              string
	AccountID          int
	ExpectedInstanceID string
	ExpectedGeneration uint64
	Process            ProcessIdentity
	ConfigDir          string
	Cwd                string
	RecordSticky       bool
	At                 time.Time
	FileProviderLease  FileProviderLeaseReceipt
	LeaseExpiresAt     time.Time
}

// SessionReconciliation partitions exact active rows after one process snapshot.
type SessionReconciliation struct {
	Live []Session
	Dead []Session
}

// Sticky is the account pinned to a working directory, used to keep resumed
// sessions on the same account for prompt-cache continuity.
type Sticky struct {
	Cwd       string
	AccountID int
	// SelectedAt is the last pin activity: set at creation (manual pin or
	// first select) and refreshed by every select that records the pin.
	SelectedAt time.Time
	// Manual marks a pin created explicitly by the user (status TUI) rather
	// than by select-path affinity. Manual pins bind without a warm cache and
	// are never repointed by the select path.
	Manual bool
}

// CwdActivity summarizes tracked session activity for one working directory,
// feeding sticky binding and expiry. It counts only pool-marked sessions; pid-0
// selects and external claude are invisible even when procscan sees them.
type CwdActivity struct {
	Live      int       // sessions still running in this cwd
	LastEnded time.Time // most recent ended_at; zero when none ended
}

// RefreshCategory is the non-secret classification of one refresh attempt.
type RefreshCategory string

const (
	// RefreshSucceeded records a successful refresh attempt.
	RefreshSucceeded RefreshCategory = "succeeded"
	// RefreshCanceled records caller cancellation.
	RefreshCanceled RefreshCategory = "canceled"
	// RefreshNetwork records a transport failure.
	RefreshNetwork RefreshCategory = "network"
	// RefreshInvalidGrant records an invalid OAuth grant.
	RefreshInvalidGrant RefreshCategory = "invalid_grant"
	// RefreshRejected records a non-grant refresh rejection.
	RefreshRejected RefreshCategory = "rejected"
	// RefreshServer records a remote service failure.
	RefreshServer RefreshCategory = "server"
	// RefreshInternal records a local invariant or system failure.
	RefreshInternal RefreshCategory = "internal"
)

// RefreshEntry is one credential-refresh attempt. Digest fingerprints the
// in-memory error without persisting its potentially secret response body.
type RefreshEntry struct {
	AccountID int
	TS        time.Time
	Category  RefreshCategory
	Digest    [32]byte
}

// AuthKind classifies why an account needs re-login, so status can tell a truly
// owned dead chain apart from a synced peer copy merely waiting on its origin's
// rotation or a chain whose ownership has not been verified.
type AuthKind string

const (
	// AuthKindOwned is a chain this host owns (or an account with no sync
	// entry): only a local `ccp login` recovers it.
	AuthKindOwned AuthKind = "owned"
	// AuthKindAwaitingOrigin is a synced peer copy whose access token expired;
	// it recovers when the origin host's rotation syncs over, or via a local
	// `ccp login` that makes this host the origin.
	AuthKindAwaitingOrigin AuthKind = "awaiting_origin"
	// AuthKindUnverified means ownership could not be proven and acting as the
	// origin would be unsafe.
	AuthKindUnverified AuthKind = "unverified"
)

// Valid reports whether k is a recognized AuthKind.
func (k AuthKind) Valid() bool {
	switch k {
	case AuthKindOwned, AuthKindAwaitingOrigin, AuthKindUnverified:
		return true
	default:
		return false
	}
}

// AuthReasonCategory is a non-secret authentication-failure classification.
type AuthReasonCategory string

const (
	// AuthReasonNone records a healthy account without an authentication failure.
	AuthReasonNone AuthReasonCategory = "none"
	// AuthReasonRequired records an interactive authentication requirement.
	AuthReasonRequired AuthReasonCategory = "auth_required"
	// AuthReasonAwaitingOrigin records a synchronized copy awaiting its origin.
	AuthReasonAwaitingOrigin AuthReasonCategory = "awaiting_origin"
	// AuthReasonInternal records an internal authentication-state failure.
	AuthReasonInternal AuthReasonCategory = "internal"
)

// Valid reports whether c is a recognized authentication reason.
func (c AuthReasonCategory) Valid() bool {
	switch c {
	case AuthReasonNone, AuthReasonRequired, AuthReasonAwaitingOrigin, AuthReasonInternal:
		return true
	default:
		return false
	}
}

// DigestReason returns the only durable representation of error detail.
func DigestReason(detail string) [32]byte { return sha256.Sum256([]byte(detail)) }

// AuthHealth is an account's authentication health. NeedsLogin is set by the
// daemon when the stored refresh token is gone/revoked and only an interactive
// `ccp login` can recover; Since marks the false→true transition. Reason and
// Digest retain classification and correlation without the raw OAuth body.
type AuthHealth struct {
	AccountID  int
	NeedsLogin bool
	Since      time.Time // zero when NeedsLogin is false
	Reason     AuthReasonCategory
	Digest     [32]byte
	Kind       AuthKind
	Gen        int64
}
