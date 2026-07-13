package store

import "time"

// Account is one pool account. ID is the account index (>= 1).
type Account struct {
	ID              int
	ConfigDir       string // exact string exported as CLAUDE_CONFIG_DIR
	KeychainService string
	KeychainAccount string
	Label           string // human note, e.g. an email or alias
	OverlayKind     string // overlay backend string: "symlink" | "nfs" | "fskit" | "fileprovider"
	AccountUUID     string // Claude accountUuid; "" until lazily backfilled
	CreatedAt       time.Time
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
	ID        int64
	AccountID int
	PID       int
	ConfigDir string
	Cwd       string // launch working directory; "" when unknown (never matches a pin)
	StartedAt time.Time
	// LastSeenAt is when a reconcile scan last observed the pid alive; nil
	// when never observed. Dead rows are closed at this time, not at reap
	// time, so an observer gap cannot fabricate a recent session end.
	LastSeenAt *time.Time
	EndedAt    *time.Time
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

// RefreshEntry is one credential-refresh attempt.
type RefreshEntry struct {
	AccountID int
	TS        time.Time
	OK        bool
	Err       string
}

// AuthKind classifies why an account needs re-login, so status can tell a truly
// owned dead chain apart from a synced peer copy merely waiting on its origin's
// rotation. The empty value is Owned, so a legacy (pre-kind-column) row and a
// default backfill both read as Owned.
type AuthKind string

const (
	// AuthKindOwned is a chain this host owns (or an account with no sync
	// entry): only a local `ccp login` recovers it.
	AuthKindOwned AuthKind = ""
	// AuthKindAwaitingOrigin is a synced peer copy whose access token expired;
	// it recovers when the origin host's rotation syncs over, or via a local
	// `ccp login` that makes this host the origin.
	AuthKindAwaitingOrigin AuthKind = "awaiting_origin"
)

// Valid reports whether k is a recognized AuthKind.
func (k AuthKind) Valid() bool {
	switch k {
	case AuthKindOwned, AuthKindAwaitingOrigin:
		return true
	default:
		return false
	}
}

// AuthHealth is an account's authentication health. NeedsLogin is set by the
// daemon when the stored refresh token is gone/revoked and only an interactive
// `ccp login` can recover; Since marks the false→true transition, LastErr the
// triggering failure, Kind why it needs login. No secrets — a flag, a
// timestamp, an error string, a kind.
type AuthHealth struct {
	AccountID  int
	NeedsLogin bool
	Since      time.Time // zero when NeedsLogin is false
	LastErr    string
	Kind       AuthKind
}

// JournalRisk records that cc-pool forgot a fuse row (removal, fallback, or
// conversion) after its holder Unmount confirmed the kernel detach but reported a
// persist-warning that survived a bounded retry — so the holder's durable journal may
// replay Dir as a live mount on its next restart. `ccp doctor` surfaces it so the user
// reconciles after the next holder restart; a later warning-free teardown of Dir, or
// doctor confirming Dir is no longer mounted, clears it. No secrets — a path, a warning
// string, a timestamp.
type JournalRisk struct {
	Dir        string
	Warning    string
	RecordedAt time.Time
}
