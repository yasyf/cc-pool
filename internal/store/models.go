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

// AuthHealth is an account's authentication health. NeedsLogin is set by the
// daemon when the stored refresh token is gone/revoked and only an interactive
// `ccp login` can recover; Since marks the false→true transition, LastErr the
// triggering failure. No secrets — a flag, a timestamp, an error string.
type AuthHealth struct {
	AccountID  int
	NeedsLogin bool
	Since      time.Time // zero when NeedsLogin is false
	LastErr    string
}
