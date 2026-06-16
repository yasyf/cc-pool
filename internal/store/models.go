package store

import "time"

// Account is one pool account. ID is the account index (>= 1).
type Account struct {
	ID              int
	ConfigDir       string // exact string exported as CLAUDE_CONFIG_DIR
	KeychainService string
	KeychainAccount string
	Label           string // human note, e.g. an email or alias
	OverlayKind     string // "symlink" | "fuse"
	CreatedAt       time.Time
}

// UsageSample is one poll of an account's quota windows. Utilization fields are
// stored as 0..100 "percent used" to feed scoring directly. The Extra* fields
// mirror the API's extra_usage (pay-as-you-go overage) block, for status
// display only — scoring ignores them.
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
// feeding the sticky binding and expiry rules. It counts only sessions the
// pool marked (sessions table) — pid-0 selects and externally launched claude
// processes are invisible here even when procscan can see them.
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
// daemon once it concludes the stored refresh token is gone/revoked and only an
// interactive `ccp login` can recover the account; Since marks the false→true
// transition, and LastErr is the failure that triggered it. No secrets — only a
// flag, a timestamp, and an error string.
type AuthHealth struct {
	AccountID  int
	NeedsLogin bool
	Since      time.Time // zero when NeedsLogin is false
	LastErr    string
}
