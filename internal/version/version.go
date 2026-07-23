// Package version reports the cc-pool binary's own build version, injected at
// link time via -ldflags and resolved through daemonkit's dev/release taxonomy so
// the daemon's version-gated eviction orders two builds correctly.
package version

import dkversion "github.com/yasyf/daemonkit/version"

var (
	// Version is the semantic version, set by -ldflags at release time; "dev" otherwise.
	Version = "dev"
	// Commit is the short git SHA, set by -ldflags at release time.
	Commit = ""
	// StatusAppVersion is the exact signed application version, set by -ldflags.
	StatusAppVersion = ""
	// StatusAppSHA256 is the exact signed application release digest, set by -ldflags.
	StatusAppSHA256 = ""
)

// String reports the running binary's build version, memoized on first call: a
// stamped release passes through; an unstamped "dev" build resolves to daemonkit's
// DevString via the executable's mtime. Commit is appended in parens when set.
func String() string {
	v := dkversion.Running(Version)
	if Commit != "" {
		v += " (" + Commit + ")"
	}
	return v
}
