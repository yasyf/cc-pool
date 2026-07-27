// Package testhome sandboxes the home directory a test's code resolves, so
// home-derived state never escapes into the invoking user's real home.
package testhome

import "testing"

// EnvOverride is daemonkit's home override variable. daemonkit resolves the
// home directory through the passwd database and deliberately ignores HOME, so
// a test that sandboxes only HOME still writes daemonkit's durable state —
// helper binaries, launchd plists, trust material — into the real user's home.
const EnvOverride = "DAEMONKIT_HOME"

// Sandbox points HOME and EnvOverride at dir for the rest of t, and returns dir
// so a call site can inline t.TempDir().
func Sandbox(t testing.TB, dir string) string {
	t.Helper()
	t.Setenv("HOME", dir)
	t.Setenv(EnvOverride, dir)
	return dir
}
