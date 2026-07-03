package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeWidgetTools puts logging fake `brew`/`open` executables on PATH and
// returns the call-log path; FAKE_TAPPED / FAKE_INSTALLED steer their answers.
func fakeWidgetTools(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "calls.log")
	brew := `#!/bin/sh
echo "brew $*" >> "$FAKE_LOG"
case "$1" in
  tap)
    if [ $# -eq 1 ]; then
      [ -n "$FAKE_TAPPED" ] && echo "yasyf/homebrew-tap"
      exit 0
    fi
    exit 0;;
  list)
    [ -n "$FAKE_INSTALLED" ] && exit 0
    exit 1;;
esac
exit 0
`
	open := `#!/bin/sh
echo "open $*" >> "$FAKE_LOG"
exit 0
`
	for name, body := range map[string]string{"brew": brew, "open": open} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil { //nolint:gosec // G306: these are executable fake-binary stub scripts; they must be 0755
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", dir)
	t.Setenv("FAKE_LOG", logPath)
	return logPath
}

// TestRunWidgetSequence pins the exact brew/open argv; xattr / --no-quarantine
// must never appear (the app is Developer ID signed, notarized, and stapled).
func TestRunWidgetSequence(t *testing.T) {
	cases := map[string]struct {
		tapped, installed bool
		want              []string
		absent            []string
	}{
		"fresh install taps and installs": {
			want: []string{
				"brew tap\n",
				"brew tap yasyf/homebrew-tap https://github.com/yasyf/homebrew-tap\n",
				"brew list --cask cc-pool-status\n",
				"brew install -y --cask yasyf/homebrew-tap/cc-pool-status\n",
				"open -g /Applications/CCPoolStatus.app\n",
			},
			absent: []string{"upgrade", "xattr", "--no-quarantine"},
		},
		"existing install upgrades without re-tapping": {
			tapped: true, installed: true,
			want: []string{
				"brew tap\n",
				"brew list --cask cc-pool-status\n",
				"brew upgrade -y --cask cc-pool-status\n",
				"open -g /Applications/CCPoolStatus.app\n",
			},
			absent: []string{"brew install", "brew tap yasyf", "xattr", "--no-quarantine"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			logPath := fakeWidgetTools(t)
			if tc.tapped {
				t.Setenv("FAKE_TAPPED", "1")
			}
			if tc.installed {
				t.Setenv("FAKE_INSTALLED", "1")
			}

			cmd := newWidgetCmd()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			if err := cmd.RunE(cmd, nil); err != nil {
				t.Fatalf("runWidget: %v\n%s", err, out.String())
			}

			logBytes, err := os.ReadFile(logPath) //nolint:gosec // G304: path is a cc-pool-managed/test-owned file, not external input
			if err != nil {
				t.Fatal(err)
			}
			log := string(logBytes)
			rest := log
			for _, want := range tc.want {
				i := strings.Index(rest, want)
				if i < 0 {
					t.Fatalf("call log missing %q (in order) — log:\n%s", want, log)
				}
				rest = rest[i+len(want):]
			}
			for _, absent := range tc.absent {
				if strings.Contains(log, absent) {
					t.Errorf("call log must not contain %q — log:\n%s", absent, log)
				}
			}
			if !strings.Contains(out.String(), "Edit Widgets") {
				t.Errorf("output missing enable instructions:\n%s", out.String())
			}
			if !strings.Contains(out.String(), "adds itself to Login Items") {
				t.Errorf("output missing the login-item self-registration note:\n%s", out.String())
			}
			// Regression pin: the app self-registers at first launch; the
			// instructions must not resurrect the manual Login Items step.
			if strings.Contains(out.String(), "Login Items → add") {
				t.Errorf("output must not tell the user to add the Login Item manually:\n%s", out.String())
			}
		})
	}
}

func TestRunWidgetRequiresBrew(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	cmd := newWidgetCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	err := cmd.RunE(cmd, nil)
	if err == nil || !strings.Contains(err.Error(), "Homebrew") {
		t.Fatalf("err = %v, want a Homebrew-missing error", err)
	}
}
