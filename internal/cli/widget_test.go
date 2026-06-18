package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeWidgetTools installs fake `brew`, `open`, and `xattr` executables on
// PATH that log every invocation (brew also logs HOMEBREW_CASK_OPTS), and
// returns the log path. FAKE_TAPPED / FAKE_INSTALLED / FAKE_QUARANTINED env
// vars steer the fakes' answers.
func fakeWidgetTools(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "calls.log")
	brew := `#!/bin/sh
echo "brew $* [casks_opts=$HOMEBREW_CASK_OPTS]" >> "$FAKE_LOG"
case "$1" in
  tap)
    if [ $# -eq 1 ]; then
      [ -n "$FAKE_TAPPED" ] && echo "yasyf/cc-pool"
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
	xattr := `#!/bin/sh
echo "xattr $*" >> "$FAKE_LOG"
case "$1" in
  -p)
    [ -n "$FAKE_QUARANTINED" ] && exit 0
    exit 1;;
esac
exit 0
`
	for name, body := range map[string]string{"brew": brew, "open": open, "xattr": xattr} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", dir)
	t.Setenv("FAKE_LOG", logPath)
	return logPath
}

// TestRunWidgetSequence pins the exact brew/xattr/open invocations: fresh
// installs tap and install, re-runs upgrade without re-tapping, both carry
// --no-quarantine via HOMEBREW_CASK_OPTS (the Homebrew 5 spelling — the
// install flag is gone, and the ad-hoc-signed app is blocked by Gatekeeper if
// quarantined), and a quarantine bit that slipped through is stripped.
func TestRunWidgetSequence(t *testing.T) {
	cases := map[string]struct {
		tapped, installed, quarantined bool
		want                           []string
		absent                         []string
	}{
		"fresh install taps and installs": {
			want: []string{
				"brew tap [",
				"brew tap yasyf/cc-pool https://github.com/yasyf/cc-pool [",
				"brew list --cask cc-pool-status [",
				"brew install -y --cask yasyf/cc-pool/cc-pool-status [casks_opts=--no-quarantine]\n",
				"xattr -p com.apple.quarantine /Applications/CCPoolStatus.app\n",
				"open -g /Applications/CCPoolStatus.app\n",
			},
			absent: []string{"upgrade", "xattr -dr"},
		},
		"existing install upgrades without re-tapping": {
			tapped: true, installed: true,
			want: []string{
				"brew tap [",
				"brew list --cask cc-pool-status [",
				"brew upgrade -y --cask cc-pool-status [casks_opts=--no-quarantine]\n",
				"open -g /Applications/CCPoolStatus.app\n",
			},
			absent: []string{"brew install", "brew tap yasyf"},
		},
		"quarantined install is stripped": {
			quarantined: true,
			want: []string{
				"brew install -y --cask yasyf/cc-pool/cc-pool-status [casks_opts=--no-quarantine]\n",
				"xattr -p com.apple.quarantine /Applications/CCPoolStatus.app\n",
				"xattr -dr com.apple.quarantine /Applications/CCPoolStatus.app\n",
				"open -g /Applications/CCPoolStatus.app\n",
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			logPath := fakeWidgetTools(t)
			t.Setenv("HOMEBREW_CASK_OPTS", "") // pin the asserted casks_opts value
			if tc.tapped {
				t.Setenv("FAKE_TAPPED", "1")
			}
			if tc.installed {
				t.Setenv("FAKE_INSTALLED", "1")
			}
			if tc.quarantined {
				t.Setenv("FAKE_QUARANTINED", "1")
			}

			cmd := newWidgetCmd()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			if err := cmd.RunE(cmd, nil); err != nil {
				t.Fatalf("runWidget: %v\n%s", err, out.String())
			}

			logBytes, err := os.ReadFile(logPath)
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
		})
	}
}

// TestBrewCaskKeepsUserOpts: a user's existing HOMEBREW_CASK_OPTS survive with
// --no-quarantine appended, never replaced.
func TestBrewCaskKeepsUserOpts(t *testing.T) {
	logPath := fakeWidgetTools(t)
	t.Setenv("HOMEBREW_CASK_OPTS", "--appdir=/Custom")

	cmd := newWidgetCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.RunE(cmd, nil); err != nil {
		t.Fatalf("runWidget: %v\n%s", err, out.String())
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(logBytes), "[casks_opts=--appdir=/Custom --no-quarantine]") {
		t.Errorf("user HOMEBREW_CASK_OPTS not preserved — log:\n%s", logBytes)
	}
}

// TestRunWidgetRequiresBrew: without brew on PATH the command fails loud with
// a pointer at the from-source path, before attempting anything.
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
