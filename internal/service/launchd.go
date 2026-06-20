// Package service manages the cc-pool user LaunchAgent. It must be a user
// agent (not a root LaunchDaemon) because credential refresh needs access to
// the user's login Keychain, which a root daemon cannot read.
package service

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"text/template"

	"github.com/yasyf/cc-pool/internal/pool"
)

// Label is the LaunchAgent label / reverse-DNS identifier.
const Label = "com.yasyf.cc-pool"

const plistTemplate = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{{.Label}}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{{.Bin}}</string>
        <string>daemon</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ThrottleInterval</key>
    <integer>10</integer>
    <key>ProcessType</key>
    <string>Background</string>
    <key>StandardOutPath</key>
    <string>{{.Log}}</string>
    <key>StandardErrorPath</key>
    <string>{{.Log}}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>{{.Path}}</string>
        <key>CGOFUSE_LIBFUSE_PATH</key>
        <string>/usr/local/lib/libfuse-t.dylib</string>
    </dict>
</dict>
</plist>
`

// PlistPath is the LaunchAgent plist location.
func PlistPath() (string, error) {
	home, err := pool.Home()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Library", "LaunchAgents", Label+".plist"), nil
}

type plistData struct {
	Label, Bin, Log, Path string
}

// xmlEscape escapes a value for safe interpolation into the plist XML.
func xmlEscape(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

// WritePlist renders and writes the LaunchAgent plist for the current binary.
func WritePlist() (string, error) {
	bin, err := os.Executable()
	if err != nil {
		return "", err
	}
	// Do NOT EvalSymlinks here: a Homebrew install exposes a stable
	// /opt/homebrew/bin/cc-pool symlink into a versioned Cellar path.
	// Resolving to the Cellar path would change every upgrade and re-trigger the
	// fuse-t "Network Volumes" TCC prompt. launchd happily execs the symlink.
	path, err := PlistPath()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return "", err
	}
	if err := pool.EnsureStateDir(); err != nil {
		return "", err
	}
	tmpl := template.Must(template.New("plist").Parse(plistTemplate))
	var buf bytes.Buffer
	// XML-escape interpolated values: a path containing <, >, or & (legal on
	// APFS) would otherwise produce a malformed plist that launchctl rejects.
	if err := tmpl.Execute(&buf, plistData{
		Label: Label,
		Bin:   xmlEscape(bin),
		Log:   xmlEscape(pool.LogPath()),
		Path:  xmlEscape(os.Getenv("PATH")),
	}); err != nil {
		return "", err
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func domainTarget() string  { return "gui/" + strconv.Itoa(os.Getuid()) }
func serviceTarget() string { return domainTarget() + "/" + Label }

func launchctl(args ...string) (string, error) {
	//nolint:gosec // G204: launchctl is fixed; args are this package's own launchd subcommand tokens, not user input
	cmd := exec.Command("launchctl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// Install writes the plist and (re)bootstraps the agent so it runs now and at
// every login. Idempotent: an existing instance is booted out first.
func Install() error {
	plist, err := WritePlist()
	if err != nil {
		return err
	}
	// Best-effort remove any previous instance so bootstrap does not conflict.
	_, _ = launchctl("bootout", serviceTarget())
	if out, err := launchctl("bootstrap", domainTarget(), plist); err != nil {
		return fmt.Errorf("launchctl bootstrap: %w: %s", err, out)
	}
	_, _ = launchctl("enable", serviceTarget())
	// bootstrap already started the agent (RunAtLoad); plain `kickstart` (no
	// `-k`) only covers the loaded-but-not-running race and is a no-op when it is
	// already running, so we don't kill and cold-start it a second time.
	if out, err := launchctl("kickstart", serviceTarget()); err != nil {
		return fmt.Errorf("launchctl kickstart: %w: %s", err, out)
	}
	return nil
}

// Uninstall boots out the agent and removes its plist. Missing is not an error.
func Uninstall() error {
	_, _ = launchctl("bootout", serviceTarget())
	path, err := PlistPath()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Loaded reports whether launchd currently knows about the agent.
func Loaded() bool {
	_, err := launchctl("print", serviceTarget())
	return err == nil
}
