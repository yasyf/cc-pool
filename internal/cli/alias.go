package cli

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/charmbracelet/huh"
	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
)

// aliasName is the command we wrap so plain `claude` launches on the emptiest
// pool account; aliasMarker tags our rc-file block so re-runs are idempotent.
const (
	aliasName   = "claude"
	aliasMarker = "# Added by cc-pool (ccp)"
)

// shellKind is the user's interactive shell, parsed from $SHELL. shellUnknown
// disables the alias offer; the next-steps hint still prints.
type shellKind int

const (
	shellUnknown shellKind = iota
	shellBash
	shellZsh
	shellFish
)

// detectShell maps a $SHELL value (e.g. "/bin/zsh") to a shellKind by its
// basename. An empty or unrecognized value (incl. "/bin/sh" or a versioned
// "bash-5.2") yields shellUnknown — we never guess into a wrong rc file.
func detectShell(shellEnv string) shellKind {
	switch filepath.Base(shellEnv) {
	case "bash":
		return shellBash
	case "zsh":
		return shellZsh
	case "fish":
		return shellFish
	default:
		return shellUnknown
	}
}

// rcPath returns the rc file cc-pool appends the alias to, given the user's
// home dir. It returns ("", false) for shellUnknown.
func rcPath(kind shellKind, home string) (string, bool) {
	switch kind {
	case shellZsh:
		return filepath.Join(home, ".zshrc"), true
	case shellFish:
		return filepath.Join(home, ".config", "fish", "config.fish"), true
	case shellBash:
		return bashRC(home), true
	default:
		return "", false
	}
}

// bashRC picks the bash rc file: whichever of ~/.bash_profile or ~/.bashrc
// already exists (profile wins if both do), else ~/.bash_profile — the file a
// macOS login shell (Terminal.app) sources.
func bashRC(home string) string {
	profile := filepath.Join(home, ".bash_profile")
	rc := filepath.Join(home, ".bashrc")
	if fileExists(profile) {
		return profile
	}
	if fileExists(rc) {
		return rc
	}
	return profile
}

// fileExists reports whether path exists and is a regular file (not a dir).
func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// aliasLine returns the exact rc-file line that wraps `claude` for the given
// shell. `ccp run` owns the full launch environment (CLAUDE_CONFIG_DIR plus
// CLAUDE_CODE_PLUGIN_CACHE_DIR — see execEnv) so the alias never goes stale
// when that environment grows; it execs the real binary via PATH lookup, which
// aliases don't affect, so the wrapper never recurses. Trailing args forward
// through (fish's alias appends $argv). shellUnknown returns "".
func aliasLine(kind shellKind) string {
	switch kind {
	case shellBash, shellZsh:
		return `alias claude='ccp run'`
	case shellFish:
		return `alias claude 'ccp run'`
	default:
		return ""
	}
}

// aliasInstalled reports whether the rc file at path already wraps `claude` —
// either our marker block or a user-defined claude alias/function (which we
// never clobber). A missing file counts as not installed; other read errors
// propagate so we never silently re-append after a partial read.
func aliasInstalled(path string) (bool, error) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: path is the user's own shell rc file resolved by cc-pool, not external input
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read %s: %w", path, err)
	}
	content := string(data)
	if strings.Contains(content, aliasMarker) {
		return true, nil
	}
	return definesAlias(content), nil
}

// definesAlias reports whether content already defines `claude` as an alias or
// function, in POSIX or fish syntax, so we never clobber the user's own. It
// scans line by line, ignoring leading whitespace and `#`-commented lines.
func definesAlias(content string) bool {
	for _, line := range strings.Split(content, "\n") {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "#") {
			continue
		}
		switch {
		case strings.HasPrefix(t, "alias claude="),
			strings.HasPrefix(t, "alias claude "),
			strings.HasPrefix(t, "function claude"),
			strings.HasPrefix(t, "claude()"),
			strings.HasPrefix(t, "claude ()"):
			return true
		}
	}
	return false
}

// aliasResult reports what appendAlias did, so the caller can print the right
// post-write message without re-reading the file.
type aliasResult struct {
	Path           string
	Wrote          bool
	AlreadyPresent bool
}

// appendAlias appends the marker + alias line to the shell's rc file, creating
// parent dirs (e.g. ~/.config/fish) when missing. It is a no-op when the alias
// is already installed (our marker or a user-defined claude), reported via the
// returned aliasResult. The file is opened append-only and never truncated.
func appendAlias(kind shellKind, home string) (aliasResult, error) {
	path, ok := rcPath(kind, home)
	if !ok {
		return aliasResult{}, fmt.Errorf("no rc file for shell %d", kind)
	}
	installed, err := aliasInstalled(path)
	if err != nil {
		return aliasResult{}, err
	}
	if installed {
		return aliasResult{Path: path, AlreadyPresent: true}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return aliasResult{}, fmt.Errorf("create %s: %w", filepath.Dir(path), err)
	}
	block := "\n" + aliasMarker + "\n" + aliasLine(kind) + "\n"
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644) //nolint:gosec // G304: path is the user's own shell rc file; 0644 matches a world-readable rc
	if err != nil {
		return aliasResult{}, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteString(block); err != nil {
		return aliasResult{}, fmt.Errorf("write %s: %w", path, err)
	}
	return aliasResult{Path: path, Wrote: true}, nil
}

// printNextSteps prints the canonical launch command for the pool. It runs
// after at least one successful add, on a TTY or not.
func printNextSteps(w io.Writer) {
	step(w, "\nLaunch Claude on the emptiest account:\n\n    ccp run\n")
}

// offerAlias prints the next-steps hint and, when appropriate, wraps `claude`
// so plain `claude` launches on the emptiest account. The hint always prints;
// the write is gated: --no-alias suppresses it, -y writes without prompting,
// otherwise we ask on a TTY. A write failure warns but never fails `ccp add` —
// the accounts are already pooled.
func offerAlias(cmd *cobra.Command, opts addOptions) {
	out := cmd.OutOrStdout()
	kind := detectShell(os.Getenv("SHELL"))
	printNextSteps(out)

	if opts.noAlias {
		return
	}
	if kind == shellUnknown {
		note(out, "Add this to your shell to wrap `claude`: %s", aliasLine(shellBash))
		return
	}

	write := opts.autoYes // -y consents to wrapping claude
	if !write {
		if !isTTY() {
			return
		}
		ok := false
		_ = huh.NewConfirm().
			Title("Wrap `claude` to always launch on the emptiest account?").
			Description("Adds an alias so plain `claude` uses the pool.").
			Value(&ok).
			WithTheme(ccpTheme()).
			Run()
		write = ok
	}
	if !write {
		return
	}

	home, err := pool.Home()
	if err != nil {
		warn(cmd.ErrOrStderr(), "couldn't add the alias: %v", err)
		return
	}
	res, err := appendAlias(kind, home)
	if err != nil {
		warn(cmd.ErrOrStderr(), "couldn't add the alias: %v", err)
		return
	}
	reportAlias(out, res)
}

// reportAlias prints the post-write outcome and the matching reload hint.
func reportAlias(w io.Writer, res aliasResult) {
	if res.AlreadyPresent {
		note(w, "`claude` is already wrapped in %s.", res.Path)
		return
	}
	success(w, "Wrapped `claude` — added an alias to %s.", res.Path)
	note(w, "Restart your shell or run `source %s` to use it now.", res.Path)
	note(w, "Run `command claude` for plain ~/.claude.")
}
