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

const (
	aliasName   = "claude"
	aliasMarker = "# Added by cc-pool (ccp)"
)

type shellKind int

const (
	shellUnknown shellKind = iota
	shellBash
	shellZsh
	shellFish
)

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

// bashRC defaults to ~/.bash_profile — the file a macOS login shell sources.
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

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// aliasLine: `ccp run` execs claude via PATH lookup, which aliases don't
// affect — no recursion. Fish's alias appends $argv, so args forward.
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

type aliasResult struct {
	Path           string
	Wrote          bool
	AlreadyPresent bool
}

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

func printNextSteps(w io.Writer) {
	step(w, "\nLaunch Claude on the emptiest account:\n\n    ccp run\n")
}

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

	write := opts.autoYes
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

func reportAlias(w io.Writer, res aliasResult) {
	if res.AlreadyPresent {
		note(w, "`claude` is already wrapped in %s.", res.Path)
		return
	}
	success(w, "Wrapped `claude` — added an alias to %s.", res.Path)
	note(w, "Restart your shell or run `source %s` to use it now.", res.Path)
	note(w, "Run `command claude` for plain ~/.claude.")
}
