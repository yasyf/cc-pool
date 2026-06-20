package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/lipgloss"
)

// Shared output styles. Color numbers are ANSI so they adapt to the user's
// terminal palette: 10 green, 11 yellow, 9 red, 8 dim gray.
var (
	hdrStyle  = lipgloss.NewStyle().Bold(true)
	dimStyle  = lipgloss.NewStyle().Faint(true)
	bestStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("10"))
	warnStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("11"))
	badStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
	okStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("10"))
	// pinStyle marks pin state: magenta (13), because green/yellow/red carry
	// health semantics in every other token.
	pinStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("13"))
)

// usageStyle tints a 0..100 percent-used figure by health: green below 70%,
// yellow from 70%, red from 90%. Shared by the usage bars and the select/run
// heading so both read the same thresholds. For a headroom (free) figure, pass
// 100−free.
func usageStyle(usedPct float64) lipgloss.Style {
	switch {
	case usedPct >= 90:
		return badStyle
	case usedPct >= 70:
		return warnStyle
	default:
		return okStyle
	}
}

// The semantic printers below are the single source of truth for line-level
// output. They all write flush-left with no ad-hoc indentation, so printed
// lines stay aligned with each other and with the interactive forms.

// step prints a top-level progress line, undecorated.
func step(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintln(w, fmt.Sprintf(format, a...))
}

// success prints a completed action with a green check.
func success(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintln(w, okStyle.Render("✓")+" "+fmt.Sprintf(format, a...))
}

// note prints a dimmed secondary line beneath a step.
func note(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintln(w, dimStyle.Render(fmt.Sprintf(format, a...)))
}

// warn prints a warning. Callers pass the command's stderr.
func warn(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintln(w, warnStyle.Render("warning:")+" "+fmt.Sprintf(format, a...))
}

// fail prints a failed action with a red cross. Callers pass the command's
// stderr.
func fail(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintln(w, badStyle.Render("✗")+" "+fmt.Sprintf(format, a...))
}

// spinnerFrames is the braille spinner cycle shared by every in-place progress
// indicator (withSpinner and the manual-login credential wait).
var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// spinnerInterval is how often the spinner advances a frame.
const spinnerInterval = 100 * time.Millisecond

// withSpinner runs fn while animating a one-line braille spinner labeled msg on
// out, so an otherwise silent blocking call reads as progress rather than a
// hang. fn runs in a goroutine while the caller's goroutine animates; callers
// must not have fn touch stdin or write to out. The spinner line is cleared
// when fn returns and fn's error is propagated. On a non-TTY there is no
// animation — fn runs inline so redirected output stays free of control codes.
func withSpinner(out io.Writer, msg string, fn func() error) error {
	if !isTTY() {
		return fn()
	}
	done := make(chan error, 1)
	go func() { done <- fn() }()
	t := time.NewTicker(spinnerInterval)
	defer t.Stop()
	for i := 0; ; i++ {
		select {
		case err := <-done:
			_, _ = fmt.Fprint(out, "\r\x1b[K")
			return err
		case <-t.C:
			_, _ = fmt.Fprintf(out, "\r%s %s", spinnerFrames[i%len(spinnerFrames)], dimStyle.Render(msg))
		}
	}
}

// plural renders a count with its noun, pluralized with a trailing "s" (e.g.
// plural(1, "account") == "1 account", plural(3, "account") == "3 accounts").
func plural(n int, noun string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", noun)
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

// ccpTheme is the huh form theme, applied to every interactive prompt so the
// forms share one look and sit consistently with the printed lines. It softens
// the Charm base by dimming descriptions.
func ccpTheme() *huh.Theme {
	t := huh.ThemeCharm()
	t.Focused.Description = t.Focused.Description.Foreground(lipgloss.Color("245"))
	t.Blurred.Description = t.Blurred.Description.Foreground(lipgloss.Color("245"))
	return t
}
