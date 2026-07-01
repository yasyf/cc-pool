package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/lipgloss"
)

// ANSI color numbers adapt to the terminal palette.
var (
	hdrStyle  = lipgloss.NewStyle().Bold(true)
	dimStyle  = lipgloss.NewStyle().Faint(true)
	bestStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("10"))
	warnStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("11"))
	badStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
	okStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("10"))
	// Magenta: green/yellow/red carry health semantics.
	pinStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("13"))
)

// usageStyle takes percent used: for a headroom (free) figure, pass 100−free.
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

// Printers write flush-left to align with the interactive forms.

func step(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintln(w, fmt.Sprintf(format, a...))
}

func success(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintln(w, okStyle.Render("✓")+" "+fmt.Sprintf(format, a...))
}

func note(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintln(w, dimStyle.Render(fmt.Sprintf(format, a...)))
}

func warn(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintln(w, warnStyle.Render("warning:")+" "+fmt.Sprintf(format, a...))
}

func fail(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintln(w, badStyle.Render("✗")+" "+fmt.Sprintf(format, a...))
}

var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

const spinnerInterval = 100 * time.Millisecond

// withSpinner animates while fn runs; fn must not touch stdin or write to out.
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

func plural(n int, noun string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", noun)
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

func ccpTheme() *huh.Theme {
	t := huh.ThemeCharm()
	t.Focused.Description = t.Focused.Description.Foreground(lipgloss.Color("245"))
	t.Blurred.Description = t.Blurred.Description.Foreground(lipgloss.Color("245"))
	return t
}
