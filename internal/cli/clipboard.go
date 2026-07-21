package cli

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const clipboardTimeout = 2 * time.Second

// copyToClipboard puts text on the macOS clipboard via pbcopy, bounded so a
// wedged pasteboard service can't stall the caller; callers treat failure as
// decorative (a note, never a failed operation). Package var: test seam.
var copyToClipboard = func(text string) error {
	ctx, cancel := context.WithTimeout(context.Background(), clipboardTimeout)
	defer cancel()
	c := exec.CommandContext(ctx, "/usr/bin/pbcopy")
	c.Stdin = strings.NewReader(text)
	if err := c.Run(); err != nil {
		return fmt.Errorf("copy to clipboard: %w", err)
	}
	return nil
}

type terminalURLAction struct {
	mu     sync.Mutex
	url    string
	copied bool
}

func (a *terminalURLAction) observe(_ context.Context, url string) error {
	copied := copyToClipboard(url) == nil
	a.mu.Lock()
	if a.url == "" {
		a.url = url
		a.copied = copied
	}
	a.mu.Unlock()
	return nil
}

func (a *terminalURLAction) annotate(out io.Writer) {
	a.mu.Lock()
	url, copied := a.url, a.copied
	a.mu.Unlock()
	if url == "" {
		return
	}
	if copied {
		note(out, "Copied the login URL to your clipboard.")
		return
	}
	note(out, "Open the login URL in your browser: %s", url)
}
