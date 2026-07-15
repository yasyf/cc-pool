package cli

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
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
