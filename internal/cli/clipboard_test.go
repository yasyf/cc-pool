package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestTerminalURLActionAnnotatesCopiedURL(t *testing.T) {
	original := copyToClipboard
	t.Cleanup(func() { copyToClipboard = original })
	var copied string
	copyToClipboard = func(value string) error {
		copied = value
		return nil
	}
	action := &terminalURLAction{}
	if err := action.observe(context.Background(), "https://example.test/login"); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	action.annotate(&out)
	if copied != "https://example.test/login" {
		t.Fatalf("copied URL = %q", copied)
	}
	if !strings.Contains(out.String(), "Copied the login URL to your clipboard.") {
		t.Fatalf("annotation = %q", out.String())
	}
}

func TestTerminalURLActionKeepsClipboardFailureDecorative(t *testing.T) {
	original := copyToClipboard
	t.Cleanup(func() { copyToClipboard = original })
	copyToClipboard = func(string) error { return errors.New("pasteboard unavailable") }
	action := &terminalURLAction{}
	if err := action.observe(context.Background(), "https://example.test/login"); err != nil {
		t.Fatalf("observe returned decorative clipboard error: %v", err)
	}
	var out bytes.Buffer
	action.annotate(&out)
	if !strings.Contains(out.String(), "Open the login URL in your browser: https://example.test/login") {
		t.Fatalf("annotation = %q", out.String())
	}
}

func TestTerminalURLActionWithoutURLIsSilent(t *testing.T) {
	var out bytes.Buffer
	(&terminalURLAction{}).annotate(&out)
	if out.Len() != 0 {
		t.Fatalf("annotation = %q, want empty", out.String())
	}
}
