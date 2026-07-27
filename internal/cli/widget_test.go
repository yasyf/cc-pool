package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/testhome"
)

func fakeOpen(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	logPath := filepath.Join(directory, "calls.log")
	body := "#!/bin/sh\necho \"open $*\" >> \"$FAKE_LOG\"\nexit 0\n"
	if err := os.WriteFile(filepath.Join(directory, "open"), []byte(body), 0o755); err != nil { //nolint:gosec // Test executable.
		t.Fatal(err)
	}
	t.Setenv("PATH", directory)
	t.Setenv("FAKE_LOG", logPath)
	return logPath
}

func TestRunWidgetReconcilesExactStackThenLaunchesUserApp(t *testing.T) {
	home := t.TempDir()
	testhome.Sandbox(t, home)
	logPath := fakeOpen(t)
	calls := 0
	swapVar(t, &installWidgetStack, func(context.Context) error {
		calls++
		return nil
	})

	cmd := newWidgetCmd()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	if err := cmd.RunE(cmd, nil); err != nil {
		t.Fatalf("runWidget: %v\n%s", err, output.String())
	}
	if calls != 1 {
		t.Fatalf("stack reconciliation calls = %d, want 1", calls)
	}
	logBytes, err := os.ReadFile(logPath) //nolint:gosec // Test-owned path.
	if err != nil {
		t.Fatal(err)
	}
	want := "open -g " + filepath.Join(home, "Applications", "CCPoolStatus.app") + "\n"
	if string(logBytes) != want {
		t.Fatalf("call log = %q, want %q", logBytes, want)
	}
	for _, required := range []string{"Edit Widgets", "adds itself to Login Items"} {
		if !strings.Contains(output.String(), required) {
			t.Errorf("output missing %q:\n%s", required, output.String())
		}
	}
	for _, forbidden := range []string{"Homebrew", "cask", "Login Items → add"} {
		if strings.Contains(output.String(), forbidden) {
			t.Errorf("output retains %q:\n%s", forbidden, output.String())
		}
	}
}

func TestRunWidgetStopsBeforeLaunchWhenReconciliationFails(t *testing.T) {
	want := errors.New("fetch failed")
	swapVar(t, &installWidgetStack, func(context.Context) error { return want })
	t.Setenv("PATH", t.TempDir())
	cmd := newWidgetCmd()
	if err := cmd.RunE(cmd, nil); !errors.Is(err, want) {
		t.Fatalf("runWidget error = %v, want %v", err, want)
	}
}
