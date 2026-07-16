package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// TestLoginFlowRefusesUnprobeableDir pins F2's fresh-add lease coverage: the
// login flow acquires+probes the PENDING account's lease before any login
// interaction, so a dead or absent mount aborts loudly and the manual login
// command is never handed out.
func TestLoginFlowRefusesUnprobeableDir(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	tempLeaseRoot(t)

	pending := &pool.PendingAdd{
		Index:        5,
		ConfigDir:    filepath.Join(t.TempDir(), "gone", "acct-05"),
		OverlayKind:  fkoverlay.BackendNFS,
		LoginCommand: "CLAUDE_CONFIG_DIR=/pool/acct-05 claude auth login",
	}
	cmd := &cobra.Command{}
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)

	err := loginFlow(cmd, pending, addOptions{})
	if err == nil || !strings.Contains(err.Error(), "ccp doctor") {
		t.Fatalf("loginFlow on an unprobeable pending dir = %v, want a probe abort naming ccp doctor", err)
	}
	if strings.Contains(stdout.String(), pending.LoginCommand) {
		t.Fatalf("the manual login command was handed out despite a failed probe:\n%s", stdout.String())
	}
}

// TestLoginFlowManualSpawnsPendingAgentBeforePrint pins G2: the non-TTY manual path
// (ccp exits after printing, so the synchronous lease dies with it) hands the pending
// dir's lease to a detached session-leader-tied agent BEFORE printing the external
// login command, so the printed login never races the holder's teardown.
func TestLoginFlowManualSpawnsPendingAgentBeforePrint(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	tempLeaseRoot(t)

	dir := filepath.Join(t.TempDir(), "acct-09")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	pending := &pool.PendingAdd{
		Index:        9,
		ConfigDir:    dir,
		OverlayKind:  fkoverlay.BackendSymlink,
		LoginCommand: "CLAUDE_CONFIG_DIR=" + dir + " claude auth login",
	}
	cmd := &cobra.Command{}
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)

	var spawned, commandAlreadyPrinted bool
	swapVar(t, &spawnPendingLeaseAgent, func(p *pool.PendingAdd) error {
		spawned = true
		commandAlreadyPrinted = strings.Contains(stdout.String(), pending.LoginCommand)
		if p.Index != pending.Index {
			t.Errorf("spawnPendingLeaseAgent got index %d, want %d", p.Index, pending.Index)
		}
		return nil
	})

	if err := loginFlow(cmd, pending, addOptions{}); err != nil {
		t.Fatalf("loginFlow (non-TTY manual) = %v, want nil", err)
	}
	if !spawned {
		t.Fatal("the non-TTY manual path did not spawn the detached pending lease agent")
	}
	if commandAlreadyPrinted {
		t.Fatal("the login command was printed BEFORE the lease agent was spawned; the agent must be spawned first")
	}
	if !strings.Contains(stdout.String(), pending.LoginCommand) {
		t.Fatalf("the login command was never printed:\n%s", stdout.String())
	}
}

// TestLoginFlowRunNowNonTTYQuiet pins the non-TTY output contract: the
// interactive lead-in and all escape output stay TTY-only.
func TestLoginFlowRunNowNonTTYQuiet(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	tempLeaseRoot(t)

	dir := filepath.Join(t.TempDir(), "acct-07")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	claudeBin := filepath.Join(bin, "claude")
	if err := os.WriteFile(claudeBin, []byte("#!/bin/sh\nexit 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(claudeBin, 0o755); err != nil { //nolint:gosec // G302: test fixture must be executable.
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	swapVar(t, &watchAndClose, func(context.Context, loginProc, bool, func() (bool, error)) (awaitOutcome, error) {
		return awaitCred, nil
	})

	pending := &pool.PendingAdd{Index: 7, ConfigDir: dir, OverlayKind: fkoverlay.BackendSymlink}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)

	if err := loginFlow(cmd, pending, addOptions{runNow: true}); err != nil {
		t.Fatalf("loginFlow (non-TTY --run-login) = %v, want nil", err)
	}
	if s := stdout.String(); strings.Contains(s, "Logging in with claude") {
		t.Errorf("non-TTY run printed the interactive lead-in:\n%s", s)
	}
	if s := stdout.String(); strings.Contains(s, "\x1b") {
		t.Errorf("non-TTY run wrote escape bytes: %q", s)
	}
}

func TestDefaultLabel(t *testing.T) {
	withIdentity := func(t *testing.T, oauthJSON string) string {
		t.Helper()
		dir := t.TempDir()
		if oauthJSON != "" {
			body := `{"oauthAccount": ` + oauthJSON + `}`
			if err := os.WriteFile(filepath.Join(dir, ".claude.json"), []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return dir
	}

	t.Run("explicit label wins over the account email", func(t *testing.T) {
		dir := withIdentity(t, `{"accountUuid": "u-1", "emailAddress": "me@example.com"}`)
		if got := defaultLabel("work", fkoverlay.BackendSymlink, dir); got != "work" {
			t.Errorf("defaultLabel = %q, want the explicit %q", got, "work")
		}
	})

	t.Run("empty label prefills a name derived from an org email", func(t *testing.T) {
		dir := withIdentity(t, `{"accountUuid": "u-1", "emailAddress": "me@example.com"}`)
		if got := defaultLabel("", fkoverlay.BackendSymlink, dir); got != "Example" {
			t.Errorf("defaultLabel = %q, want %q", got, "Example")
		}
	})

	t.Run("empty label prefills the local part of a consumer email", func(t *testing.T) {
		dir := withIdentity(t, `{"accountUuid": "u-1", "emailAddress": "me@gmail.com"}`)
		if got := defaultLabel("", fkoverlay.BackendSymlink, dir); got != "me" {
			t.Errorf("defaultLabel = %q, want %q", got, "me")
		}
	})

	t.Run("empty label strips the claude token from a consumer email", func(t *testing.T) {
		dir := withIdentity(t, `{"accountUuid": "u-1", "emailAddress": "me-claude-2@gmail.com"}`)
		if got := defaultLabel("", fkoverlay.BackendSymlink, dir); got != "me-2" {
			t.Errorf("defaultLabel = %q, want %q", got, "me-2")
		}
	})

	t.Run("unreadable identity stays empty", func(t *testing.T) {
		dir := withIdentity(t, "")
		if got := defaultLabel("", fkoverlay.BackendSymlink, dir); got != "" {
			t.Errorf("defaultLabel = %q, want empty", got)
		}
	})
}

func TestAccountHeader(t *testing.T) {
	cases := []struct {
		name string
		n    int
		opts addOptions
		want string
	}{
		{"interactive loop numbers each section", 2, addOptions{}, "Account 2"},
		{"counted run shows progress", 2, addOptions{count: 3}, "Account 2 of 3"},
		{"count of one is a lone section", 1, addOptions{count: 1}, ""},
		{"auto-yes adds exactly one account", 1, addOptions{autoYes: true}, ""},
		{"auto-yes with a count still shows progress", 1, addOptions{autoYes: true, count: 2}, "Account 1 of 2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := accountHeader(tc.n, tc.opts)
			if tc.want == "" {
				if got != "" {
					t.Errorf("accountHeader = %q, want empty", got)
				}
				return
			}
			if !strings.Contains(got, tc.want) {
				t.Errorf("accountHeader = %q, want it to contain %q", got, tc.want)
			}
		})
	}
}

func TestAddedSummary(t *testing.T) {
	cases := []struct {
		name  string
		added []store.Account
		want  string
	}{
		{"empty adds nothing", nil, ""},
		{"a single add was already named by its success line", []store.Account{{Label: "Yasyf-10"}}, ""},
		{"two adds are named", []store.Account{{Label: "Yasyf-10"}, {Label: "Yasyf-11"}}, "Added Yasyf-10 and Yasyf-11."},
		{"three adds use commas", []store.Account{{Label: "A"}, {Label: "B"}, {Label: "C"}}, "Added A, B and C."},
		{"unlabeled accounts get a placeholder", []store.Account{{Label: "A"}, {}}, "Added A and an unnamed account."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := addedSummary(tc.added); got != tc.want {
				t.Errorf("addedSummary = %q, want %q", got, tc.want)
			}
		})
	}
}
