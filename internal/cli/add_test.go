package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
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

	t.Run("unreadable identity stays empty", func(t *testing.T) {
		dir := withIdentity(t, "")
		if got := defaultLabel("", fkoverlay.BackendSymlink, dir); got != "" {
			t.Errorf("defaultLabel = %q, want empty", got)
		}
	})
}
