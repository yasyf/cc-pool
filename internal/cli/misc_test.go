package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

// TestEnvMergesBaseSettings pins that env merges base ~/.claude.json into the account file ("initialized" is the meta key ccp init writes).
func TestEnvMergesBaseSettings(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.WriteFile(filepath.Join(home, ".claude.json"), []byte(`{"mergeMarker": "yes"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	dir := filepath.Join(home, "acct-01")
	if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(pool.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetMeta("initialized", "1"); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertAccount(store.Account{
		ID: 1, ConfigDir: dir, Label: "work@example.com",
		KeychainService: "svc", KeychainAccount: "u", OverlayKind: "symlink",
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// The lease agent forks a detached `ccp lease-agent`; stub it (the test binary
	// is not ccp, so a real fork would never complete the readiness handshake).
	swapVar(t, &spawnLeaseAgent, func(store.Account) error { return nil })

	cmd := newEnvCmd()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--account", "1"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("env failed: %v (stderr=%q)", err, stripANSI(stderr.String()))
	}
	if !strings.Contains(stdout.String(), "export CLAUDE_CONFIG_DIR='"+dir+"'") {
		t.Fatalf("env exports missing the config dir: %q", stdout.String())
	}
	b, err := os.ReadFile(filepath.Join(dir, ".claude.json")) //nolint:gosec // G304: path is a cc-pool-managed/test-owned file, not external input
	if err != nil {
		t.Fatalf("account .claude.json missing after the env merge: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil || got["mergeMarker"] != "yes" {
		t.Fatalf("base marker did not reach the account file (err=%v): %v", err, got)
	}
}

// TestEnvHandsOutNothingOnLeaseFailure pins F3's no-handout contract at the
// command level: when the lease agent's acquired+probed handshake fails, `ccp
// env` prints NO exports (the shell must have nothing to eval) and exits
// non-zero with the failure on stderr.
func TestEnvHandsOutNothingOnLeaseFailure(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	dir := filepath.Join(home, "acct-01")
	if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(pool.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetMeta("initialized", "1"); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertAccount(store.Account{
		ID: 1, ConfigDir: dir, Label: "work@example.com",
		KeychainService: "svc", KeychainAccount: "u", OverlayKind: "symlink",
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	swapVar(t, &spawnLeaseAgent, func(store.Account) error {
		return errors.New("the session lease agent did not become ready")
	})

	cmd := newEnvCmd()
	cmd.SilenceUsage = true // as under the real root command
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--account", "1"})
	err = cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "session lease") {
		t.Fatalf("env with a failed lease handshake = %v, want the lease failure", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("env handed out %q despite a failed lease handshake; stdout must stay empty", stdout.String())
	}
}
