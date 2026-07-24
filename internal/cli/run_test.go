package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

func TestExecEnv(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	pluginRoot := "CLAUDE_CODE_PLUGIN_CACHE_DIR=" + filepath.Join(pool.ClaudeDir(), "plugins")
	debugDir := "CLAUDE_CODE_DEBUG_LOGS_DIR=" + filepath.Join(pool.ClaudeDir(), "debug")

	t.Run("appends CLAUDE_CONFIG_DIR, the plugin root, and the debug dir when absent", func(t *testing.T) {
		got := execEnv([]string{"PATH=/bin", "HOME=/home/me"}, "/cfg")
		if !contains(got, "CLAUDE_CONFIG_DIR=/cfg") {
			t.Errorf("CLAUDE_CONFIG_DIR=/cfg missing: %v", got)
		}
		if n := countPrefix(got, "CLAUDE_CONFIG_DIR="); n != 1 {
			t.Errorf("CLAUDE_CONFIG_DIR count = %d, want 1", n)
		}
		if !contains(got, pluginRoot) {
			t.Errorf("%s missing: %v", pluginRoot, got)
		}
		if n := countPrefix(got, "CLAUDE_CODE_PLUGIN_CACHE_DIR="); n != 1 {
			t.Errorf("CLAUDE_CODE_PLUGIN_CACHE_DIR count = %d, want 1", n)
		}
		if !contains(got, debugDir) {
			t.Errorf("%s missing: %v", debugDir, got)
		}
		if n := countPrefix(got, "CLAUDE_CODE_DEBUG_LOGS_DIR="); n != 1 {
			t.Errorf("CLAUDE_CODE_DEBUG_LOGS_DIR count = %d, want 1", n)
		}
		if !contains(got, "PATH=/bin") || !contains(got, "HOME=/home/me") {
			t.Errorf("dropped a passthrough var: %v", got)
		}
	})

	t.Run("replaces an existing CLAUDE_CONFIG_DIR without duplicating", func(t *testing.T) {
		got := execEnv([]string{"CLAUDE_CONFIG_DIR=/old", "PATH=/bin"}, "/new")
		if n := countPrefix(got, "CLAUDE_CONFIG_DIR="); n != 1 {
			t.Fatalf("CLAUDE_CONFIG_DIR count = %d, want exactly 1 (no duplicate)", n)
		}
		if contains(got, "CLAUDE_CONFIG_DIR=/old") {
			t.Errorf("stale CLAUDE_CONFIG_DIR=/old survived: %v", got)
		}
		if !contains(got, "CLAUDE_CONFIG_DIR=/new") {
			t.Errorf("CLAUDE_CONFIG_DIR=/new missing: %v", got)
		}
	})

	t.Run("preserves a user-set plugin root untouched", func(t *testing.T) {
		got := execEnv([]string{"CLAUDE_CODE_PLUGIN_CACHE_DIR=/custom/plugins", "PATH=/bin"}, "/cfg")
		if n := countPrefix(got, "CLAUDE_CODE_PLUGIN_CACHE_DIR="); n != 1 {
			t.Fatalf("CLAUDE_CODE_PLUGIN_CACHE_DIR count = %d, want exactly 1 (no override append)", n)
		}
		if !contains(got, "CLAUDE_CODE_PLUGIN_CACHE_DIR=/custom/plugins") {
			t.Errorf("user-set plugin root was overridden: %v", got)
		}
	})

	t.Run("preserves a user-set debug dir untouched", func(t *testing.T) {
		got := execEnv([]string{"CLAUDE_CODE_DEBUG_LOGS_DIR=/custom/debug", "PATH=/bin"}, "/cfg")
		if n := countPrefix(got, "CLAUDE_CODE_DEBUG_LOGS_DIR="); n != 1 {
			t.Fatalf("CLAUDE_CODE_DEBUG_LOGS_DIR count = %d, want exactly 1 (no override append)", n)
		}
		if !contains(got, "CLAUDE_CODE_DEBUG_LOGS_DIR=/custom/debug") {
			t.Errorf("user-set debug dir was overridden: %v", got)
		}
	})
}

func TestCcpAccountFromEnv(t *testing.T) {
	t.Run("unset yields no override", func(t *testing.T) {
		t.Setenv(ccpAccountEnv, "")
		got, err := ccpAccountFromEnv()
		if err != nil || got != nil {
			t.Fatalf("ccpAccountFromEnv() = %v, %v; want nil, nil", got, err)
		}
	})

	t.Run("non-integer is rejected", func(t *testing.T) {
		t.Setenv(ccpAccountEnv, "not-a-number")
		_, err := ccpAccountFromEnv()
		if err == nil || !strings.Contains(err.Error(), "must be an account id") {
			t.Fatalf("err = %v, want an 'account id' parse error", err)
		}
	})

	t.Run("valid id parses", func(t *testing.T) {
		t.Setenv(ccpAccountEnv, "5")
		got, err := ccpAccountFromEnv()
		if err != nil || got == nil || *got != 5 {
			t.Fatalf("ccpAccountFromEnv() = %v, %v; want &5, nil", got, err)
		}
	})
}

// TestResolveSelectionForcedUnknown covers the missing forced-account path.
func TestResolveSelectionForcedUnknown(t *testing.T) {
	m := &pool.Manager{Store: openTestStore(t)}
	cmd := &cobra.Command{}
	id := 999
	_, _, _, err := resolveSelection(cmd, m, selectReq{account: &id})
	if err == nil || !strings.Contains(err.Error(), "999") {
		t.Fatalf("err = %v, want a not-found error mentioning account 999", err)
	}
}

func TestRunClaudeRejectsMalformedDaemonSelectionBeforeConsequences(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "ccp-run-home")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	t.Setenv(ccpAccountEnv, "1")
	if err := os.WriteFile(filepath.Join(home, ".claude.json"), []byte(`{"mergeMarker":"yes"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	st := openTestStore(t)
	requested := store.Account{
		ID: 1, ConfigDir: filepath.Join(home, "acct-01"), Label: "requested@example.com",
		KeychainService: "svc-1", KeychainAccount: "u-1",
	}
	returned := store.Account{
		ID: 2, ConfigDir: filepath.Join(home, "acct-02"), Label: "returned@example.com",
		KeychainService: "svc-2", KeychainAccount: "u-2",
	}
	requested = admitCLITestAccount(t, st, requested)
	returned = admitCLITestAccount(t, st, returned)
	if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	aborted := make(chan struct{}, 1)
	startDaemonTestServer(t, "", func(_ context.Context, op daemon.Op, _ daemon.Request) daemon.Response {
		resp := daemon.Response{OK: true}
		if op == daemon.OpSelect {
			resp.SelectedID = &returned.ID
			resp.Prepared = true
			resp.Dir = returned.ConfigDir
			resp.ReservationToken = "malformed-selection"
		}
		if op == daemon.OpSelectAbort {
			aborted <- struct{}{}
		}
		return resp
	})

	m := &pool.Manager{Store: st, Creds: credstest.NewFake()}
	var stderr bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetErr(&stderr)
	cmd.SetContext(context.Background())
	execCalls := 0
	prevExec := runExecClaude
	runExecClaude = func(string, []string) error {
		execCalls++
		return nil
	}
	t.Cleanup(func() { runExecClaude = prevExec })
	err = runClaude(cmd, m, []string{"--version"})
	if err == nil {
		t.Fatal("runClaude accepted a daemon response for the wrong forced account")
	}
	for _, want := range []string{
		"id 2",
		"returned dir \"" + returned.ConfigDir + "\"",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if out := stripANSI(stderr.String()); out != "" {
		t.Errorf("malformed response printed a selection banner: %q", out)
	}
	if execCalls != 0 {
		t.Errorf("malformed response reached launch consequences: exec=%d", execCalls)
	}
	select {
	case <-aborted:
	case <-time.After(time.Second):
		t.Fatal("malformed daemon selection token was not aborted")
	}
	for _, acct := range []store.Account{requested, returned} {
		if _, err := os.Stat(filepath.Join(acct.ConfigDir, ".claude.json")); !os.IsNotExist(err) {
			t.Errorf("settings merged into acct-%02d after malformed response: %v", acct.ID, err)
		}
		if n, err := st.ActiveSessionCount(acct.ID); err != nil || n != 0 {
			t.Errorf("client session count for acct-%02d = %d, %v; want 0, nil", acct.ID, n, err)
		}
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := st.GetSticky(cwd); err != nil || ok {
		t.Errorf("client sticky after malformed response: ok=%v err=%v", ok, err)
	}
}

func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func countPrefix(env []string, prefix string) int {
	n := 0
	for _, e := range env {
		if strings.HasPrefix(e, prefix) {
			n++
		}
	}
	return n
}

func contains(env []string, want string) bool {
	for _, e := range env {
		if e == want {
			return true
		}
	}
	return false
}
