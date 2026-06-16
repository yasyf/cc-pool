package cli

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
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

// TestCcpAccountFromEnv pins the CCP_ACCOUNT override parsing: unset yields no
// override, a non-integer is rejected, and a valid id parses through.
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

// TestResolveSelectionForcedUnknown covers the forced-account error path shared
// by `ccp run` and `ccp select`, which returns before any overlay/Keychain
// access. The valid-id path is exercised by manual verification (it reaches
// SyncOverlay/PreflightRefresh, which need real state and must not be touched
// here).
func TestResolveSelectionForcedUnknown(t *testing.T) {
	m := &pool.Manager{Store: openTestStore(t)}
	cmd := &cobra.Command{}
	id := 999
	_, _, err := resolveSelection(cmd, m, selectReq{account: &id})
	if err == nil || !strings.Contains(err.Error(), "999") {
		t.Fatalf("err = %v, want a not-found error mentioning account 999", err)
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
