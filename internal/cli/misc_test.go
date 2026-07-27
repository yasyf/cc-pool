package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/testhome"
)

func TestEnvExportsFuseKitPresentationWithoutSessionOwnership(t *testing.T) {
	home := t.TempDir()
	testhome.Sandbox(t, home)

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
	account := admitCLITestAccountAtPublicPath(t, st, store.Account{
		ID: 1, Label: "work@example.com",
		KeychainService: "svc", KeychainAccount: "u",
	}, dir)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	cmd := newEnvCmd()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--account", "1"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("env failed: %v (stderr=%q)", err, stripANSI(stderr.String()))
	}
	if !strings.Contains(stdout.String(), "export CLAUDE_CONFIG_DIR='"+account.ConfigDir+"'") {
		t.Fatalf("env exports missing the config dir: %q", stdout.String())
	}
	st, err = store.Open(pool.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	if sessions, err := st.ListActiveSessions(); err != nil || len(sessions) != 0 {
		t.Fatalf("env active sessions = %+v, err=%v; env must not invent process ownership", sessions, err)
	}
}
