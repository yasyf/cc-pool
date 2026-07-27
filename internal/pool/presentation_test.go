package pool

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/testhome"
)

func TestStableConfigDirIsExactClaudeAndKeychainIdentity(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	account := store.Account{ID: 18, ConfigDir: testAccountConfigDir(18)}
	if creds.ServiceName(account.ConfigDir) == creds.ServiceName(AccountBackingDir(account.ID)) {
		t.Fatal("private backing unexpectedly produced the presentation Keychain identity")
	}
}

func TestScoreInputCountsOnlyStableExecutionIdentity(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	m := &Manager{Store: st}
	account := store.Account{ID: 7, ConfigDir: testAccountConfigDir(7)}
	sessions := []procscan.Session{
		{PID: 1, ConfigDir: testAccountConfigDir(8)},
		{PID: 2, ConfigDir: account.ConfigDir},
	}
	input, _, _, _, err := m.scoreInput(t.Context(), account, sessions, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if input.ActiveSessions != 1 {
		t.Fatalf("active sessions = %d, want only persisted ConfigDir session", input.ActiveSessions)
	}
}

func TestPreflightRefreshSkipsStableExecutionSession(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	account := store.Account{ID: 7, ConfigDir: testAccountConfigDir(7)}
	m := &Manager{ScanSessions: func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 1, ConfigDir: account.ConfigDir}}, nil
	}}
	if err := m.PreflightRefresh(t.Context(), account); err != nil {
		t.Fatal(err)
	}
}
