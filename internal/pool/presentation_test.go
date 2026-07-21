package pool

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
)

func TestPresentationPathIsExactClaudeAndKeychainIdentity(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	presentation := AccountPresentationDir(18)
	if presentation != AccountDir(18) {
		t.Fatalf("presentation = %q, CLAUDE_CONFIG_DIR = %q", presentation, AccountDir(18))
	}
	if creds.ServiceName(presentation) != creds.ServiceName(AccountDir(18)) {
		t.Fatal("presentation and exported CLAUDE_CONFIG_DIR produced different Keychain identities")
	}
	if creds.ServiceName(presentation) == creds.ServiceName(AccountBackingDir(18)) {
		t.Fatal("private backing unexpectedly produced the presentation Keychain identity")
	}
}

func TestScoreInputCountsOnlyPresentationIdentity(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	m := &Manager{Store: st}
	account := store.Account{ID: 7, ConfigDir: "/private/backing/acct-07"}
	sessions := []procscan.Session{
		{PID: 1, ConfigDir: AccountPresentationDir(7)},
		{PID: 2, ConfigDir: account.ConfigDir},
	}
	input, _, _, _, err := m.scoreInput(t.Context(), account, sessions, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if input.ActiveSessions != 1 {
		t.Fatalf("active sessions = %d, want only presentation-path session", input.ActiveSessions)
	}
}

func TestPreflightRefreshSkipsPresentationSession(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	account := store.Account{ID: 7, ConfigDir: "/private/backing/acct-07"}
	m := &Manager{ScanSessions: func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 1, ConfigDir: AccountPresentationDir(account.ID)}}, nil
	}}
	if err := m.PreflightRefresh(t.Context(), account); err != nil {
		t.Fatal(err)
	}
}
