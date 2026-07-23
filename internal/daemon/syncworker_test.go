package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
)

func TestHostSyncWorkerSessionsUsesDurableAndObservedActivity(t *testing.T) {
	s, _ := newWireServer(t)
	account := store.Account{
		ID: 1, ConfigDir: "/File Provider/CCPool/acct-01",
		KeychainService: "svc-worker-sessions", KeychainAccount: "cc-pool",
		AccountUUID: "u-worker-sessions",
	}
	account = admitDaemonTestAccount(t, s.m.Store, account)
	s.m.ScanSessions = s.scanSessions
	sessions := hostSyncWorkerSessions{manager: s.m}

	busy, reason, err := sessions.Busy(t.Context(), "missing")
	if err != nil || busy || reason != "" {
		t.Fatalf("unknown UUID = (%v, %q, %v), want idle", busy, reason, err)
	}
	busy, reason, err = sessions.Busy(t.Context(), account.AccountUUID)
	if err != nil || busy || reason != "" {
		t.Fatalf("idle account = (%v, %q, %v), want idle", busy, reason, err)
	}

	s.m.ScanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{
			PID: 4242, ConfigDir: account.ConfigDir, StartedAt: time.Now(),
		}}, nil
	}
	busy, reason, err = sessions.Busy(t.Context(), account.AccountUUID)
	if err != nil || !busy || !strings.Contains(reason, "live process session") {
		t.Fatalf("observed live process = (%v, %q, %v), want busy", busy, reason, err)
	}

	scanErr := errors.New("scan failed")
	s.m.ScanSessions = func(context.Context) ([]procscan.Session, error) { return nil, scanErr }
	if _, _, err := sessions.Busy(t.Context(), account.AccountUUID); !errors.Is(err, scanErr) {
		t.Fatalf("scan failure = %v, want %v", err, scanErr)
	}

	s.m.ScanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil }
	activateDaemonTestSession(t, s, account.ID, 4243, "/project", time.Now())
	busy, reason, err = sessions.Busy(t.Context(), account.AccountUUID)
	if err != nil || !busy || !strings.Contains(reason, "active session") {
		t.Fatalf("durable active session = (%v, %q, %v), want busy", busy, reason, err)
	}
}

func TestClassifyAuthKindOwnerRequiresExactMeshMember(t *testing.T) {
	tests := map[string]struct {
		origin  string
		want    store.AuthKind
		wantErr error
	}{
		"self":           {origin: "host-self", want: store.AuthKindOwned},
		"known peer":     {origin: "peer-b", want: store.AuthKindAwaitingOrigin},
		"missing origin": {wantErr: hostsync.ErrAuthKindOriginMissing},
		"foreign origin": {origin: "intruder", wantErr: hostsync.ErrAuthKindOriginForeign},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := classifyAuthKindOwner(test.origin, "host-self", []string{"peer-b"})
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("classify origin %q = (%q, %v), want %v", test.origin, got, err, test.wantErr)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("classify origin %q = (%q, %v), want %q", test.origin, got, err, test.want)
			}
		})
	}
}
