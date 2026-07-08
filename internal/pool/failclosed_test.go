package pool

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
)

// newFailClosedManager builds a Manager over one idle, near-expiry account so any
// idle-refresh path spends a token — letting a test assert a failed scan spends none.
func newFailClosedManager(t *testing.T) (*Manager, store.Account, *fakeOAuth, *credstest.Fake) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "svc", KeychainAccount: "user"}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	fk := credstest.NewFake()
	seed := &creds.Credential{}
	seed.ClaudeAiOauth.AccessToken = "at-0"
	seed.ClaudeAiOauth.RefreshToken = "rt-0"
	seed.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Minute).UnixMilli()
	fk.Put(a.KeychainService, a.KeychainAccount, seed)
	fo := &fakeOAuth{currentRT: "rt-0"}
	return &Manager{Store: st, OAuth: fo, Creds: fk, LockDir: t.TempDir()}, a, fo, fk
}

func refreshCount(fo *fakeOAuth) int {
	fo.mu.Lock()
	defer fo.mu.Unlock()
	return fo.refreshes
}

// TestSampleStaleFailsClosedOnScanError pins that a failed scan (scanOK=false)
// samples usage but refreshes no token; a clean scan still refreshes.
func TestSampleStaleFailsClosedOnScanError(t *testing.T) {
	t.Run("failed scan samples usage but refreshes nothing", func(t *testing.T) {
		m, a, fo, fk := newFailClosedManager(t)
		before := fk.WriteCount()

		m.sampleStale(context.Background(), []store.Account{a}, nil, false, DefaultFreshFor)

		if got := refreshCount(fo); got != 0 {
			t.Fatalf("failed scan refreshed %d time(s), want 0", got)
		}
		if got := fk.WriteCount(); got != before {
			t.Fatalf("failed scan wrote the credential %d time(s), want 0", got-before)
		}
		if _, ok, err := m.Store.LatestUsageSample(a.ID); err != nil {
			t.Fatalf("LatestUsageSample: %v", err)
		} else if !ok {
			t.Fatal("failed scan must still sample usage (a safe read)")
		}
	})

	t.Run("clean scan refreshes an idle near-expiry account", func(t *testing.T) {
		m, a, fo, _ := newFailClosedManager(t)

		m.sampleStale(context.Background(), []store.Account{a}, nil, true, DefaultFreshFor)

		if got := refreshCount(fo); got != 1 {
			t.Fatalf("clean scan refreshed %d time(s), want 1", got)
		}
	})
}

// TestPreflightRefreshFailsClosedOnScanError pins that PreflightRefresh skips the
// refresh on a failed scan or a live session, and refreshes only on a clean idle scan.
func TestPreflightRefreshFailsClosedOnScanError(t *testing.T) {
	orig := scanSessions
	t.Cleanup(func() { scanSessions = orig })

	t.Run("scan error skips refresh", func(t *testing.T) {
		m, a, fo, _ := newFailClosedManager(t)
		scanSessions = func(context.Context) ([]procscan.Session, error) {
			return nil, errors.New("procscan: simulated EIO")
		}
		if err := m.PreflightRefresh(context.Background(), a); err != nil {
			t.Fatalf("PreflightRefresh must swallow a scan error, got %v", err)
		}
		if got := refreshCount(fo); got != 0 {
			t.Fatalf("a scan error refreshed %d time(s), want 0", got)
		}
	})

	t.Run("clean scan refreshes an idle near-expiry account", func(t *testing.T) {
		m, a, fo, _ := newFailClosedManager(t)
		scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil }
		if err := m.PreflightRefresh(context.Background(), a); err != nil {
			t.Fatalf("PreflightRefresh: %v", err)
		}
		if got := refreshCount(fo); got != 1 {
			t.Fatalf("a clean idle scan refreshed %d time(s), want 1", got)
		}
	})

	t.Run("live session skips refresh", func(t *testing.T) {
		m, a, fo, _ := newFailClosedManager(t)
		scanSessions = func(context.Context) ([]procscan.Session, error) {
			return []procscan.Session{{PID: 1, ConfigDir: a.ConfigDir, StartedAt: time.Now()}}, nil
		}
		if err := m.PreflightRefresh(context.Background(), a); err != nil {
			t.Fatalf("PreflightRefresh: %v", err)
		}
		if got := refreshCount(fo); got != 0 {
			t.Fatalf("a busy account refreshed %d time(s), want 0", got)
		}
	})
}
