package hostsync

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/synckit/syncservice"
)

// fakeSessions is an injectable Sessions seam: busy[uuid] reports liveness and
// err forces the fail-path. Shared by the consumer List and the converge teardown
// tests.
type fakeSessions struct {
	busy   map[string]bool
	reason string
	err    error
}

func (s fakeSessions) Busy(_ context.Context, uuid string) (bool, string, error) {
	if s.err != nil {
		return false, "", s.err
	}
	return s.busy[uuid], s.reason, nil
}

func enabled(on bool) func() (bool, error) { return func() (bool, error) { return on, nil } }

func containsStr(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

func TestCapabilitiesIncludeFetch(t *testing.T) {
	s, _ := newTestService(t)
	c := NewConsumer(s, enabled(true))

	caps, err := c.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if caps.Name != "cc-pool" {
		t.Errorf("Name = %q, want cc-pool", caps.Name)
	}
	if caps.ProtocolVersion != syncservice.ProtocolVersion {
		t.Errorf("ProtocolVersion = %d, want %d", caps.ProtocolVersion, syncservice.ProtocolVersion)
	}
	if !containsStr(caps.Methods, MethodFetchCredential) {
		t.Errorf("Methods %v missing the custom %s", caps.Methods, MethodFetchCredential)
	}
	// The five contract methods must still be advertised alongside the custom one.
	for _, m := range []string{syncservice.MethodCapabilities, syncservice.MethodList, syncservice.MethodReconcile, syncservice.MethodSync, syncservice.MethodGetState} {
		if !containsStr(caps.Methods, m) {
			t.Errorf("Methods %v missing contract method %s", caps.Methods, m)
		}
	}
}

func TestDisabledFailsLoud(t *testing.T) {
	ctx := context.Background()

	t.Run("disabled returns ErrSyncDisabled", func(t *testing.T) {
		s, _ := newTestService(t)
		c := NewConsumer(s, enabled(false))
		if _, err := c.Capabilities(ctx); !errors.Is(err, ErrSyncDisabled) {
			t.Errorf("Capabilities err = %v, want ErrSyncDisabled", err)
		}
		if _, err := c.List(ctx); !errors.Is(err, ErrSyncDisabled) {
			t.Errorf("List err = %v, want ErrSyncDisabled", err)
		}
		if _, err := c.Reconcile(ctx, ""); !errors.Is(err, ErrSyncDisabled) {
			t.Errorf("Reconcile err = %v, want ErrSyncDisabled", err)
		}
		if _, err := c.Sync(ctx, ""); !errors.Is(err, ErrSyncDisabled) {
			t.Errorf("Sync err = %v, want ErrSyncDisabled", err)
		}
		if _, err := c.GetState(ctx); !errors.Is(err, ErrSyncDisabled) {
			t.Errorf("GetState err = %v, want ErrSyncDisabled", err)
		}
	})

	t.Run("Enabled error propagates and is not mistaken for disabled", func(t *testing.T) {
		s, _ := newTestService(t)
		sentinel := errors.New("meta read boom")
		c := NewConsumer(s, func() (bool, error) { return false, sentinel })
		_, err := c.Capabilities(ctx)
		if !errors.Is(err, sentinel) {
			t.Errorf("Capabilities err = %v, want the wrapped sentinel", err)
		}
		if errors.Is(err, ErrSyncDisabled) {
			t.Errorf("an Enabled error must not surface as ErrSyncDisabled: %v", err)
		}
	})
}

// TestGetStateSecretless writes a real credential through the registry's cred-write
// endpoint (NoteCredWrite) and pins that the bytes GetState ships to a peer carry
// the chain hash but NEVER the access or refresh token.
func TestGetStateSecretless(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestService(t)

	const uuid = "u-secret"
	if err := s.PublishAccount(ctx, acctVal(uuid, "who@example.com", "lbl", "hostA", 1000)); err != nil {
		t.Fatalf("PublishAccount: %v", err)
	}
	secret := cred("ACCESS-TOKEN-SEKRIT-11111", "REFRESH-TOKEN-SEKRIT-22222")
	secret.ClaudeAiOauth.ExpiresAt = 9_000_000_000_000
	chain := ChainStamp{Origin: "hostA", ExpiresAt: secret.ClaudeAiOauth.ExpiresAt, Hash: creds.AccessHash(secret), RotatedAt: 42}
	if err := s.NoteCredWrite(ctx, uuid, chain); err != nil {
		t.Fatalf("NoteCredWrite: %v", err)
	}

	raw, err := NewConsumer(s, enabled(true)).GetState(ctx)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	got := string(raw)
	for _, secretPart := range []string{"SEKRIT-11111", "SEKRIT-22222", "ACCESS-TOKEN", "REFRESH-TOKEN"} {
		if strings.Contains(got, secretPart) {
			t.Fatalf("GetState leaked a token substring %q into the wire state: %s", secretPart, got)
		}
	}
	// Sanity: the state is non-empty and carries the chain hash, proving the cred
	// was actually processed (so the absence above is not a vacuous pass).
	if !strings.Contains(got, chain.Hash) {
		t.Fatalf("GetState missing the chain hash %q; state = %s", chain.Hash, got)
	}
}

// TestGetStateEmptyRegistry pins that a not-yet-created registry answers with an
// empty registry JSON rather than an error.
func TestGetStateEmptyRegistry(t *testing.T) {
	s, _ := newTestService(t)
	raw, err := NewConsumer(s, enabled(true)).GetState(context.Background())
	if err != nil {
		t.Fatalf("GetState on empty registry: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("GetState returned empty bytes; want a marshaled empty registry")
	}
}

// TestListReportsRegistryAccounts pins List's per-account shape: sorted by uuid,
// each with the stamp dir, the entry fingerprint, and busy from the Sessions seam.
func TestListReportsRegistryAccounts(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestService(t)
	s.Sessions = fakeSessions{busy: map[string]bool{"u2": true}, reason: "live session"}

	if err := s.PublishAccount(ctx, acctVal("u1", "a@x", "l1", "hostA", 1000)); err != nil {
		t.Fatalf("PublishAccount u1: %v", err)
	}
	if err := s.PublishAccount(ctx, acctVal("u2", "b@x", "l2", "hostB", 2000)); err != nil {
		t.Fatalf("PublishAccount u2: %v", err)
	}

	items, err := NewConsumer(s, enabled(true)).List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("List returned %d items, want 2", len(items))
	}
	if items[0].ID != "u1" || items[1].ID != "u2" {
		t.Fatalf("List not sorted by uuid: %q, %q", items[0].ID, items[1].ID)
	}
	for _, it := range items {
		wantDir := filepath.Join(s.StampDir, it.ID)
		if len(it.WatchDirs) != 1 || it.WatchDirs[0] != wantDir {
			t.Errorf("%s WatchDirs = %v, want [%s]", it.ID, it.WatchDirs, wantDir)
		}
		e, ok := loadEntry(t, s, it.ID)
		if !ok {
			t.Fatalf("%s missing from registry", it.ID)
		}
		if it.Fingerprint != Fingerprint(e) {
			t.Errorf("%s Fingerprint = %q, want %q", it.ID, it.Fingerprint, Fingerprint(e))
		}
	}
	if items[0].Busy {
		t.Errorf("u1 Busy = true, want false")
	}
	if !items[1].Busy || items[1].BusyReason != "live session" {
		t.Errorf("u2 Busy/reason = %v/%q, want true/live session", items[1].Busy, items[1].BusyReason)
	}
}
