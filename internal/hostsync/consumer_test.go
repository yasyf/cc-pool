package hostsync

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/synckit/converge"
	"github.com/yasyf/synckit/cregistry"
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

func TestCapabilitiesAreExactSynckitContract(t *testing.T) {
	s, _ := newTestService(t)
	c := NewConsumer(s, enabled(true))

	caps, err := c.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if caps.Name != "cc-pool" {
		t.Errorf("Name = %q, want cc-pool", caps.Name)
	}
	for _, m := range []string{
		syncservice.MethodCapabilities,
		syncservice.MethodList,
		syncservice.MethodReconcile,
		syncservice.MethodExport,
		syncservice.MethodApply,
	} {
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
		if _, err := c.Export(ctx, syncservice.ExportRequest{}); !errors.Is(err, ErrSyncDisabled) {
			t.Errorf("Export err = %v, want ErrSyncDisabled", err)
		}
		if _, err := c.Apply(ctx, syncservice.ChangeEnvelope{}); !errors.Is(err, ErrSyncDisabled) {
			t.Errorf("Apply err = %v, want ErrSyncDisabled", err)
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

// TestExportCarriesAccessOnlyCredential pins the snapshot boundary: Synckit
// carries the access token needed for Apply, but never its refresh token.
func TestExportCarriesAccessOnlyCredential(t *testing.T) {
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
	stripped := secret.Strip()
	blob, err := stripped.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	s.CredentialSnapshot = func(context.Context, Registry) (map[string]CredentialEnvelope, error) {
		return map[string]CredentialEnvelope{uuid: {
			Credential: blob, ExpiresAt: stripped.ClaudeAiOauth.ExpiresAt, Hash: creds.AccessHash(stripped),
		}}, nil
	}

	change, err := NewConsumer(s, enabled(true)).Export(ctx, syncservice.ExportRequest{
		ServiceID: SyncServiceID, SchemaFingerprint: SyncSchemaFingerprint,
		SinceRevision: syncservice.NewRevision(0),
	})
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if change.Kind != syncservice.ChangeSnapshot || change.SourceRevision == syncservice.NewRevision(0) {
		t.Fatalf("Export change = %+v", change)
	}
	got := string(change.Payload)
	if !strings.Contains(got, "ACCESS-TOKEN-SEKRIT-11111") {
		t.Fatalf("Export omitted delivery access token: %s", got)
	}
	for _, secretPart := range []string{"SEKRIT-22222", "REFRESH-TOKEN"} {
		if strings.Contains(got, secretPart) {
			t.Fatalf("Export leaked a token substring %q into the wire state: %s", secretPart, got)
		}
	}
	// Sanity: the state is non-empty and carries the chain hash, proving the cred
	// was actually processed (so the absence above is not a vacuous pass).
	if !strings.Contains(got, chain.Hash) {
		t.Fatalf("Export missing the chain hash %q; state = %s", chain.Hash, got)
	}
}

// TestExportEmptyRegistry pins the immutable initial revision and payload.
func TestExportEmptyRegistry(t *testing.T) {
	s, _ := newTestService(t)
	change, err := NewConsumer(s, enabled(true)).Export(context.Background(), syncservice.ExportRequest{
		ServiceID: SyncServiceID, SchemaFingerprint: SyncSchemaFingerprint,
		SinceRevision: syncservice.NewRevision(0),
	})
	if err != nil {
		t.Fatalf("Export on empty registry: %v", err)
	}
	if change.SourceRevision != syncservice.NewRevision(1) || string(change.Payload) != `{"registry":{},"credentials":{}}` {
		t.Fatalf("empty Export = revision %q payload %q", change.SourceRevision, change.Payload)
	}
}

func TestExportRejectsAcknowledgementAheadOfProductRevision(t *testing.T) {
	s, _ := newTestService(t)
	_, err := NewConsumer(s, enabled(true)).Export(context.Background(), syncservice.ExportRequest{
		ServiceID: SyncServiceID, SchemaFingerprint: SyncSchemaFingerprint,
		SinceRevision: syncservice.NewRevision(2),
	})
	if err == nil {
		t.Fatal("Export accepted an acknowledgement ahead of product revision 1")
	}
}

type consumerApplyDriver struct {
	service *Service
	ids     []string
	origins []string
}

type consumerApplyMesh struct{ peers []string }

func (m consumerApplyMesh) Resolve(context.Context) (string, []string, error) {
	return "self", m.peers, nil
}

func (d *consumerApplyDriver) LoadRegistry(context.Context) (cregistry.Registry[AccountValue], error) {
	return d.service.Registry.Load()
}

func (d *consumerApplyDriver) SaveRegistry(_ context.Context, reg cregistry.Registry[AccountValue]) error {
	return d.service.Registry.Save(reg)
}

func (d *consumerApplyDriver) Reconcile(
	_ context.Context,
	id string,
	_ cregistry.Entry[AccountValue],
	_ []string,
	origin string,
) (converge.Outcome, error) {
	d.ids = append(d.ids, id)
	d.origins = append(d.origins, origin)
	return OutcomeUnchanged, nil
}

func TestApplyMergesOnceAndAcknowledgesSourceRevision(t *testing.T) {
	s, _ := newTestService(t)
	s.Mesh = consumerApplyMesh{peers: []string{"hostB"}}
	driver := &consumerApplyDriver{service: s}
	s.Driver = driver
	incoming := cregistry.New[AccountValue]()
	incoming.Add("u-remote", AccountValue{
		UUID: "u-remote", Email: "remote@example.com",
		OAuthAccount: json.RawMessage(`{"accountUuid":"u-remote"}`),
	}, 10)
	payload, err := encodeSyncSnapshot(syncSnapshot{
		Registry: incoming, Credentials: map[string]CredentialEnvelope{},
	})
	if err != nil {
		t.Fatal(err)
	}
	change, err := syncservice.NewExportedChange(
		SyncServiceID, SyncSchemaFingerprint, syncservice.ChangeSnapshot,
		syncservice.NewRevision(0), syncservice.NewRevision(7), payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	change, err = syncservice.BindDelivery(change, "hostA")
	if err != nil {
		t.Fatal(err)
	}
	consumer := NewConsumer(s, enabled(true))
	for attempt := 0; attempt < 2; attempt++ {
		ack, applyErr := consumer.Apply(context.Background(), change)
		if applyErr != nil {
			t.Fatalf("Apply attempt %d: %v", attempt, applyErr)
		}
		if ack.NeedSnapshot || ack.AckedRevision != change.SourceRevision {
			t.Fatalf("Apply attempt %d ack = %+v", attempt, ack)
		}
	}
	state, err := s.Registry.LoadState()
	if err != nil {
		t.Fatal(err)
	}
	if state.Revision != 2 || !state.Snapshot["u-remote"].Present() {
		t.Fatalf("applied state = %+v", state)
	}
	if len(driver.origins) != 2 || driver.origins[0] != "hostA" || driver.origins[1] != "hostA" {
		t.Fatalf("reconcile origins = %v", driver.origins)
	}
}

func TestApplyDeltaRequestsSnapshotWithoutMutation(t *testing.T) {
	s, _ := newTestService(t)
	payload := []byte(`{}`)
	change, err := syncservice.NewExportedChange(
		SyncServiceID, SyncSchemaFingerprint, syncservice.ChangeDelta,
		syncservice.NewRevision(1), syncservice.NewRevision(2), payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	change, err = syncservice.BindDelivery(change, "hostA")
	if err != nil {
		t.Fatal(err)
	}
	result, err := NewConsumer(s, enabled(true)).Apply(context.Background(), change)
	if err != nil {
		t.Fatal(err)
	}
	if !result.NeedSnapshot || result.AckedRevision != "" {
		t.Fatalf("delta Apply = %+v, want snapshot request", result)
	}
	state, err := s.Registry.LoadState()
	if err != nil {
		t.Fatal(err)
	}
	if state.Revision != 1 {
		t.Fatalf("delta Apply changed local revision to %d", state.Revision)
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
