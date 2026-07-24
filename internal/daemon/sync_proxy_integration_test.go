package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/converge"
	"github.com/yasyf/synckit/cregistry"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

type proxiedSyncTransport struct{ client *rpc.Client }

func (t *proxiedSyncTransport) Do(
	ctx context.Context,
	request *rpc.Request,
) (*syncservice.Response, error) {
	response, err := t.client.Call(ctx, request)
	if err != nil {
		return nil, err
	}
	return &syncservice.Response{OK: response.OK, Result: response.Result, Error: response.Error}, nil
}

func (t *proxiedSyncTransport) Close() error { return t.client.Close() }

func proxiedSyncClient(t *testing.T, socket string) *syncservice.Client {
	t.Helper()
	clientSide, proxySide := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rpc.Proxy(ctx, proxySide, proxySide, socket) }()
	var mu sync.Mutex
	used := false
	client := syncservice.NewClient(&proxiedSyncTransport{client: rpc.NewClient(rpc.ClientConfig{
		WireBuild: rpc.WireBuild,
		Dial: func(context.Context) (net.Conn, error) {
			mu.Lock()
			defer mu.Unlock()
			if used {
				return nil, errors.New("test proxy connection already consumed")
			}
			used = true
			return clientSide, nil
		},
	})})
	t.Cleanup(func() {
		_ = client.Close()
		cancel()
		_ = proxySide.Close()
		<-done
	})
	return client
}

type proxyApplyMesh struct{}

func (proxyApplyMesh) Resolve(context.Context) (string, []string, error) {
	return "sim@destination", nil, nil
}

type proxyApplyDriver struct {
	registry *hostsync.RegistryFile
	wantID   string
	wantHash string
	seen     bool
}

func (d *proxyApplyDriver) LoadRegistry(context.Context) (hostsync.Registry, error) {
	return d.registry.Load()
}

func (d *proxyApplyDriver) SaveRegistry(_ context.Context, registry hostsync.Registry) error {
	return d.registry.Save(registry)
}

func (d *proxyApplyDriver) Reconcile(
	ctx context.Context,
	id string,
	entry cregistry.Entry[hostsync.AccountValue],
	_ []string,
	_ string,
) (converge.Outcome, error) {
	credential, err := hostsync.ResolveAppliedCredential(ctx, id, entry.Value.Chain)
	if err != nil {
		return hostsync.OutcomeDeferred, err
	}
	if id != d.wantID || credential.HasRefreshToken() || creds.AccessHash(credential) != d.wantHash {
		return hostsync.OutcomeDeferred, errors.New("proxy delivery credential mismatch")
	}
	d.seen = true
	return hostsync.OutcomeCredInstalled, nil
}

func TestSyncProxyCarriesAccessOnlyExportAndApplyAcknowledges(t *testing.T) {
	const (
		uuid         = "u-proxy"
		origin       = "sim@source"
		accessToken  = "ACCESS-PROXY-DELIVERY"
		refreshToken = "REFRESH-MUST-STAY-LOCAL"
	)
	credential := &creds.Credential{}
	credential.ClaudeAiOauth.AccessToken = accessToken
	credential.ClaudeAiOauth.RefreshToken = refreshToken
	credential.ClaudeAiOauth.ExpiresAt = 9_000_000_000_000
	chain := hostsync.ChainStamp{
		Origin: origin, ExpiresAt: credential.ClaudeAiOauth.ExpiresAt,
		Hash: creds.AccessHash(credential), RotatedAt: 42,
	}

	sourceRegistry := hostsync.NewRegistryFile(t.TempDir())
	source := &hostsync.Service{Registry: sourceRegistry, StampDir: t.TempDir()}
	if err := source.PublishAccount(t.Context(), hostsync.AccountValue{
		UUID: uuid, Email: "proxy@example.test", Label: "proxy",
		OAuthAccount: json.RawMessage(`{"accountUuid":"u-proxy"}`), Chain: chain,
	}); err != nil {
		t.Fatal(err)
	}
	source.CredentialSnapshot = func(ctx context.Context, registry hostsync.Registry) (map[string]hostsync.CredentialEnvelope, error) {
		return hostsync.BuildCredentialSnapshot(
			ctx, registry, origin,
			func(string) (store.Account, bool, error) {
				return store.Account{ID: 1, AccountUUID: uuid}, true, nil
			},
			func(context.Context, store.Account) (*creds.Credential, error) { return credential, nil },
		)
	}
	sourceSocket, cancelSource, sourceDone := startTestSyncHelper(
		t, hostsync.NewConsumer(source, func() (bool, error) { return true, nil }),
	)
	t.Cleanup(func() { cancelSource(); <-sourceDone })

	destinationRegistry := hostsync.NewRegistryFile(t.TempDir())
	driver := &proxyApplyDriver{registry: destinationRegistry, wantID: uuid, wantHash: chain.Hash}
	destination := &hostsync.Service{
		Registry: destinationRegistry, StampDir: t.TempDir(), Mesh: proxyApplyMesh{}, Driver: driver,
	}
	destinationSocket, cancelDestination, destinationDone := startTestSyncHelper(
		t, hostsync.NewConsumer(destination, func() (bool, error) { return true, nil }),
	)
	t.Cleanup(func() { cancelDestination(); <-destinationDone })

	sourceClient := proxiedSyncClient(t, sourceSocket)
	change, err := sourceClient.Export(t.Context(), syncservice.ExportRequest{
		ServiceID: hostsync.SyncServiceID, SchemaFingerprint: hostsync.SyncSchemaFingerprint,
		SinceRevision: syncservice.NewRevision(0),
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := string(change.Payload)
	if !strings.Contains(payload, accessToken) || !strings.Contains(payload, chain.Hash) {
		t.Fatalf("captured Export omitted access identity: %s", payload)
	}
	if strings.Contains(payload, "refreshToken") || strings.Contains(payload, refreshToken) {
		t.Fatalf("captured Export leaked refresh material: %s", payload)
	}
	delivery, err := syncservice.BindDelivery(change, origin)
	if err != nil {
		t.Fatal(err)
	}
	destinationClient := proxiedSyncClient(t, destinationSocket)
	ack, err := destinationClient.Apply(t.Context(), delivery)
	if err != nil {
		t.Fatal(err)
	}
	if ack.NeedSnapshot || ack.AckedRevision != change.SourceRevision || !driver.seen {
		t.Fatalf("Apply acknowledgement = %+v seen=%t", ack, driver.seen)
	}
}

var (
	_ syncservice.Transport                  = (*proxiedSyncTransport)(nil)
	_ converge.Driver[hostsync.AccountValue] = (*proxyApplyDriver)(nil)
)
