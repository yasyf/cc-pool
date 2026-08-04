package hostsync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/testhome"
	"github.com/yasyf/cc-pool/internal/workerexec"
)

const materializeManifest = "/cfg/synckit/manifests/cc-pool.json"

// stubRefresher satisfies pool.Refresher: Usage always succeeds and Refresh is
// never reached by these tests because their delivered credentials are unexpired.
type stubRefresher struct{}

func (stubRefresher) Refresh(context.Context, string, string) (*oauth.TokenResponse, error) {
	return nil, errors.New("materialized peer credential must never refresh locally")
}

func (stubRefresher) Usage(context.Context, string) (*oauth.Usage, error) {
	return &oauth.Usage{}, nil
}

// runRecorder captures every external-command exec the Service issues (the
// synckitd nudge), so tests assert the nudge fired without spawning a real binary.
type runRecorder struct{ calls [][]string }

func (r *runRecorder) run(_ context.Context, name string, args ...string) error {
	r.calls = append(r.calls, append([]string{name}, args...))
	return nil
}

type inlineMaterializeTaskRunner struct {
	credentials backingCredentials
}

func (runner inlineMaterializeTaskRunner) Run(ctx context.Context, task workerexec.CommandRequest) (workerexec.CommandResult, error) {
	var output bytes.Buffer
	switch {
	case pool.IsBackingWorkerInvocation(task.Args):
		err := pool.RunBackingWorker(ctx, bytes.NewReader(task.Stdin), &output)
		return workerexec.CommandResult{Stdout: output.Bytes()}, err
	case pool.IsCredentialCASWorkerInvocation(task.Args):
		request, err := pool.DecodeCredentialCASRequest(bytes.NewReader(task.Stdin))
		if err != nil {
			return workerexec.CommandResult{}, err
		}
		account := store.Account{
			ID: request.AccountID, ConfigDir: request.ConfigDir,
			KeychainService: request.KeychainService, KeychainAccount: request.KeychainAccount,
		}
		after := request.Expected
		if len(request.Credential) != 0 {
			var credential creds.Credential
			if err := json.Unmarshal(request.Credential, &credential); err != nil {
				return workerexec.CommandResult{}, err
			}
			if err := runner.credentials.Store(account, creds.SourceKeychain).Write(ctx, &credential); err != nil {
				return workerexec.CommandResult{}, err
			}
			digest := store.CredentialDigest(sha256.Sum256(request.Credential))
			after.Keychain = store.CredentialSlotObservation{
				State: store.CredentialSlotPresent, Digest: &digest,
			}
		} else if request.Delete {
			if err := runner.credentials.Store(account, creds.SourceKeychain).Delete(ctx); err != nil {
				return workerexec.CommandResult{}, err
			}
			after.Keychain = store.CredentialSlotObservation{State: store.CredentialSlotEmpty}
		} else {
			return workerexec.CommandResult{}, errors.New("unsupported materialize test credential CAS mutation")
		}
		err = pool.WriteCredentialCASResponse(&output, pool.CredentialCASResponse{
			Before: request.Expected, After: after,
		})
		return workerexec.CommandResult{Stdout: output.Bytes()}, err
	default:
		return workerexec.CommandResult{}, errors.New("unexpected materialize test worker")
	}
}

type backingCredentials struct{ *credstest.Fake }

func (c backingCredentials) Store(a store.Account, _ creds.Source) creds.Store {
	return c.Fake.Store(a, creds.SourceKeychain)
}

func (c backingCredentials) Stores(a store.Account) []creds.Store {
	return []creds.Store{c.Store(a, creds.SourceKeychain)}
}

type fixtureAccountRemover struct {
	mu    sync.Mutex
	m     *pool.Manager
	fail  map[int]error
	calls []int
}

type fixtureAccountRemoval struct {
	remover *fixtureAccountRemover
	removal store.AccountRemoval
}

type fixtureAccountPreparer struct {
	prepare func(
		context.Context,
		store.PendingAccountReservation,
		string,
	) (store.FileProviderPresentationIdentity, error)
	abort func(
		context.Context,
		store.PendingAccountReservation,
	) (pool.PendingAddRetirementProof, error)
}

func (prepare fixtureAccountPreparer) PrepareReservedAccount(
	ctx context.Context,
	reservation store.PendingAccountReservation,
	label string,
) (store.FileProviderPresentationIdentity, error) {
	return prepare.prepare(ctx, reservation, label)
}

func (prepare fixtureAccountPreparer) AbortReservedAccount(
	ctx context.Context,
	reservation store.PendingAccountReservation,
) (pool.PendingAddRetirementProof, error) {
	if prepare.abort != nil {
		return prepare.abort(ctx, reservation)
	}
	return pool.PendingAddRetirementProof{
		AccountID: reservation.ID, AccountInstanceID: reservation.InstanceID,
		AccountGeneration: reservation.Generation,
		PublicPath:        materializeFileProviderPublicPath(reservation.ID),
	}, nil
}

func (r *fixtureAccountRemover) BeginAccountRemoval(id int, deleteCredential bool) (AccountRemoval, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, id)
	if err := r.fail[id]; err != nil {
		return nil, err
	}
	if !deleteCredential {
		return nil, errors.New("fixture removal requires credential deletion")
	}
	removal, err := r.m.Store.BeginAccountRemoval(id, deleteCredential)
	if err != nil {
		return nil, err
	}
	return fixtureAccountRemoval{remover: r, removal: removal}, nil
}

func (r *fixtureAccountRemover) setFailure(id int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail[id] = err
}

func (r *fixtureAccountRemover) callsSnapshot() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.calls...)
}

func (p fixtureAccountRemoval) Finish(ctx context.Context) error {
	presentation, err := p.remover.m.Store.AccountPresentation(p.removal.AccountID)
	if err != nil {
		return err
	}
	return p.remover.m.FinishAccountRemoval(
		ctx, p.removal, presentation.Identity.PublicPath,
	)
}

func materializeFileProviderPublicPath(id int) string {
	home, err := pool.Home()
	if err != nil {
		panic(err)
	}
	return filepath.Join(home, "Library", "CloudStorage", "cc-pool-"+pool.AccountDirName(id))
}

func materializePreparationProof(
	reservation store.PendingAccountReservation,
	publicPath string,
) store.FileProviderPresentationIdentity {
	tenantID := "account-" + reservation.InstanceID
	return store.FileProviderPresentationIdentity{
		TenantID: tenantID, DomainID: "domain-" + reservation.InstanceID,
		Generation: reservation.Generation, PublicPath: publicPath,
	}
}

var (
	_ AccountRemover  = (*fixtureAccountRemover)(nil)
	_ AccountRemoval  = fixtureAccountRemoval{}
	_ AccountPreparer = fixtureAccountPreparer{}
)

// newMaterializeService wires a Service over a real temporary pool Manager,
// an in-memory Keychain, and a captured command runner. Its remover models the
// holder-first lifecycle boundary before deleting manager-owned private state.
func newMaterializeService(t *testing.T) (*Service, *pool.Manager, *credstest.Fake, *runRecorder) {
	t.Helper()
	testhome.Sandbox(t, t.TempDir())
	t.Setenv("USER", "tester")
	if err := os.MkdirAll(pool.ClaudeDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "materialize.db"))
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	owner, err := store.MintOwnerRecord(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	fk := credstest.NewFake()
	credentials := backingCredentials{fk}
	authority, err := pool.NewWorkerAuthority(
		inlineMaterializeTaskRunner{credentials: credentials}, executable, owner,
	)
	if err != nil {
		t.Fatal(err)
	}
	m, err := pool.NewManager(
		st, stubRefresher{},
		func(context.Context) ([]procscan.Session, error) { return nil, nil },
		authority,
	)
	if err != nil {
		t.Fatal(err)
	}
	var credentialClaim sync.Mutex
	m.ClaimCredentialMutation = func(int) (func(), error) {
		credentialClaim.Lock()
		return credentialClaim.Unlock, nil
	}
	t.Cleanup(func() { _ = m.Close() })
	m.Creds = credentials
	m.OAuth = stubRefresher{}
	m.BuildCredentialWritePublication = func(
		store.Account,
		*creds.Credential,
		store.CredentialOperationID,
		time.Time,
	) ([]byte, error) {
		return []byte("materialize-test"), nil
	}
	m.SettleCredentialWrite = func(context.Context, pool.CredentialWriteSettlement) error { return nil }
	if _, err := m.Init(); err != nil {
		t.Fatal(err)
	}
	rf := tempRegistry(t)
	rec := &runRecorder{}
	s := &Service{
		M:        m,
		Registry: &rf,
		StampDir: filepath.Join(t.TempDir(), "stamps"),
		Run:      rec.run,
		Remover:  &fixtureAccountRemover{m: m, fail: map[int]error{}},
		Preparer: fixtureAccountPreparer{prepare: func(
			_ context.Context,
			reservation store.PendingAccountReservation,
			_ string,
		) (store.FileProviderPresentationIdentity, error) {
			path := materializeFileProviderPublicPath(reservation.ID)
			if err := os.MkdirAll(path, 0o700); err != nil {
				return store.FileProviderPresentationIdentity{}, err
			}
			return materializePreparationProof(reservation, path), nil
		}},
	}
	return s, m, fk, rec
}

func mustMutationOwner(t *testing.T, manager *pool.Manager) store.OwnerRecord {
	t.Helper()
	owner, err := manager.MutationOwner()
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

// freshEnvelope is delivered access-only material whose token is unexpired.
func freshEnvelope(access string) *creds.Credential {
	c := cred(access, "")
	c.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli()
	return c
}

// materializeVal builds a peer-added registry value with a full, verbatim
// oauthAccount object and an origin-owned chain stamp.
func materializeVal(uuid, email string, oauthAccount json.RawMessage) AccountValue {
	return AccountValue{
		UUID:         uuid,
		Email:        email,
		Label:        "peer-" + uuid,
		OAuthAccount: oauthAccount,
		Chain:        ChainStamp{Origin: "hostA", ExpiresAt: 1_800_000_000_000, Hash: "h-" + uuid},
	}
}

// TestMaterializeHappyPath pins the full peer-add: a dir, the verbatim identity,
// the delivered credential in the Keychain, an exact UUID-bound row, and a
// synckitd nudge — with no interactive login.
func TestMaterializeHappyPath(t *testing.T) {
	s, m, fk, rec := newMaterializeService(t)

	// A ~/.claude.json exists, so PrepareAdd seeds SeedCopied (not the bootstrap path).
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{"numStartups":3,"oauthAccount":{"accountUuid":"PLAIN"}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	oauthAccount := json.RawMessage(`{"accountUuid":"u-happy","emailAddress":"happy@example.com","organizationUuid":"org-1","nested":{"k":1},"flag":true}`)
	env := freshEnvelope("at-happy")

	res, err := s.Materialize(context.Background(), materializeVal("u-happy", "happy@example.com", oauthAccount), env, materializeManifest)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if res.Deferred || res.Bootstrapped {
		t.Fatalf("result flags = %+v, want all false on the keychain happy path", res)
	}
	if res.UUID != "u-happy" || res.AccountID != 1 {
		t.Fatalf("result = %+v, want uuid u-happy / acct 1", res)
	}

	publicPath := materializeFileProviderPublicPath(1)
	backingDir := pool.AccountBackingDir(1)
	if _, err := os.Stat(backingDir); err != nil {
		t.Fatalf("account backing not created: %v", err)
	}
	if info, err := os.Stat(publicPath); err != nil || !info.IsDir() {
		t.Fatalf("prepared presentation path = %+v, %v", info, err)
	}

	// Identity injected verbatim and resolvable through the established reader.
	id, err := m.AccountIdentity(t.Context(), 1, publicPath)
	if err != nil {
		t.Fatalf("AccountIdentity: %v", err)
	}
	if id.AccountUUID != "u-happy" || id.EmailAddress != "happy@example.com" {
		t.Fatalf("identity = %+v, want u-happy / happy@example.com", id)
	}
	raw, err := os.ReadFile(filepath.Join(backingDir, ".claude.json")) //nolint:gosec // G304: backingDir is a cc-pool-managed/test-owned dir, not external input
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatal(err)
	}
	var gotOAuth, wantOAuth any
	if err := json.Unmarshal(top["oauthAccount"], &gotOAuth); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(oauthAccount, &wantOAuth); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotOAuth, wantOAuth) {
		t.Fatalf("oauthAccount = %s, want %s (verbatim)", top["oauthAccount"], oauthAccount)
	}

	// Row registered with its immutable UUID and credential in the Keychain.
	row, err := m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if row.AccountUUID != "u-happy" {
		t.Fatalf("row AccountUUID = %q, want u-happy", row.AccountUUID)
	}
	wantConfigDir, err := pool.AccountConfigDir(row.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	wantService, err := pool.AccountKeychainService(row.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if row.ConfigDir != wantConfigDir || row.ConfigDir == pool.AccountBackingDir(1) ||
		row.KeychainService != wantService {
		t.Fatalf("persisted execution identity = %+v, want immutable instance path %q", row, wantConfigDir)
	}
	if target, err := os.Readlink(row.ConfigDir); err != nil || target != publicPath {
		t.Fatalf("stable execution link target = %q, %v; want %q", target, err, publicPath)
	}
	presentation, err := m.Store.AccountPresentation(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if presentation.AccountInstanceID != row.InstanceID ||
		presentation.AccountGeneration != row.Generation ||
		presentation.Identity.PublicPath != publicPath || presentation.Identity.TenantID == "" ||
		presentation.Identity.DomainID == "" {
		t.Fatalf("persisted presentation identity = %+v", presentation)
	}
	byUUID, ok, err := m.Store.GetAccountByUUID("u-happy")
	if err != nil || !ok {
		t.Fatalf("GetAccountByUUID: ok=%v err=%v", ok, err)
	}
	if byUUID.ID != 1 {
		t.Fatalf("GetAccountByUUID id = %d, want 1", byUUID.ID)
	}
	gotCred, src, err := m.ReadCredential(t.Context(), row)
	if err != nil {
		t.Fatalf("ReadCredential: %v", err)
	}
	if src != creds.SourceKeychain {
		t.Fatalf("credential source = %v, want keychain", src)
	}
	if gotCred.ClaudeAiOauth.AccessToken != "at-happy" || gotCred.HasRefreshToken() {
		t.Fatalf("installed credential = %+v, want the stripped at-happy with no refresh token", gotCred.ClaudeAiOauth)
	}
	if _, ok := fk.Get(row.KeychainService, creds.AccountLabel()); !ok {
		t.Fatalf("keychain item %q/%q absent, want the installed envelope", row.KeychainService, creds.AccountLabel())
	}
	health, err := m.Store.GetAuthHealth(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if health.NeedsLogin {
		t.Fatalf("valid stripped credential left account awaiting origin: %+v", health)
	}

	// synckitd nudged with the manifest path.
	if !recorded(rec, []string{"synckitd", "register", materializeManifest}) {
		t.Fatalf("nudge calls = %v, want a synckitd register of %q", rec.calls, materializeManifest)
	}
}

func TestMaterializeResolvesCommittedPromotionBeforeCleanup(t *testing.T) {
	s, manager, _, _ := newMaterializeService(t)
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s.promoteSyncedAdd = func(
		ctx context.Context,
		pending *pool.PendingAdd,
		label string,
		uuid string,
	) (*store.Account, error) {
		if _, err := manager.PromoteSyncedAdd(ctx, pending, label, uuid); err != nil {
			return nil, err
		}
		return nil, errors.New("injected lost promotion response")
	}
	result, err := s.Materialize(
		t.Context(),
		materializeVal("u-lost-response", "lost@example.com", json.RawMessage(`{"accountUuid":"u-lost-response"}`)),
		freshEnvelope("at-lost-response"), materializeManifest,
	)
	if err != nil || result.AccountID != 1 {
		t.Fatalf("Materialize after lost promotion response = %+v err=%v", result, err)
	}
	account, err := manager.Store.GetAccount(result.AccountID)
	if err != nil || account.AccountUUID != "u-lost-response" {
		t.Fatalf("durable account = %+v err=%v", account, err)
	}
	if _, err := os.Stat(pool.AccountBackingDir(result.AccountID)); err != nil {
		t.Fatalf("committed backing was abandoned: %v", err)
	}
}

func TestMaterializeAbandonsOnlyProvenUntouchedPromotion(t *testing.T) {
	s, manager, _, _ := newMaterializeService(t)
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	preparer := s.Preparer.(fixtureAccountPreparer)
	prepare := preparer.prepare
	var retired store.PendingAccountReservation
	var marker string
	preparer.prepare = func(
		ctx context.Context,
		reservation store.PendingAccountReservation,
		label string,
	) (store.FileProviderPresentationIdentity, error) {
		proof, err := prepare(ctx, reservation, label)
		if err != nil {
			return store.FileProviderPresentationIdentity{}, err
		}
		marker = filepath.Join(proof.PublicPath, "presentation-survives")
		if err := os.WriteFile(marker, []byte("public"), 0o600); err != nil {
			return store.FileProviderPresentationIdentity{}, err
		}
		return proof, nil
	}
	preparer.abort = func(
		ctx context.Context,
		reservation store.PendingAccountReservation,
	) (pool.PendingAddRetirementProof, error) {
		if err := ctx.Err(); err != nil {
			return pool.PendingAddRetirementProof{}, err
		}
		retired = reservation
		return pool.PendingAddRetirementProof{
			AccountID: reservation.ID, AccountInstanceID: reservation.InstanceID,
			AccountGeneration: reservation.Generation,
			PublicPath:        materializeFileProviderPublicPath(reservation.ID),
		}, nil
	}
	s.Preparer = preparer
	s.promoteSyncedAdd = func(
		context.Context, *pool.PendingAdd, string, string,
	) (*store.Account, error) {
		return nil, errors.New("injected pre-commit failure")
	}
	_, err := s.Materialize(
		t.Context(),
		materializeVal("u-precommit", "precommit@example.com", json.RawMessage(`{"accountUuid":"u-precommit"}`)),
		freshEnvelope("at-precommit"), materializeManifest,
	)
	if err == nil {
		t.Fatal("Materialize pre-commit failure succeeded")
	}
	if _, err := manager.Store.GetAccount(1); !errors.Is(err, store.ErrAccountNotFound) {
		t.Fatalf("account after proven pre-commit failure = %v", err)
	}
	if _, statErr := os.Stat(pool.AccountBackingDir(1)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("uncommitted backing survived safe abandon: stat=%v materialize=%v", statErr, err)
	}
	if retired.ID != 1 {
		t.Fatalf("retired reservation = %+v, want acct-01", retired)
	}
	configDir, err := pool.AccountConfigDir(retired.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(configDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stable link survived proven retirement: %v", err)
	}
	// #nosec G304 -- marker is created beneath this test's private temporary root.
	if got, err := os.ReadFile(marker); err != nil || string(got) != "public" {
		t.Fatalf("public target after cleanup = %q err=%v", got, err)
	}
	reused, err := manager.ReserveAdd()
	if err != nil || reused.ID != retired.ID {
		t.Fatalf("reservation after exact cleanup = %+v err=%v", reused, err)
	}
}

func TestMaterializeCancellationRetiresReservationWithLiveCleanupContext(t *testing.T) {
	s, manager, _, _ := newMaterializeService(t)
	ctx, cancel := context.WithCancel(t.Context())
	var retired store.PendingAccountReservation
	s.Preparer = fixtureAccountPreparer{
		prepare: func(
			context.Context,
			store.PendingAccountReservation,
			string,
		) (store.FileProviderPresentationIdentity, error) {
			cancel()
			return store.FileProviderPresentationIdentity{}, context.Canceled
		},
		abort: func(
			cleanup context.Context,
			reservation store.PendingAccountReservation,
		) (pool.PendingAddRetirementProof, error) {
			if err := cleanup.Err(); err != nil {
				return pool.PendingAddRetirementProof{}, fmt.Errorf("cleanup inherited cancellation: %w", err)
			}
			retired = reservation
			return pool.PendingAddRetirementProof{
				AccountID: reservation.ID, AccountInstanceID: reservation.InstanceID,
				AccountGeneration: reservation.Generation,
				PublicPath:        materializeFileProviderPublicPath(reservation.ID),
			}, nil
		},
	}
	_, err := s.Materialize(
		ctx,
		materializeVal("u-canceled", "cancel@example.com", json.RawMessage(`{"accountUuid":"u-canceled"}`)),
		freshEnvelope("at-canceled"), materializeManifest,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled materialize = %v", err)
	}
	if retired.ID != 1 {
		t.Fatalf("retired reservation = %+v, want acct-01", retired)
	}
	reused, reserveErr := manager.ReserveAdd()
	if reserveErr != nil || reused.ID != retired.ID {
		t.Fatalf("reservation after canceled retirement = %+v err=%v", reused, reserveErr)
	}
}

func TestMaterializeRetainsReservationWhenRetirementIsAmbiguous(t *testing.T) {
	s, manager, _, _ := newMaterializeService(t)
	s.Preparer = fixtureAccountPreparer{
		prepare: func(
			context.Context,
			store.PendingAccountReservation,
			string,
		) (store.FileProviderPresentationIdentity, error) {
			return store.FileProviderPresentationIdentity{}, errors.New("partial provisioning")
		},
		abort: func(
			context.Context,
			store.PendingAccountReservation,
		) (pool.PendingAddRetirementProof, error) {
			return pool.PendingAddRetirementProof{}, errors.New("retirement unavailable")
		},
	}
	_, err := s.Materialize(
		t.Context(),
		materializeVal("u-retained", "retained@example.com", json.RawMessage(`{"accountUuid":"u-retained"}`)),
		freshEnvelope("at-retained"), materializeManifest,
	)
	if err == nil || !strings.Contains(err.Error(), "retirement unavailable") {
		t.Fatalf("ambiguous retirement = %v", err)
	}
	indexes, indexErr := manager.Store.PendingAddIndexes()
	if indexErr != nil || !reflect.DeepEqual(indexes, []int{1}) {
		t.Fatalf("retained reservation indexes = %v err=%v", indexes, indexErr)
	}
	next, reserveErr := manager.ReserveAdd()
	if reserveErr != nil || next.ID != 2 {
		t.Fatalf("reservation after ambiguous retirement = %+v err=%v", next, reserveErr)
	}
}

func TestMaterializeNeverAbandonsAmbiguousPromotion(t *testing.T) {
	s, manager, _, _ := newMaterializeService(t)
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s.promoteSyncedAdd = func(
		context.Context, *pool.PendingAdd, string, string,
	) (*store.Account, error) {
		return nil, errors.New("injected unknown promotion result")
	}
	s.resolvePromotedSyncedAdd = func(
		*pool.PendingAdd, string, string,
	) (*store.Account, bool, error) {
		return nil, false, store.ErrSyncedPromotionAmbiguous
	}
	_, err := s.Materialize(
		t.Context(),
		materializeVal("u-ambiguous", "ambiguous@example.com", json.RawMessage(`{"accountUuid":"u-ambiguous"}`)),
		freshEnvelope("at-ambiguous"), materializeManifest,
	)
	if !errors.Is(err, store.ErrSyncedPromotionAmbiguous) {
		t.Fatalf("Materialize ambiguous promotion err=%v", err)
	}
	if _, err := os.Stat(pool.AccountBackingDir(1)); err != nil {
		t.Fatalf("ambiguous backing was destroyed: %v", err)
	}
	indexes, err := manager.Store.PendingAddIndexes()
	if err != nil || !reflect.DeepEqual(indexes, []int{1}) {
		t.Fatalf("ambiguous reservation indexes = %v err=%v", indexes, err)
	}
}

func TestMaterializeRejectsExistingExternalUUIDBeforeMutation(t *testing.T) {
	s, manager, _, _ := newMaterializeService(t)
	existing := admitHostsyncTestAccount(t, manager, store.Account{
		ID:              9,
		KeychainService: "existing-service", KeychainAccount: "existing-account",
		AccountUUID: "duplicate",
	})
	identityPath := filepath.Join(pool.AccountBackingDir(existing.ID), ".claude.json")
	if err := os.MkdirAll(filepath.Dir(identityPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		identityPath,
		[]byte(`{"oauthAccount":{"accountUuid":"duplicate","emailAddress":"existing@example.com"}}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	_, err := s.Materialize(
		t.Context(),
		materializeVal(
			"duplicate", "peer@example.com",
			json.RawMessage(`{"accountUuid":"duplicate","emailAddress":"peer@example.com"}`),
		),
		freshEnvelope("unused-delivery"),
		materializeManifest,
	)
	if !errors.Is(err, store.ErrDuplicateAccountUUID) {
		t.Fatalf("duplicate materialize = %v", err)
	}
}

func TestMaterializeDoesNotReportSuccessBeforeTenantPreparation(t *testing.T) {
	s, m, _, rec := newMaterializeService(t)
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{"oauthAccount":{"accountUuid":"PLAIN"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("presentation unavailable")
	var prepared []store.PendingAccountReservation
	s.Preparer = fixtureAccountPreparer{prepare: func(
		_ context.Context,
		reservation store.PendingAccountReservation,
		label string,
	) (store.FileProviderPresentationIdentity, error) {
		prepared = append(prepared, reservation)
		if label != "peer-u-prepare" {
			t.Fatalf("preparation label = %q", label)
		}
		return store.FileProviderPresentationIdentity{}, wantErr
	}}
	oauthAccount := json.RawMessage(`{"accountUuid":"u-prepare","emailAddress":"prepare@example.com"}`)
	result, err := s.Materialize(
		t.Context(),
		materializeVal("u-prepare", "prepare@example.com", oauthAccount),
		freshEnvelope("at-prepare"), materializeManifest,
	)
	if !errors.Is(err, wantErr) || result != (MaterializeResult{}) {
		t.Fatalf("materialize before preparation = result %+v err %v", result, err)
	}
	if len(prepared) != 1 || prepared[0].ID != 1 {
		t.Fatalf("prepared reservations = %+v, want acct-01", prepared)
	}
	if _, readErr := m.Store.GetAccount(1); !errors.Is(readErr, store.ErrAccountNotFound) {
		t.Fatalf("account after preparation failure = %v, want not found", readErr)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("synckit nudge ran before tenant preparation: %v", rec.calls)
	}
}

// TestMaterializeSeedNoSourceBootstraps pins carry-forward #2: with no
// ~/.claude.json to seed from, the materializer bootstraps a minimal private
// onboarding document so WriteIdentity lands, and the account completes end to end.
func TestMaterializeSeedNoSourceBootstraps(t *testing.T) {
	s, m, _, _ := newMaterializeService(t)
	// No ~/.claude.json written: PrepareAdd reports SeedNoSource.

	oauthAccount := json.RawMessage(`{"accountUuid":"u-nosrc","emailAddress":"nosrc@example.com"}`)
	res, err := s.Materialize(context.Background(), materializeVal("u-nosrc", "nosrc@example.com", oauthAccount), freshEnvelope("at-n"), materializeManifest)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if !res.Bootstrapped {
		t.Fatalf("result = %+v, want Bootstrapped true on the SeedNoSource path", res)
	}
	if res.AccountID != 1 || res.Deferred {
		t.Fatalf("result = %+v, want a completed acct 1", res)
	}

	publicPath := materializeFileProviderPublicPath(1)
	id, err := m.AccountIdentity(t.Context(), 1, publicPath)
	if err != nil {
		t.Fatalf("AccountIdentity: %v", err)
	}
	if id.AccountUUID != "u-nosrc" {
		t.Fatalf("identity uuid = %q, want u-nosrc", id.AccountUUID)
	}
	// The worker-created onboarding flag survives identity injection.
	raw, err := os.ReadFile(filepath.Join(pool.AccountBackingDir(1), ".claude.json")) //nolint:gosec // G304: backing is cc-pool-managed test state
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatal(err)
	}
	if len(top) != 2 || string(top["hasCompletedOnboarding"]) != "true" {
		t.Fatalf("bootstrapped doc = %s, want onboarding and oauthAccount", raw)
	}
	row, err := m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if row.AccountUUID != "u-nosrc" {
		t.Fatalf("row AccountUUID = %q, want u-nosrc", row.AccountUUID)
	}
}

func TestMaterializeKeychainUnavailableFailsClosed(t *testing.T) {
	s, m, fk, _ := newMaterializeService(t)
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	// The login keychain is unsearchable this session: every keychain probe/read
	// reports ErrUnavailable.
	fk.KeychainFaults = credstest.Faults{Read: creds.ErrUnavailable}

	oauthAccount := json.RawMessage(`{"accountUuid":"u-file","emailAddress":"file@example.com"}`)
	res, err := s.Materialize(context.Background(), materializeVal("u-file", "file@example.com", oauthAccount), freshEnvelope("at-f"), materializeManifest)
	if !errors.Is(err, pool.ErrCredentialUnverifiable) {
		t.Fatalf("Materialize error = %v, want ErrCredentialUnverifiable", err)
	}
	if res.AccountID != 1 {
		t.Fatalf("result = %+v, want durable awaiting-origin acct 1", res)
	}

	publicPath := materializeFileProviderPublicPath(1)
	if _, ok := fk.Get(creds.ServiceName(publicPath), creds.AccountLabel()); ok {
		t.Fatal("keychain item present after failed install")
	}
	row, err := m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	health, err := m.Store.GetAuthHealth(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !health.NeedsLogin || health.Kind != store.AuthKindAwaitingOrigin {
		t.Fatalf("auth health = %+v, want awaiting origin", health)
	}
	if row.AccountUUID != "u-file" {
		t.Fatalf("row AccountUUID = %q, want u-file", row.AccountUUID)
	}
}

// TestMaterializeRejectsUnavailableOrInvalidDeliveryBeforeMutation pins the
// required-envelope boundary before any account state is created.
func TestMaterializeRejectsUnavailableOrInvalidDeliveryBeforeMutation(t *testing.T) {
	cases := map[string]struct {
		credential *creds.Credential
		wantErr    error
	}{
		"missing":         {nil, ErrCredentialMaterialUnavailable},
		"tokenless":       {&creds.Credential{}, pool.ErrEnvelopeNoAccessToken},
		"refresh-bearing": {cred("access", "refresh"), pool.ErrEnvelopeCarriesSecret},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, manager, _, rec := newMaterializeService(t)
			oauthAccount := json.RawMessage(`{"accountUuid":"u-rejected"}`)
			result, err := s.Materialize(t.Context(), materializeVal("u-rejected", "r.com", oauthAccount), tc.credential, materializeManifest)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Materialize error = %v, want %v", err, tc.wantErr)
			}
			if result != (MaterializeResult{}) {
				t.Fatalf("result = %+v, want zero", result)
			}
			if _, statErr := os.Stat(pool.AccountBackingDir(1)); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("delivery rejection mutated backing: %v", statErr)
			}
			reservation, reserveErr := manager.Store.ReserveAccountIndex(mustMutationOwner(t, manager))
			if reserveErr != nil || reservation.ID != 1 {
				t.Fatalf("reservation after rejection = %+v err=%v", reservation, reserveErr)
			}
			if len(rec.calls) != 0 {
				t.Fatalf("nudge calls = %v", rec.calls)
			}
		})
	}
}

// TestMaterializeNeverReusesRetainedCredentialReservation pins interrupted-add
// retention: a pending instance keeps its numeric reservation, stable link,
// credential, and backing while a peer materializes into the next slot.
func TestMaterializeNeverReusesRetainedCredentialReservation(t *testing.T) {
	s, m, fk, rec := newMaterializeService(t)
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}

	reservation, err := m.ReserveAdd()
	if err != nil {
		t.Fatal(err)
	}
	publicPath := materializeFileProviderPublicPath(1)
	if err := os.MkdirAll(publicPath, 0o700); err != nil {
		t.Fatal(err)
	}
	pending, err := m.PrepareReservedAdd(t.Context(), reservation, publicPath)
	if err != nil {
		t.Fatal(err)
	}
	keptBacking := pool.AccountBackingDir(pending.Reservation.ID)
	const keptIdentity = `{"oauthAccount":{"accountUuid":"u-kept","emailAddress":"kept@example.com"}}`
	if err := os.WriteFile(filepath.Join(keptBacking, ".claude.json"), []byte(keptIdentity), 0o600); err != nil {
		t.Fatal(err)
	}
	retained := cred("at-kept", "rt-kept")
	retained.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli()
	fk.Put(pending.KeychainService, "claude-login-label", retained)

	oauthAccount := json.RawMessage(`{"accountUuid":"u-peer","emailAddress":"peer@example.com"}`)
	res, err := s.Materialize(context.Background(), materializeVal("u-peer", "peer@example.com", oauthAccount), freshEnvelope("unused-delivery"), materializeManifest)
	if err != nil || res.AccountID != 2 {
		t.Fatalf("materialize beside retained reservation = %+v err=%v", res, err)
	}

	got, ok := fk.Get(pending.KeychainService, "claude-login-label")
	if !ok || got.ClaudeAiOauth.RefreshToken != "rt-kept" {
		t.Fatalf("retained credential = %+v ok=%v, want rt-kept intact", got, ok)
	}
	// #nosec G304 -- keptBacking is the pool-owned backing root created by this test.
	raw, err := os.ReadFile(filepath.Join(keptBacking, ".claude.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != keptIdentity {
		t.Fatalf("kept identity mutated: %s", raw)
	}
	if target, err := os.Readlink(pending.ConfigDir); err != nil || target != pending.PublicPath {
		t.Fatalf("retained stable link = %q err=%v", target, err)
	}
	indexes, err := m.Store.PendingAddIndexes()
	if err != nil || len(indexes) != 1 || indexes[0] != pending.Reservation.ID {
		t.Fatalf("retained reservation indexes = %v err=%v", indexes, err)
	}
	if len(rec.calls) != 1 {
		t.Fatalf("nudge calls = %v, want one peer materialization", rec.calls)
	}
}

// TestMaterializeEmptyOAuthDefers pins carry-forward #3: an entry with no
// oauthAccount is deferred — no dir, reservation, or material lookup — never an error
// loop, so a later origin publication can supply the identity.
func TestMaterializeEmptyOAuthDefers(t *testing.T) {
	cases := map[string]json.RawMessage{
		"absent": nil,
		"null":   json.RawMessage("null"),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			s, m, _, rec := newMaterializeService(t)
			res, err := s.Materialize(context.Background(), materializeVal("u-empty", "e@example.com", raw), nil, materializeManifest)
			if err != nil {
				t.Fatalf("Materialize: %v, want a clean deferral", err)
			}
			if !res.Deferred || res.UUID != "u-empty" || res.AccountID != 0 {
				t.Fatalf("result = %+v, want Deferred true / u-empty / no acct", res)
			}

			// Nothing materialized: no dir, no reservation, no row, no nudge.
			if _, statErr := os.Stat(pool.AccountBackingDir(1)); !os.IsNotExist(statErr) {
				t.Fatalf("account dir stat err = %v, want not-exist (nothing created)", statErr)
			}
			n, rerr := m.Store.ReserveAccountIndex(mustMutationOwner(t, m))
			if rerr != nil {
				t.Fatal(rerr)
			}
			if n.ID != 1 {
				t.Fatalf("next reserved index = %d, want 1 (no reservation was taken)", n.ID)
			}
			accounts, lerr := m.Store.ListAccounts()
			if lerr != nil {
				t.Fatal(lerr)
			}
			if len(accounts) != 0 {
				t.Fatalf("accounts = %+v, want none for a deferred entry", accounts)
			}
			if len(rec.calls) != 0 {
				t.Fatalf("nudge calls = %v, want none for a deferred entry", rec.calls)
			}
		})
	}
}

// recorded reports whether rec captured an exact command invocation.
func recorded(rec *runRecorder, want []string) bool {
	for _, c := range rec.calls {
		if reflect.DeepEqual(c, want) {
			return true
		}
	}
	return false
}
