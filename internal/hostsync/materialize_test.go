package hostsync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
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
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"
)

const materializeManifest = "/cfg/synckit/manifests/cc-pool.json"

// stubRefresher satisfies pool.Refresher: Usage always succeeds and Refresh is
// never reached by these tests because their pulled credentials are unexpired.
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

func (runner inlineMaterializeTaskRunner) Run(ctx context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
	var output bytes.Buffer
	switch {
	case pool.IsBackingWorkerInvocation(task.Args):
		err := pool.RunBackingWorker(ctx, bytes.NewReader(task.Stdin), &output)
		return worker.CommandResult{Stdout: output.Bytes()}, err
	case pool.IsCredentialCASWorkerInvocation(task.Args):
		request, err := pool.DecodeCredentialCASRequest(bytes.NewReader(task.Stdin))
		if err != nil {
			return worker.CommandResult{}, err
		}
		account := store.Account{
			ID: request.AccountID, ConfigDir: request.ConfigDir,
			KeychainService: request.KeychainService, KeychainAccount: request.KeychainAccount,
		}
		after := request.Expected
		if len(request.Credential) != 0 {
			var credential creds.Credential
			if err := json.Unmarshal(request.Credential, &credential); err != nil {
				return worker.CommandResult{}, err
			}
			if err := runner.credentials.Store(account, creds.SourceKeychain).Write(ctx, &credential); err != nil {
				return worker.CommandResult{}, err
			}
			digest := store.CredentialDigest(sha256.Sum256(request.Credential))
			after.Keychain = store.CredentialSlotObservation{
				State: store.CredentialSlotPresent, Digest: &digest,
			}
		} else if request.Delete {
			if err := runner.credentials.Store(account, creds.SourceKeychain).Delete(ctx); err != nil {
				return worker.CommandResult{}, err
			}
			after.Keychain = store.CredentialSlotObservation{State: store.CredentialSlotEmpty}
		} else {
			return worker.CommandResult{}, errors.New("unsupported materialize test credential CAS mutation")
		}
		err = pool.WriteCredentialCASResponse(&output, pool.CredentialCASResponse{
			Before: request.Expected, After: after,
		})
		return worker.CommandResult{Stdout: output.Bytes()}, err
	default:
		return worker.CommandResult{}, errors.New("unexpected materialize test worker")
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
	) (store.PresentationPreparationProof, error)
	refresh func(
		context.Context,
		store.Account,
		store.PresentationPreparationProof,
	) (store.PresentationPreparationProof, error)
}

func (prepare fixtureAccountPreparer) PrepareReservedAccount(
	ctx context.Context,
	reservation store.PendingAccountReservation,
	label string,
) (store.PresentationPreparationProof, error) {
	return prepare.prepare(ctx, reservation, label)
}

func (prepare fixtureAccountPreparer) RefreshPreparedAccount(
	ctx context.Context,
	account store.Account,
	proof store.PresentationPreparationProof,
) (store.PresentationPreparationProof, error) {
	if prepare.refresh != nil {
		return prepare.refresh(ctx, account, proof)
	}
	if err := store.ValidateReservedPresentationPreparationProof(store.PendingAccountReservation{
		ID: account.ID, InstanceID: account.InstanceID, Generation: account.Generation,
	}, proof); err != nil {
		return store.PresentationPreparationProof{}, err
	}
	return proof, nil
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
	if err := os.RemoveAll(materializePresentationPath(p.removal.AccountID)); err != nil {
		return err
	}
	return p.remover.m.FinishAccountRemoval(ctx, p.removal)
}

func materializePresentationPath(id int) string {
	home, err := pool.Home()
	if err != nil {
		panic(err)
	}
	return filepath.Join(home, "Library", "CloudStorage", "cc-pool-"+pool.AccountDirName(id))
}

func materializePreparationProof(
	reservation store.PendingAccountReservation,
	publicPath string,
) store.PresentationPreparationProof {
	tenantID := "account-" + reservation.InstanceID
	return store.PresentationPreparationProof{
		CatalogTenantID: tenantID, CatalogGeneration: reservation.Generation,
		Requested: 1, Desired: 1, Observed: 1, Verified: 1, Applied: 1,
		SourceAuthority: "source-" + reservation.InstanceID,
		SourceRevision:  1, CatalogRevision: 1,
		ChangeID: "change-" + reservation.InstanceID, OperationID: "operation-" + reservation.InstanceID,
		PresentationKind: store.PresentationKindFileProvider,
		FileProvider: store.FileProviderPreparationProof{
			TenantID: tenantID, DomainID: "domain-" + reservation.InstanceID,
			Generation: reservation.Generation, ActivationGeneration: "activation-" + reservation.InstanceID,
			PublicPath: publicPath,
		},
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
	t.Setenv("HOME", t.TempDir())
	t.Setenv("USER", "tester")
	if err := os.MkdirAll(pool.ClaudeDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "materialize.db"))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := proc.CurrentIdentity()
	if err != nil {
		t.Fatal(err)
	}
	ownerGenerationDigest := sha256.Sum256([]byte(t.Name()))
	var ownerGeneration proc.OwnerGeneration
	copy(ownerGeneration[:], ownerGenerationDigest[:len(ownerGeneration)])
	owner := proc.Record{
		RecoveryID: pool.CredentialOwnerRecoveryID,
		PID:        identity.PID, StartTime: identity.StartTime, Boot: identity.Boot,
		Comm: identity.Comm, Executable: identity.Executable,
		AuditToken: identity.AuditToken, Generation: ownerGeneration,
	}
	fk := credstest.NewFake()
	credentials := backingCredentials{fk}
	authority, err := pool.NewWorkerAuthority(
		inlineMaterializeTaskRunner{credentials: credentials}, identity.Executable, owner,
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
	t.Cleanup(func() { _ = m.Close(t.Context()) })
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
		) (store.PresentationPreparationProof, error) {
			path := materializePresentationPath(reservation.ID)
			if err := os.MkdirAll(path, 0o700); err != nil {
				return store.PresentationPreparationProof{}, err
			}
			return materializePreparationProof(reservation, path), nil
		}},
	}
	return s, m, fk, rec
}

func mustMutationOwner(t *testing.T, manager *pool.Manager) proc.Record {
	t.Helper()
	owner, err := manager.MutationOwner()
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

// freshEnvelope is a pulled stripped envelope whose access token is unexpired.
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

func pullConst(c *creds.Credential) PullCredential {
	return func(context.Context, string, ChainStamp, []string) (*creds.Credential, error) { return c, nil }
}

// TestMaterializeHappyPath pins the full peer-add: a dir, the verbatim identity,
// the pulled credential in the Keychain, an exact UUID-bound row, and a
// synckitd nudge — with no interactive login.
func TestMaterializeHappyPath(t *testing.T) {
	s, m, fk, rec := newMaterializeService(t)

	// A ~/.claude.json exists, so PrepareAdd seeds SeedCopied (not the bootstrap path).
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{"numStartups":3,"oauthAccount":{"accountUuid":"PLAIN"}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	oauthAccount := json.RawMessage(`{"accountUuid":"u-happy","emailAddress":"happy@example.com","organizationUuid":"org-1","nested":{"k":1},"flag":true}`)
	env := freshEnvelope("at-happy")

	res, err := s.Materialize(context.Background(), materializeVal("u-happy", "happy@example.com", oauthAccount), []string{"hostB"}, pullConst(env), materializeManifest)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if res.Deferred || res.Bootstrapped {
		t.Fatalf("result flags = %+v, want all false on the keychain happy path", res)
	}
	if res.UUID != "u-happy" || res.AccountID != 1 {
		t.Fatalf("result = %+v, want uuid u-happy / acct 1", res)
	}

	configDir := materializePresentationPath(1)
	backingDir := pool.AccountBackingDir(1)
	if _, err := os.Stat(backingDir); err != nil {
		t.Fatalf("account backing not created: %v", err)
	}
	if info, err := os.Stat(configDir); err != nil || !info.IsDir() {
		t.Fatalf("prepared presentation path = %+v, %v", info, err)
	}

	// Identity injected verbatim and resolvable through the established reader.
	id, err := m.AccountIdentity(t.Context(), 1, configDir)
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
	if row.ConfigDir != configDir || row.ConfigDir == pool.AccountBackingDir(1) ||
		row.KeychainService != creds.ServiceName(configDir) {
		t.Fatalf("persisted presentation binding = %+v, want exact proven path %q", row, configDir)
	}
	presentation, err := m.Store.AccountPresentation(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if presentation.AccountInstanceID != row.InstanceID ||
		presentation.AccountGeneration != row.Generation ||
		presentation.Proof.FileProvider.PublicPath != row.ConfigDir ||
		presentation.Proof.SourceAuthority == "" || presentation.Proof.SourceRevision == 0 ||
		presentation.Proof.ChangeID == "" || presentation.Proof.OperationID == "" {
		t.Fatalf("persisted complete presentation proof = %+v", presentation)
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
		[]string{"hostB"}, pullConst(freshEnvelope("at-lost-response")), materializeManifest,
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
	s.promoteSyncedAdd = func(
		context.Context, *pool.PendingAdd, string, string,
	) (*store.Account, error) {
		return nil, errors.New("injected pre-commit failure")
	}
	_, err := s.Materialize(
		t.Context(),
		materializeVal("u-precommit", "precommit@example.com", json.RawMessage(`{"accountUuid":"u-precommit"}`)),
		[]string{"hostB"}, pullConst(freshEnvelope("at-precommit")), materializeManifest,
	)
	if err == nil {
		t.Fatal("Materialize pre-commit failure succeeded")
	}
	if _, err := manager.Store.GetAccount(1); !errors.Is(err, store.ErrAccountNotFound) {
		t.Fatalf("account after proven pre-commit failure = %v", err)
	}
	if _, err := os.Stat(pool.AccountBackingDir(1)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("uncommitted backing survived safe abandon: %v", err)
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
		[]string{"hostB"}, pullConst(freshEnvelope("at-ambiguous")), materializeManifest,
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

func TestMaterializeRejectsExistingExternalUUIDBeforePull(t *testing.T) {
	s, manager, _, _ := newMaterializeService(t)
	existing := admitHostsyncTestAccount(t, manager, store.Account{
		ID: 9, ConfigDir: materializePresentationPath(9),
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
	pulled := false
	_, err := s.Materialize(
		t.Context(),
		materializeVal(
			"duplicate", "peer@example.com",
			json.RawMessage(`{"accountUuid":"duplicate","emailAddress":"peer@example.com"}`),
		),
		[]string{"hostB"},
		func(context.Context, string, ChainStamp, []string) (*creds.Credential, error) {
			pulled = true
			return freshEnvelope("should-not-pull"), nil
		},
		materializeManifest,
	)
	if !errors.Is(err, store.ErrDuplicateAccountUUID) {
		t.Fatalf("duplicate materialize = %v", err)
	}
	if pulled {
		t.Fatal("duplicate materialize pulled a credential")
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
	) (store.PresentationPreparationProof, error) {
		prepared = append(prepared, reservation)
		if label != "peer-u-prepare" {
			t.Fatalf("preparation label = %q", label)
		}
		return store.PresentationPreparationProof{}, wantErr
	}}
	oauthAccount := json.RawMessage(`{"accountUuid":"u-prepare","emailAddress":"prepare@example.com"}`)
	result, err := s.Materialize(
		t.Context(),
		materializeVal("u-prepare", "prepare@example.com", oauthAccount),
		[]string{"hostB"}, pullConst(freshEnvelope("at-prepare")), materializeManifest,
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

func TestMaterializeReprepareFailureCannotPromoteOrAdmit(t *testing.T) {
	s, m, _, _ := newMaterializeService(t)
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("activation changed")
	s.Preparer = fixtureAccountPreparer{
		prepare: func(
			_ context.Context,
			reservation store.PendingAccountReservation,
			_ string,
		) (store.PresentationPreparationProof, error) {
			return materializePreparationProof(reservation, materializePresentationPath(reservation.ID)), nil
		},
		refresh: func(
			context.Context,
			store.Account,
			store.PresentationPreparationProof,
		) (store.PresentationPreparationProof, error) {
			return store.PresentationPreparationProof{}, wantErr
		},
	}
	result, err := s.Materialize(
		t.Context(),
		materializeVal(
			"u-reprepare", "reprepare@example.com",
			json.RawMessage(`{"accountUuid":"u-reprepare","emailAddress":"reprepare@example.com"}`),
		),
		[]string{"hostB"}, pullConst(freshEnvelope("at-reprepare")), materializeManifest,
	)
	if !errors.Is(err, wantErr) || result != (MaterializeResult{}) {
		t.Fatalf("materialize with stale proof = result %+v err %v", result, err)
	}
	if _, err := m.Store.GetAccount(1); !errors.Is(err, store.ErrAccountNotFound) {
		t.Fatalf("account after failed reprepare = %v, want absent", err)
	}
	if _, err := m.Store.AccountPresentation(1); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("proof after failed reprepare = %v, want absent", err)
	}
	health, err := m.Store.GetAuthHealth(1)
	if err != nil || health.NeedsLogin {
		t.Fatalf("auth health after failed reprepare = %+v err=%v", health, err)
	}
}

func TestSyncedAdmissionRefreshesActivationAfterRestart(t *testing.T) {
	s, m, _, _ := newMaterializeService(t)
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	reservation, err := m.ReserveAdd()
	if err != nil {
		t.Fatal(err)
	}
	path := materializePresentationPath(reservation.ID)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	proofA := materializePreparationProof(reservation, path)
	proofA.FileProvider.ActivationGeneration = "activation-A"
	pending, err := m.PrepareReservedSyncedAdd(t.Context(), reservation, proofA)
	if err != nil {
		t.Fatal(err)
	}
	identity := json.RawMessage(`{"accountUuid":"u-restart","emailAddress":"restart@example.com"}`)
	if err := m.WriteIdentity(t.Context(), reservation.ID, path, identity); err != nil {
		t.Fatal(err)
	}
	account, err := m.PromoteSyncedAdd(t.Context(), pending, "peer-restart", "u-restart")
	if err != nil {
		t.Fatal(err)
	}
	envelope := freshEnvelope("at-restart")
	installed, err := m.InstallSyncedCredential(t.Context(), *account, envelope)
	if err != nil || !installed {
		t.Fatalf("install after promotion = %v err=%v", installed, err)
	}

	proofB := proofA
	proofB.FileProvider.ActivationGeneration = "activation-B"
	drifted := proofB
	drifted.FileProvider.PublicPath += "-other"
	if _, err := m.Store.StageSyncedAccountAdmission(
		*account, proofA, drifted, store.SyncedCredentialAdmissionFence{},
	); !errors.Is(err, store.ErrAccountPresentationEvidence) {
		t.Fatalf("drifted admission err=%v", err)
	}
	if presentation, err := m.Store.AccountPresentation(account.ID); err != nil || presentation.Proof != proofA {
		t.Fatalf("proof after refused admission = %+v err=%v", presentation, err)
	}
	if health, err := m.Store.GetAuthHealth(account.ID); err != nil || !health.NeedsLogin {
		t.Fatalf("health after refused admission = %+v err=%v", health, err)
	}
	s.Preparer = fixtureAccountPreparer{refresh: func(
		_ context.Context,
		got store.Account,
		retained store.PresentationPreparationProof,
	) (store.PresentationPreparationProof, error) {
		if got.ID != account.ID || retained != proofA {
			t.Fatalf("refresh input = account %+v proof %+v", got, retained)
		}
		return proofB, nil
	}}
	admitted, err := s.AdmitSyncedAccount(t.Context(), *account, creds.AccessHash(envelope))
	if err != nil || !admitted {
		t.Fatalf("admit after activation rollover = %v err=%v", admitted, err)
	}
	presentation, err := m.Store.AccountPresentation(account.ID)
	if err != nil || presentation.Proof != proofB {
		t.Fatalf("refreshed proof = %+v err=%v, want activation B", presentation, err)
	}
	health, err := m.Store.GetAuthHealth(account.ID)
	if err != nil || health.NeedsLogin || health.Kind != store.AuthKindOwned {
		t.Fatalf("auth health after recovery = %+v err=%v", health, err)
	}
}

// TestMaterializeSeedNoSourceBootstraps pins carry-forward #2: with no
// ~/.claude.json to seed from, the materializer bootstraps a minimal private
// onboarding document so WriteIdentity lands, and the account completes end to end.
func TestMaterializeSeedNoSourceBootstraps(t *testing.T) {
	s, m, _, _ := newMaterializeService(t)
	// No ~/.claude.json written: PrepareAdd reports SeedNoSource.

	oauthAccount := json.RawMessage(`{"accountUuid":"u-nosrc","emailAddress":"nosrc@example.com"}`)
	res, err := s.Materialize(context.Background(), materializeVal("u-nosrc", "nosrc@example.com", oauthAccount), []string{"hostB"}, pullConst(freshEnvelope("at-n")), materializeManifest)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if !res.Bootstrapped {
		t.Fatalf("result = %+v, want Bootstrapped true on the SeedNoSource path", res)
	}
	if res.AccountID != 1 || res.Deferred {
		t.Fatalf("result = %+v, want a completed acct 1", res)
	}

	configDir := materializePresentationPath(1)
	id, err := m.AccountIdentity(t.Context(), 1, configDir)
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
	res, err := s.Materialize(context.Background(), materializeVal("u-file", "file@example.com", oauthAccount), []string{"hostB"}, pullConst(freshEnvelope("at-f")), materializeManifest)
	if !errors.Is(err, pool.ErrCredentialUnverifiable) {
		t.Fatalf("Materialize error = %v, want ErrCredentialUnverifiable", err)
	}
	if res.AccountID != 1 {
		t.Fatalf("result = %+v, want durable awaiting-origin acct 1", res)
	}

	configDir := materializePresentationPath(1)
	if _, ok := fk.Get(creds.ServiceName(configDir), creds.AccountLabel()); ok {
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

// TestMaterializeNoEnvelopeAborts pins the required-envelope rule: a pull with
// no credential rolls the half-built account back — dir gone, reservation
// released, no row — and returns a retryable error.
func TestMaterializeNoEnvelopeAborts(t *testing.T) {
	pullBoom := errors.New("all peers unreachable")
	cases := map[string]PullCredential{
		"pull errors": func(context.Context, string, ChainStamp, []string) (*creds.Credential, error) {
			return nil, pullBoom
		},
		"pull returns nil credential": func(context.Context, string, ChainStamp, []string) (*creds.Credential, error) {
			return nil, nil
		},
	}
	for name, pull := range cases {
		t.Run(name, func(t *testing.T) {
			s, m, _, rec := newMaterializeService(t)
			if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
				t.Fatal(err)
			}
			oauthAccount := json.RawMessage(`{"accountUuid":"u-x","emailAddress":"x@example.com"}`)

			res, err := s.Materialize(context.Background(), materializeVal("u-x", "x@example.com", oauthAccount), []string{"hostB"}, pull, materializeManifest)
			if !errors.Is(err, ErrMaterializeNoEnvelope) {
				t.Fatalf("err = %v, want errors.Is ErrMaterializeNoEnvelope", err)
			}
			if res != (MaterializeResult{}) {
				t.Fatalf("result = %+v, want zero on abort", res)
			}

			// Dir torn down.
			if _, statErr := os.Stat(pool.AccountBackingDir(1)); !os.IsNotExist(statErr) {
				t.Fatalf("account dir stat err = %v, want not-exist (AbandonAdd must remove it)", statErr)
			}
			// Reservation released: the freed index is handed straight back.
			n, rerr := m.Store.ReserveAccountIndex(mustMutationOwner(t, m))
			if rerr != nil {
				t.Fatal(rerr)
			}
			if n.ID != 1 {
				t.Fatalf("next reserved index = %d, want the released 1", n.ID)
			}
			// No row registered.
			accounts, lerr := m.Store.ListAccounts()
			if lerr != nil {
				t.Fatal(lerr)
			}
			if len(accounts) != 0 {
				t.Fatalf("accounts = %+v, want none after abort", accounts)
			}
			// No nudge on a failed materialization.
			if len(rec.calls) != 0 {
				t.Fatalf("nudge calls = %v, want none on abort", rec.calls)
			}
		})
	}
}

// TestMaterializeRejectedEnvelopeReleasesNotAbandons pins the rejection path:
// a tokenless or RT-bearing envelope aborts WITHOUT AbandonAdd — the dir (and
// any retained login state in it) survives, only the reservation is released,
// and no row or nudge lands.
func TestMaterializeRejectedEnvelopeReleasesNotAbandons(t *testing.T) {
	tokenless := &creds.Credential{}
	tokenless.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli()
	rtBearing := cred("at-secret", "rt-secret")
	rtBearing.ClaudeAiOauth.ExpiresAt = tokenless.ClaudeAiOauth.ExpiresAt

	cases := map[string]struct {
		env       *creds.Credential
		wantErrIs error
	}{
		"tokenless envelope":  {tokenless, pool.ErrEnvelopeNoAccessToken},
		"RT-bearing envelope": {rtBearing, pool.ErrEnvelopeCarriesSecret},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, m, fk, rec := newMaterializeService(t)
			if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
				t.Fatal(err)
			}
			oauthAccount := json.RawMessage(`{"accountUuid":"u-rej","emailAddress":"r@example.com"}`)

			res, err := s.Materialize(context.Background(), materializeVal("u-rej", "r@example.com", oauthAccount), []string{"hostB"}, pullConst(tc.env), materializeManifest)
			if !errors.Is(err, tc.wantErrIs) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.wantErrIs)
			}
			if res != (MaterializeResult{}) {
				t.Fatalf("result = %+v, want zero on rejection", res)
			}
			// The dir is kept (release, not abandon) and no credential was written.
			if _, statErr := os.Stat(pool.AccountBackingDir(1)); statErr != nil {
				t.Fatalf("account dir stat err = %v, want kept on rejection", statErr)
			}
			if _, ok := fk.Get(creds.ServiceName(materializePresentationPath(1)), creds.AccountLabel()); ok {
				t.Fatal("a rejected envelope landed in the keychain")
			}
			// Reservation released: the freed index is handed straight back.
			n, rerr := m.Store.ReserveAccountIndex(mustMutationOwner(t, m))
			if rerr != nil {
				t.Fatal(rerr)
			}
			if n.ID != 1 {
				t.Fatalf("next reserved index = %d, want the released 1", n.ID)
			}
			accounts, lerr := m.Store.ListAccounts()
			if lerr != nil {
				t.Fatal(lerr)
			}
			if len(accounts) != 0 {
				t.Fatalf("accounts = %+v, want none after rejection", accounts)
			}
			if len(rec.calls) != 0 {
				t.Fatalf("nudge calls = %v, want none on rejection", rec.calls)
			}
		})
	}
}

// TestMaterializeRejectedEnvelopeThroughRealPullerReleases pins the rejection
// path END TO END: FetchCredential (the production puller) propagates the
// per-peer rejection sentinel, so Materialize takes ReleaseAdd — a credential
// a released `ccp add` login writes mid-pull survives (AbandonAdd would have
// deleted it).
func TestMaterializeRejectedEnvelopeThroughRealPullerReleases(t *testing.T) {
	future := time.Now().Add(2 * time.Hour).UnixMilli()
	tokenless := &creds.Credential{}
	tokenless.ClaudeAiOauth.ExpiresAt = future
	rtBearing := cred("at-secret", "rt-secret")
	rtBearing.ClaudeAiOauth.ExpiresAt = future

	cases := map[string]struct {
		served    *creds.Credential
		wantErrIs error
	}{
		"RT-bearing peer": {rtBearing, pool.ErrEnvelopeCarriesSecret},
		"tokenless peer":  {tokenless, pool.ErrEnvelopeNoAccessToken},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, m, fk, rec := newMaterializeService(t)
			if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
				t.Fatal(err)
			}
			retained := cred("at-login", "rt-login")
			retained.ClaudeAiOauth.ExpiresAt = future
			pull := func(ctx context.Context, uuid string, chain ChainStamp, peers []string) (*creds.Credential, error) {
				// The released add's still-running login lands mid-pull.
				fk.Put(creds.ServiceName(materializePresentationPath(1)), creds.AccountLabel(), retained)
				dial := func(string) syncservice.Transport {
					return envelopeTransport(t, tc.served, creds.AccessHash(tc.served))
				}
				return FetchCredential(ctx, dial, uuid, chain, 0, peers)
			}
			oauthAccount := json.RawMessage(`{"accountUuid":"u-real","emailAddress":"r@example.com"}`)

			res, err := s.Materialize(context.Background(), materializeVal("u-real", "r@example.com", oauthAccount), []string{"hostB"}, pull, materializeManifest)
			if !errors.Is(err, tc.wantErrIs) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.wantErrIs)
			}
			if res != (MaterializeResult{}) {
				t.Fatalf("result = %+v, want zero on rejection", res)
			}
			// ReleaseAdd, not AbandonAdd: dir kept, the login's credential intact.
			if _, statErr := os.Stat(pool.AccountBackingDir(1)); statErr != nil {
				t.Fatalf("account dir stat err = %v, want kept on rejection", statErr)
			}
			got, ok := fk.Get(creds.ServiceName(materializePresentationPath(1)), creds.AccountLabel())
			if !ok || got.ClaudeAiOauth.RefreshToken != "rt-login" {
				t.Fatalf("retained credential = %+v ok=%v, want rt-login intact", got, ok)
			}
			n, rerr := m.Store.ReserveAccountIndex(mustMutationOwner(t, m))
			if rerr != nil {
				t.Fatal(rerr)
			}
			if n.ID != 1 {
				t.Fatalf("next reserved index = %d, want the released 1", n.ID)
			}
			if len(rec.calls) != 0 {
				t.Fatalf("nudge calls = %v, want none on rejection", rec.calls)
			}
		})
	}
}

// TestMaterializePullFailureNeverDestroysConcurrentLogin pins the slot-driven
// rollback for non-sentinel pull failures, through the REAL puller: a hash
// mismatch or an unknown-method peer must not AbandonAdd when a concurrent
// `ccp add` login landed an owned credential mid-pull — the login survives
// and only the reservation is released.
func TestMaterializePullFailureNeverDestroysConcurrentLogin(t *testing.T) {
	cases := map[string]func(t *testing.T) syncservice.Transport{
		"hash-mismatch envelope": func(t *testing.T) syncservice.Transport {
			return envelopeTransport(t, freshEnvelope("at-peer"), "garbage-hash")
		},
		"method not found": func(_ *testing.T) syncservice.Transport {
			return &fakeTransport{do: func(context.Context, *rpc.Request) (*syncservice.Response, error) {
				return &syncservice.Response{OK: false, Error: `unknown method "ccp.fetch_stripped_credential"`}, nil
			}}
		},
	}
	for name, transport := range cases {
		t.Run(name, func(t *testing.T) {
			s, m, fk, rec := newMaterializeService(t)
			if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
				t.Fatal(err)
			}
			owned := cred("at-login", "rt-login")
			owned.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli()
			pull := func(ctx context.Context, uuid string, chain ChainStamp, peers []string) (*creds.Credential, error) {
				// The released add's still-running login lands mid-pull.
				fk.Put(creds.ServiceName(materializePresentationPath(1)), creds.AccountLabel(), owned)
				dial := func(string) syncservice.Transport { return transport(t) }
				return FetchCredential(ctx, dial, uuid, chain, 0, peers)
			}
			oauthAccount := json.RawMessage(`{"accountUuid":"u-pf","emailAddress":"pf@example.com"}`)

			res, err := s.Materialize(context.Background(), materializeVal("u-pf", "pf@example.com", oauthAccount), []string{"hostB"}, pull, materializeManifest)
			if !errors.Is(err, ErrMaterializeNoEnvelope) {
				t.Fatalf("err = %v, want errors.Is ErrMaterializeNoEnvelope", err)
			}
			if res != (MaterializeResult{}) {
				t.Fatalf("result = %+v, want zero on abort", res)
			}
			// ReleaseAdd, not AbandonAdd: dir kept, the login's credential intact.
			if _, statErr := os.Stat(pool.AccountBackingDir(1)); statErr != nil {
				t.Fatalf("account dir stat err = %v, want kept", statErr)
			}
			got, ok := fk.Get(creds.ServiceName(materializePresentationPath(1)), creds.AccountLabel())
			if !ok || got.ClaudeAiOauth.RefreshToken != "rt-login" {
				t.Fatalf("slot credential = %+v ok=%v, want the owned login intact", got, ok)
			}
			n, rerr := m.Store.ReserveAccountIndex(mustMutationOwner(t, m))
			if rerr != nil {
				t.Fatal(rerr)
			}
			if n.ID != 1 {
				t.Fatalf("next reserved index = %d, want the released 1", n.ID)
			}
			accounts, lerr := m.Store.ListAccounts()
			if lerr != nil {
				t.Fatal(lerr)
			}
			if len(accounts) != 0 {
				t.Fatalf("accounts = %+v, want none", accounts)
			}
			if len(rec.calls) != 0 {
				t.Fatalf("nudge calls = %v, want none", rec.calls)
			}
		})
	}
}

// TestMaterializePullFailureUnprovableSlotReleases pins the fail-safe: when a
// pull fails and the slot cannot be proven empty (unsearchable Keychain), the
// dir is released, never abandoned — nothing unprovable is deleted.
func TestMaterializePullFailureUnprovableSlotReleases(t *testing.T) {
	s, m, fk, _ := newMaterializeService(t)
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	pullBoom := errors.New("all peers unreachable")
	pull := func(context.Context, string, ChainStamp, []string) (*creds.Credential, error) {
		// The keychain becomes unsearchable mid-pull.
		fk.KeychainFaults = credstest.Faults{Read: creds.ErrUnavailable}
		return nil, pullBoom
	}
	oauthAccount := json.RawMessage(`{"accountUuid":"u-up","emailAddress":"up@example.com"}`)

	_, err := s.Materialize(context.Background(), materializeVal("u-up", "up@example.com", oauthAccount), []string{"hostB"}, pull, materializeManifest)
	if !errors.Is(err, ErrMaterializeNoEnvelope) {
		t.Fatalf("err = %v, want errors.Is ErrMaterializeNoEnvelope", err)
	}
	if _, statErr := os.Stat(pool.AccountBackingDir(1)); statErr != nil {
		t.Fatalf("account dir stat err = %v, want kept on an unprovable slot", statErr)
	}
	n, rerr := m.Store.ReserveAccountIndex(mustMutationOwner(t, m))
	if rerr != nil {
		t.Fatal(rerr)
	}
	if n.ID != 1 {
		t.Fatalf("next reserved index = %d, want the released 1", n.ID)
	}
}

// TestMaterializeInstallNeverClobbersConcurrentLogin pins the write-time slot
// guard: an owned login landing AFTER the pre-flight retained-slot check (here
// mid-pull, when a valid envelope is about to install) is never overwritten —
// the pass releases with ErrCredentialChangedUnderfoot and the login survives.
func TestMaterializeInstallNeverClobbersConcurrentLogin(t *testing.T) {
	s, m, fk, rec := newMaterializeService(t)
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	owned := cred("at-login", "rt-login")
	owned.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli()
	pull := func(context.Context, string, ChainStamp, []string) (*creds.Credential, error) {
		// The released add's still-running login completes mid-pull, after the
		// pre-flight slot check but before the install write.
		fk.Put(creds.ServiceName(materializePresentationPath(1)), creds.AccountLabel(), owned)
		return freshEnvelope("at-peer"), nil
	}
	oauthAccount := json.RawMessage(`{"accountUuid":"u-race","emailAddress":"race@example.com"}`)

	res, err := s.Materialize(context.Background(), materializeVal("u-race", "race@example.com", oauthAccount), []string{"hostB"}, pull, materializeManifest)
	if !errors.Is(err, pool.ErrCredentialChangedUnderfoot) {
		t.Fatalf("err = %v, want errors.Is ErrCredentialChangedUnderfoot", err)
	}
	if res != (MaterializeResult{}) {
		t.Fatalf("result = %+v, want zero on abort", res)
	}
	// The owned login is intact — neither overwritten nor deleted by AbandonAdd.
	got, ok := fk.Get(creds.ServiceName(materializePresentationPath(1)), creds.AccountLabel())
	if !ok || got.ClaudeAiOauth.RefreshToken != "rt-login" || got.ClaudeAiOauth.AccessToken != "at-login" {
		t.Fatalf("slot credential = %+v ok=%v, want the owned login intact", got, ok)
	}
	// Release, not abandon: dir kept, reservation freed, no row, no nudge.
	if _, statErr := os.Stat(pool.AccountBackingDir(1)); statErr != nil {
		t.Fatalf("account dir stat err = %v, want kept", statErr)
	}
	n, rerr := m.Store.ReserveAccountIndex(mustMutationOwner(t, m))
	if rerr != nil {
		t.Fatal(rerr)
	}
	if n.ID != 1 {
		t.Fatalf("next reserved index = %d, want the released 1", n.ID)
	}
	accounts, lerr := m.Store.ListAccounts()
	if lerr != nil {
		t.Fatal(lerr)
	}
	if len(accounts) != 0 {
		t.Fatalf("accounts = %+v, want none", accounts)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("nudge calls = %v, want none", rec.calls)
	}
}

// TestMaterializeNeverOverwritesRetainedCredential pins the interrupted-add
// guard: a kept dir whose slot retains a usable credential (from a prior
// ReleaseAdd) aborts before writing identity or pulling — the retained
// credential and identity survive intact and the reservation is released.
func TestMaterializeNeverOverwritesRetainedCredential(t *testing.T) {
	s, m, fk, rec := newMaterializeService(t)
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}

	// A kept dir from an interrupted `ccp add`: its own identity + owned credential.
	keptDir := materializePresentationPath(1)
	keptBacking := pool.AccountBackingDir(1)
	if err := os.MkdirAll(keptBacking, 0o700); err != nil {
		t.Fatal(err)
	}
	const keptIdentity = `{"oauthAccount":{"accountUuid":"u-kept","emailAddress":"kept@example.com"}}`
	if err := os.WriteFile(filepath.Join(keptBacking, ".claude.json"), []byte(keptIdentity), 0o600); err != nil {
		t.Fatal(err)
	}
	retained := cred("at-kept", "rt-kept")
	retained.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli()
	fk.Put(creds.ServiceName(keptDir), "claude-login-label", retained)

	pullFatal := PullCredential(func(context.Context, string, ChainStamp, []string) (*creds.Credential, error) {
		t.Fatal("pull invoked despite a retained slot credential")
		return nil, nil
	})
	oauthAccount := json.RawMessage(`{"accountUuid":"u-peer","emailAddress":"peer@example.com"}`)
	res, err := s.Materialize(context.Background(), materializeVal("u-peer", "peer@example.com", oauthAccount), []string{"hostB"}, pullFatal, materializeManifest)
	if err == nil || !strings.Contains(err.Error(), "retains a credential") {
		t.Fatalf("err = %v, want the retained-credential abort", err)
	}
	if res != (MaterializeResult{}) {
		t.Fatalf("result = %+v, want zero on abort", res)
	}

	// Retained credential intact, identity untouched (WriteIdentity never ran).
	got, ok := fk.Get(creds.ServiceName(keptDir), "claude-login-label")
	if !ok || got.ClaudeAiOauth.RefreshToken != "rt-kept" {
		t.Fatalf("retained credential = %+v ok=%v, want rt-kept intact", got, ok)
	}
	// #nosec G304 -- keptDir is a test-controlled temporary account directory.
	raw, err := os.ReadFile(filepath.Join(keptBacking, ".claude.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != keptIdentity {
		t.Fatalf("kept identity mutated: %s", raw)
	}
	// Reservation released, no row, no nudge.
	n, rerr := m.Store.ReserveAccountIndex(mustMutationOwner(t, m))
	if rerr != nil {
		t.Fatal(rerr)
	}
	if n.ID != 1 {
		t.Fatalf("next reserved index = %d, want the released 1", n.ID)
	}
	accounts, lerr := m.Store.ListAccounts()
	if lerr != nil {
		t.Fatal(lerr)
	}
	if len(accounts) != 0 {
		t.Fatalf("accounts = %+v, want none", accounts)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("nudge calls = %v, want none", rec.calls)
	}
}

// TestMaterializeEmptyOAuthDefers pins carry-forward #3: an entry with no
// oauthAccount is deferred — no dir, no reservation, no pull — never an error
// loop, so a later origin publication can supply the identity.
func TestMaterializeEmptyOAuthDefers(t *testing.T) {
	cases := map[string]json.RawMessage{
		"absent": nil,
		"null":   json.RawMessage("null"),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			s, m, _, rec := newMaterializeService(t)
			pullFatal := PullCredential(func(context.Context, string, ChainStamp, []string) (*creds.Credential, error) {
				t.Fatal("pull invoked for a deferred (empty-oauth) entry")
				return nil, nil
			})

			res, err := s.Materialize(context.Background(), materializeVal("u-empty", "e@example.com", raw), []string{"hostB"}, pullFatal, materializeManifest)
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
