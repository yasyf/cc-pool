package daemon

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/wire"
)

type handlerLifecycle struct{}

func (handlerLifecycle) Health(context.Context) (dkdaemon.Health, error) {
	return dkdaemon.Health{
		Build: version.String(), Protocol: int(wire.ProtocolVersion), PID: os.Getpid(), State: dkdaemon.StateHealthy,
	}, nil
}

func (handlerLifecycle) Shutdown(context.Context) error { return nil }
func (handlerLifecycle) Handoff(context.Context) error  { return nil }

// newCredMoveServer builds the shared test server with existing config dirs
// and a distinct keychain service per account — the fixture's shared service
// would alias every account onto one item, so a move on acct-1 would look
// like a move on acct-2 too.
func newCredMoveServer(t *testing.T) (*Server, map[int]string, *credstest.Fake) {
	t.Helper()
	s, dirs := newTestServer(t)
	for id := range dirs {
		a := acct(t, s, id)
		dir := a.ConfigDir
		dirs[id] = dir
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		a.KeychainService = creds.ServiceName(dir)
		if err := s.m.Store.UpsertAccount(a); err != nil {
			t.Fatal(err)
		}
	}
	return s, dirs, s.m.Creds.(*credstest.Fake)
}

// credFixture builds the credential the tests seed and expect back, expired
// on purpose: a move transfers the credential as-is, so a refresh attempt
// (which would rotate the tokens through fakeOAuth) fails the exact-token
// assertions.
func credFixture() *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.AccessToken = "at-move"
	c.ClaudeAiOauth.RefreshToken = "rt-move"
	c.ClaudeAiOauth.ExpiresAt = 1_700_000_000_000
	return c
}

func acct(t *testing.T, s *Server, id int) store.Account {
	t.Helper()
	a, err := s.m.Store.GetAccount(id)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func credMoveReq(account *int, to string) Request {
	return Request{Op: OpCredMove, Account: account, To: to}
}

func resultByID(t *testing.T, resp Response, id int) CredentialMoveResult {
	t.Helper()
	for _, r := range resp.CredentialMoves {
		if r.ID == id {
			return r
		}
	}
	t.Fatalf("no result for account %d: %+v", id, resp.CredentialMoves)
	return CredentialMoveResult{}
}

func outcomes(resp Response) map[int]CredentialMoveOutcome {
	result := make(map[int]CredentialMoveOutcome, len(resp.CredentialMoves))
	for _, move := range resp.CredentialMoves {
		result[move.ID] = move.Outcome
	}
	return result
}

// credUntouched asserts a's credential still lives in the keychain alone —
// the refused/skipped account's state after any gate fires.
func credUntouched(t *testing.T, fk *credstest.Fake, a store.Account) {
	t.Helper()
	if _, ok := fk.Get(a.KeychainService, a.KeychainAccount); !ok {
		t.Error("keychain item missing after a refused move")
	}
	if fileCredentialExistsForTest(a.ConfigDir) {
		t.Error("file credential appeared despite the refusal")
	}
}

// fileCredEquals asserts dir's file credential carries exactly the seeded
// tokens — moved as-is, never refreshed.
func fileCredEquals(t *testing.T, dir string) {
	t.Helper()
	got, err := readFileCredentialForTest(dir)
	if err != nil {
		t.Fatalf("read moved credential: %v", err)
	}
	want := credFixture()
	if got.ClaudeAiOauth.AccessToken != want.ClaudeAiOauth.AccessToken ||
		got.ClaudeAiOauth.RefreshToken != want.ClaudeAiOauth.RefreshToken ||
		got.ClaudeAiOauth.ExpiresAt != want.ClaudeAiOauth.ExpiresAt {
		t.Errorf("moved credential = %+v, want the seeded tokens (moved as-is)", got.ClaudeAiOauth)
	}
}

// serveHandlerOnSocket binds the production persistent v1 business handlers
// without starting scheduler or reconciliation side effects.
func serveHandlerOnSocket(t *testing.T, s *Server) string {
	t.Helper()
	// macOS caps sun_path at 104 bytes; t.TempDir paths overflow it.
	sockDir, err := os.MkdirTemp("/tmp", "ccp-credmove")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	socket := filepath.Join(sockDir, "d.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ladder, err := operationLadder()
	if err != nil {
		t.Fatal(err)
	}
	server := &wire.Server{
		Build: BusinessBuild, LifecycleBuild: version.String(), Ladder: ladder,
		MaxSessions: 2, ReservedProtectedSessions: 1,
		ProtectedSessionClassifier: buildTestProtectedClassifier{},
	}
	for _, op := range []Op{OpSelect, OpSelectCommit, OpSelectAbort, OpStatus, OpCredMove} {
		op := op
		server.RegisterConcurrent(wire.Op(op), func(ctx context.Context, request wire.Request) (any, error) {
			var payload Request
			if err := decodeStrict(request.Payload, &payload); err != nil {
				return nil, err
			}
			payload.Op = op
			return s.dispatch(ctx, payload), nil
		})
	}
	server.RegisterLifecycle(handlerLifecycle{})
	intake := &drain.Intake{}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx, ln, func() error { return nil }, intake.Admit, intake.AdmitLifecycle)
	}()
	t.Cleanup(func() {
		intake.Close()
		_ = server.CloseIntake()
		_ = intake.Settle(context.Background())
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("serve v1 test socket: %v", err)
		}
	})
	return socket
}

// TestCredMoveEndToEndOverSocket drives keychain->file through the real wire:
// Client.CredMove -> unix socket -> handle -> dispatch -> handleCredMove. A
// missing dispatch case or client op would fail here, not in handler units.
func TestCredMoveEndToEndOverSocket(t *testing.T) {
	s, dirs, fk := newCredMoveServer(t)
	a1, a2 := acct(t, s, 1), acct(t, s, 2)
	fk.Put(a1.KeychainService, a1.KeychainAccount, credFixture())
	fk.Put(a2.KeychainService, a2.KeychainAccount, credFixture())

	cl := &Client{socket: serveHandlerOnSocket(t, s)}
	resp, err := cl.CredMove(nil, "file")
	if err != nil {
		t.Fatalf("CredMove: %v", err)
	}
	if !resp.OK {
		t.Fatalf("resp = %+v, want OK", resp)
	}
	got := outcomes(*resp)
	if got[1] != CredentialMoveDone || got[2] != CredentialMoveDone {
		t.Fatalf("outcomes = %v, want both done", got)
	}
	r := resultByID(t, *resp, 1)
	if r.From != "keychain" || r.To != "file" {
		t.Fatalf("result from/to = %q -> %q, want keychain -> file", r.From, r.To)
	}
	for id, a := range map[int]store.Account{1: a1, 2: a2} {
		if _, ok := fk.Get(a.KeychainService, a.KeychainAccount); ok {
			t.Errorf("acct-%d keychain item still present after the move", id)
		}
		fileCredEquals(t, dirs[id])
	}
}

// TestHandleCredMoveAlreadyOnTarget: a file-backed credential moved to "file"
// reports already and writes nothing — write faults on both backends fail the
// case if any write is attempted.
func TestHandleCredMoveAlreadyOnTarget(t *testing.T) {
	s, dirs, fk := newCredMoveServer(t)
	errNoWrite := errors.New("unexpected write: an already-on-target move must not write")
	fk.KeychainFaults = credstest.Faults{Write: errNoWrite}
	fk.FileFaults = credstest.Faults{Write: errNoWrite}
	a := acct(t, s, 1)
	if err := writeFileCredentialForTest(a.ConfigDir, credFixture()); err != nil {
		t.Fatal(err)
	}

	one := 1
	resp := s.handleCredMove(t.Context(), credMoveReq(&one, "file"))
	if !resp.OK {
		t.Fatalf("credmove failed: %s", resp.Error)
	}
	r := resultByID(t, resp, 1)
	if r.Outcome != CredentialMoveAlready || r.Detail != "" {
		t.Fatalf("result = %+v, want a detail-less already", r)
	}
	if r.From != "file" || r.To != "file" {
		t.Fatalf("result from/to = %q -> %q, want file -> file", r.From, r.To)
	}
	fileCredEquals(t, dirs[1])
	if _, ok := fk.Get(a.KeychainService, a.KeychainAccount); ok {
		t.Error("a keychain item appeared on a file no-op")
	}
}

// TestHandleCredMoveAlreadyOnKeychainCleansStray: the no-op still deletes an
// unreachable file copy (keychain-first resolution shadows it) and says so.
func TestHandleCredMoveAlreadyOnKeychainCleansStray(t *testing.T) {
	s, _, fk := newCredMoveServer(t)
	errNoWrite := errors.New("unexpected write: a stray cleanup must not write")
	fk.KeychainFaults = credstest.Faults{Write: errNoWrite}
	fk.FileFaults = credstest.Faults{Write: errNoWrite}
	a := acct(t, s, 1)
	fk.Put(a.KeychainService, a.KeychainAccount, credFixture())
	if err := writeFileCredentialForTest(a.ConfigDir, credFixture()); err != nil {
		t.Fatal(err)
	}

	one := 1
	resp := s.handleCredMove(t.Context(), credMoveReq(&one, "keychain"))
	if !resp.OK {
		t.Fatalf("credmove failed: %s", resp.Error)
	}
	r := resultByID(t, resp, 1)
	if r.Outcome != CredentialMoveAlready || r.Detail != "cleaned a stray file copy" {
		t.Fatalf("result = %+v, want already with the stray-cleanup detail", r)
	}
	if fileCredentialExistsForTest(a.ConfigDir) {
		t.Error("stray file copy not deleted")
	}
	if _, ok := fk.Get(a.KeychainService, a.KeychainAccount); !ok {
		t.Error("keychain item disturbed by the stray cleanup")
	}
}

// TestHandleCredMoveLiveSessionGate: a live session defers the move, and a
// failed scan fails closed — there is no force override for cred moves.
func TestHandleCredMoveLiveSessionGate(t *testing.T) {
	s, _, fk := newCredMoveServer(t)
	a := acct(t, s, 1)
	fk.Put(a.KeychainService, a.KeychainAccount, credFixture())
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: pool.AccountPresentationDir(a.ID)}}, nil
	}
	if delta := s.heartbeatFor().refresh(t.Context(), 0); !delta.success {
		t.Fatal("heartbeat did not observe the live session")
	}

	one := 1
	resp := s.handleCredMove(t.Context(), credMoveReq(&one, "file"))
	if !resp.OK {
		t.Fatalf("credmove failed: %s", resp.Error)
	}
	r := resultByID(t, resp, 1)
	if r.Outcome != CredentialMoveBusy || !strings.Contains(r.Detail, "held by a live session") {
		t.Fatalf("result = %+v, want busy naming the live session", r)
	}
	if s.cl.held(1) {
		t.Fatal("gate refusal leaked an exclusive claim")
	}
	if got := fk.TouchedServices(); len(got) != 0 {
		t.Fatalf("a gated account's keychain was probed: %v", got)
	}
	credUntouched(t, fk, a)
}

func TestHandleCredMovePendingSelectionGate(t *testing.T) {
	s, _, fk := newCredMoveServer(t)
	a := acct(t, s, 1)
	fk.Put(a.KeychainService, a.KeychainAccount, credFixture())
	token, err := s.cl.beginReservation(a)
	if err != nil {
		t.Fatal(err)
	}
	defer s.cl.abortReservation(token)

	one := 1
	resp := s.handleCredMove(t.Context(), credMoveReq(&one, "file"))
	if !resp.OK {
		t.Fatalf("credmove failed: %s", resp.Error)
	}
	r := resultByID(t, resp, 1)
	if r.Outcome != CredentialMoveBusy || !strings.Contains(r.Detail, "pending selection") {
		t.Fatalf("result = %+v, want busy naming the pending selection", r)
	}
	if got := fk.TouchedServices(); len(got) != 0 {
		t.Fatalf("a reserved account's keychain was probed: %v", got)
	}
}

// TestHandleCredMoveValidation: an unknown target and an unknown account are
// refused before any store is touched.
func TestHandleCredMoveValidation(t *testing.T) {
	s, _, fk := newCredMoveServer(t)
	a := acct(t, s, 1)
	fk.Put(a.KeychainService, a.KeychainAccount, credFixture())

	// The wire vocabulary is exact: no aliases, no case folding.
	for _, to := range []string{"vault", "", "Keychain"} {
		resp := s.handleCredMove(t.Context(), credMoveReq(nil, to))
		if resp.OK || !strings.Contains(resp.Error, "unknown credential target") {
			t.Fatalf("to=%q: resp = %+v, want an unknown-target error", to, resp)
		}
		if len(resp.CredentialMoves) != 0 {
			t.Fatalf("to=%q: unknown target produced results: %+v", to, resp.CredentialMoves)
		}
	}

	nine := 9
	resp := s.handleCredMove(t.Context(), credMoveReq(&nine, "file"))
	if resp.OK || !strings.Contains(resp.Error, "account 9 not found") {
		t.Fatalf("unknown account: %+v", resp)
	}

	if got := fk.TouchedServices(); len(got) != 0 {
		t.Fatalf("validation failures touched credential stores: %v", got)
	}
	credUntouched(t, fk, a)
}

// TestHandleCredMoveAllAccountsMixed: a nil-account fan reports one outcome
// per account — done, already, and failed side by side.
func TestHandleCredMoveAllAccountsMixed(t *testing.T) {
	s, dirs, fk := newCredMoveServer(t)
	dir3 := filepath.Join(t.TempDir(), "acct")
	if err := os.MkdirAll(dir3, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := s.m.Store.UpsertAccount(store.Account{
		ID: 3, ConfigDir: dir3, InstanceID: "instance-3", Generation: 1,
		KeychainService: "svc-acct-3", KeychainAccount: "ccp-test",
	}); err != nil {
		t.Fatal(err)
	}
	a1 := acct(t, s, 1)
	fk.Put(a1.KeychainService, a1.KeychainAccount, credFixture()) // -> done
	a2 := acct(t, s, 2)
	if err := writeFileCredentialForTest(a2.ConfigDir, credFixture()); err != nil { // -> already
		t.Fatal(err)
	}
	fk.Remove(a2.KeychainService, a2.KeychainAccount)
	// acct-3 holds no credential anywhere. -> failed

	resp := s.handleCredMove(t.Context(), credMoveReq(nil, "file"))
	if !resp.OK {
		t.Fatalf("credmove failed: %s", resp.Error)
	}
	if len(resp.CredentialMoves) != 3 {
		t.Fatalf("results = %+v, want one per account", resp.CredentialMoves)
	}
	got := outcomes(resp)
	if got[1] != CredentialMoveDone || got[2] != CredentialMoveAlready || got[3] != CredentialMoveFailed {
		t.Fatalf("outcomes = %v, want done/already/failed", got)
	}
	r1 := resultByID(t, resp, 1)
	if r1.From != "keychain" || r1.To != "file" {
		t.Fatalf("acct-1 from/to = %q -> %q, want keychain -> file", r1.From, r1.To)
	}
	r3 := resultByID(t, resp, 3)
	if !strings.Contains(r3.Detail, "ccp login 3") {
		t.Fatalf("acct-3 detail = %q, want the login hint", r3.Detail)
	}
	if r3.From != "" {
		t.Fatalf("acct-3 from = %q, want empty (no source was ever resolved)", r3.From)
	}

	if _, ok := fk.Get(a1.KeychainService, a1.KeychainAccount); ok {
		t.Error("acct-1 keychain item still present after the move")
	}
	fileCredEquals(t, dirs[1])
	fileCredEquals(t, dirs[2])
	if fileCredentialExistsForTest(dir3) {
		t.Error("acct-3 grew a file credential from nowhere")
	}
}
