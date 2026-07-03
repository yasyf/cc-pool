package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
)

// newCredMoveServer builds the shared test server with existing config dirs
// and a distinct keychain service per account — the fixture's shared service
// would alias every account onto one item, so a move on acct-1 would look
// like a move on acct-2 too.
func newCredMoveServer(t *testing.T) (*Server, map[int]string, *credstest.Fake) {
	t.Helper()
	s, dirs := newTestServer(t)
	for id, dir := range dirs {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		a := acct(t, s, id)
		a.KeychainService = fmt.Sprintf("svc-acct-%d", id)
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

func resultByID(t *testing.T, resp Response, id int) MigrationResult {
	t.Helper()
	for _, r := range resp.Migrations {
		if r.ID == id {
			return r
		}
	}
	t.Fatalf("no result for account %d: %+v", id, resp.Migrations)
	return MigrationResult{}
}

// credUntouched asserts a's credential still lives in the keychain alone —
// the refused/skipped account's state after any gate fires.
func credUntouched(t *testing.T, fk *credstest.Fake, a store.Account) {
	t.Helper()
	if _, ok := fk.Get(a.KeychainService, a.KeychainAccount); !ok {
		t.Error("keychain item missing after a refused move")
	}
	if creds.FileCredentialExists(a.ConfigDir) {
		t.Error("file credential appeared despite the refusal")
	}
}

// fileCredEquals asserts dir's file credential carries exactly the seeded
// tokens — moved as-is, never refreshed.
func fileCredEquals(t *testing.T, dir string) {
	t.Helper()
	got, err := creds.ReadFileCredential(dir)
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

// serveHandlerOnSocket binds a real unix socket whose connections flow through
// the production handler (handle -> dispatch), without serve's scheduler and
// reconcile side effects rotating credentials mid-test.
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
	ctx := t.Context()
	var wg sync.WaitGroup
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() { defer wg.Done(); s.handle(ctx, conn) }()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
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
	if !resp.OK || resp.Proto != ProtocolVersion {
		t.Fatalf("resp = %+v, want OK at proto %d", resp, ProtocolVersion)
	}
	got := outcomes(*resp)
	if got[1] != MigrationDone || got[2] != MigrationDone {
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
	if err := creds.WriteFileCredential(a.ConfigDir, credFixture()); err != nil {
		t.Fatal(err)
	}

	one := 1
	resp := s.handleCredMove(t.Context(), credMoveReq(&one, "file"))
	if !resp.OK {
		t.Fatalf("credmove failed: %s", resp.Error)
	}
	r := resultByID(t, resp, 1)
	if r.Outcome != MigrationAlready || r.Detail != "" {
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
	if err := creds.WriteFileCredential(a.ConfigDir, credFixture()); err != nil {
		t.Fatal(err)
	}

	one := 1
	resp := s.handleCredMove(t.Context(), credMoveReq(&one, "keychain"))
	if !resp.OK {
		t.Fatalf("credmove failed: %s", resp.Error)
	}
	r := resultByID(t, resp, 1)
	if r.Outcome != MigrationAlready || r.Detail != "cleaned a stray file copy" {
		t.Fatalf("result = %+v, want already with the stray-cleanup detail", r)
	}
	if creds.FileCredentialExists(a.ConfigDir) {
		t.Error("stray file copy not deleted")
	}
	if _, ok := fk.Get(a.KeychainService, a.KeychainAccount); !ok {
		t.Error("keychain item disturbed by the stray cleanup")
	}
}

// TestHandleCredMoveBusyClaims: every claim kind refuses with the busy reason,
// touches no store, and never releases the claim it did not take.
func TestHandleCredMoveBusyClaims(t *testing.T) {
	cases := map[string]struct {
		hold      func(s *Server) bool
		stillHeld func(s *Server) bool
		release   func(s *Server)
	}{
		"live select reservation": {
			hold:      func(s *Server) bool { return s.tryReserve(1) },
			stillHeld: func(s *Server) bool { return s.reservedCount(1) == 1 },
		},
		"daemon poll claim": {
			hold:      func(s *Server) bool { return s.beginPoll(1) },
			stillHeld: func(s *Server) bool { return !s.beginPoll(1) },
			release:   func(s *Server) { s.endPoll(1) },
		},
		"another conversion": {
			hold:      func(s *Server) bool { return s.beginConvert(1) },
			stillHeld: func(s *Server) bool { return s.isConverting(1) },
			release:   func(s *Server) { s.endConvert(1) },
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, _, fk := newCredMoveServer(t)
			a := acct(t, s, 1)
			fk.Put(a.KeychainService, a.KeychainAccount, credFixture())
			if !tc.hold(s) {
				t.Fatal("claim setup failed on a free account")
			}
			if tc.release != nil {
				defer tc.release(s)
			}

			one := 1
			resp := s.handleCredMove(t.Context(), credMoveReq(&one, "file"))
			if !resp.OK {
				t.Fatalf("credmove failed: %s", resp.Error)
			}
			r := resultByID(t, resp, 1)
			if r.Outcome != MigrationBusy {
				t.Fatalf("outcome = %s (%s), want busy", r.Outcome, r.Detail)
			}
			if r.Detail != "held by a pending select, a daemon poll, or another conversion; retry shortly" {
				t.Fatalf("detail = %q, want the claim-busy reason", r.Detail)
			}
			if !tc.stillHeld(s) {
				t.Fatal("the busy refusal released a claim it did not take")
			}
			if got := fk.TouchedServices(); len(got) != 0 {
				t.Fatalf("a busy account's keychain was probed: %v", got)
			}
			credUntouched(t, fk, a)
		})
	}
}

// TestHandleCredMoveLiveSessionGate: a live session defers the move, and a
// failed scan fails closed — there is no force override for cred moves.
func TestHandleCredMoveLiveSessionGate(t *testing.T) {
	cases := map[string]struct {
		scanKind    string // "live" or "err"
		wantOutcome MigrationOutcome
		wantDetail  string
	}{
		"live session defers":       {scanKind: "live", wantOutcome: MigrationBusy, wantDetail: "1 live session(s)"},
		"scan failure fails closed": {scanKind: "err", wantOutcome: MigrationFailed, wantDetail: "session scan"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, dirs, fk := newCredMoveServer(t)
			a := acct(t, s, 1)
			fk.Put(a.KeychainService, a.KeychainAccount, credFixture())
			switch tc.scanKind {
			case "live":
				s.scanSessions = func(context.Context) ([]procscan.Session, error) {
					return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
				}
			case "err":
				s.scanSessions = func(context.Context) ([]procscan.Session, error) {
					return nil, errors.New("ps exploded")
				}
			}

			one := 1
			resp := s.handleCredMove(t.Context(), credMoveReq(&one, "file"))
			if !resp.OK {
				t.Fatalf("credmove failed: %s", resp.Error)
			}
			r := resultByID(t, resp, 1)
			if r.Outcome != tc.wantOutcome || !strings.Contains(r.Detail, tc.wantDetail) {
				t.Fatalf("result = %+v, want %s with %q", r, tc.wantOutcome, tc.wantDetail)
			}
			if s.isConverting(1) {
				t.Fatal("gate refusal leaked a converting claim")
			}
			if got := fk.TouchedServices(); len(got) != 0 {
				t.Fatalf("a gated account's keychain was probed: %v", got)
			}
			credUntouched(t, fk, a)
		})
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
		if len(resp.Migrations) != 0 {
			t.Fatalf("to=%q: unknown target produced results: %+v", to, resp.Migrations)
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
		ID: 3, ConfigDir: dir3, OverlayKind: "symlink",
		KeychainService: "svc-acct-3", KeychainAccount: "ccp-test",
	}); err != nil {
		t.Fatal(err)
	}
	a1 := acct(t, s, 1)
	fk.Put(a1.KeychainService, a1.KeychainAccount, credFixture()) // -> done
	a2 := acct(t, s, 2)
	if err := creds.WriteFileCredential(a2.ConfigDir, credFixture()); err != nil { // -> already
		t.Fatal(err)
	}
	// acct-3 holds no credential anywhere. -> failed

	resp := s.handleCredMove(t.Context(), credMoveReq(nil, "file"))
	if !resp.OK {
		t.Fatalf("credmove failed: %s", resp.Error)
	}
	if len(resp.Migrations) != 3 {
		t.Fatalf("results = %+v, want one per account", resp.Migrations)
	}
	got := outcomes(resp)
	if got[1] != MigrationDone || got[2] != MigrationAlready || got[3] != MigrationFailed {
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
	if creds.FileCredentialExists(dir3) {
		t.Error("acct-3 grew a file credential from nowhere")
	}
}
