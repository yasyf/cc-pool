package cli

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/version"
)

func TestRenderCredMoves(t *testing.T) {
	cases := map[string]struct {
		resp     daemon.Response
		explicit bool
		wantErr  string   // substring; "" means success
		wantOut  []string // substrings of (ANSI-stripped) stdout
	}{
		"sweep with busy stragglers": {
			resp: daemon.Response{OK: true, Migrations: []daemon.MigrationResult{
				{ID: 4, Label: "a@x.com", From: "keychain", To: "file", Outcome: daemon.MigrationDone},
				{ID: 5, Label: "b@x.com", To: "file", Outcome: daemon.MigrationAlready},
				{ID: 1, Label: "c@x.com", To: "file", Outcome: daemon.MigrationBusy, Detail: "2 live session(s)"},
			}},
			wantOut: []string{
				"acct-04 (a@x.com) keychain → file",
				"acct-05 (b@x.com) already file",
				"acct-01 (c@x.com) skipped: 2 live session(s)",
				"Moved 1 of 3; 1 busy — re-run `ccp cred move` when their sessions end.",
			},
		},
		"already with cleaned stray shows the detail": {
			resp: daemon.Response{OK: true, Migrations: []daemon.MigrationResult{
				{ID: 2, Label: "b@x.com", To: "keychain", Outcome: daemon.MigrationAlready, Detail: "cleaned a stray file copy"},
			}},
			wantOut: []string{"acct-02 (b@x.com) already keychain — cleaned a stray file copy"},
		},
		"failure exits nonzero": {
			resp: daemon.Response{OK: true, Migrations: []daemon.MigrationResult{
				{ID: 4, From: "file", To: "keychain", Outcome: daemon.MigrationDone},
				{ID: 5, To: "keychain", Outcome: daemon.MigrationFailed, Detail: "keychain state is unknowable in this session"},
			}},
			wantErr: "1 account(s) failed",
			wantOut: []string{
				"acct-04 ((unnamed)) file → keychain",
				"keychain state is unknowable in this session",
				"Moved 1 credential(s).",
			},
		},
		"explicit busy account exits nonzero": {
			resp: daemon.Response{OK: true, Migrations: []daemon.MigrationResult{
				{ID: 6, To: "file", Outcome: daemon.MigrationBusy, Detail: "1 live session(s)"},
			}},
			explicit: true,
			wantErr:  "did not move",
		},
		"explicit already is success": {
			resp: daemon.Response{OK: true, Migrations: []daemon.MigrationResult{
				{ID: 6, To: "file", Outcome: daemon.MigrationAlready},
			}},
			explicit: true,
		},
		"op-level error propagates after truthful rendering": {
			resp: daemon.Response{OK: false, Error: "list accounts: disk I/O", Migrations: []daemon.MigrationResult{
				{ID: 4, From: "keychain", To: "file", Outcome: daemon.MigrationDone},
			}},
			wantErr: "disk I/O",
			wantOut: []string{"keychain → file"},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			cmd := &cobra.Command{}
			cmd.SetOut(&buf)
			err := renderCredMoves(cmd, &tc.resp, tc.explicit)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("renderCredMoves: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tc.wantErr)
			}
			out := stripANSI(buf.String())
			for _, want := range tc.wantOut {
				if !strings.Contains(out, want) {
					t.Errorf("output missing %q:\n%s", want, out)
				}
			}
		})
	}
}

func TestCredMoveFlagValidation(t *testing.T) {
	cases := map[string]struct {
		args    []string
		wantErr string
	}{
		"missing --to":            {[]string{"move"}, `required flag(s) "to" not set`},
		"bogus backend":           {[]string{"move", "--to", "disk"}, `unknown credential backend "disk" (want keychain or file)`},
		"case-sensitive keychain": {[]string{"move", "--to", "Keychain"}, `unknown credential backend "Keychain"`},
		"case-sensitive file":     {[]string{"move", "--to", "FILE"}, `unknown credential backend "FILE"`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			_, err := execCred(t, tc.args...)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestCredMoveDaemonRequest(t *testing.T) {
	cases := map[string]struct {
		args          []string
		healthVersion string // "" means this ccp's version
		resp          daemon.Response
		wantAccount   *int // nil = all accounts
		wantNoRequest bool // the preamble must refuse before sending credmove
		wantErr       string
		wantOut       []string
	}{
		"moves all accounts": {
			args: []string{"move", "--to", "file"},
			resp: daemon.Response{OK: true, Migrations: []daemon.MigrationResult{
				{ID: 1, Label: "a@x.com", From: "keychain", To: "file", Outcome: daemon.MigrationDone},
				{ID: 2, Label: "b@x.com", To: "file", Outcome: daemon.MigrationAlready},
			}},
			wantOut: []string{"acct-01 (a@x.com) keychain → file", "acct-02 (b@x.com) already file", "Moved 1 credential(s)."},
		},
		"account flag scopes the request": {
			args: []string{"move", "--to", "file", "--account", "6"},
			resp: daemon.Response{OK: true, Migrations: []daemon.MigrationResult{
				{ID: 6, Label: "c@x.com", From: "keychain", To: "file", Outcome: daemon.MigrationDone},
			}},
			wantAccount: intp(6),
			wantOut:     []string{"acct-06 (c@x.com) keychain → file"},
		},
		"version skew refused before sending": {
			args:          []string{"move", "--to", "file"},
			healthVersion: "0.0.0-old",
			wantNoRequest: true,
			wantErr:       "the daemon is 0.0.0-old but this ccp is",
		},
		"op-level error surfaces": {
			args:        []string{"move", "--to", "keychain", "--account", "9"},
			resp:        daemon.Response{OK: false, Error: "account 9 not found"},
			wantAccount: intp(9),
			wantErr:     "account 9 not found",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// Short HOME under /tmp: macOS caps sun_path at 104 bytes; t.TempDir's /var/folders path exceeds it.
			home, err := os.MkdirTemp("/tmp", "ccp-home")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(home) })
			t.Setenv("HOME", home)
			seedInitializedPool(t)
			hv := tc.healthVersion
			if hv == "" {
				hv = version.String()
			}
			got := startCredMoveDaemon(t, hv, tc.resp)

			out, err := execCred(t, tc.args...)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("cred move: %v (out=%q)", err, out)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tc.wantErr)
			}
			for _, want := range tc.wantOut {
				if !strings.Contains(out, want) {
					t.Errorf("output missing %q:\n%s", want, out)
				}
			}

			select {
			case req := <-got:
				if tc.wantNoRequest {
					t.Fatalf("credmove request sent despite the refused preamble: %+v", req)
				}
				if req.Op != daemon.OpCredMove {
					t.Errorf("op = %q, want %q", req.Op, daemon.OpCredMove)
				}
				if want := tc.args[2]; req.To != want {
					t.Errorf("request to = %q, want %q", req.To, want)
				}
				switch {
				case tc.wantAccount == nil && req.Account != nil:
					t.Errorf("request account = %d, want nil (all accounts)", *req.Account)
				case tc.wantAccount != nil && (req.Account == nil || *req.Account != *tc.wantAccount):
					t.Errorf("request account = %v, want %d", req.Account, *tc.wantAccount)
				}
				if req.Force {
					t.Error("credmove has no force override; request must not carry one")
				}
			default:
				if !tc.wantNoRequest {
					t.Fatal("no credmove request reached the daemon")
				}
			}
		})
	}
}

func TestCredMoveDaemonUnreachable(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	seedInitializedPool(t)
	_, err := execCred(t, "move", "--to", "file")
	if err == nil {
		t.Fatal("cred move must fail without a daemon")
	}
	for _, want := range []string{"credential moves run inside the daemon", "start it with `ccp service install`"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want substring %q", err, want)
		}
	}
}

func TestCredMoveRegisteredOnRoot(t *testing.T) {
	cmd, _, err := NewRootCmd().Find([]string{"cred", "move"})
	if err != nil || cmd.Name() != "move" {
		t.Fatalf("Find(cred move) = %v, %v; want the move subcommand", cmd, err)
	}
}

// TestCredMoveHelpEncodesMoveSemantics pins the help copy: move-not-copy,
// single-use tokens, the daemon's GUI Keychain reach, and the live-session skip.
func TestCredMoveHelpEncodesMoveSemantics(t *testing.T) {
	long := newCredMoveCmd().Long
	for _, want := range []string{"never a copy", "single-use", "GUI login session", "skipped"} {
		if !strings.Contains(long, want) {
			t.Errorf("cred move help missing %q:\n%s", want, long)
		}
	}
}

func intp(i int) *int { return &i }

// execCred runs `ccp cred <args...>` against buffered output, returning the
// ANSI-stripped combined output and the command error.
func execCred(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := newCredCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return stripANSI(buf.String()), err
}

// seedInitializedPool marks the pool under the isolated HOME as initialized
// ("initialized" is the meta key ccp init writes) so requireDaemon's init
// check passes.
func seedInitializedPool(t *testing.T) {
	t.Helper()
	if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(pool.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetMeta("initialized", "1"); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

// startCredMoveDaemon serves a fake daemon socket: health probes answer with
// healthVersion, credmove requests are captured on the returned channel and
// answered with resp.
func startCredMoveDaemon(t *testing.T, healthVersion string, resp daemon.Response) <-chan daemon.Request {
	t.Helper()
	if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", pool.SocketPath())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	got := make(chan daemon.Request, 4)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			var req daemon.Request
			_ = json.NewDecoder(conn).Decode(&req)
			out := daemon.Response{Proto: daemon.ProtocolVersion, OK: true, Version: healthVersion}
			if req.Op == daemon.OpCredMove {
				got <- req
				out = resp
				out.Proto = daemon.ProtocolVersion
			}
			_ = json.NewEncoder(conn).Encode(out)
			_ = conn.Close()
		}
	}()
	return got
}
