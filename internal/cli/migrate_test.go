package cli

import (
	"bytes"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

func TestRenderMigrations(t *testing.T) {
	cases := map[string]struct {
		resp     daemon.Response
		explicit bool
		wantErr  string   // substring; "" means success
		wantOut  []string // substrings of (ANSI-stripped) stdout
	}{
		"sweep with busy stragglers": {
			resp: daemon.Response{OK: true, Migrations: []daemon.MigrationResult{
				{ID: 4, Label: "a@x.com", From: "symlink", To: "fuse", Outcome: daemon.MigrationDone},
				{ID: 5, Label: "b@x.com", To: "fuse", Outcome: daemon.MigrationAlready},
				{ID: 1, Label: "c@x.com", To: "fuse", Outcome: daemon.MigrationBusy, Detail: "3 live session(s)"},
			}},
			wantOut: []string{
				"acct-04 (a@x.com) symlink → fuse",
				"acct-05 (b@x.com) already fuse",
				"acct-01 (c@x.com) skipped: 3 live session(s)",
				"re-run `ccp migrate` when their sessions end",
				"New accounts will use the fuse overlay",
			},
		},
		"failure exits nonzero": {
			resp: daemon.Response{OK: true, Migrations: []daemon.MigrationResult{
				{ID: 4, To: "fuse", Outcome: daemon.MigrationDone, From: "symlink"},
				{ID: 5, To: "fuse", Outcome: daemon.MigrationFailed, Detail: "mount did not come up"},
			}},
			wantErr: "1 account(s) failed",
			wantOut: []string{"mount did not come up"},
		},
		"explicit busy account exits nonzero": {
			resp: daemon.Response{OK: true, Migrations: []daemon.MigrationResult{
				{ID: 6, To: "fuse", Outcome: daemon.MigrationBusy, Detail: "1 live session(s)"},
			}},
			explicit: true,
			wantErr:  "did not migrate",
		},
		"explicit already is success": {
			resp: daemon.Response{OK: true, Migrations: []daemon.MigrationResult{
				{ID: 6, To: "fuse", Outcome: daemon.MigrationAlready},
			}},
			explicit: true,
		},
		"op-level error propagates after truthful rendering": {
			resp: daemon.Response{OK: false, Error: "recording fuse as the default for new accounts failed: disk I/O", Migrations: []daemon.MigrationResult{
				{ID: 4, From: "symlink", To: "fuse", Outcome: daemon.MigrationDone},
			}},
			wantErr: "recording fuse as the default",
			wantOut: []string{"symlink → fuse"},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			cmd := &cobra.Command{}
			cmd.SetOut(&buf)
			for _, r := range tc.resp.Migrations {
				renderMigrationResult(cmd.OutOrStdout(), r)
			}
			err := renderMigrationSummary(cmd, &tc.resp, "fuse", tc.explicit)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("renderMigrationSummary: %v", err)
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

// TestMigrateHelpIsMountSafe pins the help copy: holder-held mounts make daemon restarts mount-safe.
func TestMigrateHelpIsMountSafe(t *testing.T) {
	long := newMigrateCmd().Long
	for _, want := range []string{"fusekit-holder", "never disturb them"} {
		if !strings.Contains(long, want) {
			t.Errorf("migrate help missing %q:\n%s", want, long)
		}
	}
	for _, stale := range []string{"force-unmount", "unmounts any already-migrated", "restart unmounts"} {
		if strings.Contains(long, stale) {
			t.Errorf("migrate help still carries the stale claim %q", stale)
		}
	}
}

// startMigrateDaemon serves a fake daemon socket: health probes answer with this
// ccp's version so requireDaemon passes, and every migrate request is captured on
// the returned channel and answered with a Done result for its account (an
// all-accounts request answers with no migrations, the empty-pool shape).
// gateFailID > 0 makes that account's request answer the machine-wide gate-failure
// shape: OK=false, an op-level Error, and no migrations.
func startMigrateDaemon(t *testing.T, gateFailID int) <-chan daemon.Request {
	t.Helper()
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", pool.SocketPath())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	got := make(chan daemon.Request, 16)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			var req daemon.Request
			_ = json.NewDecoder(conn).Decode(&req)
			out := daemon.Response{Proto: daemon.ProtocolVersion, OK: true, Version: version.String()}
			if req.Op == daemon.OpMigrate {
				got <- req
				switch {
				case req.Account != nil && *req.Account == gateFailID:
					out.OK = false
					out.Error = "fileprovider gate: control socket probe failed"
				case req.Account != nil:
					out.Migrations = []daemon.MigrationResult{
						{ID: *req.Account, Label: "a@x.com", From: "fuse", To: req.To, Outcome: daemon.MigrationDone},
					}
				}
			}
			_ = json.NewEncoder(conn).Encode(out)
			_ = conn.Close()
		}
	}()
	return got
}

// TestMigrateFleetFansOutPerAccount pins the CLI fan-out: a fleet migrate issues
// exactly one daemon Migrate RPC per account (so each gets its own per-account
// budget), and --account scopes to a single RPC. Never one all-accounts RPC —
// that is the budget-starvation shape the storm exposed.
func TestMigrateFleetFansOutPerAccount(t *testing.T) {
	cases := map[string]struct {
		args      []string
		wantAccts []int // exactly one migrate RPC per id, none all-accounts
	}{
		"fleet issues one RPC per account": {
			args:      []string{"--to", "symlink"},
			wantAccts: []int{1, 2, 3},
		},
		"account flag issues exactly one RPC": {
			args:      []string{"--to", "symlink", "--account", "2"},
			wantAccts: []int{2},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tempHome(t)
			seedInitializedPool(t)
			seedAccounts(t,
				store.Account{ID: 1, ConfigDir: "/d1", OverlayKind: "fuse"},
				store.Account{ID: 2, ConfigDir: "/d2", OverlayKind: "fuse"},
				store.Account{ID: 3, ConfigDir: "/d3", OverlayKind: "fuse"},
			)
			reqs := startMigrateDaemon(t, 0)

			cmd := newMigrateCmd()
			var buf bytes.Buffer
			cmd.SetOut(&buf)
			cmd.SetErr(&buf)
			cmd.SetArgs(tc.args)
			if err := cmd.Execute(); err != nil {
				t.Fatalf("migrate: %v (out=%q)", err, stripANSI(buf.String()))
			}

			seen := map[int]int{}
			timeout := time.After(2 * time.Second)
			for range tc.wantAccts {
				select {
				case r := <-reqs:
					if r.Op != daemon.OpMigrate {
						t.Fatalf("op = %q, want migrate", r.Op)
					}
					if r.Account == nil {
						t.Fatal("fan-out sent an all-accounts request; want one RPC per account")
					}
					seen[*r.Account]++
				case <-timeout:
					t.Fatalf("only %d migrate request(s) arrived, want %d", len(seen), len(tc.wantAccts))
				}
			}
			// No request beyond the expected set.
			select {
			case r := <-reqs:
				t.Fatalf("an extra migrate request arrived: %+v", r)
			default:
			}
			for _, id := range tc.wantAccts {
				if seen[id] != 1 {
					t.Fatalf("acct-%d got %d migrate request(s), want exactly 1", id, seen[id])
				}
			}
		})
	}
}

// TestMigrateFleetGateFailureKeepsPriorResults pins the abort shape: when a
// machine-wide gate fails partway through the fan-out, the already-converted
// accounts stay in the aggregate — the user sees their lines AND the tally,
// and the op-level error still fails the command.
func TestMigrateFleetGateFailureKeepsPriorResults(t *testing.T) {
	tempHome(t)
	seedInitializedPool(t)
	seedAccounts(t,
		store.Account{ID: 1, ConfigDir: "/d1", OverlayKind: "fuse"},
		store.Account{ID: 2, ConfigDir: "/d2", OverlayKind: "fuse"},
		store.Account{ID: 3, ConfigDir: "/d3", OverlayKind: "fuse"},
	)
	_ = startMigrateDaemon(t, 3)

	cmd := newMigrateCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--to", "symlink"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("gate failure must fail the command")
	}
	if !strings.Contains(err.Error(), "control socket probe failed") {
		t.Fatalf("error = %q, want the op-level gate error", err)
	}
	out := stripANSI(buf.String())
	for _, want := range []string{"acct-01", "acct-02", "Migrated 2 account(s)."} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q after mid-fleet gate failure:\n%s", want, out)
		}
	}
}

// TestMigrateToValidation pins the CLI --to contract: junk is refused naming
// all three arms, fileprovider is gated on the extension with install
// guidance, and an available fileprovider passes validation (failing only at
// the daemon dial in this test env). The flag default stays fuse — the pool's
// fileprovider flip is an explicit Phase-5 step, never a silent default.
func TestMigrateToValidation(t *testing.T) {
	if def, err := newMigrateCmd().Flags().GetString("to"); err != nil || def != "fuse" {
		t.Fatalf("--to default = %q (err=%v), want fuse", def, err)
	}

	cases := map[string]struct {
		to      string
		avail   bool
		wantErr []string // substrings of the returned error
		notErr  []string // substrings the error must NOT carry
	}{
		"junk refused naming all three arms": {
			to:      "granite",
			wantErr: []string{`unknown overlay kind "granite"`, "want fuse, symlink, or fileprovider"},
		},
		"fileprovider without the extension points at the guided onboard": {
			to:      "fileprovider",
			avail:   false,
			wantErr: []string{"fileprovider is not available", pool.FPExtensionBundleID, pool.WidgetAppPath(), "ccp fp onboard"},
		},
		"fileprovider accepted when the extension is enabled": {
			to:     "fileprovider",
			avail:  true,
			notErr: []string{"unknown overlay kind", "fileprovider is not available"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tempHome(t)
			swapVar(t, &fpAvailable, func(fkoverlay.Spec) bool { return tc.avail })
			cmd := newMigrateCmd()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs([]string{"--to", tc.to})

			err := cmd.Execute()
			if err == nil {
				t.Fatal("migrate succeeded; want an error (no daemon in this test env)")
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q missing %q", err, want)
				}
			}
			for _, not := range tc.notErr {
				if strings.Contains(err.Error(), not) {
					t.Errorf("error %q wrongly carries %q", err, not)
				}
			}
		})
	}
}
