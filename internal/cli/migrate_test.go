package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
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
			err := renderMigrations(cmd, &tc.resp, "fuse", tc.explicit)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("renderMigrations: %v", err)
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

// TestMigrateHelpIsMountSafe pins the rewritten copy: the help names the
// detached mount holder and no longer claims a daemon restart unmounts fuse
// accounts — restarts are mount-safe since the holder landed.
func TestMigrateHelpIsMountSafe(t *testing.T) {
	long := newMigrateCmd().Long
	for _, want := range []string{"mount-holder process", "never disturb them"} {
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
