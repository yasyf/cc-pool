package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
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
		"fileprovider without the extension carries install guidance": {
			to:      "fileprovider",
			avail:   false,
			wantErr: []string{"fileprovider is not available", pool.FPExtensionBundleID, pool.WidgetAppPath()},
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
