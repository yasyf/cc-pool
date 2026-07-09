package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/pool"
)

// TestDaemonRunEReexecsFromStableBin pins the launchd entry's self-exec seam: it
// re-execs from pool.StableBinDir() so the macOS TCC app-group grant survives keg
// upgrades, and on any re-exec failure it loud-logs-and-continues to daemon.Run —
// never fatal. The real syscall.Exec is seam-replaced so no test binary self-execs
// (the fork-storm hazard class the launchd entry must keep unreachable from tests).
func TestDaemonRunEReexecsFromStableBin(t *testing.T) {
	cases := map[string]struct {
		reexecErr error
		wantWarn  bool
	}{
		"clean re-exec runs the daemon with no warning":   {reexecErr: nil, wantWarn: false},
		"re-exec failure warns and still runs the daemon": {reexecErr: errors.New("exec boom"), wantWarn: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tempHome(t)
			var gotDir, gotName string
			swapVar(t, &reexecStable, func(dir, name string) error {
				gotDir, gotName = dir, name
				return tc.reexecErr
			})
			daemonRan := false
			swapVar(t, &daemonRun, func(context.Context) error {
				daemonRan = true
				return nil
			})

			cmd := newDaemonCmd()
			var errBuf bytes.Buffer
			cmd.SetErr(&errBuf)
			cmd.SetContext(context.Background())

			if err := cmd.RunE(cmd, nil); err != nil {
				t.Fatalf("RunE returned error: %v", err)
			}
			if !daemonRan {
				t.Fatal("daemon.Run seam was not invoked; the re-exec path must always fall through to it")
			}
			if gotDir != pool.StableBinDir() || gotName != "cc-pool" {
				t.Errorf("reexecStable(%q, %q), want (%q, %q)", gotDir, gotName, pool.StableBinDir(), "cc-pool")
			}
			out := errBuf.String()
			switch {
			case tc.wantWarn:
				for _, frag := range []string{pool.StableBinDir(), "re-prompt after upgrades"} {
					if !strings.Contains(out, frag) {
						t.Errorf("warning missing %q:\n%s", frag, out)
					}
				}
			case out != "":
				t.Errorf("clean re-exec must not warn; got:\n%s", out)
			}
		})
	}
}
