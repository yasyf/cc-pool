package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/fileproviderd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/version"
)

// scriptFPElections shrinks the settle window to test scale and scripts
// tryEnableFP answers in order, holding the last once exhausted; it returns the
// call counter so tests pin exactly how far the bounded retry ran.
func scriptFPElections(t *testing.T, errs []error) *int {
	t.Helper()
	swapVar(t, &fpElectSettleBudget, 250*time.Millisecond)
	swapVar(t, &fpElectSettleInterval, time.Millisecond)
	calls := new(int)
	swapVar(t, &tryEnableFP, func(id string) error {
		if id != pool.FPExtensionBundleID {
			t.Errorf("elected bundle %q, want %q", id, pool.FPExtensionBundleID)
		}
		i := *calls
		*calls++
		if i >= len(errs) {
			i = len(errs) - 1
		}
		return errs[i]
	})
	return calls
}

// TestElectFPForOnboard pins the interactive onboard election: the bounded
// settle wait retries through the post-launch pluginkit registration race and
// stops on the first success (no Settings guidance for a transient blip); once
// the budget closes the LAST error is classified — the Settings-managed
// sentinel prints the loud manual-toggle guidance and opens the pane exactly
// once, any other failure is returned to fail onboard loudly with no spurious
// Settings deep-link.
func TestElectFPForOnboard(t *testing.T) {
	ineffective := fmt.Errorf("elect: %w", fkoverlay.ErrFileProviderElectionIneffective)
	otherErr := errors.New("pluginkit: executable file not found")
	cases := map[string]struct {
		electErrs      []error
		wantErr        bool
		wantSettings   int
		wantCallsExact int  // >0: retry must stop after exactly this many attempts
		wantRetried    bool // a persistent failure must be retried within the budget
		wantFrags      []string
		notFrags       []string
	}{
		"first-attempt success: brief note, no settings": {
			electErrs:      []error{nil},
			wantCallsExact: 1,
			wantFrags:      []string{"enabled"},
			notFrags:       []string{"Settings toggle", "File Providers"},
		},
		"registration race settles: transient failures then success, no guidance": {
			electErrs:      []error{otherErr, ineffective, nil},
			wantCallsExact: 3,
			wantFrags:      []string{"enabled"},
			notFrags:       []string{"Settings toggle"},
		},
		"persistent sentinel after the budget: guidance shown, pane opened once": {
			electErrs:    []error{ineffective},
			wantRetried:  true,
			wantSettings: 1,
			wantFrags:    []string{"Settings toggle", "File Providers"},
		},
		"persistent pluginkit failure fails loud with no settings": {
			electErrs:   []error{otherErr},
			wantRetried: true,
			wantErr:     true,
			notFrags:    []string{"Settings toggle"},
		},
		"the LAST error is classified: exec failure then persistent sentinel is guidance, not loud": {
			electErrs:    []error{otherErr, ineffective},
			wantRetried:  true,
			wantSettings: 1,
			wantFrags:    []string{"Settings toggle"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			calls := scriptFPElections(t, tc.electErrs)
			settings := 0
			swapVar(t, &fpOpenSettings, func(context.Context) error { settings++; return nil })

			var out bytes.Buffer
			err := electFPForOnboard(t.Context(), &out)

			if tc.wantErr && err == nil {
				t.Fatal("electFPForOnboard succeeded; want an error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("electFPForOnboard: %v", err)
			}
			if tc.wantCallsExact > 0 && *calls != tc.wantCallsExact {
				t.Errorf("tryEnableFP called %d times, want exactly %d (retry must stop on success)", *calls, tc.wantCallsExact)
			}
			if tc.wantRetried && *calls < 2 {
				t.Errorf("tryEnableFP called %d times; a persistent failure must be retried within the settle budget", *calls)
			}
			if settings != tc.wantSettings {
				t.Errorf("opened settings %d times, want %d", settings, tc.wantSettings)
			}
			for _, frag := range tc.wantFrags {
				if !strings.Contains(out.String(), frag) {
					t.Errorf("output %q missing %q", out.String(), frag)
				}
			}
			for _, frag := range tc.notFrags {
				if strings.Contains(out.String(), frag) {
					t.Errorf("output %q unexpectedly contains %q", out.String(), frag)
				}
			}
		})
	}
}

// TestSettleFPElectionCancelUnwinds pins that the settle wait honors ^C: a
// canceled context unwinds after the in-flight attempt instead of burning the
// rest of the budget.
func TestSettleFPElectionCancelUnwinds(t *testing.T) {
	calls := scriptFPElections(t, []error{errors.New("appex not registered yet")})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := settleFPElection(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("settleFPElection = %v, want context.Canceled", err)
	}
	if *calls != 1 {
		t.Errorf("tryEnableFP called %d times after cancel, want exactly 1", *calls)
	}
}

// TestRunFPPostInstall pins the non-interactive brew post_install mode: it never
// returns an error (a post_install must not fail the formula), no-ops fast when
// the extension is already enabled, prints the manual install command when the
// host app is missing and brew is absent, elects the appex through the bounded
// settle wait when the app is present (a transient registration-race failure
// settles into success, a budget-exhausted failure prints manual guidance), and
// — unlike interactive onboard — never opens System Settings even on the
// Settings-managed sentinel.
func TestRunFPPostInstall(t *testing.T) {
	ineffective := fmt.Errorf("elect: %w", fkoverlay.ErrFileProviderElectionIneffective)
	otherErr := errors.New("pluginkit: executable file not found")
	cases := map[string]struct {
		fpEnabled    bool // extension already enabled
		appInstalled bool
		brewOnPath   bool
		electErrs    []error
		wantElect    bool // tryEnableFP must be called
		wantFrags    []string
		notFrags     []string
	}{
		"already enabled is a fast no-op": {
			fpEnabled: true,
			electErrs: []error{errors.New("elect must not run when already enabled")},
			wantFrags: []string{"already enabled"},
			notFrags:  []string{"ccp init", "brew install"},
		},
		"app missing and no brew prints the manual command and exits 0": {
			electErrs: []error{errors.New("elect must not run without the host app")},
			wantFrags: []string{"brew install --cask", "ccp fp onboard"},
		},
		"app present and election succeeds needs no settings": {
			appInstalled: true,
			electErrs:    []error{nil},
			wantElect:    true,
			wantFrags:    []string{"enabled", "ccp init"},
		},
		"registration race settles: transient failure then success": {
			appInstalled: true,
			electErrs:    []error{otherErr, nil},
			wantElect:    true,
			wantFrags:    []string{"enabled", "ccp init"},
			notFrags:     []string{"System Settings"},
		},
		"persistent settings-managed sentinel prints the manual command, no settings": {
			appInstalled: true,
			electErrs:    []error{ineffective},
			wantElect:    true,
			wantFrags:    []string{"System Settings", "ccp fp onboard"},
		},
		"persistent pluginkit failure still exits 0 with the manual command": {
			appInstalled: true,
			electErrs:    []error{otherErr},
			wantElect:    true,
			wantFrags:    []string{"ccp fp onboard"},
			notFrags:     []string{"System Settings"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			if !tc.brewOnPath {
				// Empty PATH so exec.LookPath("brew") fails deterministically on any
				// host — a post_install must never actually shell out to brew here.
				t.Setenv("PATH", "")
			}
			swapVar(t, &fpAvailable, func(fkoverlay.Spec) bool { return tc.fpEnabled })
			swapVar(t, &widgetAppInstalled, func() bool { return tc.appInstalled })
			electCalls := scriptFPElections(t, tc.electErrs)
			settings := 0
			swapVar(t, &fpOpenSettings, func(context.Context) error { settings++; return nil })

			cmd := &cobra.Command{}
			cmd.SetContext(t.Context())
			var out bytes.Buffer
			cmd.SetOut(&out)

			if err := runFPPostInstall(cmd); err != nil {
				t.Fatalf("runFPPostInstall returned %v; post_install must always exit 0", err)
			}
			if (*electCalls > 0) != tc.wantElect {
				t.Errorf("tryEnableFP called %d times, wantElect=%v", *electCalls, tc.wantElect)
			}
			if tc.wantElect && *electCalls < len(tc.electErrs) {
				t.Errorf("only %d election attempts for %d scripted results — the settle retry never ran", *electCalls, len(tc.electErrs))
			}
			if settings != 0 {
				t.Errorf("post_install opened System Settings %d times; it must never prompt", settings)
			}
			for _, frag := range tc.wantFrags {
				if !strings.Contains(out.String(), frag) {
					t.Errorf("output %q missing %q", out.String(), frag)
				}
			}
			for _, frag := range tc.notFrags {
				if strings.Contains(out.String(), frag) {
					t.Errorf("output %q unexpectedly contains %q", out.String(), frag)
				}
			}
		})
	}
}

// TestCheckFPRungs pins the bounded control and dial-only bridge rungs. The
// data-plane self-test is exercised separately once those two rungs pass.
func TestCheckFPRungs(t *testing.T) {
	controlErr := errors.New("dial unix: connect: no such file or directory")
	cases := map[string]struct {
		controlUpAfter int // probe calls that fail before Health answers; -1 = never
		bridgeUp       *bool
		daemonAlive    bool
		consentPending bool
		wantErr        []string // substrings of the error; empty = success
		wantNoBridge   bool     // bridge must never be probed
	}{
		"all green first try": {
			controlUpAfter: 0,
			bridgeUp:       ptr(true),
		},
		"control slow then up still passes": {
			controlUpAfter: 3,
			bridgeUp:       ptr(true),
		},
		"control never answers names the app rung and skips the bridge": {
			controlUpAfter: -1,
			wantErr:        []string{"control socket", "CCPoolStatus", "ccp fp onboard"},
			wantNoBridge:   true,
		},
		"bridge down with the daemon dead points at service install": {
			controlUpAfter: 0,
			bridgeUp:       ptr(false),
			daemonAlive:    false,
			wantErr:        []string{"daemon isn't running", "ccp service install"},
		},
		"bridge down with a live daemon points at the consent lever": {
			controlUpAfter: 0,
			bridgeUp:       ptr(false),
			daemonAlive:    true,
			wantErr:        []string{"isn't accepting", "ccp fp consent", "no restart"},
		},
		"nil bridge state from a pre-upgrade daemon prescribes a restart": {
			controlUpAfter: 0,
			bridgeUp:       nil,
			daemonAlive:    true,
			wantErr:        []string{"predates bridge-health reporting", "brew services restart cc-pool"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			controlCalls, bridgeCalls, healthCalls, checkCalls := 0, 0, 0, 0
			swapVar(t, &fpControlHealth, func(context.Context) (string, error) {
				controlCalls++
				if tc.controlUpAfter < 0 || controlCalls <= tc.controlUpAfter {
					return "", controlErr
				}
				return "9.9.9", nil
			})
			// The bridge rung and its stuck-diagnosis both read the daemon's
			// status probe (bridgeUp is its third return) — the CLI never dials
			// the group-container socket.
			swapVar(t, &fpDaemonProbe, func() (bool, bool, *bool) {
				bridgeCalls++
				return tc.daemonAlive, tc.consentPending, tc.bridgeUp
			})
			swapVar(t, &fpDaemonHealth, func() (*daemon.Response, error) {
				healthCalls++
				return &daemon.Response{OK: true, Version: version.String()}, nil
			})
			swapVar(t, &fpBridgeCheck, func() (*daemon.Response, error) {
				checkCalls++
				return &daemon.Response{OK: true, FPBridge: &daemon.FPBridgeStatus{Verdict: daemon.FPBridgeServing}}, nil
			})

			var out bytes.Buffer
			cmd := &cobra.Command{}
			cmd.SetContext(t.Context())
			cmd.SetOut(&out)
			err := checkFPRungs(cmd, time.Millisecond)

			if len(tc.wantErr) == 0 {
				if err != nil {
					t.Fatalf("checkFPRungs: %v", err)
				}
				for _, frag := range []string{"9.9.9", "bridge socket up", "bridge serving"} {
					if !strings.Contains(out.String(), frag) {
						t.Errorf("output %q missing %q", out.String(), frag)
					}
				}
				if healthCalls != 1 || checkCalls != 1 {
					t.Errorf("health calls=%d bridge checks=%d, want 1/1", healthCalls, checkCalls)
				}
				return
			}
			if err == nil {
				t.Fatalf("checkFPRungs succeeded; want an error carrying %q", tc.wantErr)
			}
			for _, frag := range tc.wantErr {
				if !strings.Contains(err.Error(), frag) {
					t.Errorf("error %q missing %q", err, frag)
				}
			}
			if tc.wantNoBridge && bridgeCalls != 0 {
				t.Errorf("bridge probed %d times behind a dead control socket; the app rung is the root fault", bridgeCalls)
			}
			if healthCalls != 0 || checkCalls != 0 {
				t.Errorf("ran data-plane self-test behind a failed rung: health=%d check=%d", healthCalls, checkCalls)
			}
		})
	}
}

func TestCheckFPRungsCancelUnwinds(t *testing.T) {
	swapVar(t, &fpControlHealth, func(context.Context) (string, error) { return "", errors.New("down") })
	swapVar(t, &fpDaemonProbe, func() (bool, bool, *bool) { t.Error("daemon probed after cancel"); return false, false, nil })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetContext(ctx)
	cmd.SetOut(&out)
	if err := checkFPRungs(cmd, time.Millisecond); !errors.Is(err, context.Canceled) {
		t.Fatalf("checkFPRungs = %v, want context.Canceled", err)
	}
}

func TestCheckFPRungsBridgeVerdict(t *testing.T) {
	healthErr := errors.New("health failed")
	checkErr := errors.New("self-test transport failed")
	boundDead := "the daemon's bridge is bound but not serving; restart the daemon (brew services restart cc-pool)"
	cases := map[string]struct {
		healthVersion string
		healthErr     error
		response      *daemon.Response
		checkErr      error
		wantCheck     bool
		wantErr       []string
	}{
		"serving passes": {
			healthVersion: version.String(),
			response:      &daemon.Response{OK: true, FPBridge: &daemon.FPBridgeStatus{Verdict: daemon.FPBridgeServing}},
			wantCheck:     true,
		},
		"health failure stops before self-test": {
			healthErr: healthErr,
			wantErr:   []string{"health check before bridge self-test", "health failed"},
		},
		"old daemon version prescribes restart": {
			healthVersion: "v0.55.0",
			wantErr:       []string{"daemon is v0.55.0", "restart", "bridge self-test"},
		},
		"unknown op prescribes upgraded daemon takeover": {
			healthVersion: version.String(),
			response:      &daemon.Response{Error: "unknown op: fpbridgecheck"},
			wantCheck:     true,
			wantErr:       []string{"predates the bridge self-test", "restart"},
		},
		"transport failure is loud": {
			healthVersion: version.String(),
			checkErr:      checkErr,
			wantCheck:     true,
			wantErr:       []string{"daemon bridge self-test", "transport failed"},
		},
		"operation rejection is loud": {
			healthVersion: version.String(),
			response:      &daemon.Response{Error: "self-test rejected"},
			wantCheck:     true,
			wantErr:       []string{"daemon bridge self-test", "self-test rejected"},
		},
		"missing verdict prescribes restart": {
			healthVersion: version.String(),
			response:      &daemon.Response{OK: true},
			wantCheck:     true,
			wantErr:       []string{"no verdict", "restart"},
		},
		"bound-dead returns the daemon lever verbatim": {
			healthVersion: version.String(),
			response:      &daemon.Response{OK: true, FPBridge: &daemon.FPBridgeStatus{Verdict: daemon.FPBridgeBoundDead, Detail: boundDead}},
			wantCheck:     true,
			wantErr:       []string{boundDead},
		},
		"detail-free non-serving verdict is still loud": {
			healthVersion: version.String(),
			response:      &daemon.Response{OK: true, FPBridge: &daemon.FPBridgeStatus{Verdict: daemon.FPBridgeDown}},
			wantCheck:     true,
			wantErr:       []string{"daemon bridge self-test", "down"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			swapVar(t, &fpControlHealth, func(context.Context) (string, error) { return "9.9.9", nil })
			swapVar(t, &fpDaemonProbe, func() (bool, bool, *bool) { return true, false, ptr(true) })
			healthCalls, checkCalls := 0, 0
			swapVar(t, &fpDaemonHealth, func() (*daemon.Response, error) {
				healthCalls++
				return &daemon.Response{OK: true, Version: tc.healthVersion}, tc.healthErr
			})
			swapVar(t, &fpBridgeCheck, func() (*daemon.Response, error) {
				checkCalls++
				return tc.response, tc.checkErr
			})

			var out bytes.Buffer
			cmd := &cobra.Command{}
			cmd.SetContext(t.Context())
			cmd.SetOut(&out)
			err := checkFPRungs(cmd, time.Millisecond)
			if len(tc.wantErr) == 0 && err != nil {
				t.Fatalf("checkFPRungs: %v", err)
			}
			if len(tc.wantErr) > 0 && err == nil {
				t.Fatal("checkFPRungs succeeded; want an error")
			}
			for _, frag := range tc.wantErr {
				if !strings.Contains(err.Error(), frag) {
					t.Errorf("error %q missing %q", err, frag)
				}
			}
			if healthCalls != 1 {
				t.Errorf("health calls=%d, want 1", healthCalls)
			}
			wantChecks := 0
			if tc.wantCheck {
				wantChecks = 1
			}
			if checkCalls != wantChecks {
				t.Errorf("bridge checks=%d, want %d", checkCalls, wantChecks)
			}
			if len(tc.wantErr) == 0 && !strings.Contains(out.String(), "Daemon bridge serving") {
				t.Errorf("output %q missing serving verdict", out.String())
			}
		})
	}
}

func TestCheckFPRungsRunsConsentInline(t *testing.T) {
	tempHome(t)
	stable := filepath.Join(pool.StableBinDir(), "cc-pool")
	if err := os.MkdirAll(filepath.Dir(stable), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stable, []byte("test"), 0o700); err != nil {
		t.Fatal(err)
	}
	swapVar(t, &fpControlHealth, func(context.Context) (string, error) { return "9.9.9", nil })
	probeCalls := 0
	swapVar(t, &fpDaemonProbe, func() (bool, bool, *bool) {
		probeCalls++
		if probeCalls == 1 {
			return true, true, ptr(false)
		}
		return true, false, ptr(true)
	})
	execCalls := 0
	swapVar(t, &fpConsentProbeExec, func(_ context.Context, path string, _ io.Reader, _, _ io.Writer) error {
		execCalls++
		if path != stable {
			t.Errorf("probe path=%q, want %q", path, stable)
		}
		return nil
	})
	swapVar(t, &fpConsentNow, func() time.Time { return time.Unix(1_700_000_000, 0) })
	swapVar(t, &fpDaemonHealth, func() (*daemon.Response, error) {
		return &daemon.Response{OK: true, Version: version.String()}, nil
	})
	swapVar(t, &fpBridgeCheck, func() (*daemon.Response, error) {
		return &daemon.Response{OK: true, FPBridge: &daemon.FPBridgeStatus{Verdict: daemon.FPBridgeServing}}, nil
	})

	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetContext(t.Context())
	cmd.SetOut(&out)
	if err := checkFPRungs(cmd, time.Nanosecond); err != nil {
		t.Fatalf("checkFPRungs: %v", err)
	}
	if execCalls != 1 {
		t.Errorf("consent probe exec calls=%d, want 1", execCalls)
	}
	for _, frag := range []string{"waiting for its one-time local consent grant", "bridge bound", "Daemon bridge serving"} {
		if !strings.Contains(out.String(), frag) {
			t.Errorf("output %q missing %q", out.String(), frag)
		}
	}
}

// TestFPOnboardRegistered pins the command wiring: `ccp fp onboard` exists
// under the fp group with no args accepted.
func TestFPOnboardRegistered(t *testing.T) {
	root := NewRootCmd()
	fp, _, err := root.Find([]string{"fp", "onboard"})
	if err != nil || fp.Name() != "onboard" {
		t.Fatalf("Find(fp onboard) = (%v, %v), want the onboard command", fp, err)
	}
	if err := fp.Args(fp, []string{"extra"}); err == nil {
		t.Fatal("onboard accepted positional args; want cobra.NoArgs")
	}
	if fp.Flags().Lookup("post-install") == nil {
		t.Fatal("onboard missing the --post-install flag")
	}
}

// TestFPRepairRegistered pins the command wiring: `ccp fp repair` exists under
// the fp group, accepts no positional args, and carries the --account flag.
func TestFPRepairRegistered(t *testing.T) {
	root := NewRootCmd()
	fp, _, err := root.Find([]string{"fp", "repair"})
	if err != nil || fp.Name() != "repair" {
		t.Fatalf("Find(fp repair) = (%v, %v), want the repair command", fp, err)
	}
	if err := fp.Args(fp, []string{"extra"}); err == nil {
		t.Fatal("repair accepted positional args; want cobra.NoArgs")
	}
	if fp.Flags().Lookup("account") == nil {
		t.Fatal("repair missing the --account flag")
	}
	if fp.Flags().Lookup("retreat") == nil {
		t.Fatal("repair missing the --retreat flag")
	}
}

func TestFPConsentCommandsRegistered(t *testing.T) {
	cases := map[string]struct {
		path   []string
		hidden bool
	}{
		"operator command": {path: []string{"fp", "consent"}},
		"probe command":    {path: []string{"fp", "consent-probe"}, hidden: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			root := NewRootCmd()
			cmd, _, err := root.Find(tc.path)
			if err != nil || cmd.Name() != tc.path[len(tc.path)-1] {
				t.Fatalf("Find(%v) = (%v, %v)", tc.path, cmd, err)
			}
			if cmd.Hidden != tc.hidden {
				t.Errorf("Hidden = %v, want %v", cmd.Hidden, tc.hidden)
			}
			if err := cmd.Args(cmd, []string{"extra"}); err == nil {
				t.Fatal("command accepted positional args; want cobra.NoArgs")
			}
		})
	}
}

func TestRunFPConsentProbe(t *testing.T) {
	tempHome(t)
	dir := filepath.Dir(pool.FPBridgeSocketPath())
	probe := filepath.Join(dir, ".consent-probe")
	if err := runFPConsentProbe(); err != nil {
		t.Fatalf("runFPConsentProbe: %v", err)
	}
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		t.Fatalf("bridge directory stat = (%v, %v), want a directory", fi, err)
	}
	if _, err := os.Stat(probe); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("consent probe remains after success: %v", err)
	}
}

type fpDaemonState struct {
	alive, pending bool
	bridgeUp       *bool
}

func TestRunFPConsent(t *testing.T) {
	probeErr := errors.New("probe failed")
	hostErr := errors.New("hostname failed")
	base := time.Unix(1_700_000_000, 0)
	cases := map[string]struct {
		sshConnection string
		sshTTY        string
		hostErr       error
		stable        string
		execErr       error
		states        []fpDaemonState
		times         []time.Time
		cancel        bool
		wantExec      int
		wantProbe     int
		wantErr       []string
		wantOut       []string
		wantIs        error
	}{
		"SSH_CONNECTION refuses with the local host": {
			sshConnection: "client server",
			wantErr:       []string{"local terminal on test-host"},
		},
		"SSH_TTY refuses with the local host": {
			sshTTY:  "/dev/ttys001",
			wantErr: []string{"local terminal on test-host"},
		},
		"hostname failure is loud": {
			sshConnection: "client server",
			hostErr:       hostErr,
			wantErr:       []string{"resolve local hostname"},
			wantIs:        hostErr,
		},
		"missing stable daemon binary points at service install": {
			wantErr: []string{"stable daemon binary", "ccp service install"},
		},
		"a directory is not accepted as the stable daemon binary": {
			stable:  "dir",
			wantErr: []string{"not a regular file", "ccp service install"},
		},
		"probe failure preserves the stable-path identity": {
			stable:   "file",
			execErr:  probeErr,
			wantExec: 1,
			wantErr:  []string{"grant File Provider consent", "~/.cc-pool/bin/cc-pool"},
			wantIs:   probeErr,
		},
		"bridge binds without a daemon restart": {
			stable:    "file",
			states:    []fpDaemonState{{alive: true, bridgeUp: ptr(false)}, {alive: true, bridgeUp: ptr(true)}},
			wantExec:  1,
			wantProbe: 2,
			wantOut:   []string{"bridge bound", "no daemon restart needed"},
		},
		"live daemon expiry points at doctor": {
			stable:    "file",
			states:    []fpDaemonState{{alive: true, bridgeUp: ptr(false)}},
			times:     []time.Time{base, base.Add(fpConsentBridgeWindow)},
			wantExec:  1,
			wantProbe: 1,
			wantErr:   []string{"live daemon", "ccp doctor"},
		},
		"dead daemon expiry points at service install": {
			stable:    "file",
			states:    []fpDaemonState{{bridgeUp: ptr(false)}},
			times:     []time.Time{base, base.Add(fpConsentBridgeWindow)},
			wantExec:  1,
			wantProbe: 1,
			wantErr:   []string{"daemon is not running", "ccp service install"},
		},
		"cancellation unwinds the bridge wait": {
			stable:    "file",
			states:    []fpDaemonState{{alive: true, bridgeUp: ptr(false)}},
			cancel:    true,
			wantExec:  1,
			wantProbe: 1,
			wantIs:    context.Canceled,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			home := tempHome(t)
			t.Setenv("SSH_CONNECTION", tc.sshConnection)
			t.Setenv("SSH_TTY", tc.sshTTY)
			stable := filepath.Join(pool.StableBinDir(), "cc-pool")
			switch tc.stable {
			case "file":
				if err := os.MkdirAll(filepath.Dir(stable), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(stable, []byte("test"), 0o700); err != nil {
					t.Fatal(err)
				}
			case "dir":
				if err := os.MkdirAll(stable, 0o700); err != nil {
					t.Fatal(err)
				}
			}

			var out, errOut bytes.Buffer
			in := strings.NewReader("input")
			cmd := &cobra.Command{}
			cmd.SetIn(in)
			cmd.SetOut(&out)
			cmd.SetErr(&errOut)
			ctx := t.Context()
			if tc.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			cmd.SetContext(ctx)

			swapVar(t, &fpHostname, func() (string, error) { return "test-host", tc.hostErr })
			execCalls := 0
			swapVar(t, &fpConsentProbeExec, func(gotCtx context.Context, gotPath string, gotIn io.Reader, gotOut, gotErr io.Writer) error {
				execCalls++
				if gotCtx != ctx || gotPath != stable {
					t.Errorf("probe exec context/path = (%v, %q), want (%v, %q)", gotCtx, gotPath, ctx, stable)
				}
				if gotIn != in || gotOut != &out || gotErr != &errOut {
					t.Error("probe exec did not inherit command stdio")
				}
				return tc.execErr
			})
			probeCalls := 0
			swapVar(t, &fpDaemonProbe, func() (bool, bool, *bool) {
				probeCalls++
				if len(tc.states) == 0 {
					t.Error("daemon probed before consent probe succeeded")
					return false, false, nil
				}
				i := probeCalls - 1
				if i >= len(tc.states) {
					i = len(tc.states) - 1
				}
				st := tc.states[i]
				return st.alive, st.pending, st.bridgeUp
			})
			nowCalls := 0
			swapVar(t, &fpConsentNow, func() time.Time {
				nowCalls++
				if len(tc.times) == 0 {
					return base
				}
				i := nowCalls - 1
				if i >= len(tc.times) {
					i = len(tc.times) - 1
				}
				return tc.times[i]
			})

			interval := time.Nanosecond
			if tc.cancel {
				interval = time.Hour
			}
			err := runFPConsent(cmd, interval)
			if len(tc.wantErr) == 0 && tc.wantIs == nil && err != nil {
				t.Fatalf("runFPConsent: %v", err)
			}
			if (len(tc.wantErr) > 0 || tc.wantIs != nil) && err == nil {
				t.Fatal("runFPConsent succeeded; want an error")
			}
			for _, frag := range tc.wantErr {
				if !strings.Contains(err.Error(), frag) {
					t.Errorf("error %q missing %q", err, frag)
				}
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Errorf("error %v does not wrap %v", err, tc.wantIs)
			}
			for _, frag := range tc.wantOut {
				if !strings.Contains(out.String(), frag) {
					t.Errorf("output %q missing %q", out.String(), frag)
				}
			}
			if execCalls != tc.wantExec || probeCalls != tc.wantProbe {
				t.Errorf("exec calls=%d daemon probes=%d, want %d/%d", execCalls, probeCalls, tc.wantExec, tc.wantProbe)
			}
			if !strings.HasPrefix(stable, home) {
				t.Fatalf("stable path %q escaped test home %q", stable, home)
			}
		})
	}
}

func TestRunFPRepairDaemonDownRefuses(t *testing.T) {
	tempHome(t)
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(pool.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.UpsertAccount(store.Account{ID: 1, ConfigDir: filepath.Join(pool.AccountsDir(), "acct-01"), OverlayKind: string(fkoverlay.BackendFileProvider), KeychainService: "pool-test", KeychainAccount: "test"}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetMeta("initialized", "1"); err != nil {
		t.Fatal(err)
	}
	m := &pool.Manager{Store: st}
	for _, retreat := range []bool{false, true} {
		t.Run(fmt.Sprintf("retreat=%v", retreat), func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.SetContext(t.Context())
			err := runFPRepair(cmd, m, 1, retreat)
			if err == nil {
				t.Fatal("runFPRepair succeeded without the daemon")
			}
			for _, frag := range []string{"daemon isn't running", "ccp service install"} {
				if !strings.Contains(err.Error(), frag) {
					t.Errorf("error %q missing %q", err, frag)
				}
			}
		})
	}
}

// capResult scripts one fpCapabilityProbe answer.
type capResult struct {
	ok  bool
	err error
}

func scriptCapability(t *testing.T, results []capResult) *int {
	t.Helper()
	reads := new(int)
	swapVar(t, &fpCapabilityProbe, func(context.Context) (bool, error) {
		i := *reads
		*reads++
		if i >= len(results) {
			i = len(results) - 1
		}
		return results[i].ok, results[i].err
	})
	return reads
}

// TestAwaitFPCapability pins the three probe lanes: dial refusal means the app is
// coming up, other errors mean its answering probe is failing, and a definitive
// can't-serve verdict reaches the unbounded Settings-toggle lane.
func TestAwaitFPCapability(t *testing.T) {
	cantServe := fmt.Errorf("companion app cannot control File Provider: %w", fileproviderd.ErrCannotControl)
	cases := map[string]struct {
		results      []capResult
		wantSettings int
		wantFrags    []string
		notFrags     []string
	}{
		"serves on the first probe needs no settings": {
			results:   []capResult{{ok: true}},
			wantFrags: []string{"enabled and serving"},
		},
		"app still coming up then serves stays quiet": {
			results:   []capResult{{err: fileproviderd.ErrAppDialRefused}, {ok: true}},
			wantFrags: []string{"enabled and serving"},
			notFrags:  []string{"app answering", "election is not consent"},
		},
		"plain app-unavailable is an answering failure, not dial refusal": {
			results:   []capResult{{err: fileproviderd.ErrAppUnavailable}, {ok: true}},
			wantFrags: []string{"app answering but capability probe failing", "enabled and serving"},
			notFrags:  []string{"election is not consent"},
		},
		"busy is an answering failure then serves without a settings prompt": {
			results:   []capResult{{err: fileproviderd.ErrBusy}, {ok: true}},
			wantFrags: []string{"app answering but capability probe failing", "enabled and serving"},
			notFrags:  []string{"election is not consent"},
		},
		"ok plus error is an error lane, never false success": {
			results:   []capResult{{ok: true, err: fileproviderd.ErrBusy}, {ok: true}},
			wantFrags: []string{"app answering but capability probe failing", "enabled and serving"},
		},
		"elected but not consented reaches settings then the probe gates success": {
			results:      []capResult{{err: cantServe}, {err: cantServe}, {ok: true}},
			wantSettings: 1,
			wantFrags:    []string{"election is not consent", "File Providers", "enabled and serving"},
		},
		"probe ok=false without error is a can't-serve verdict, not success": {
			results:      []capResult{{ok: false}, {ok: true}},
			wantSettings: 1,
			wantFrags:    []string{"election is not consent", "enabled and serving"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			reads := scriptCapability(t, tc.results)
			settings := 0
			swapVar(t, &fpOpenSettings, func(context.Context) error { settings++; return nil })

			var out bytes.Buffer
			if err := awaitFPCapability(t.Context(), &out, time.Millisecond); err != nil {
				t.Fatalf("awaitFPCapability: %v", err)
			}
			if settings != tc.wantSettings {
				t.Errorf("opened settings %d times, want %d", settings, tc.wantSettings)
			}
			if *reads < len(tc.results) {
				t.Errorf("only %d probe reads for %d scripted results — success was not gated on the probe passing", *reads, len(tc.results))
			}
			for _, frag := range tc.wantFrags {
				if !strings.Contains(out.String(), frag) {
					t.Errorf("output %q missing %q", out.String(), frag)
				}
			}
			for _, frag := range tc.notFrags {
				if strings.Contains(out.String(), frag) {
					t.Errorf("output %q unexpectedly contains %q — a transient blip spuriously narrated the consent walkthrough", out.String(), frag)
				}
			}
		})
	}
}

func TestAwaitFPCapabilityStallLanes(t *testing.T) {
	firstOther := errors.New("first capability failure")
	lastOther := errors.New("last capability failure")
	firstDial := fmt.Errorf("first dial: %w", fileproviderd.ErrAppDialRefused)
	lastDial := fmt.Errorf("last dial: %w", fileproviderd.ErrAppDialRefused)
	cases := map[string]struct {
		results []capResult
		wantIs  error
		wantErr []string
		wantOut []string
	}{
		"dial-refused startup lane expires with launch remediation and last error": {
			results: []capResult{{err: firstDial}, {err: lastDial}},
			wantIs:  fileproviderd.ErrAppDialRefused,
			wantErr: []string{"did not come up within 2m0s", "last dial", "launch", "ccp fp onboard"},
			wantOut: []string{"waiting for the CCPoolStatus app to come up"},
		},
		"answering failure lane expires with doctor remediation and last error": {
			results: []capResult{{err: firstOther}, {err: lastOther}},
			wantIs:  lastOther,
			wantErr: []string{"answering", "kept failing for 2m0s", "last capability failure", "ccp doctor"},
			wantOut: []string{"app answering but capability probe failing"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			scriptCapability(t, tc.results)
			swapVar(t, &fpOpenSettings, func(context.Context) error {
				t.Error("bounded no-verdict lane opened System Settings")
				return nil
			})
			base := time.Unix(1_700_000_000, 0)
			calls := 0
			swapVar(t, &fpCapabilityNow, func() time.Time {
				calls++
				if calls == 1 {
					return base
				}
				return base.Add(fpCapabilityStallWindow)
			})
			var out bytes.Buffer
			err := awaitFPCapability(t.Context(), &out, time.Nanosecond)
			if err == nil {
				t.Fatal("awaitFPCapability succeeded after the stall window")
			}
			if !errors.Is(err, tc.wantIs) {
				t.Errorf("error %v does not wrap last error %v", err, tc.wantIs)
			}
			for _, frag := range tc.wantErr {
				if !strings.Contains(err.Error(), frag) {
					t.Errorf("error %q missing %q", err, frag)
				}
			}
			for _, frag := range tc.wantOut {
				if !strings.Contains(out.String(), frag) {
					t.Errorf("output %q missing %q", out.String(), frag)
				}
			}
		})
	}
}

func TestAwaitFPCapabilityDefinitiveLaneIsUnbounded(t *testing.T) {
	cantServe := fmt.Errorf("cannot control: %w", fileproviderd.ErrCannotControl)
	results := make([]capResult, 100, 101)
	for i := range results {
		results[i] = capResult{err: cantServe}
	}
	results = append(results, capResult{ok: true})
	reads := scriptCapability(t, results)
	settings := 0
	swapVar(t, &fpOpenSettings, func(context.Context) error { settings++; return nil })
	swapVar(t, &fpCapabilityNow, func() time.Time {
		t.Fatal("definitive Settings lane consulted the bounded stall clock")
		return time.Time{}
	})

	var out bytes.Buffer
	if err := awaitFPCapability(t.Context(), &out, time.Nanosecond); err != nil {
		t.Fatalf("awaitFPCapability: %v", err)
	}
	if *reads != len(results) {
		t.Errorf("probe reads=%d, want %d; definitive lane did not remain unbounded", *reads, len(results))
	}
	if settings != 1 {
		t.Errorf("opened settings %d times, want exactly 1", settings)
	}
}

func TestAwaitFPCapabilityDefinitiveVerdictResetsStall(t *testing.T) {
	firstErr := errors.New("first no-verdict")
	secondErr := errors.New("second no-verdict")
	scriptCapability(t, []capResult{
		{err: firstErr},
		{ok: false},
		{err: secondErr},
		{ok: true},
	})
	settings := 0
	swapVar(t, &fpOpenSettings, func(context.Context) error { settings++; return nil })
	base := time.Unix(1_700_000_000, 0)
	calls := 0
	swapVar(t, &fpCapabilityNow, func() time.Time {
		calls++
		if calls == 1 {
			return base
		}
		return base.Add(fpCapabilityStallWindow)
	})

	var out bytes.Buffer
	if err := awaitFPCapability(t.Context(), &out, time.Nanosecond); err != nil {
		t.Fatalf("awaitFPCapability failed after definitive verdict reset the clock: %v", err)
	}
	if settings != 1 {
		t.Errorf("opened settings %d times, want 1", settings)
	}
}

func TestAwaitFPCapabilityCancelUnwinds(t *testing.T) {
	scriptCapability(t, []capResult{{err: errors.New("not consented")}})
	swapVar(t, &fpOpenSettings, func(context.Context) error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	if err := awaitFPCapability(ctx, &out, time.Millisecond); !errors.Is(err, context.Canceled) {
		t.Fatalf("awaitFPCapability = %v, want context.Canceled", err)
	}
}

// TestCheckFPRungsWidgetTooOld pins the min-widget-version gate: a control socket
// that answers with a version older than pool.MinWidgetVersion (too old for the
// probe-domain op the wedge detector and migrate gate rely on) fails the rung
// with the cask-upgrade guidance and never probes the bridge.
func TestCheckFPRungsWidgetTooOld(t *testing.T) {
	swapVar(t, &fpControlHealth, func(context.Context) (string, error) { return "0.1.0", nil })
	swapVar(t, &fpDaemonProbe, func() (bool, bool, *bool) {
		t.Error("bridge probed behind a too-old widget")
		return false, false, nil
	})

	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetContext(t.Context())
	cmd.SetOut(&out)
	err := checkFPRungs(cmd, time.Millisecond)
	if err == nil {
		t.Fatal("checkFPRungs passed a widget too old for probe-domain")
	}
	for _, frag := range []string{"0.1.0", pool.MinWidgetVersion, "brew upgrade", "probe-domain"} {
		if !strings.Contains(err.Error(), frag) {
			t.Errorf("error %q missing %q", err, frag)
		}
	}
}

// TestEnsureWidgetInstalledSkipsWhenPresent pins the brew fix: an already-present
// CCPoolStatus app skips the tap add and the slow brew install/upgrade entirely.
func TestEnsureWidgetInstalledSkipsWhenPresent(t *testing.T) {
	swapVar(t, &widgetAppInstalled, func() bool { return true })
	cmd := &cobra.Command{}
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := ensureWidgetInstalled(cmd); err != nil {
		t.Fatalf("ensureWidgetInstalled: %v", err)
	}
	if !strings.Contains(out.String(), "already installed") {
		t.Errorf("output %q missing the already-installed skip note", out.String())
	}
}

// TestRenderFPServeVerdicts pins the per-account post-migrate probe rendering:
// only MigrationDone rows are probed, a serving/empty/missing domain is ✓, a
// no-verdict is "unverified", and a not-serving domain is ✗.
func TestRenderFPServeVerdicts(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	for id := 1; id <= 4; id++ {
		if err := st.UpsertAccount(store.Account{
			ID: id, ConfigDir: fmt.Sprintf("/p/acct-%02d", id),
			OverlayKind: string(fkoverlay.BackendFileProvider), KeychainService: fmt.Sprintf("s%d", id), KeychainAccount: "u",
		}); err != nil {
			t.Fatal(err)
		}
	}
	swapVar(t, &fpDomainProbeAt, func(dir string) error {
		switch dir {
		case "/p/acct-01":
			return nil // serving
		case "/p/acct-02":
			return fmt.Errorf("%w: hung", overlay.ErrFPProbeWedged)
		case "/p/acct-03":
			return fmt.Errorf("%w: app busy", overlay.ErrFPProbeNoVerdict)
		default:
			t.Errorf("probed a non-migrated row %s", dir)
			return nil
		}
	})
	m := &pool.Manager{Store: st}
	resp := &daemon.Response{Migrations: []daemon.MigrationResult{
		{ID: 1, Outcome: daemon.MigrationDone},
		{ID: 2, Outcome: daemon.MigrationDone},
		{ID: 3, Outcome: daemon.MigrationDone},
		{ID: 4, Outcome: daemon.MigrationAlready}, // not probed
	}}
	cmd := &cobra.Command{}
	var out bytes.Buffer
	cmd.SetOut(&out)
	renderFPServeVerdicts(cmd, m, resp)

	got := out.String()
	for _, want := range []string{"acct-01", "domain serving", "acct-02", "does not serve reads", "acct-03", "unverified"} {
		if !strings.Contains(got, want) {
			t.Errorf("output %q missing %q", got, want)
		}
	}
	if strings.Contains(got, "acct-04") {
		t.Errorf("output probed the already-migrated acct-04: %q", got)
	}
}

// TestBusyMigrationNames pins the busy-account recap fed to the onboard guidance.
func TestBusyMigrationNames(t *testing.T) {
	resp := &daemon.Response{Migrations: []daemon.MigrationResult{
		{ID: 1, Label: "a", Outcome: daemon.MigrationDone},
		{ID: 2, Label: "b", Outcome: daemon.MigrationBusy},
		{ID: 3, Label: "c", Outcome: daemon.MigrationBusy},
	}}
	got := busyMigrationNames(resp)
	if len(got) != 2 || !strings.Contains(got[0], "acct-02") || !strings.Contains(got[1], "acct-03") {
		t.Fatalf("busyMigrationNames = %v, want acct-02 and acct-03", got)
	}
}
