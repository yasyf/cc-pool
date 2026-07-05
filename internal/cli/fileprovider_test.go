package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestClassifyFPElection pins the trichotomy parse of pluginkit -m output:
// empty means never registered, any '+' line means elected (stale duplicates
// must not mask a live election), anything else registered-but-unelected.
func TestClassifyFPElection(t *testing.T) {
	cases := map[string]struct {
		out  string
		want fpElection
	}{
		"empty output means not registered":    {"", fpNotRegistered},
		"whitespace only means not registered": {"\n  \n", fpNotRegistered},
		"plus is elected":                      {"+    com.yasyf.cc-pool.status.fileprovider(1.2.3)\n", fpElected},
		"minus is registered but not elected":  {"-    com.yasyf.cc-pool.status.fileprovider(1.2.3)\n", fpNotElected},
		"question mark is not elected":         {"?    com.yasyf.cc-pool.status.fileprovider(1.2.3)\n", fpNotElected},
		"bang is not elected":                  {"!    com.yasyf.cc-pool.status.fileprovider(1.2.3)\n", fpNotElected},
		"elected copy among stale duplicates wins": {
			"?    com.yasyf.cc-pool.status.fileprovider(1.2.2)\n+    com.yasyf.cc-pool.status.fileprovider(1.2.3)\n",
			fpElected,
		},
		"duplicates with no elected copy stay unelected": {
			"?    com.yasyf.cc-pool.status.fileprovider(1.2.2)\n-    com.yasyf.cc-pool.status.fileprovider(1.2.3)\n",
			fpNotElected,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := classifyFPElection(tc.out); got != tc.want {
				t.Fatalf("classifyFPElection(%q) = %v, want %v", tc.out, got, tc.want)
			}
		})
	}
}

// scriptElections returns states in order, holding the last one once exhausted.
func scriptElections(t *testing.T, states []fpElection) *int {
	t.Helper()
	reads := new(int)
	swapVar(t, &fpElectionState, func(context.Context) (fpElection, error) {
		i := *reads
		*reads++
		if i >= len(states) {
			i = len(states) - 1
		}
		return states[i], nil
	})
	return reads
}

// TestAwaitFPElection drives the election state machine: already-elected is a
// no-op, an unelected reading gets exactly one headless election, a persistent
// user-disabled hold opens Settings exactly once with the pane named, and
// errors/cancellation unwind cleanly.
func TestAwaitFPElection(t *testing.T) {
	cases := map[string]struct {
		states       []fpElection
		wantElects   int
		wantSettings int
		wantFrags    []string // must appear in output
	}{
		"already elected does nothing": {
			states: []fpElection{fpElected},
		},
		"headless election succeeds without settings": {
			states:     []fpElection{fpNotElected, fpElected},
			wantElects: 1,
		},
		"user-disabled opens settings once and waits for the toggle": {
			states:       []fpElection{fpNotElected, fpNotElected, fpNotElected, fpElected},
			wantElects:   1,
			wantSettings: 1,
			wantFrags:    []string{"File Providers", "Settings toggle"},
		},
		"not registered then registered then elected": {
			states:     []fpElection{fpNotRegistered, fpNotElected, fpElected},
			wantElects: 1,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			reads := scriptElections(t, tc.states)
			elects, settings := 0, 0
			swapVar(t, &fpElect, func(context.Context) error { elects++; return nil })
			swapVar(t, &fpOpenSettings, func(context.Context) error { settings++; return nil })

			var out bytes.Buffer
			if err := awaitFPElection(t.Context(), &out, time.Millisecond); err != nil {
				t.Fatalf("awaitFPElection: %v", err)
			}
			if elects != tc.wantElects || settings != tc.wantSettings {
				t.Errorf("elects=%d settings=%d, want %d/%d", elects, settings, tc.wantElects, tc.wantSettings)
			}
			if *reads < len(tc.states) {
				t.Errorf("only %d probe reads for %d scripted states", *reads, len(tc.states))
			}
			for _, frag := range tc.wantFrags {
				if !strings.Contains(out.String(), frag) {
					t.Errorf("output %q missing %q", out.String(), frag)
				}
			}
		})
	}
}

func TestAwaitFPElectionProbeErrorAborts(t *testing.T) {
	probeErr := errors.New("pluginkit: executable file not found")
	swapVar(t, &fpElectionState, func(context.Context) (fpElection, error) { return fpNotRegistered, probeErr })
	swapVar(t, &fpElect, func(context.Context) error { t.Error("elect ran after a probe failure"); return nil })

	var out bytes.Buffer
	if err := awaitFPElection(t.Context(), &out, time.Millisecond); !errors.Is(err, probeErr) {
		t.Fatalf("awaitFPElection = %v, want %v", err, probeErr)
	}
}

func TestAwaitFPElectionCancelUnwinds(t *testing.T) {
	scriptElections(t, []fpElection{fpNotRegistered})
	swapVar(t, &fpElect, func(context.Context) error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	if err := awaitFPElection(ctx, &out, time.Millisecond); !errors.Is(err, context.Canceled) {
		t.Fatalf("awaitFPElection = %v, want context.Canceled", err)
	}
}

// TestCheckFPRungs pins the rung walk: control then bridge, each with a
// bounded poll, rung-specific stuck messages, no bridge probe behind a dead
// control socket, and the daemon's consent-pending signal upgrading the
// bridge verdict from generic dead-socket to the precise TCC guidance.
func TestCheckFPRungs(t *testing.T) {
	controlErr := errors.New("dial unix: connect: no such file or directory")
	cases := map[string]struct {
		controlUpAfter int // probe calls that fail before Health answers; -1 = never
		bridgeUp       bool
		daemonAlive    bool
		consentPending bool
		wantErr        []string // substrings of the error; empty = success
		wantNoBridge   bool     // bridge must never be probed
	}{
		"all green first try": {
			controlUpAfter: 0,
			bridgeUp:       true,
		},
		"control slow then up still passes": {
			controlUpAfter: 3,
			bridgeUp:       true,
		},
		"control never answers names the app rung and skips the bridge": {
			controlUpAfter: -1,
			wantErr:        []string{"control socket", "CCPoolStatus", "ccp fp onboard"},
			wantNoBridge:   true,
		},
		"bridge down with the daemon dead points at service install": {
			controlUpAfter: 0,
			bridgeUp:       false,
			daemonAlive:    false,
			wantErr:        []string{"daemon isn't running", "ccp service install"},
		},
		"bridge down with consent pending names the TCC prompt": {
			controlUpAfter: 0,
			bridgeUp:       false,
			daemonAlive:    true,
			consentPending: true,
			wantErr:        []string{"app group container consent prompt", "restart the daemon"},
		},
		"bridge down without the signal keeps the generic consent guidance": {
			controlUpAfter: 0,
			bridgeUp:       false,
			daemonAlive:    true,
			wantErr:        []string{"isn't accepting", "consent prompt", "restart the daemon"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			controlCalls, bridgeCalls := 0, 0
			swapVar(t, &fpControlHealth, func(context.Context) (string, error) {
				controlCalls++
				if tc.controlUpAfter < 0 || controlCalls <= tc.controlUpAfter {
					return "", controlErr
				}
				return "9.9.9", nil
			})
			swapVar(t, &fpBridgeReachable, func() bool {
				bridgeCalls++
				return tc.bridgeUp
			})
			swapVar(t, &fpDaemonProbe, func() (bool, bool) { return tc.daemonAlive, tc.consentPending })

			var out bytes.Buffer
			err := checkFPRungs(t.Context(), &out, time.Millisecond)

			if len(tc.wantErr) == 0 {
				if err != nil {
					t.Fatalf("checkFPRungs: %v", err)
				}
				for _, frag := range []string{"9.9.9", "bridge socket up"} {
					if !strings.Contains(out.String(), frag) {
						t.Errorf("output %q missing %q", out.String(), frag)
					}
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
		})
	}
}

func TestCheckFPRungsCancelUnwinds(t *testing.T) {
	swapVar(t, &fpControlHealth, func(context.Context) (string, error) { return "", errors.New("down") })
	swapVar(t, &fpBridgeReachable, func() bool { t.Error("bridge probed after cancel"); return false })
	swapVar(t, &fpDaemonProbe, func() (bool, bool) { t.Error("daemon probed after cancel"); return false, false })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	if err := checkFPRungs(ctx, &out, time.Millisecond); !errors.Is(err, context.Canceled) {
		t.Fatalf("checkFPRungs = %v, want context.Canceled", err)
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
}
