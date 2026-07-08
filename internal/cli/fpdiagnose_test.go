package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/fusekit/fileproviderd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// TestFPDiagnose pins the renderer-agnostic File Provider rung producer that both
// doctor and the add-failure diagnosis share: silent when the opt-in stack is
// unused, the enablement guidance (root fault, no socket probing) when rows exist
// with the extension absent, and per-socket verdicts once it is enabled — including
// the TCC-precise consent detail.
func TestFPDiagnose(t *testing.T) {
	controlErr := errors.New("dial unix: connect: no such file or directory")
	type wantFinding struct {
		label   string
		healthy bool
		frags   []string
	}
	cases := map[string]struct {
		available  bool
		fpRows     int
		healthVer  string
		healthErr  error
		bridgeUp   bool
		consent    bool
		want       []wantFinding
		wantProbes bool
	}{
		"unavailable with zero rows is empty": {
			available: false,
			fpRows:    0,
		},
		"unavailable with rows fails with the onboard pointer and guidance, probes nothing": {
			available: false,
			fpRows:    2,
			want: []wantFinding{
				{"file provider extension", false, []string{
					"2 fileprovider accounts", "ccp fp onboard", pool.WidgetAppPath(), "Login Items & Extensions",
				}},
			},
		},
		"all green renders extension, app, and bridge healthy": {
			available: true,
			fpRows:    1,
			healthVer: "1.2.3",
			bridgeUp:  true,
			want: []wantFinding{
				{"file provider extension", true, []string{pool.FPExtensionBundleID, "1 fileprovider account"}},
				{"file provider app", true, []string{"1.2.3"}},
				{"file provider bridge", true, nil},
			},
			wantProbes: true,
		},
		"dead control socket fails the app line": {
			available: true,
			fpRows:    1,
			healthErr: controlErr,
			bridgeUp:  true,
			want: []wantFinding{
				{"file provider extension", true, []string{"1 fileprovider account"}},
				{"file provider app", false, []string{controlErr.Error(), pool.WidgetAppPath()}},
				{"file provider bridge", true, nil},
			},
			wantProbes: true,
		},
		"unreachable bridge fails with the group-container hint": {
			available: true,
			fpRows:    1,
			healthVer: "1.2.3",
			bridgeUp:  false,
			want: []wantFinding{
				{"file provider extension", true, nil},
				{"file provider app", true, []string{"1.2.3"}},
				{"file provider bridge", false, []string{"app group container", "restart the daemon", "ccp fp onboard"}},
			},
			wantProbes: true,
		},
		"bridge down with the consent signal names the TCC rung precisely": {
			available: true,
			fpRows:    1,
			healthVer: "1.2.3",
			bridgeUp:  false,
			consent:   true,
			want: []wantFinding{
				{"file provider extension", true, nil},
				{"file provider app", true, []string{"1.2.3"}},
				{"file provider bridge", false, []string{
					"parked on the one-time app group container consent prompt",
					"re-asks only if the binary's signing identity changes", "unsigned local build",
					"restart the daemon", "ccp fp onboard",
				}},
			},
			wantProbes: true,
		},
		"extension enabled with zero rows still reports and probes": {
			available: true,
			fpRows:    0,
			healthVer: "1.2.3",
			bridgeUp:  true,
			want: []wantFinding{
				{"file provider extension", true, []string{"0 fileprovider accounts"}},
				{"file provider app", true, []string{"1.2.3"}},
				{"file provider bridge", true, nil},
			},
			wantProbes: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			controlProbes, bridgeProbes := 0, 0
			swapVar(t, &fpAvailable, func(fkoverlay.Spec) bool { return tc.available })
			swapVar(t, &fpControlHealth, func(context.Context) (string, error) {
				controlProbes++
				return tc.healthVer, tc.healthErr
			})
			swapVar(t, &fpBridgeReachable, func() bool {
				bridgeProbes++
				return tc.bridgeUp
			})

			got := fpDiagnose(t.Context(), fkoverlay.Spec{}, tc.fpRows, tc.consent)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d findings %+v, want %d", len(got), got, len(tc.want))
			}
			for i, want := range tc.want {
				if got[i].label != want.label || got[i].healthy != want.healthy {
					t.Errorf("finding[%d] = %+v, want label %q healthy %v", i, got[i], want.label, want.healthy)
				}
				for _, frag := range want.frags {
					if !strings.Contains(got[i].detail, frag) {
						t.Errorf("finding[%d] detail %q missing %q", i, got[i].detail, frag)
					}
				}
			}
			wantProbes := 0
			if tc.wantProbes {
				wantProbes = 1
			}
			if controlProbes != wantProbes || bridgeProbes != wantProbes {
				t.Errorf("control probed %d times, bridge %d, want %d each", controlProbes, bridgeProbes, wantProbes)
			}
		})
	}
}

// TestIsFPSetupFailure pins the sentinel set the add-failure diagnosis keys on:
// every class FileProviderProvider.Setup (EnsureReport/Register + waitDomainServes)
// can surface — including the transient ErrBusy and ErrRegisterFailed a reached,
// entitled app's register can return — classifies as a setup failure, while non-FP
// errors do not. Sentinels are tested wrapped as Setup actually returns them.
func TestIsFPSetupFailure(t *testing.T) {
	cases := map[string]struct {
		err  error
		want bool
	}{
		"domain not serving":          {fileproviderd.ErrDomainNotServing, true},
		"cannot control":              {fileproviderd.ErrCannotControl, true},
		"op unsupported":              {fileproviderd.ErrOpUnsupported, true},
		"app unavailable":             {fileproviderd.ErrAppUnavailable, true},
		"busy":                        {fileproviderd.ErrBusy, true},
		"register failed":             {fileproviderd.ErrRegisterFailed, true},
		"busy wrapped as Setup wraps": {fmt.Errorf("file provider setup acct-01: %w", fileproviderd.ErrBusy), true},
		"register failed wrapped":     {fmt.Errorf("file provider setup acct-01: %w", fileproviderd.ErrRegisterFailed), true},
		"removal unconfirmed":         {fileproviderd.ErrDomainRemovalUnconfirmed, true},
		"removal unconfirmed wrapped": {fmt.Errorf("file provider setup acct-01: %w", fileproviderd.ErrDomainRemovalUnconfirmed), true},
		"non-fp sentinel":             {pool.ErrNotInitialized, false},
		"bare error":                  {errors.New("disk full"), false},
		"nil":                         {nil, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := isFPSetupFailure(tc.err); got != tc.want {
				t.Errorf("isFPSetupFailure(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestDiagnoseFPAddFailure pins the add-failure diagnosis: an FP Setup sentinel
// with the daemon down warns the bridge is down, renders only the unhealthy rungs
// (root fault first), and closes with the onboard pointer — while a non-FP error
// produces zero output and never touches a probe seam.
func TestDiagnoseFPAddFailure(t *testing.T) {
	t.Run("fp sentinel with daemon down warns and renders the unhealthy rungs", func(t *testing.T) {
		swapVar(t, &fpDaemonProbe, func() (bool, bool) { return false, false })
		swapVar(t, &fpAvailable, func(fkoverlay.Spec) bool { return true })
		controlErr := errors.New("connect: no such file or directory")
		swapVar(t, &fpControlHealth, func(context.Context) (string, error) { return "", controlErr })
		swapVar(t, &fpBridgeReachable, func() bool { return false })

		var buf bytes.Buffer
		cmd := &cobra.Command{}
		cmd.SetErr(&buf)
		cmd.SetContext(t.Context())

		diagnoseFPAddFailure(cmd, &pool.Manager{}, fmt.Errorf("set up overlay: %w", fileproviderd.ErrDomainNotServing))

		out := buf.String()
		for _, frag := range []string{
			"the cc-pool daemon isn't running", // daemon-down warn
			"control socket",                   // app rung (unhealthy, root fault via fail)
			"data socket",                      // bridge rung (unhealthy, warn)
			"run `ccp fp onboard` to walk",     // closing note
		} {
			if !strings.Contains(out, frag) {
				t.Errorf("diagnosis missing %q:\n%s", frag, out)
			}
		}
		// The healthy extension rung must NOT appear — only unhealthy findings render.
		if strings.Contains(out, pool.FPExtensionBundleID) {
			t.Errorf("healthy extension rung rendered; only unhealthy findings should:\n%s", out)
		}
	})

	t.Run("orphan-materialization note gates on the deferred-creation sentinels", func(t *testing.T) {
		swapVar(t, &fpDaemonProbe, func() (bool, bool) { return true, false })
		swapVar(t, &fpAvailable, func(fkoverlay.Spec) bool { return true })
		swapVar(t, &fpControlHealth, func(context.Context) (string, error) { return "1.2.3", nil })
		swapVar(t, &fpBridgeReachable, func() bool { return true })

		const orphanNote = "materialize this account's File Provider domain later as an orphan"
		cases := map[string]struct {
			err  error
			want bool
		}{
			"wrapped domain not serving":  {fmt.Errorf("set up overlay: %w", fileproviderd.ErrDomainNotServing), true},
			"wrapped removal unconfirmed": {fmt.Errorf("set up overlay: %w", fileproviderd.ErrDomainRemovalUnconfirmed), true},
			"cannot control omits it":     {fmt.Errorf("set up overlay: %w", fileproviderd.ErrCannotControl), false},
		}
		for name, tc := range cases {
			t.Run(name, func(t *testing.T) {
				var buf bytes.Buffer
				cmd := &cobra.Command{}
				cmd.SetErr(&buf)
				cmd.SetContext(t.Context())

				diagnoseFPAddFailure(cmd, &pool.Manager{}, tc.err)

				if got := strings.Contains(buf.String(), orphanNote); got != tc.want {
					t.Errorf("orphan note present = %v, want %v:\n%s", got, tc.want, buf.String())
				}
			})
		}
	})

	t.Run("non-fp error is a no-op and never probes", func(t *testing.T) {
		for name, err := range map[string]error{
			"not-initialized sentinel": pool.ErrNotInitialized,
			"bare error":               errors.New("disk full"),
		} {
			t.Run(name, func(t *testing.T) {
				swapVar(t, &fpDaemonProbe, func() (bool, bool) {
					t.Error("fpDaemonProbe called for a non-FP failure")
					return false, false
				})
				swapVar(t, &fpAvailable, func(fkoverlay.Spec) bool {
					t.Error("fpAvailable probed for a non-FP failure")
					return false
				})
				var buf bytes.Buffer
				cmd := &cobra.Command{}
				cmd.SetErr(&buf)
				cmd.SetContext(t.Context())

				diagnoseFPAddFailure(cmd, &pool.Manager{}, err)

				if buf.Len() != 0 {
					t.Errorf("non-FP failure produced output: %q", buf.String())
				}
			})
		}
	})
}
