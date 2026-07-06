package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

// TestFPRepairTargets pins the daemon-down target selection: --account picks that
// one File Provider row (error for a non-FP or unknown id), and no --account
// picks every File Provider row (the daemon-down path cannot tell wedged from
// healthy).
func TestFPRepairTargets(t *testing.T) {
	accts := []store.Account{
		{ID: 1, ConfigDir: "/p/acct-01", OverlayKind: string(fkoverlay.BackendFileProvider)},
		{ID: 2, ConfigDir: "/p/acct-02", OverlayKind: string(fkoverlay.BackendSymlink)},
		{ID: 3, ConfigDir: "/p/acct-03", OverlayKind: string(fkoverlay.BackendFileProvider)},
	}
	cases := map[string]struct {
		account int
		wantIDs []int
		wantErr string
	}{
		"no account picks every fp row":   {account: 0, wantIDs: []int{1, 3}},
		"explicit fp account picks it":    {account: 1, wantIDs: []int{1}},
		"explicit symlink account errors": {account: 2, wantErr: "not file provider"},
		"explicit unknown account errors": {account: 9, wantErr: "not found"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := fpRepairTargets(accts, tc.account)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("fpRepairTargets: %v", err)
			}
			var ids []int
			for _, a := range got {
				ids = append(ids, a.ID)
			}
			if fmt.Sprint(ids) != fmt.Sprint(tc.wantIDs) {
				t.Fatalf("target ids = %v, want %v", ids, tc.wantIDs)
			}
		})
	}
}

// fakeFPProvider records Teardown/Setup for the daemon-down direct repair path,
// so the test never registers a real File Provider domain.
type fakeFPProvider struct {
	setups, teardowns int
	setupErr          error
}

func (f *fakeFPProvider) Backend() fkoverlay.Backend    { return fkoverlay.BackendFileProvider }
func (f *fakeFPProvider) PrivateRoot(dir string) string { return fkoverlay.FusePrivateRoot(dir) }
func (f *fakeFPProvider) Health(_, _ string) error      { return nil }
func (f *fakeFPProvider) Sync(_, _ string) error        { return nil }
func (f *fakeFPProvider) Teardown(_, _ string) error    { f.teardowns++; return nil }
func (f *fakeFPProvider) Setup(_, _ string) error       { f.setups++; return f.setupErr }

// TestRepairFPDirect pins the daemon-down direct repair: it re-registers every
// File Provider row (Teardown+Setup) through the injected provider, warns that
// the daemon is down, and surfaces a per-domain failure without stranding the row.
func TestRepairFPDirect(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	for _, a := range []store.Account{
		{ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct-01"), OverlayKind: string(fkoverlay.BackendFileProvider), KeychainService: "s1", KeychainAccount: "u"},
		{ID: 2, ConfigDir: filepath.Join(t.TempDir(), "acct-02"), OverlayKind: string(fkoverlay.BackendSymlink), KeychainService: "s2", KeychainAccount: "u"},
	} {
		if err := st.UpsertAccount(a); err != nil {
			t.Fatal(err)
		}
	}
	m := &pool.Manager{Store: st}

	t.Run("re-registers every fp row and warns the daemon is down", func(t *testing.T) {
		fake := &fakeFPProvider{}
		swapVar(t, &fpOverlayProvider, func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil })
		cmd := &cobra.Command{}
		var out bytes.Buffer
		cmd.SetOut(&out)

		if err := repairFPDirect(cmd, m, 0, false); err != nil {
			t.Fatalf("repairFPDirect: %v", err)
		}
		if fake.setups != 1 || fake.teardowns != 1 {
			t.Fatalf("setups=%d teardowns=%d, want 1/1 (only the one fp row)", fake.setups, fake.teardowns)
		}
		for _, frag := range []string{"daemon is not running", "re-registered"} {
			if !strings.Contains(out.String(), frag) {
				t.Errorf("output %q missing %q", out.String(), frag)
			}
		}
	})

	t.Run("a cannot-control Setup surfaces a failure with the symlink hint", func(t *testing.T) {
		fake := &fakeFPProvider{setupErr: fmt.Errorf("register: %w", fileproviderd.ErrCannotControl)}
		swapVar(t, &fpOverlayProvider, func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil })
		cmd := &cobra.Command{}
		var out bytes.Buffer
		cmd.SetOut(&out)

		if err := repairFPDirect(cmd, m, 1, false); err == nil || !strings.Contains(err.Error(), "failed to re-register") {
			t.Fatalf("repairFPDirect err = %v, want a re-register failure", err)
		}
		if !strings.Contains(out.String(), "cannot serve") {
			t.Errorf("output %q missing the cannot-serve guidance", out.String())
		}
	})
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

// TestAwaitFPCapability pins the truthful consent gate: election is not consent,
// so a "+"-elected extension whose probe still fails REACHES the Settings branch
// (consent explanation + deep link, exactly once) and success is gated on the
// PROBE passing — never on the election. A control socket that is merely still
// coming up (ErrAppUnavailable) is a quiet wait, not a false consent prompt.
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
			results:   []capResult{{err: fileproviderd.ErrAppUnavailable}, {ok: true}},
			wantFrags: []string{"enabled and serving"},
		},
		"busy is transient — quiet wait, then serves, never a spurious settings prompt": {
			results:   []capResult{{err: fileproviderd.ErrBusy}, {ok: true}},
			wantFrags: []string{"enabled and serving"},
			notFrags:  []string{"election is not consent"},
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
	swapVar(t, &fpBridgeReachable, func() bool { t.Error("bridge probed behind a too-old widget"); return false })
	swapVar(t, &fpDaemonProbe, func() (bool, bool) { return true, false })

	var out bytes.Buffer
	err := checkFPRungs(t.Context(), &out, time.Millisecond)
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

// TestRepairFPDirectRetreatErrors pins the daemon-down `--retreat` contract: the
// symlink cutover's live-session gate lives in the daemon, so a daemon-down
// retreat refuses (rather than yank a domain out from under a running session)
// and points at starting the daemon. The account row is left untouched.
func TestRepairFPDirectRetreatErrors(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.UpsertAccount(store.Account{ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct-01"), OverlayKind: string(fkoverlay.BackendFileProvider), KeychainService: "s1", KeychainAccount: "u"}); err != nil {
		t.Fatal(err)
	}
	m := &pool.Manager{Store: st, OverlayFor: func(fkoverlay.Backend) (fkoverlay.Provider, error) {
		t.Error("daemon-down retreat resolved a provider instead of refusing")
		return nil, nil
	}}
	cmd := &cobra.Command{}
	cmd.SetContext(t.Context())
	var out bytes.Buffer
	cmd.SetOut(&out)

	err = repairFPDirect(cmd, m, 1, true)
	if err == nil || !strings.Contains(err.Error(), "needs the daemon") {
		t.Fatalf("repairFPDirect retreat err = %v, want a daemon-required refusal", err)
	}
	got, gerr := st.GetAccount(1)
	if gerr != nil {
		t.Fatal(gerr)
	}
	if got.OverlayKind != string(fkoverlay.BackendFileProvider) {
		t.Errorf("row overlay = %q, want the untouched fileprovider row", got.OverlayKind)
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
