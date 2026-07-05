package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/version"
)

type reportCall struct {
	label   string
	healthy bool
	detail  string
}

func captureReports() (func(string, bool, string), *[]reportCall) {
	var calls []reportCall
	return func(label string, healthy bool, detail string) {
		calls = append(calls, reportCall{label, healthy, detail})
	}, &calls
}

func TestReportHolder(t *testing.T) {
	cur := version.String()
	cases := map[string]struct {
		facts    holderFacts
		fuseRows int
		want     []reportCall
		none     bool
	}{
		"unreachable with fuse rows fails with the cask-install hint": {
			facts:    holderFacts{reachable: false},
			fuseRows: 2,
			want: []reportCall{
				{"mount holder", false, "not running with 2 fuse accounts; install the fusekit-holder cask (`ccp fuse enable`)"},
			},
		},
		"unreachable with no fuse rows says nothing": {
			facts:    holderFacts{reachable: false},
			fuseRows: 0,
			none:     true,
		},
		"reachable holder with no fuse rows just reports its version (no orphan line)": {
			facts:    holderFacts{reachable: true, version: cur},
			fuseRows: 0,
			want:     []reportCall{{"mount holder", true, cur}},
		},
		"a skewed-version holder is just reported, not flagged (separate product)": {
			facts:    holderFacts{reachable: true, version: "0.0.1-old"},
			fuseRows: 1,
			want:     []reportCall{{"mount holder", true, "0.0.1-old"}},
		},
		"healthy current holder passes plainly": {
			facts:    holderFacts{reachable: true, version: cur},
			fuseRows: 1,
			want:     []reportCall{{"mount holder", true, cur}},
		},
		"cached TCC block fails with the settings deep link": {
			facts: holderFacts{
				reachable: true, version: cur,
				cached: &daemon.HolderStatus{TCCError: "grant Network Volumes access", TCCBlockedBackend: fkoverlay.BackendNFS},
			},
			fuseRows: 1,
			want: []reportCall{
				{"mount holder", true, cur},
				{"mount holder grant", false, "grant Network Volumes access — " + fuseGrantHint(fkoverlay.BackendNFS) + " (cc-pool falls back to symlink automatically if the grant never lands)"},
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			report, calls := captureReports()
			reportHolder(tc.facts, tc.fuseRows, report)
			if tc.none {
				if len(*calls) != 0 {
					t.Fatalf("want no reports, got %+v", *calls)
				}
				return
			}
			if len(*calls) != len(tc.want) {
				t.Fatalf("got %d reports %+v, want %d", len(*calls), *calls, len(tc.want))
			}
			for i, want := range tc.want {
				got := (*calls)[i]
				if got.label != want.label || got.healthy != want.healthy || !strings.Contains(got.detail, want.detail) {
					t.Errorf("report[%d] = %+v, want label %q healthy %v detail containing %q", i, got, want.label, want.healthy, want.detail)
				}
			}
		})
	}
}

// TestReportHolderMitigations pins doctor's NFS kernel-panic mitigation check:
// ✓ at exactly pool.MinHolderVersion, ✗ with the brew-upgrade hint below it,
// and silent when the holder is unreachable (reportHolder owns that failure).
func TestReportHolderMitigations(t *testing.T) {
	if pool.MinHolderVersion != "v0.29.0" {
		t.Fatalf("pool.MinHolderVersion = %q; the mitigation floor moved — re-derive this matrix", pool.MinHolderVersion)
	}
	cases := map[string]struct {
		facts       holderFacts
		none        bool
		wantHealthy bool
		wantDetail  []string // every fragment must appear in the detail
	}{
		"holder at exactly the minimum version passes": {
			facts:       holderFacts{reachable: true, version: "v0.29.0"},
			wantHealthy: true,
		},
		"newer holder passes": {
			facts:       holderFacts{reachable: true, version: "v0.30.0"},
			wantHealthy: true,
		},
		"dev holder (locally built, current source) passes": {
			facts:       holderFacts{reachable: true, version: "dev"},
			wantHealthy: true,
		},
		"pre-mux holder fails with observed, required, and the brew hint": {
			facts:      holderFacts{reachable: true, version: "v0.28.0"},
			wantDetail: []string{"v0.28.0", "v0.29.0", "brew upgrade --cask fusekit-holder"},
		},
		"commit-suffixed pre-mux holder still fails": {
			facts:      holderFacts{reachable: true, version: "v0.28.0 (abc1234)"},
			wantDetail: []string{"v0.28.0 (abc1234)", "v0.29.0", "brew upgrade --cask fusekit-holder"},
		},
		"unreachable holder says nothing": {
			facts: holderFacts{reachable: false},
			none:  true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			report, calls := captureReports()
			reportHolderMitigations(tc.facts, report)
			if tc.none {
				if len(*calls) != 0 {
					t.Fatalf("want no reports, got %+v", *calls)
				}
				return
			}
			if len(*calls) != 1 {
				t.Fatalf("got %d reports %+v, want exactly one", len(*calls), *calls)
			}
			got := (*calls)[0]
			if got.label != "holder panic mitigations" {
				t.Errorf("label = %q, want %q", got.label, "holder panic mitigations")
			}
			if got.healthy != tc.wantHealthy {
				t.Errorf("healthy = %v, want %v (detail %q)", got.healthy, tc.wantHealthy, got.detail)
			}
			if tc.wantHealthy && got.detail != "" {
				t.Errorf("passing check carries detail %q, want none", got.detail)
			}
			for _, frag := range tc.wantDetail {
				if !strings.Contains(got.detail, frag) {
					t.Errorf("detail %q missing %q", got.detail, frag)
				}
			}
		})
	}
}

func TestReportCarcasses(t *testing.T) {
	accts := []store.Account{
		{ID: 1, ConfigDir: "/p/acct-01", OverlayKind: string(fkoverlay.BackendNFS)},
		{ID: 2, ConfigDir: "/p/acct-02", OverlayKind: string(fkoverlay.BackendNFS)},
		{ID: 3, ConfigDir: "/p/acct-03", OverlayKind: string(fkoverlay.BackendNFS)},
		{ID: 4, ConfigDir: "/p/acct-04", OverlayKind: string(fkoverlay.BackendSymlink)},
	}
	swapVar(t, &dirMounted, func(dir string) bool {
		return dir != "/p/acct-03"
	})
	swapVar(t, &mountAliveAt, func(_, dir string) bool {
		return dir == "/p/acct-02"
	})

	report, calls := captureReports()
	reportCarcasses(accts, report)

	if len(*calls) != 1 {
		t.Fatalf("got %d reports %+v, want exactly the carcass", len(*calls), *calls)
	}
	got := (*calls)[0]
	if got.label != "acct-01 mount" || got.healthy || !strings.Contains(got.detail, "dead mount (carcass)") {
		t.Errorf("report = %+v, want acct-01 mount flagged as a dead mount (carcass)", got)
	}
}

// TestReportCarcassesBoundedOnParkedProbe proves the carcass verdict stays bounded
// when the aliveness stat (mountAliveAt, the only wedge-prone arm) parks in
// uninterruptible sleep, running the real overlay.StatProbes behind it.
func TestReportCarcassesBoundedOnParkedProbe(t *testing.T) {
	const parkedDir, healthyDir = "/p/acct-01", "/p/acct-02"
	accts := []store.Account{
		{ID: 1, ConfigDir: parkedDir, OverlayKind: string(fkoverlay.BackendNFS)},
		{ID: 2, ConfigDir: healthyDir, OverlayKind: string(fkoverlay.BackendNFS)},
	}
	const probeTimeout = 20 * time.Millisecond
	var aliveProbes overlay.StatProbes[bool]
	release := make(chan struct{})
	swapVar(t, &dirMounted, func(string) bool { return true })
	swapVar(t, &mountAliveAt, func(_, dir string) bool {
		alive, ok := aliveProbes.Do(dir, probeTimeout, func() bool {
			if dir == parkedDir {
				<-release
			}
			return true
		})
		// overlay.MountAliveWithin's fold: an unanswered stat reads NOT alive.
		return ok && alive
	})
	// Unpark and drain the probe body before the seam restores run (cleanups run LIFO).
	t.Cleanup(func() {
		close(release)
		deadline := time.Now().Add(5 * time.Second)
		for aliveProbes.Inflight() != 0 {
			if time.Now().After(deadline) {
				t.Error("parked probe body never drained")
				return
			}
			time.Sleep(time.Millisecond)
		}
	})

	report, calls := captureReports()
	start := time.Now()
	reportCarcasses(accts, report)
	elapsed := time.Since(start)

	// 2s is the production statProbeTimeout; the wide margin over the 20ms fake keeps this unflaky.
	if elapsed >= 2*time.Second {
		t.Fatalf("reportCarcasses took %v against a parked aliveness probe, want a bounded verdict", elapsed)
	}
	if len(*calls) != 1 {
		t.Fatalf("got %d reports %+v, want exactly the parked carcass", len(*calls), *calls)
	}
	got := (*calls)[0]
	if got.label != "acct-01 mount" || got.healthy || !strings.Contains(got.detail, "dead mount (carcass)") {
		t.Errorf("report = %+v, want acct-01 mount flagged as a dead mount (carcass)", got)
	}
}

func TestReportWedges(t *testing.T) {
	accts := []store.Account{
		{ID: 1, ConfigDir: "/p/acct-01", OverlayKind: string(fkoverlay.BackendNFS)},
		{ID: 2, ConfigDir: "/p/acct-02", OverlayKind: string(fkoverlay.BackendSymlink)},
	}
	wedgeIdleDetail := "wedged (serves metadata but hangs reads) — the daemon will remount it"
	// The daemon never force-unmounts a busy mirror: it panics the kernel.
	wedgeBusyDetail := "left mounted under 1 live session(s); relaunch them — the daemon will NOT force-unmount a busy mirror"
	cases := map[string]struct {
		mounts     []mountd.MountInfo
		sessions   []procscan.Session
		probeErr   error
		want       []reportCall
		wantProbed []string
	}{
		"live row with failing deep probe and no sessions reports the idle copy": {
			mounts:     []mountd.MountInfo{{Dir: "/p/acct-01", Base: "/b", Live: true}},
			probeErr:   fmt.Errorf("%w: hung", overlay.ErrProbeWedged),
			want:       []reportCall{{"acct-01 mirror", false, wedgeIdleDetail}},
			wantProbed: []string{"/p/acct-01"},
		},
		"wedged row backing a live session reports the relaunch copy": {
			mounts:     []mountd.MountInfo{{Dir: "/p/acct-01", Base: "/b", Live: true}},
			sessions:   []procscan.Session{{PID: 4242, ConfigDir: "/p/acct-01"}},
			probeErr:   fmt.Errorf("%w: hung", overlay.ErrProbeWedged),
			want:       []reportCall{{"acct-01 mirror", false, wedgeBusyDetail}},
			wantProbed: []string{"/p/acct-01"},
		},
		"live row with missing probe file is silent": {
			mounts:     []mountd.MountInfo{{Dir: "/p/acct-01", Base: "/b", Live: true}},
			probeErr:   fmt.Errorf("%w: /p/acct-01/.ccp-probe", overlay.ErrProbeMissing),
			wantProbed: []string{"/p/acct-01"},
		},
		"live healthy row is silent": {
			mounts:     []mountd.MountInfo{{Dir: "/p/acct-01", Base: "/b", Live: true}},
			wantProbed: []string{"/p/acct-01"},
		},
		"holder unreachable (nil mounts) is silent and never probes": {
			mounts: nil,
		},
		"not-Live row is silent (reportCarcasses' beat)": {
			mounts: []mountd.MountInfo{{Dir: "/p/acct-01", Base: "/b"}},
		},
		"symlink row is never probed or reported": {
			mounts: []mountd.MountInfo{{Dir: "/p/acct-02", Base: "/b", Live: true}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var probed []string
			swapVar(t, &deepProbeAt, func(dir string) error {
				probed = append(probed, dir)
				return tc.probeErr
			})
			report, calls := captureReports()
			reportWedges(accts, tc.mounts, tc.sessions, report)
			if len(*calls) != len(tc.want) {
				t.Fatalf("got %d reports %+v, want %d", len(*calls), *calls, len(tc.want))
			}
			for i, want := range tc.want {
				got := (*calls)[i]
				if got.label != want.label || got.healthy != want.healthy || !strings.Contains(got.detail, want.detail) {
					t.Errorf("report[%d] = %+v, want label %q healthy %v detail containing %q", i, got, want.label, want.healthy, want.detail)
				}
			}
			if len(probed) != len(tc.wantProbed) {
				t.Fatalf("deepProbeAt called with %v, want %v", probed, tc.wantProbed)
			}
			for i, dir := range tc.wantProbed {
				if probed[i] != dir {
					t.Errorf("deepProbeAt call[%d] = %q, want %q", i, probed[i], dir)
				}
			}
		})
	}
}

func TestReportStaleSessions(t *testing.T) {
	mounted := time.Date(2026, 6, 12, 13, 32, 1, 0, time.Local)
	fuse := store.Account{ID: 1, ConfigDir: "/p/acct-01", OverlayKind: string(fkoverlay.BackendNFS)}
	sym := store.Account{ID: 2, ConfigDir: "/p/acct-02", OverlayKind: string(fkoverlay.BackendSymlink)}
	row := func(dir string, at time.Time) mountd.MountInfo {
		mi := mountd.MountInfo{Dir: dir, Base: "/b", Live: true, Epoch: 2}
		if !at.IsZero() {
			mi.MountedAt = at.Unix()
		}
		return mi
	}
	cases := map[string]struct {
		accts    []store.Account
		mounts   []mountd.MountInfo
		sessions []procscan.Session
		want     []reportCall
	}{
		"session predating the mirror is flagged": {
			accts:    []store.Account{fuse},
			mounts:   []mountd.MountInfo{row("/p/acct-01", mounted)},
			sessions: []procscan.Session{{PID: 4242, ConfigDir: "/p/acct-01", StartedAt: mounted.Add(-6 * time.Second)}},
			want: []reportCall{{
				"acct-01 session", false,
				"pid 4242 predates the current mirror (remounted 13:32:01) — it is bound to a yanked mount; relaunch it",
			}},
		},
		"exactly the slack is not flagged (strict >)": {
			accts:    []store.Account{fuse},
			mounts:   []mountd.MountInfo{row("/p/acct-01", mounted)},
			sessions: []procscan.Session{{PID: 4242, ConfigDir: "/p/acct-01", StartedAt: mounted.Add(-staleSessionSlack)}},
		},
		"zero MountedAt (old holder) skips silently": {
			accts:    []store.Account{fuse},
			mounts:   []mountd.MountInfo{row("/p/acct-01", time.Time{})},
			sessions: []procscan.Session{{PID: 4242, ConfigDir: "/p/acct-01", StartedAt: mounted.Add(-time.Hour)}},
		},
		"zero StartedAt (unparseable etime) skips silently": {
			accts:    []store.Account{fuse},
			mounts:   []mountd.MountInfo{row("/p/acct-01", mounted)},
			sessions: []procscan.Session{{PID: 4242, ConfigDir: "/p/acct-01"}},
		},
		"symlink account is skipped": {
			accts:    []store.Account{sym},
			mounts:   []mountd.MountInfo{row("/p/acct-02", mounted)},
			sessions: []procscan.Session{{PID: 4242, ConfigDir: "/p/acct-02", StartedAt: mounted.Add(-time.Hour)}},
		},
		"session born after the mount is not flagged": {
			accts:    []store.Account{fuse},
			mounts:   []mountd.MountInfo{row("/p/acct-01", mounted)},
			sessions: []procscan.Session{{PID: 4242, ConfigDir: "/p/acct-01", StartedAt: mounted.Add(10 * time.Second)}},
		},
		"session on another dir is not flagged": {
			accts:    []store.Account{fuse},
			mounts:   []mountd.MountInfo{row("/p/acct-01", mounted)},
			sessions: []procscan.Session{{PID: 4242, ConfigDir: "/p/elsewhere", StartedAt: mounted.Add(-time.Hour)}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			report, calls := captureReports()
			reportStaleSessions(tc.accts, tc.mounts, tc.sessions, report)
			if len(*calls) != len(tc.want) {
				t.Fatalf("got %d reports %+v, want %d", len(*calls), *calls, len(tc.want))
			}
			for i, want := range tc.want {
				got := (*calls)[i]
				if got.label != want.label || got.healthy != want.healthy || !strings.Contains(got.detail, want.detail) {
					t.Errorf("report[%d] = %+v, want label %q healthy %v detail containing %q", i, got, want.label, want.healthy, want.detail)
				}
			}
		})
	}
}

// TestReportFileProvider pins doctor's File Provider stack section: silent on
// machines that never opted in, the Enablement guidance (and no socket
// probing behind the root fault) when fileprovider rows exist with the
// extension absent, and per-socket verdicts once the extension is enabled.
func TestReportFileProvider(t *testing.T) {
	fpRow := func(id int) store.Account {
		return store.Account{ID: id, ConfigDir: fmt.Sprintf("/p/acct-%02d", id), OverlayKind: string(fkoverlay.BackendFileProvider)}
	}
	symRow := store.Account{ID: 8, ConfigDir: "/p/acct-08", OverlayKind: string(fkoverlay.BackendSymlink)}
	nfsRow := store.Account{ID: 9, ConfigDir: "/p/acct-09", OverlayKind: string(fkoverlay.BackendNFS)}
	controlErr := errors.New("dial unix: connect: no such file or directory")

	type wantReport struct {
		label   string
		healthy bool
		frags   []string // every fragment must appear in the detail
	}
	cases := map[string]struct {
		available  bool
		accts      []store.Account
		healthVer  string
		healthErr  error
		bridgeUp   bool
		want       []wantReport
		wantProbes bool // control and bridge each probed exactly once
	}{
		"extension absent with no fileprovider rows is silent": {
			available: false,
			accts:     []store.Account{symRow, nfsRow},
		},
		"extension absent with fileprovider rows fails with the enablement guidance and probes nothing": {
			available: false,
			accts:     []store.Account{fpRow(1), fpRow(2), symRow},
			want: []wantReport{
				{"file provider extension", false, []string{
					"2 fileprovider accounts", pool.WidgetAppPath(), "Login Items & Extensions",
				}},
			},
		},
		"all green renders extension, app, and bridge healthy": {
			available: true,
			accts:     []store.Account{fpRow(1), symRow},
			healthVer: "1.2.3",
			bridgeUp:  true,
			want: []wantReport{
				{"file provider extension", true, []string{pool.FPExtensionBundleID, "1 fileprovider account"}},
				{"file provider app", true, []string{"1.2.3"}},
				{"file provider bridge", true, nil},
			},
			wantProbes: true,
		},
		"dead control socket fails the app line with the launch hint": {
			available: true,
			accts:     []store.Account{fpRow(1)},
			healthErr: controlErr,
			bridgeUp:  true,
			want: []wantReport{
				{"file provider extension", true, []string{"1 fileprovider account"}},
				{"file provider app", false, []string{controlErr.Error(), pool.WidgetAppPath()}},
				{"file provider bridge", true, nil},
			},
			wantProbes: true,
		},
		"unreachable bridge fails with the group-container hint": {
			available: true,
			accts:     []store.Account{fpRow(1)},
			healthVer: "1.2.3",
			bridgeUp:  false,
			want: []wantReport{
				{"file provider extension", true, nil},
				{"file provider app", true, []string{"1.2.3"}},
				{"file provider bridge", false, []string{"app group container", "restart the daemon"}},
			},
			wantProbes: true,
		},
		"extension enabled with zero fileprovider rows still reports and probes": {
			available: true,
			accts:     []store.Account{symRow, nfsRow},
			healthVer: "1.2.3",
			bridgeUp:  true,
			want: []wantReport{
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

			report, calls := captureReports()
			reportFileProvider(t.Context(), &pool.Manager{}, tc.accts, report)

			if len(*calls) != len(tc.want) {
				t.Fatalf("got %d reports %+v, want %d", len(*calls), *calls, len(tc.want))
			}
			for i, want := range tc.want {
				got := (*calls)[i]
				if got.label != want.label || got.healthy != want.healthy {
					t.Errorf("report[%d] = %+v, want label %q healthy %v", i, got, want.label, want.healthy)
				}
				for _, frag := range want.frags {
					if !strings.Contains(got.detail, frag) {
						t.Errorf("report[%d] detail %q missing %q", i, got.detail, frag)
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

// TestReportContentHealth pins the daemon content-source line: silent when
// healthy (or when there is no daemon to ask), a failure carrying the daemon's
// verbatim message plus the log pointer otherwise.
func TestReportContentHealth(t *testing.T) {
	report, calls := captureReports()
	reportContentHealth("", report)
	if len(*calls) != 0 {
		t.Fatalf("healthy content source must be silent, got %+v", *calls)
	}

	msg := "merge claude.json for /p/acct-01: unexpected end of JSON input"
	reportContentHealth(msg, report)
	if len(*calls) != 1 {
		t.Fatalf("got %d reports %+v, want exactly one", len(*calls), *calls)
	}
	got := (*calls)[0]
	if got.label != "content source" || got.healthy {
		t.Errorf("report = %+v, want an unhealthy content source line", got)
	}
	for _, frag := range []string{msg, "daemon.log"} {
		if !strings.Contains(got.detail, frag) {
			t.Errorf("detail %q missing %q", got.detail, frag)
		}
	}
}

func TestCheckFileProviderFallback(t *testing.T) {
	cases := map[string]struct {
		acctKind    string
		defaultKind fkoverlay.Backend // "" = no default recorded
		available   bool
		wantReport  bool
		wantOn      string // fragment naming the current backend
	}{
		"symlink account, fileprovider default, extension enabled -> surfaced": {
			acctKind: "symlink", defaultKind: fkoverlay.BackendFileProvider, available: true,
			wantReport: true, wantOn: "on symlink",
		},
		"fuse account, fileprovider default, extension enabled -> surfaced": {
			acctKind: "nfs", defaultKind: fkoverlay.BackendFileProvider, available: true,
			wantReport: true, wantOn: "on nfs",
		},
		"fileprovider account -> quiet": {
			acctKind: "fileprovider", defaultKind: fkoverlay.BackendFileProvider, available: true,
		},
		"symlink account, fuse default -> quiet (checkFuseFallback's beat)": {
			acctKind: "symlink", defaultKind: fkoverlay.BackendNFS, available: true,
		},
		"symlink account, fileprovider default, extension absent -> quiet": {
			acctKind: "symlink", defaultKind: fkoverlay.BackendFileProvider, available: false,
		},
		"unparseable kind -> quiet": {
			acctKind: "garbage", defaultKind: fkoverlay.BackendFileProvider, available: true,
		},
		"no recorded default -> quiet": {
			acctKind: "symlink", defaultKind: "", available: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			st, err := store.Open(filepath.Join(home, "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			// SetDefaultOverlayKind refuses a fuse default when CanHostFuse is false.
			m := &pool.Manager{Store: st, CanHostFuse: func() bool { return true }}
			if tc.defaultKind != "" {
				if err := m.SetDefaultOverlayKind(tc.defaultKind); err != nil {
					t.Fatal(err)
				}
			}
			swapVar(t, &fpAvailable, func(fkoverlay.Spec) bool { return tc.available })

			a := store.Account{ID: 5, ConfigDir: filepath.Join(home, "acct-05"), OverlayKind: tc.acctKind}
			report, calls := captureReports()
			checkFileProviderFallback(m, a, report)

			got := false
			for _, c := range *calls {
				if strings.Contains(c.label, "fileprovider fallback") {
					got = true
					if c.healthy {
						t.Errorf("fileprovider-fallback report marked healthy; want a failure")
					}
					for _, frag := range []string{"ccp migrate --to fileprovider", tc.wantOn} {
						if !strings.Contains(c.detail, frag) {
							t.Errorf("detail %q missing %q", c.detail, frag)
						}
					}
				}
			}
			if got != tc.wantReport {
				t.Fatalf("surfaced = %v, want %v (calls=%+v)", got, tc.wantReport, *calls)
			}
		})
	}
}

// TestCheckStrandedPrivateSkipsNonSymlinkRows pins that a fuse or fileprovider
// row's private root — its live backing store, not migration wreckage — is
// never reported as stranded (the doctor-side mirror of HealStrandedPrivate's
// fence), while the same layout on a symlink row still is.
func TestCheckStrandedPrivateSkipsNonSymlinkRows(t *testing.T) {
	cases := map[string]struct {
		kind       string
		wantReport bool
	}{
		"fileprovider row's live private store is not stranded": {kind: "fileprovider"},
		"fuse row's live private store is not stranded":         {kind: "nfs"},
		"symlink row with private leftovers is stranded":        {kind: "symlink", wantReport: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "acct-01")
			priv := fkoverlay.FusePrivateRoot(dir)
			if err := os.MkdirAll(priv, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(priv, ".last-update-result.json"), []byte("{}"), 0o600); err != nil {
				t.Fatal(err)
			}
			a := store.Account{ID: 1, ConfigDir: dir, OverlayKind: tc.kind}

			report, calls := captureReports()
			var out bytes.Buffer
			checkStrandedPrivate(&pool.Manager{}, a, false, &out, report)

			if tc.wantReport {
				if len(*calls) != 1 || (*calls)[0].healthy || !strings.Contains((*calls)[0].detail, "stranded in "+priv) {
					t.Fatalf("got %+v, want one stranded report naming %s", *calls, priv)
				}
				return
			}
			if len(*calls) != 0 {
				t.Fatalf("%s row reported %+v, want silence", tc.kind, *calls)
			}
		})
	}
}

func TestCountFuse(t *testing.T) {
	accts := []store.Account{
		{ID: 1, OverlayKind: string(fkoverlay.BackendNFS)},
		{ID: 2, OverlayKind: string(fkoverlay.BackendSymlink)},
		{ID: 3, OverlayKind: ""}, // legacy rows default to symlink
		{ID: 4, OverlayKind: string(fkoverlay.BackendNFS)},
	}
	if got := countFuse(accts); got != 2 {
		t.Errorf("countFuse = %d, want 2", got)
	}
	if got := countFuse(nil); got != 0 {
		t.Errorf("countFuse(nil) = %d, want 0", got)
	}
}

func TestCountFileProvider(t *testing.T) {
	accts := []store.Account{
		{ID: 1, OverlayKind: string(fkoverlay.BackendFileProvider)},
		{ID: 2, OverlayKind: string(fkoverlay.BackendNFS)},
		{ID: 3, OverlayKind: string(fkoverlay.BackendSymlink)},
		{ID: 4, OverlayKind: ""},        // legacy rows default to symlink
		{ID: 5, OverlayKind: "garbage"}, // corruption reads non-fileprovider
		{ID: 6, OverlayKind: string(fkoverlay.BackendFileProvider)},
	}
	if got := countFileProvider(accts); got != 2 {
		t.Errorf("countFileProvider = %d, want 2", got)
	}
	if got := countFileProvider(nil); got != 0 {
		t.Errorf("countFileProvider(nil) = %d, want 0", got)
	}
}

// TestDoctorHealReportsDiscardedDuplicate pins that a daemon-less `doctor --fix`
// reports rather than silently discards the losing duplicate; it installs no
// ResolvedConflictLogf seam, so it catches the silence if checkStrandedPrivate stops wiring one.
func TestDoctorHealReportsDiscardedDuplicate(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	base := filepath.Join(home, ".claude")
	if err := os.MkdirAll(filepath.Join(base, "projects"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "settings.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	dir := filepath.Join(home, "acct-01")
	symProv, err := pool.OverlayProviderFor(fkoverlay.BackendSymlink)
	if err != nil {
		t.Fatal(err)
	}
	if err := symProv.Setup(base, dir); err != nil {
		t.Fatal(err)
	}

	// The collision an abnormal shutdown leaves: same private file in both roots, differing content.
	priv := fkoverlay.FusePrivateRoot(dir)
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	inDir := filepath.Join(dir, ".last-update-result.json")
	if err := os.WriteFile(inDir, []byte("stale-in-dir"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(inDir, now.Add(-time.Hour), now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(priv, ".last-update-result.json"), []byte("fresh-from-priv"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(priv, ".last-update-result.json"), now, now); err != nil {
		t.Fatal(err)
	}

	st, err := store.Open(filepath.Join(home, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	a := store.Account{ID: 1, ConfigDir: dir, KeychainService: "svc", KeychainAccount: "user", OverlayKind: "symlink"}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	m := &pool.Manager{Store: st}

	report, calls := captureReports()
	var out bytes.Buffer
	checkStrandedPrivate(m, a, true, &out, report)

	if !strings.Contains(out.String(), "quarantined at") {
		t.Errorf("doctor heal output does not report the quarantined duplicate: %q", out.String())
	}
	quarantines, err := filepath.Glob(inDir + ".conflict-*")
	if err != nil || len(quarantines) != 1 {
		t.Fatalf("quarantine glob = %v, %v; want exactly one", quarantines, err)
	}
	if got, err := os.ReadFile(quarantines[0]); err != nil || string(got) != "stale-in-dir" {
		t.Errorf("quarantined loser = %q, %v; want stale-in-dir", got, err)
	}
	healed := false
	for _, c := range *calls {
		if strings.Contains(c.label, "private files") && c.healthy && strings.Contains(c.detail, "restored from") {
			healed = true
		}
	}
	if !healed {
		t.Errorf("missing healthy 'restored from' report: %+v", *calls)
	}
	got, err := os.ReadFile(inDir) //nolint:gosec // G304: path is a cc-pool-managed/test-owned file, not external input
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "fresh-from-priv" {
		t.Errorf("healed file = %q, want the newer backing copy", got)
	}

	before := out.Len()
	fkoverlay.ResolvedConflictLogf("leak probe")
	if out.Len() != before {
		t.Errorf("seam leaked past the heal: a write after checkStrandedPrivate still hit the doctor buffer")
	}
}

// TestCheckCredential pins doctor's per-backend credential truth table over the
// Manager seam: healthy verdicts name the live backend, a file copy behind a
// readable Keychain item is divergence (--fix stays advisory and points at
// `ccp cred move` — never deletes, so a fresh headless re-login is never lost),
// and an unsearchable Keychain is unknown state — never reported as absence.
func TestCheckCredential(t *testing.T) {
	usable := &creds.Credential{}
	usable.ClaudeAiOauth.AccessToken = "at"
	usable.ClaudeAiOauth.RefreshToken = "rt"
	usable.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
	hardErr := errors.New("security find-generic-password: exit status 51")

	cases := map[string]struct {
		seedKeychain bool
		file         string // "", "valid", "corrupt"
		keychainRead error  // injected keychain Read fault
		fileDelete   error  // injected file Delete fault
		fix          bool
		wantLabel    string
		wantHealthy  bool
		wantDetail   string // exact match when non-empty
		wantContains []string
		wantOmits    []string
		wantFileGone bool
	}{
		"keychain only is healthy and names its backend": {
			seedKeychain: true,
			wantLabel:    "acct-01 credential", wantHealthy: true, wantDetail: "keychain",
		},
		"file only is healthy and names its backend": {
			file:      "valid",
			wantLabel: "acct-01 credential", wantHealthy: true, wantDetail: "file",
		},
		"unsearchable keychain with a file credential is healthy, not flagged": {
			file:         "valid",
			keychainRead: creds.ErrUnavailable,
			wantLabel:    "acct-01 credential", wantHealthy: true, wantDetail: "file",
		},
		"copies in both backends are divergence": {
			seedKeychain: true, file: "valid",
			wantLabel:    "acct-01 credential",
			wantContains: []string{"BOTH", "diverge", "single-use"},
		},
		"a corrupt file behind the keychain still counts as both": {
			seedKeychain: true, file: "corrupt",
			wantLabel:    "acct-01 credential",
			wantContains: []string{"BOTH", "diverge"},
		},
		"unsearchable keychain and no file is unknown state, never absence": {
			keychainRead: creds.ErrUnavailable,
			wantLabel:    "acct-01 credential",
			wantContains: []string{"unreachable", "unknown", "ccp cred move --to file"},
			wantOmits:    []string{"not found"},
		},
		"no credential in either backend names the login fix": {
			wantLabel:    "acct-01 credential",
			wantContains: []string{"no credential in either backend", "ccp login 1"},
		},
		"a corrupt file with an empty keychain surfaces the parse error": {
			file:         "corrupt",
			wantLabel:    "acct-01 credential",
			wantContains: []string{"parse credential blob"},
		},
		"a hard keychain error is reported verbatim": {
			keychainRead: hardErr,
			wantLabel:    "acct-01 keychain",
			wantContains: []string{hardErr.Error()},
		},
		"fix stays advisory and points at cred move, never deleting": {
			seedKeychain: true, file: "valid", fix: true,
			wantLabel: "acct-01 credential", wantHealthy: false,
			wantContains: []string{"BOTH", "diverge", "ccp cred move", "fresher"},
			wantFileGone: false,
		},
		"fix on an unreachable keychain says there is nothing to do locally": {
			keychainRead: creds.ErrUnavailable, fix: true,
			wantLabel:    "acct-01 credential",
			wantContains: []string{"unreachable", "unknown", "nothing for --fix to do locally"},
			wantOmits:    []string{"not found"},
		},
		"fix reports a reassert that keeps failing": {
			keychainRead: hardErr, fix: true,
			wantLabel:    "acct-01 keychain",
			wantContains: []string{hardErr.Error()},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "svc-01", KeychainAccount: "user"}
			fk := credstest.NewFake()
			fk.KeychainFaults = credstest.Faults{Read: tc.keychainRead}
			fk.FileFaults = credstest.Faults{Delete: tc.fileDelete}
			if tc.seedKeychain {
				fk.Put(a.KeychainService, a.KeychainAccount, usable)
			}
			switch tc.file {
			case "valid":
				if err := creds.WriteFileCredential(a.ConfigDir, usable); err != nil {
					t.Fatal(err)
				}
			case "corrupt":
				if err := os.WriteFile(creds.FileCredentialPath(a.ConfigDir), []byte("{not json"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			m := &pool.Manager{Creds: fk}

			report, calls := captureReports()
			checkCredential(m, a, tc.fix, report)

			if len(*calls) != 1 {
				t.Fatalf("got %d reports %+v, want exactly one", len(*calls), *calls)
			}
			got := (*calls)[0]
			if got.label != tc.wantLabel {
				t.Errorf("label = %q, want %q", got.label, tc.wantLabel)
			}
			if got.healthy != tc.wantHealthy {
				t.Errorf("healthy = %v, want %v (detail %q)", got.healthy, tc.wantHealthy, got.detail)
			}
			if tc.wantDetail != "" && got.detail != tc.wantDetail {
				t.Errorf("detail = %q, want exactly %q", got.detail, tc.wantDetail)
			}
			for _, frag := range tc.wantContains {
				if !strings.Contains(got.detail, frag) {
					t.Errorf("detail %q missing %q", got.detail, frag)
				}
			}
			for _, frag := range tc.wantOmits {
				if strings.Contains(got.detail, frag) {
					t.Errorf("detail %q must not contain %q", got.detail, frag)
				}
			}
			if tc.file != "" {
				if gone := !creds.FileCredentialExists(a.ConfigDir); gone != tc.wantFileGone {
					t.Errorf("file credential gone = %v, want %v", gone, tc.wantFileGone)
				}
			}
			if tc.seedKeychain {
				if _, ok := fk.Get(a.KeychainService, a.KeychainAccount); !ok {
					t.Error("keychain copy deleted; doctor must never remove the authoritative credential")
				}
			}
			if got := fk.WriteCount(); got != 0 {
				t.Errorf("keychain writes = %d, want 0 (only a successful reassert writes)", got)
			}
		})
	}
}

// oneShotReadFault fails only the first keychain Read — the ACL denial doctor
// classifies on — so the --fix reassert's re-read (the GUI-prompt recovery)
// reaches the underlying store.
type oneShotReadFault struct {
	creds.Store
	err   error
	reads int
}

func (s *oneShotReadFault) Read() (*creds.Credential, error) {
	s.reads++
	if s.reads == 1 {
		return nil, s.err
	}
	return s.Store.Read()
}

// keychainOverride hands out kc for the Keychain backend and delegates the
// rest to the embedded Credentials.
type keychainOverride struct {
	pool.Credentials
	kc creds.Store
}

func (c keychainOverride) Store(a store.Account, src creds.Source) creds.Store {
	if src == creds.SourceKeychain {
		return c.kc
	}
	return c.Credentials.Store(a, src)
}

// TestCheckCredentialFixReassertsKeychain pins the --fix recovery for a
// keychain item our ACL cannot read: reassert re-reads then writes the item
// back through the seam (FinalizeAdd's ownership re-assertion), reporting
// re-asserted on success.
func TestCheckCredentialFixReassertsKeychain(t *testing.T) {
	a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "svc-01", KeychainAccount: "user"}
	usable := &creds.Credential{}
	usable.ClaudeAiOauth.AccessToken = "at"
	usable.ClaudeAiOauth.RefreshToken = "rt"
	fk := credstest.NewFake()
	fk.Put(a.KeychainService, a.KeychainAccount, usable)
	kc := &oneShotReadFault{Store: fk.Store(a, creds.SourceKeychain), err: errors.New("security: write access denied")}
	m := &pool.Manager{Creds: keychainOverride{Credentials: fk, kc: kc}}

	report, calls := captureReports()
	checkCredential(m, a, true, report)

	if len(*calls) != 1 {
		t.Fatalf("got %d reports %+v, want exactly one", len(*calls), *calls)
	}
	got := (*calls)[0]
	if got.label != "acct-01 keychain" || !got.healthy || got.detail != "re-asserted" {
		t.Fatalf("report = %+v, want healthy %q re-asserted", got, "acct-01 keychain")
	}
	if writes := fk.WriteCount(); writes != 1 {
		t.Errorf("keychain writes = %d, want exactly 1 (the ACL re-assertion)", writes)
	}
	if kc.reads != 2 {
		t.Errorf("keychain reads = %d, want 2 (classify + reassert)", kc.reads)
	}
}

func TestDoctorSurfacesFuseFallback(t *testing.T) {
	cases := map[string]struct {
		acctKind    string
		defaultKind fkoverlay.Backend
		canHostFuse bool
		wantReport  bool
	}{
		"symlink account, fuse default, can host -> surfaced": {"symlink", fkoverlay.BackendNFS, true, true},
		"symlink account, symlink default -> quiet":           {"symlink", fkoverlay.BackendSymlink, true, false},
		"symlink account, fuse default, cannot host -> quiet": {"symlink", fkoverlay.BackendNFS, false, false},
		"fuse account -> quiet":                               {"nfs", fkoverlay.BackendNFS, true, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			st, err := store.Open(filepath.Join(home, "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			// SetDefaultOverlayKind refuses a fuse default when CanHostFuse is false.
			m := &pool.Manager{Store: st, CanHostFuse: func() bool { return true }}
			if err := m.SetDefaultOverlayKind(tc.defaultKind); err != nil {
				t.Fatal(err)
			}
			m.CanHostFuse = func() bool { return tc.canHostFuse }

			a := store.Account{ID: 3, ConfigDir: filepath.Join(home, "acct-03"), OverlayKind: tc.acctKind}
			report, calls := captureReports()
			checkFuseFallback(m, a, report)

			got := false
			for _, c := range *calls {
				if strings.Contains(c.label, "fuse fallback") {
					got = true
					if c.healthy {
						t.Errorf("fuse-fallback report marked healthy; want a failure")
					}
					if !strings.Contains(c.detail, "ccp migrate") {
						t.Errorf("detail missing the re-promote guidance: %q", c.detail)
					}
				}
			}
			if got != tc.wantReport {
				t.Fatalf("surfaced = %v, want %v (calls=%+v)", got, tc.wantReport, *calls)
			}
		})
	}
}
