package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
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
	"github.com/yasyf/fusekit/fileproviderd"
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

// TestReportCaskRelauncher pins doctor's verify-only check of the fusekit-holder
// cask's KeepAlive LaunchAgent: silent with no fuse accounts, ✓ when the
// relauncher is loaded, ✗ with the reinstall hint when it is not — always through
// the read-only Loaded() seam, never installing anything.
// TestReportJournalRisks pins J4a's doctor visibility: a recorded stale-journal risk
// is surfaced loud; a dir still in the holder's mount list reads as a live resurrection;
// an unreachable holder keeps the risk loud and unclearable; and --fix clears a risk the
// reachable holder no longer lists.
func TestReportJournalRisks(t *testing.T) {
	const dir = "/cfg/acct-01"
	newMgr := func(t *testing.T) *pool.Manager {
		m := &pool.Manager{Store: openTestStore(t)}
		if err := m.Store.RecordJournalRisk(dir, "journal save failed", time.Now()); err != nil {
			t.Fatal(err)
		}
		return m
	}

	t.Run("a still-mounted dir reads as a live resurrection", func(t *testing.T) {
		m := newMgr(t)
		report, calls := captureReports()
		reportJournalRisks(m, []mountd.MountInfo{{Dir: dir}}, true, true, report)
		if len(*calls) != 1 || (*calls)[0].healthy || !strings.Contains((*calls)[0].detail, "resurrected") {
			t.Fatalf("reports = %+v, want one loud resurrection report", *calls)
		}
		if risks, _ := m.Store.ListJournalRisks(); len(risks) != 1 {
			t.Fatal("--fix cleared a still-mounted risk; only removal should")
		}
	})

	t.Run("an unreachable holder keeps the risk loud and unclearable", func(t *testing.T) {
		m := newMgr(t)
		report, calls := captureReports()
		reportJournalRisks(m, nil, false, true, report)
		if len(*calls) != 1 || (*calls)[0].healthy || !strings.Contains((*calls)[0].detail, "unreachable") {
			t.Fatalf("reports = %+v, want one loud unreachable report", *calls)
		}
		if risks, _ := m.Store.ListJournalRisks(); len(risks) != 1 {
			t.Fatal("cleared a risk while the holder was unreachable; it can't be confirmed moot")
		}
	})

	t.Run("--fix clears a risk the reachable holder no longer lists", func(t *testing.T) {
		m := newMgr(t)
		report, calls := captureReports()
		reportJournalRisks(m, nil, true, true, report)
		if len(*calls) != 1 || !(*calls)[0].healthy {
			t.Fatalf("reports = %+v, want one healthy 'cleared' report", *calls)
		}
		if risks, _ := m.Store.ListJournalRisks(); len(risks) != 0 {
			t.Fatal("--fix did not clear a moot risk")
		}
	})

	t.Run("without --fix a moot risk is reported loud but kept", func(t *testing.T) {
		m := newMgr(t)
		report, calls := captureReports()
		reportJournalRisks(m, nil, true, false, report)
		if len(*calls) != 1 || (*calls)[0].healthy {
			t.Fatalf("reports = %+v, want one loud report", *calls)
		}
		if risks, _ := m.Store.ListJournalRisks(); len(risks) != 1 {
			t.Fatal("a read-only doctor run cleared the risk")
		}
	})
}

func TestReportCaskRelauncher(t *testing.T) {
	cases := map[string]struct {
		fuseRows int
		loaded   bool
		none     bool
		want     []reportCall
	}{
		"no fuse accounts is silent": {fuseRows: 0, none: true},
		"a loaded relauncher passes": {
			fuseRows: 1, loaded: true,
			want: []reportCall{{label: "cask relauncher", healthy: true}},
		},
		"a missing relauncher fails with the reinstall hint": {
			fuseRows: 2, loaded: false,
			want: []reportCall{{label: "cask relauncher", healthy: false, detail: "brew reinstall --cask fusekit-holder"}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			called := false
			swapVar(t, &holderRelauncherLoaded, func() bool { called = true; return tc.loaded })
			report, calls := captureReports()
			reportCaskRelauncher(tc.fuseRows, report)

			if tc.none {
				if len(*calls) != 0 {
					t.Fatalf("want no reports, got %+v", *calls)
				}
				return
			}
			if !called {
				t.Fatal("verify never consulted the read-only Loaded() seam")
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

// TestReportHolderWedges pins F10's fail-closed rendering: every FINAL WEDGE dir
// reports LOUD (healthy=false), a contract-violation suffix escalates the detail
// and is stripped from the label, and an unresolved journal persist-warning
// reports LOUD too. A clean snapshot stays silent.
func TestReportHolderWedges(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	t.Run("wedges and warning render loud", func(t *testing.T) {
		report, calls := captureReports()
		st := &mountd.HealthStatus{
			WedgedDirs: []string{
				"/pool/acct-01" + mountd.WedgeContractViolation,
				"/pool/acct-02",
			},
			Warning: "journal save failed: no space left on device",
		}
		reportHolderWedges(st, report)
		if len(*calls) != 3 {
			t.Fatalf("reportHolderWedges emitted %d lines, want 3 (2 wedges + 1 warning): %+v", len(*calls), *calls)
		}
		for _, c := range *calls {
			if c.healthy {
				t.Fatalf("line %q reported healthy; every wedge/warning line must be LOUD", c.label)
			}
		}
		cv := (*calls)[0]
		if strings.Contains(cv.label, mountd.WedgeContractViolation) {
			t.Fatalf("contract-violation suffix leaked into the label %q; it must be stripped", cv.label)
		}
		if !strings.Contains(cv.detail, "CONTRACT VIOLATION") {
			t.Fatalf("contract-violation wedge detail %q missing the escalated severity", cv.detail)
		}
		if strings.Contains((*calls)[1].detail, "CONTRACT VIOLATION") {
			t.Fatalf("a plain wedge got the contract-violation severity: %q", (*calls)[1].detail)
		}
		if !strings.Contains((*calls)[2].detail, "journal save failed: no space left on device") {
			t.Fatalf("warning line %q dropped the holder's persist-warning text", (*calls)[2].detail)
		}
	})

	t.Run("clean snapshot is silent", func(t *testing.T) {
		report, calls := captureReports()
		reportHolderWedges(&mountd.HealthStatus{}, report)
		if len(*calls) != 0 {
			t.Fatalf("a clean snapshot emitted %+v, want silence", *calls)
		}
	})
}

// TestReportLeasesSilentWhenHolderUnreachable pins the leases panel's fail-quiet:
// with no holder on the socket it emits nothing (reportHolder owns that failure),
// so a daemonless doctor run never blocks on a dead holder.
func TestReportLeasesSilentWhenHolderUnreachable(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.MkdirAll(filepath.Join(home, ".fusekit"), 0o700); err != nil {
		t.Fatal(err)
	}
	report, calls := captureReports()
	reportLeases(report)
	if len(*calls) != 0 {
		t.Fatalf("reportLeases with no holder emitted %+v, want silence", *calls)
	}
}

// startDoctorHolder serves a canned proto-2 holder on this test HOME's default
// socket (~/.fusekit/holder.sock): hello answers the full feature set, health
// answers one wedged dir and a persist-warning with a 1-of-3 lease summary, the
// own-scope leases op answers own, and the cross-tenant (All) leases op answers
// a hard error so the LeasesAll-surfacing leg is pinned.
func startDoctorHolder(t *testing.T, home string, own []mountd.LeaseInfo) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(home, ".fusekit"), 0o700); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", filepath.Join(home, ".fusekit", "holder.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			var req mountd.Request
			if err := json.NewDecoder(conn).Decode(&req); err != nil {
				_ = conn.Close() // Available()'s bare probe dial
				continue
			}
			resp := mountd.Response{OK: true, Proto: mountd.MountProtoVersion, Version: version.String(), Features: mountd.HolderFeatures}
			switch req.Op {
			case mountd.OpHealth:
				resp.LeasesTotal = 3
				resp.LeasesHeld = 1
				resp.WedgedDirs = []string{filepath.Join(home, "pool", "acct-08") + mountd.WedgeContractViolation}
				resp.Warning = "save mounts.json: disk full"
			case mountd.OpLeases:
				if req.All {
					resp.OK = false
					resp.Error = "registry walk failed"
				} else {
					resp.Leases = own
				}
			}
			_ = json.NewEncoder(conn).Encode(resp)
			_ = conn.Close()
		}
	}()
}

// TestReportLeasesEndToEnd pins F10's reportLeases wire path against a canned
// holder: the pool-wide summary, the health snapshot's wedge/warning wired into
// reportHolderWedges, the held lease naming its live session, and a LeasesAll
// failure SURFACED instead of ignored.
func TestReportLeasesEndToEnd(t *testing.T) {
	// A short /tmp home: the unix-socket path must fit AF_UNIX's 104-byte cap,
	// which t.TempDir()'s /var/folders path overruns.
	home, err := os.MkdirTemp("/tmp", "ccp-doc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	held := mountd.LeaseInfo{File: "ab.lease", Held: true, Dir: filepath.Join(home, "pool", "acct-01"), Owner: pool.HolderOwner, PID: 4242, Argv0: "claude"}
	startDoctorHolder(t, home, []mountd.LeaseInfo{held, {File: "cd.lease", Held: false}})

	report, calls := captureReports()
	reportLeases(report)

	find := func(wantLabel, wantDetail string) reportCall {
		t.Helper()
		for _, c := range *calls {
			if strings.Contains(c.label, wantLabel) && strings.Contains(c.detail, wantDetail) {
				return c
			}
		}
		t.Fatalf("no report matching label~%q detail~%q in %+v", wantLabel, wantDetail, *calls)
		return reportCall{}
	}

	if c := find("holder leases", "1 held of 3 lease files"); !c.healthy {
		t.Fatalf("lease summary must be healthy: %+v", c)
	}
	if c := find("holder wedge", "FINAL WEDGE"); c.healthy || !strings.Contains(c.label, "acct-08") || !strings.Contains(c.detail, "CONTRACT VIOLATION") {
		t.Fatalf("the health snapshot's wedge did not render through reportHolderWedges: %+v", c)
	}
	if c := find("holder journal", "save mounts.json: disk full"); c.healthy {
		t.Fatalf("an unresolved journal persist-warning must not read healthy: %+v", c)
	}
	if c := find("lease", "pid 4242"); !strings.Contains(c.detail, "held by a live session") {
		t.Fatalf("held-lease line wrong: %+v", c)
	}
	if c := find("holder leases", "read the cross-tenant lease view"); c.healthy || !strings.Contains(c.detail, "registry walk failed") {
		t.Fatalf("a LeasesAll failure must surface, not be ignored: %+v", c)
	}
}

// startLeaseDoctorHolder serves a canned proto-2 holder whose hello feature set and
// OpHealth response the caller controls, for reportLeases ordering tests.
func startLeaseDoctorHolder(t *testing.T, home string, features []string, health func(*mountd.Response)) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(home, ".fusekit"), 0o700); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", filepath.Join(home, ".fusekit", "holder.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			var req mountd.Request
			if err := json.NewDecoder(conn).Decode(&req); err != nil {
				_ = conn.Close()
				continue
			}
			resp := mountd.Response{OK: true, Proto: mountd.MountProtoVersion, Version: version.String(), Features: features}
			if req.Op == mountd.OpHealth && health != nil {
				health(&resp)
			}
			_ = json.NewEncoder(conn).Encode(resp)
			_ = conn.Close()
		}
	}()
}

// TestReportLeasesRendersStatusBeforeGate pins G11: holder-wide health (WedgedDirs,
// journal warning) renders BEFORE any FeatureLeases gating — so a holder too old for
// the leases op still surfaces its wedges and warnings — and a Status error is
// surfaced, never silently swallowed.
func TestReportLeasesRendersStatusBeforeGate(t *testing.T) {
	shortHome := func(t *testing.T) string {
		// A short /tmp home: the unix-socket path must fit AF_UNIX's 104-byte cap.
		home, err := os.MkdirTemp("/tmp", "ccp-doc")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(home) })
		t.Setenv("HOME", home)
		return home
	}
	find := func(calls *[]reportCall, label, detail string) (reportCall, bool) {
		for _, c := range *calls {
			if strings.Contains(c.label, label) && strings.Contains(c.detail, detail) {
				return c, true
			}
		}
		return reportCall{}, false
	}

	t.Run("wedges and warnings surface without the leases capability", func(t *testing.T) {
		home := shortHome(t)
		startLeaseDoctorHolder(t, home,
			[]string{mountd.FeatureMux, mountd.FeatureBridge, mountd.FeatureLeaseGate, mountd.FeatureWarning},
			func(resp *mountd.Response) {
				resp.WedgedDirs = []string{filepath.Join(home, "pool", "acct-04") + mountd.WedgeContractViolation}
				resp.Warning = "save mounts.json: disk full"
			})
		report, calls := captureReports()
		reportLeases(report)
		if c, ok := find(calls, "holder wedge", "FINAL WEDGE"); !ok || c.healthy {
			t.Fatalf("wedge must render before the leases gate (found=%v): %+v", ok, c)
		}
		if c, ok := find(calls, "holder journal", "disk full"); !ok || c.healthy {
			t.Fatalf("journal warning must render before the leases gate (found=%v): %+v", ok, c)
		}
		if _, ok := find(calls, "holder leases", "held of"); ok {
			t.Fatal("a lease summary rendered on a holder lacking the leases capability")
		}
	})

	t.Run("a Status error is surfaced", func(t *testing.T) {
		home := shortHome(t)
		startLeaseDoctorHolder(t, home, mountd.HolderFeatures, func(resp *mountd.Response) {
			resp.OK = false
			resp.Error = "status probe failed"
		})
		report, calls := captureReports()
		reportLeases(report)
		if c, ok := find(calls, "holder leases", "read holder status"); !ok || c.healthy || !strings.Contains(c.detail, "status probe failed") {
			t.Fatalf("a Status error must surface, not be swallowed (found=%v): %+v", ok, c)
		}
	})
}

// TestReportLedgers pins the one daemon-up self-heal surface: the composed
// Ledgers block rendered in the findings style, with the holder cache's TCC
// grant walkthrough verbatim. Healthy rows are silent; a parked fuse.remount
// row reads as a DEFERRED retreat (the escalation clears the row when the
// retreat actually fires), never as done.
func TestReportLedgers(t *testing.T) {
	accts := []store.Account{
		{ID: 1, ConfigDir: "/p/acct-01", OverlayKind: string(fkoverlay.BackendNFS)},
		{ID: 2, ConfigDir: "/p/acct-02", OverlayKind: string(fkoverlay.BackendFileProvider)},
	}
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	type wantCall struct {
		label    string
		frags    []string
		notFrags []string
	}
	cases := map[string]struct {
		ledgers []daemon.LedgerState
		want    []wantCall
	}{
		"healthy pool (no rows, no grant) is silent": {},
		"healthy rows are silent (mid-debounce, benign deferral, elapsed backoff)": {
			ledgers: []daemon.LedgerState{
				{Policy: "auth.streak", Resource: "/p/acct-01", Strikes: 2},
				{Policy: "fp.domain", Resource: "/p/acct-02", Strikes: 1},
				{Policy: "fp.shallow", Resource: "/p/acct-02", Strikes: 1},
				{Policy: "fp.bridge", Resource: "pool", Strikes: 1},
				{Policy: "fuse.deepwedge", Resource: "/p/acct-01", Strikes: 1},
				{Policy: "fuse.remount", Resource: "/p/acct-01", Attempts: 3},
				{Policy: "ratelimit.acct", Resource: "/p/acct-01", Attempts: 2, NextDue: now.Add(-time.Second)},
			},
		},
		"wedged mirror (fuse.deepwedge faulted)": {
			ledgers: []daemon.LedgerState{{Policy: "fuse.deepwedge", Resource: "/p/acct-01", Faulted: true}},
			want:    []wantCall{{label: "acct-01 mirror", frags: []string{"wedged (serves metadata but hangs reads)", "remount"}}},
		},
		"dead mirror (fuse.shallowdead faulted)": {
			ledgers: []daemon.LedgerState{{Policy: "fuse.shallowdead", Resource: "/p/acct-01", Faulted: true}},
			want:    []wantCall{{label: "acct-01 mirror", frags: []string{"dead mirror (fails reads outright"}}},
		},
		"parked hazard remount reads as a deferred retreat, never done": {
			ledgers: []daemon.LedgerState{{Policy: "fuse.remount", Resource: "/p/acct-01", Strikes: 5, Attempts: 7, Parked: true}},
			want: []wantCall{{
				label:    "acct-01 remount",
				frags:    []string{"remount breaker tripped", "5 failed attempts", "retreat to symlink pending", "re-fires", "next elapsed backoff window"},
				notFrags: []string{"fell back to symlink"},
			}},
		},
		"parked TCC lane reads as a deferred retreat too": {
			ledgers: []daemon.LedgerState{{Policy: "fuse.remount", Resource: "/p/acct-01", AltHits: 6, Attempts: 9, Parked: true}},
			want: []wantCall{{
				label:    "acct-01 remount",
				frags:    []string{"TCC breaker tripped", "retreat to symlink pending"},
				notFrags: []string{"fell back to symlink"},
			}},
		},
		"TCC grace counting carries its settings guidance directly": {
			ledgers: []daemon.LedgerState{{Policy: "fuse.remount", Resource: "/p/acct-01", AltHits: 2, Attempts: 2}},
			want: []wantCall{{
				label:    "acct-01 remount",
				frags:    []string{"macOS grant", "2 waits", fuseGrantHint(fkoverlay.BackendNFS), "falls back to symlink"},
				notFrags: []string{"mount holder grant line"},
			}},
		},
		"remount hazard mid-ladder carries its last error": {
			ledgers: []daemon.LedgerState{{Policy: "fuse.remount", Resource: "/p/acct-01", Strikes: 2, Attempts: 2, LastErr: "mount: EIO"}},
			want:    []wantCall{{label: "acct-01 remount", frags: []string{"remount failing", "2 attempts", "mount: EIO"}}},
		},
		"wedged fp domain points at ccp fp repair": {
			ledgers: []daemon.LedgerState{{Policy: "fp.domain", Resource: "/p/acct-02", Faulted: true, Attempts: 2}},
			want:    []wantCall{{label: "acct-02 file provider", frags: []string{"domain wedged (serves control ops but hangs reads)", "ccp fp repair"}}},
		},
		"parked fp domain carries the repair and retreat levers": {
			ledgers: []daemon.LedgerState{{Policy: "fp.domain", Resource: "/p/acct-02", Faulted: true, Attempts: 5, Parked: true}},
			want: []wantCall{{
				label: "acct-02 file provider",
				frags: []string{"parked", "automated recovery is exhausted", "ccp fp repair --account 2", "--retreat"},
			}},
		},
		// A control-plane heal (Missing → deregistered/uncontrollable) can exhaust the
		// recovery attempts and park the domain WITHOUT the data plane ever wedging, so
		// the row is parked but never faulted. ledgerFooter/self-heal banner counts it,
		// so reportLedgers must explain it too — never render a banner with no detail.
		"parked fp domain that never faulted still explains itself": {
			ledgers: []daemon.LedgerState{{Policy: "fp.domain", Resource: "/p/acct-02", Attempts: 5, Parked: true}},
			want: []wantCall{{
				label:    "acct-02 file provider",
				frags:    []string{"parked", "automated recovery is exhausted", "ccp fp repair --account 2", "--retreat"},
				notFrags: []string{"wedged"},
			}},
		},
		"faulted fp shallow lane reports the listability failure and last error": {
			ledgers: []daemon.LedgerState{{Policy: "fp.shallow", Resource: "/p/acct-02", Faulted: true, LastErr: "domain not serving"}},
			want: []wantCall{{
				label: "acct-02 file provider shallow probe",
				frags: []string{"no longer listable", "shallow liveness probe failed", "domain not serving"},
			}},
		},
		"faulted fp bridge reports the pool-wide verdict and lever": {
			ledgers: []daemon.LedgerState{{Policy: "fp.bridge", Resource: "pool", Faulted: true, LastErr: "bound-dead: restart the daemon"}},
			want: []wantCall{{
				label: "pool file provider bridge",
				frags: []string{"content bridge is not serving", "bound-dead", "restart the daemon"},
			}},
		},
		"auth streak fault carries the login hint": {
			ledgers: []daemon.LedgerState{{Policy: "auth.streak", Resource: "/p/acct-01", Faulted: true, LastErr: "401 unauthorized"}},
			want:    []wantCall{{label: "acct-01 auth", frags: []string{"needs re-login", "ccp login 1", "401 unauthorized"}}},
		},
		"account rate limit backing off": {
			ledgers: []daemon.LedgerState{{Policy: "ratelimit.acct", Resource: "/p/acct-01", Attempts: 3, NextDue: now.Add(90 * time.Second)}},
			want:    []wantCall{{label: "acct-01 rate limit", frags: []string{"429 backoff", "1m30s", "3 hits"}}},
		},
		"pool rate limit backing off": {
			ledgers: []daemon.LedgerState{{Policy: "ratelimit.pool", Resource: "pool", Attempts: 1, NextDue: now.Add(3 * time.Minute)}},
			want:    []wantCall{{label: "pool rate limit", frags: []string{"429 backoff", "3m0s", "1 hit"}}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			report, calls := captureReports()
			reportLedgers(accts, tc.ledgers, now, report)
			if len(*calls) != len(tc.want) {
				t.Fatalf("got %d reports %+v, want %d", len(*calls), *calls, len(tc.want))
			}
			for i, want := range tc.want {
				got := (*calls)[i]
				if got.label != want.label || got.healthy {
					t.Errorf("report[%d] = %+v, want unhealthy %q", i, got, want.label)
				}
				for _, frag := range want.frags {
					if !strings.Contains(got.detail, frag) {
						t.Errorf("report[%d] detail %q missing %q", i, got.detail, frag)
					}
				}
				for _, frag := range want.notFrags {
					if strings.Contains(got.detail, frag) {
						t.Errorf("report[%d] detail %q must not claim %q", i, got.detail, frag)
					}
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
		mounts      []mountd.MountInfo
		sessions    []procscan.Session
		probeErr    error
		daemonAlive bool
		want        []reportCall
		wantProbed  []string
	}{
		"daemon alive never probes and stays silent (reportLedgers owns the verdicts)": {
			mounts:      []mountd.MountInfo{{Dir: "/p/acct-01", Base: "/b", Live: true}},
			probeErr:    fmt.Errorf("%w: hung", overlay.ErrProbeWedged),
			daemonAlive: true,
		},
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
			reportWedges(accts, tc.mounts, tc.sessions, tc.daemonAlive, report)
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
		bridge     *daemon.FPBridgeStatus
		bridgeUp   *bool
		consent    bool // daemon's consent-pending signal
		want       []wantReport
		wantProbes bool // control and bridge each probed exactly once
	}{
		"extension absent with no fileprovider rows is silent": {
			available: false,
			accts:     []store.Account{symRow, nfsRow},
		},
		"extension absent with fileprovider rows fails with the onboard pointer and enablement guidance and probes nothing": {
			available: false,
			accts:     []store.Account{fpRow(1), fpRow(2), symRow},
			want: []wantReport{
				{"file provider extension", false, []string{
					"2 fileprovider accounts", "ccp fp onboard", pool.WidgetAppPath(), "Login Items & Extensions",
				}},
			},
		},
		"all green renders extension, app, and bridge healthy": {
			available: true,
			accts:     []store.Account{fpRow(1), symRow},
			healthVer: "1.2.3",
			bridgeUp:  ptr(true),
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
			bridgeUp:  ptr(true),
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
			bridgeUp:  ptr(false),
			want: []wantReport{
				{"file provider extension", true, nil},
				{"file provider app", true, []string{"1.2.3"}},
				{"file provider bridge", false, []string{"app group container", "ccp fp consent", "no restart", "ccp fp onboard"}},
			},
			wantProbes: true,
		},
		"unreachable bridge with the daemon's consent-pending signal names the TCC rung precisely": {
			available: true,
			accts:     []store.Account{fpRow(1)},
			healthVer: "1.2.3",
			bridgeUp:  ptr(false),
			consent:   true,
			want: []wantReport{
				{"file provider extension", true, nil},
				{"file provider app", true, []string{"1.2.3"}},
				{"file provider bridge", false, []string{
					"parked on the app group container consent prompt",
					"ccp fp consent", "local terminal", "no restart", "ccp fp onboard",
				}},
			},
			wantProbes: true,
		},
		"nil bridge state from a pre-upgrade daemon prescribes a restart, not a false failure": {
			available: true,
			accts:     []store.Account{fpRow(1)},
			healthVer: "1.2.3",
			bridgeUp:  nil,
			want: []wantReport{
				{"file provider extension", true, nil},
				{"file provider app", true, []string{"1.2.3"}},
				{"file provider bridge", false, []string{
					"bridge health unknown", "predates bridge reporting", "brew services restart cc-pool",
				}},
			},
			wantProbes: true,
		},
		"bound-dead bridge self-test overrides dial-only green": {
			available: true,
			accts:     []store.Account{fpRow(1)},
			healthVer: "1.2.3",
			bridge: &daemon.FPBridgeStatus{
				Verdict: daemon.FPBridgeBoundDead,
				Detail:  "the daemon's bridge is bound but not serving; restart the daemon (brew services restart cc-pool)",
			},
			bridgeUp: ptr(true),
			want: []wantReport{
				{"file provider extension", true, nil},
				{"file provider app", true, []string{"1.2.3"}},
				{"file provider bridge", false, []string{"bound but not serving", "brew services restart cc-pool"}},
			},
			wantProbes: true,
		},
		"extension enabled with zero fileprovider rows still reports and probes": {
			available: true,
			accts:     []store.Account{symRow, nfsRow},
			healthVer: "1.2.3",
			bridgeUp:  ptr(true),
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
			controlProbes := 0
			swapVar(t, &fpAvailable, func(fkoverlay.Spec) bool { return tc.available })
			swapVar(t, &fpControlHealth, func(context.Context) (string, error) {
				controlProbes++
				return tc.healthVer, tc.healthErr
			})

			report, calls := captureReports()
			reportFileProvider(t.Context(), &pool.Manager{}, tc.accts, tc.bridge, tc.consent, tc.bridgeUp, report)

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
			if controlProbes != wantProbes {
				t.Errorf("control probed %d times, want %d", controlProbes, wantProbes)
			}
		})
	}
}

// TestReportFPWedges pins doctor's File Provider wedge section: with the daemon
// up it renders the daemon's cached verdicts (recovering vs breaker-exhausted)
// and never probes; with the daemon down it probes each File Provider row itself
// and reports only the ones that fail to serve.
func TestReportFPWedges(t *testing.T) {
	fpRow := func(id int) store.Account {
		return store.Account{ID: id, ConfigDir: fmt.Sprintf("/p/acct-%02d", id), OverlayKind: string(fkoverlay.BackendFileProvider)}
	}
	symRow := store.Account{ID: 9, ConfigDir: "/p/acct-09", OverlayKind: string(fkoverlay.BackendSymlink)}

	t.Run("daemon alive never probes and stays silent (reportLedgers owns the verdicts)", func(t *testing.T) {
		swapVar(t, &fpDomainProbeAt, func(string) error { t.Error("probed with the daemon alive"); return nil })
		swapVar(t, &fpRawProbeAt, func(string) error { t.Error("raw-probed with the daemon alive"); return nil })
		report, calls := captureReports()
		reportFPWedges([]store.Account{fpRow(1), fpRow(2), symRow}, true, false, report)
		if len(*calls) != 0 {
			t.Fatalf("want silence, got %+v", *calls)
		}
	})

	t.Run("daemon down shallow control-op probe: wedged flags, no-verdict says cannot verify, healthy/missing stay silent", func(t *testing.T) {
		probed := map[string]bool{}
		swapVar(t, &fpDomainProbeAt, func(dir string) error {
			probed[dir] = true
			switch dir {
			case "/p/acct-01":
				return fmt.Errorf("%w: hung", overlay.ErrFPProbeWedged)
			case "/p/acct-03":
				return fmt.Errorf("%w: no identity", overlay.ErrFPProbeMissing)
			case "/p/acct-04":
				return fmt.Errorf("%w: app down", overlay.ErrFPProbeNoVerdict)
			default:
				return nil // acct-02 healthy
			}
		})
		swapVar(t, &fpRawProbeAt, func(string) error { t.Error("raw probe ran without --fp-raw-probe"); return nil })
		report, calls := captureReports()
		reportFPWedges([]store.Account{fpRow(1), fpRow(2), fpRow(3), fpRow(4), symRow}, false, false, report)

		for _, dir := range []string{"/p/acct-01", "/p/acct-02", "/p/acct-03", "/p/acct-04"} {
			if !probed[dir] {
				t.Errorf("did not probe fp row %s", dir)
			}
		}
		if probed["/p/acct-09"] {
			t.Error("probed a symlink row")
		}
		if len(*calls) != 2 {
			t.Fatalf("got %d reports %+v, want 2 (wedged acct-01 + unverifiable acct-04)", len(*calls), *calls)
		}
		if (*calls)[0].label != "acct-01 file provider" || (*calls)[0].healthy || !strings.Contains((*calls)[0].detail, "daemon is down") {
			t.Errorf("report[0] = %+v, want wedged acct-01 with the daemon-down attribution", (*calls)[0])
		}
		if (*calls)[1].label != "acct-04 file provider" || (*calls)[1].healthy || !strings.Contains((*calls)[1].detail, "cannot verify") {
			t.Errorf("report[1] = %+v, want acct-04 flagged 'cannot verify'", (*calls)[1])
		}
	})

	t.Run("daemon down old app names the cask upgrade instead of generic unprobeable guidance", func(t *testing.T) {
		swapVar(t, &fpDomainProbeAt, func(string) error {
			return fmt.Errorf("%w: %w", overlay.ErrFPProbeNoVerdict, fileproviderd.ErrOpUnsupported)
		})
		swapVar(t, &fpRawProbeAt, func(string) error { t.Error("raw probe ran without --fp-raw-probe"); return nil })
		report, calls := captureReports()
		reportFPWedges([]store.Account{fpRow(1)}, false, false, report)

		if len(*calls) != 1 {
			t.Fatalf("got %d reports %+v, want one upgrade finding", len(*calls), *calls)
		}
		got := (*calls)[0]
		if got.healthy || !strings.Contains(got.detail, "brew upgrade --cask cc-pool-status") || strings.Contains(got.detail, "isn't answering its control socket") {
			t.Errorf("old-app finding = %+v, want specific cask upgrade guidance", got)
		}
	})

	t.Run("daemon down with --fp-raw-probe swaps in the raw filesystem read", func(t *testing.T) {
		swapVar(t, &fpDomainProbeAt, func(string) error { t.Error("control-op probe ran under --fp-raw-probe"); return nil })
		raw := map[string]bool{}
		swapVar(t, &fpRawProbeAt, func(dir string) error {
			raw[dir] = true
			if dir == "/p/acct-01" {
				return fmt.Errorf("%w: read did not answer", overlay.ErrFPProbeWedged)
			}
			return nil
		})
		report, calls := captureReports()
		reportFPWedges([]store.Account{fpRow(1), fpRow(2)}, false, true, report)

		if !raw["/p/acct-01"] || !raw["/p/acct-02"] {
			t.Fatalf("raw probe skipped a row: %v", raw)
		}
		if len(*calls) != 1 || (*calls)[0].label != "acct-01 file provider" || (*calls)[0].healthy {
			t.Fatalf("got %+v, want exactly the raw-wedged acct-01 flagged", *calls)
		}
	})
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
	refreshOnlyCred := &creds.Credential{}
	refreshOnlyCred.ClaudeAiOauth.RefreshToken = "rt"
	refreshOnlyCred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
	syncedUnexpired := &creds.Credential{} // access token, no refresh token
	syncedUnexpired.ClaudeAiOauth.AccessToken = "at"
	syncedUnexpired.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
	syncedExpired := &creds.Credential{}
	syncedExpired.ClaudeAiOauth.AccessToken = "at"
	syncedExpired.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
	hardErr := errors.New("security find-generic-password: exit status 51")

	cases := map[string]struct {
		seedKeychain bool
		keychainCred *creds.Credential // overrides the seeded keychain cred (defaults to usable)
		file         string            // "", "valid", "corrupt", "deadblob"
		keychainRead error             // injected keychain Read fault
		fileDelete   error             // injected file Delete fault
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
		"a refresh-only keychain blob is heal-pending, not an error": {
			seedKeychain: true, keychainCred: refreshOnlyCred,
			wantLabel: "acct-01 credential", wantHealthy: true,
			wantContains: []string{"access token empty", "daemon"},
		},
		"an unexpired synced copy is a healthy read-only replica": {
			seedKeychain: true, keychainCred: syncedUnexpired,
			wantLabel: "acct-01 credential", wantHealthy: true,
			wantContains: []string{"synced read-only copy"},
			wantOmits:    []string{"needs", "ccp login"},
		},
		"an expired synced copy fails with an origin-aware re-login remedy": {
			seedKeychain: true, keychainCred: syncedExpired,
			wantLabel:    "acct-01 credential",
			wantContains: []string{"synced copy expired", "hasn't rotated", "ccp login 1"},
		},
		"a tokenless keychain blob demands re-login and skips the --fix reassert": {
			keychainRead: creds.ErrNoTokens, fix: true,
			wantLabel:    "acct-01 credential",
			wantContains: []string{"no tokens", "ccp login 1"},
		},
		"a tokenless file blob demands re-login": {
			file:         "deadblob",
			wantLabel:    "acct-01 credential",
			wantContains: []string{"no tokens", "ccp login 1"},
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
				seed := usable
				if tc.keychainCred != nil {
					seed = tc.keychainCred
				}
				fk.Put(a.KeychainService, a.KeychainAccount, seed)
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
			case "deadblob":
				if err := os.WriteFile(creds.FileCredentialPath(a.ConfigDir), []byte(`{"claudeAiOauth":{}}`), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			m := &pool.Manager{Creds: fk}

			report, calls := captureReports()
			warnf, warns := captureWarns()
			checkCredential(m, a, tc.fix, report, warnf)

			if len(*warns) != 0 {
				t.Fatalf("got %d unexpected warnings %+v", len(*warns), *warns)
			}
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
	warnf, _ := captureWarns()
	checkCredential(m, a, true, report, warnf)

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

// TestReportOrphanedHolder pins the dead-holder-with-orphans doctor line (2026-07
// incident): an unreachable holder still holding mounts is surfaced with the count
// and the blocking sessions to relaunch, while a reachable holder or one holding no
// mounts stays silent.
func TestReportOrphanedHolder(t *testing.T) {
	accts := []store.Account{
		{ID: 1, ConfigDir: "/p/acct-01", OverlayKind: string(fkoverlay.BackendNFS)},
		{ID: 2, ConfigDir: "/p/acct-02", OverlayKind: string(fkoverlay.BackendNFS)},
		{ID: 3, ConfigDir: "/p/acct-03", OverlayKind: string(fkoverlay.BackendSymlink)},
	}
	cases := map[string]struct {
		reachable bool
		mounted   func(string) bool
		sessions  []procscan.Session
		want      []reportCall
	}{
		"reachable holder is silent": {
			reachable: true,
			mounted:   func(string) bool { return true },
		},
		"unreachable holder holding no mounts is silent": {
			mounted: func(string) bool { return false },
		},
		"dead holder holding the mux root, idle: carcass-clear-and-remount copy": {
			mounted: func(d string) bool { return d == pool.MuxRootDir() },
			want: []reportCall{{
				"mount holder orphans", false,
				"holder dead, 2 orphaned mounts — a fresh mount-holder clears the orphaned mount on its next mount (the fenced pre-mount carcass clear) and remounts automatically",
			}},
		},
		"dead holder holding the mux root, sessions block the unmount": {
			mounted: func(d string) bool { return d == pool.MuxRootDir() },
			sessions: []procscan.Session{
				{PID: 4242, ConfigDir: "/p/acct-01"},
				{PID: 77, ConfigDir: "/p/acct-02"},
				{PID: 9, ConfigDir: "/p/acct-03"}, // a symlink account: never a fuse blocker
			},
			want: []reportCall{{
				"mount holder orphans", false,
				"holder dead, 2 orphaned mounts, waiting on sessions pid 4242, pid 77 — relaunch them so a fresh mount-holder can clear the orphaned mount (its fenced pre-mount carcass clear) and remount",
			}},
		},
		"dead holder holding a legacy per-dir mount only": {
			mounted: func(d string) bool { return d == "/p/acct-01" }, // not the mux root
			want: []reportCall{{
				"mount holder orphans", false,
				"holder dead, 1 orphaned mount — a fresh mount-holder clears the orphaned mount on its next mount (the fenced pre-mount carcass clear) and remounts automatically",
			}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			swapVar(t, &dirMounted, tc.mounted)
			swapVar(t, &scanSessions, func(context.Context) ([]procscan.Session, error) { return tc.sessions, nil })
			report, calls := captureReports()

			reportOrphanedHolder(context.Background(), tc.reachable, accts, report)

			if len(*calls) != len(tc.want) {
				t.Fatalf("got %d reports %+v, want %d", len(*calls), *calls, len(tc.want))
			}
			for i, w := range tc.want {
				if (*calls)[i] != w {
					t.Fatalf("report[%d] = %+v, want %+v", i, (*calls)[i], w)
				}
			}
		})
	}
}

func TestReportStaleWidgetAppex(t *testing.T) {
	started := time.Date(2026, 7, 4, 16, 18, 9, 0, time.Local)
	cases := map[string]struct {
		appexes []daemon.WidgetAppex
		err     error
		want    int
	}{
		"stale appex reported": {appexes: []daemon.WidgetAppex{{PID: 4242, StartedAt: started}}, want: 1},
		"none is silent":       {want: 0},
		"scan error is silent": {err: errors.New("boom"), want: 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			orig := staleWidgetAppexes
			t.Cleanup(func() { staleWidgetAppexes = orig })
			staleWidgetAppexes = func(context.Context, string) ([]daemon.WidgetAppex, error) { return tc.appexes, tc.err }
			report, calls := captureReports()

			reportStaleWidgetAppex(context.Background(), report)

			if len(*calls) != tc.want {
				t.Fatalf("got %d report(s) %+v, want %d", len(*calls), *calls, tc.want)
			}
			if tc.want == 0 {
				return
			}
			c := (*calls)[0]
			if c.label != "widget appex" || c.healthy ||
				!strings.Contains(c.detail, "pid 4242") || !strings.Contains(c.detail, "kill -9 4242") {
				t.Fatalf("report = %+v, want unhealthy widget appex naming pid 4242 and its kill command", c)
			}
		})
	}
}
