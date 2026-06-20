package cli

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/fusekit/mountd"
)

// reportCall is one captured doctor report line.
type reportCall struct {
	label   string
	healthy bool
	detail  string
}

// captureReports returns a report func and the slice it appends to.
func captureReports() (func(string, bool, string), *[]reportCall) {
	var calls []reportCall
	return func(label string, healthy bool, detail string) {
		calls = append(calls, reportCall{label, healthy, detail})
	}, &calls
}

// TestReportHolder pins the doctor's mount-holder checks over every
// HolderStatus shape: unreachable-with-fuse-rows fails with the respawn hint,
// unreachable-without-rows says nothing, an orphan holder (no fuse rows, no
// daemon) is noted, skew is a note not a failure, a healthy current holder is
// a plain pass, and the daemon's cached TCC/spawn failures each fail loudly.
func TestReportHolder(t *testing.T) {
	cur := version.String()
	cases := map[string]struct {
		facts    holderFacts
		fuseRows int
		want     []reportCall // label + healthy must match; detail is a substring
		none     bool
	}{
		"unreachable with fuse rows fails with the respawn hint": {
			facts:    holderFacts{reachable: false, daemonUp: true},
			fuseRows: 2,
			want: []reportCall{
				{"mount holder", false, "not running with 2 fuse accounts; the daemon respawns it automatically"},
			},
		},
		"unreachable with no fuse rows says nothing": {
			facts:    holderFacts{reachable: false, daemonUp: true},
			fuseRows: 0,
			none:     true,
		},
		"orphan holder with no fuse rows and no daemon is noted": {
			facts:    holderFacts{reachable: true, version: cur, daemonUp: false},
			fuseRows: 0,
			want: []reportCall{
				{"mount holder", true, "orphan (" + cur + ") running with no fuse accounts; `ccp service uninstall` stops it"},
			},
		},
		"version skew is a note, not a failure": {
			facts:    holderFacts{reachable: true, version: "0.0.1-old", daemonUp: true},
			fuseRows: 1,
			want: []reportCall{
				{"mount holder", true, "0.0.1-old (version skew; the daemon replaces it when the pool is idle)"},
			},
		},
		"healthy current holder passes plainly": {
			facts:    holderFacts{reachable: true, version: cur, daemonUp: true},
			fuseRows: 1,
			want:     []reportCall{{"mount holder", true, cur}},
		},
		"cached TCC block fails": {
			facts: holderFacts{
				reachable: true, version: cur, daemonUp: true,
				cached: &daemon.HolderStatus{TCCError: "grant Network Volumes access"},
			},
			fuseRows: 1,
			want: []reportCall{
				{"mount holder", true, cur},
				{"mount holder TCC", false, "grant Network Volumes access"},
			},
		},
		"cached spawn failure fails": {
			facts: holderFacts{
				reachable: false, daemonUp: true,
				cached: &daemon.HolderStatus{SpawnError: "spawn mount holder: exec format error"},
			},
			fuseRows: 1,
			want: []reportCall{
				{"mount holder", false, "the daemon respawns it automatically"},
				{"mount holder spawn", false, "exec format error"},
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

// TestReportCarcasses pins the dead-mount check: only a fuse row whose dir is
// a mountpoint with ~/.claude not visible through it is flagged; live mounts,
// unmounted dirs, and symlink rows are silent.
func TestReportCarcasses(t *testing.T) {
	accts := []store.Account{
		{ID: 1, ConfigDir: "/p/acct-01", OverlayKind: string(overlay.KindFuse)},    // carcass
		{ID: 2, ConfigDir: "/p/acct-02", OverlayKind: string(overlay.KindFuse)},    // live
		{ID: 3, ConfigDir: "/p/acct-03", OverlayKind: string(overlay.KindFuse)},    // unmounted
		{ID: 4, ConfigDir: "/p/acct-04", OverlayKind: string(overlay.KindSymlink)}, // wrong kind
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

// TestReportCarcassesBoundedOnParkedProbe pins doctor's behavior on the exact
// failure the carcass check exists for: a wedged mirror whose kernel stats
// park in uninterruptible sleep. The mountpoint arm (dirMounted) is now the
// non-blocking overlay.Mounted — a cached Getfsstat read that cannot park — so
// the only wedge-prone seam left is the aliveness arm (mountAliveAt). The bound
// therefore comes solely from MountAliveWithin/aliveProbes; the harness runs
// that real machinery (overlay.StatProbes, what the production seam is built
// on) with the timeout shrunk and the parked row's aliveness stat stuck on a
// channel, while dirMounted answers mounted instantly for both rows.
// reportCarcasses must still return within the bound and flag the parked row
// (an unanswered aliveness stat reads NOT alive, and a mountpoint that is not
// alive is a carcass), while the healthy fast row stays silent.
func TestReportCarcassesBoundedOnParkedProbe(t *testing.T) {
	const parkedDir, healthyDir = "/p/acct-01", "/p/acct-02"
	accts := []store.Account{
		{ID: 1, ConfigDir: parkedDir, OverlayKind: string(overlay.KindFuse)},
		{ID: 2, ConfigDir: healthyDir, OverlayKind: string(overlay.KindFuse)},
	}
	const probeTimeout = 20 * time.Millisecond
	var aliveProbes overlay.StatProbes[bool]
	release := make(chan struct{})
	// The mountpoint arm is the non-blocking overlay.Mounted: it reads the
	// kernel mount table and cannot park, so both rows are seen mounted
	// instantly. Only the aliveness arm reads base THROUGH the mount and wedges.
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
	// Unpark and drain the probe body before the seam restores run
	// (cleanups run LIFO).
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

	// Beating the 2s production statProbeTimeout proves the verdict is
	// bounded, with a wide margin over the single 20ms fake timeout to keep the
	// assertion unflaky.
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

// TestReportWedges pins the partial-wedge check: doctor deep-probes every
// shallow-Live fuse row locally (the holder ships no verdict — the daemon owns
// the probe, and doctor's independent probe surfaces a wedge even with the
// daemon down) and reports it only when the probe fails with a real verdict
// (ErrProbeMissing is no verdict); a not-Live row is left to reportCarcasses;
// nil mounts (unreachable holder) and symlink rows report nothing and never
// probe. The wedge copy must carry the metadata-vs-reads signature and the
// relaunch guidance.
func TestReportWedges(t *testing.T) {
	accts := []store.Account{
		{ID: 1, ConfigDir: "/p/acct-01", OverlayKind: string(overlay.KindFuse)},
		{ID: 2, ConfigDir: "/p/acct-02", OverlayKind: string(overlay.KindSymlink)},
	}
	wedgeDetail := "wedged (serves metadata but hangs reads) — daemon will remount; relaunch its sessions"
	cases := map[string]struct {
		mounts     []mountd.MountInfo
		probeErr   error // returned by deepProbeAt when probed
		want       []reportCall
		wantProbed []string // dirs deepProbeAt must be called with, in order
	}{
		"live row with failing deep probe reported": {
			mounts:     []mountd.MountInfo{{Dir: "/p/acct-01", Base: "/b", Live: true}},
			probeErr:   fmt.Errorf("%w: hung", overlay.ErrProbeWedged),
			want:       []reportCall{{"acct-01 mirror", false, wedgeDetail}},
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
			reportWedges(accts, tc.mounts, report)
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

// TestReportStaleSessions pins the yanked-mount session check: only a session
// on a fuse row that started MORE than staleSessionSlack before the holder's
// current mount of that dir is flagged (exactly the slack is not — the >
// semantics); a zero MountedAt (old holder), a zero StartedAt (unparseable
// etime), a symlink row, and a session born after the mount all stay silent.
func TestReportStaleSessions(t *testing.T) {
	mounted := time.Date(2026, 6, 12, 13, 32, 1, 0, time.Local)
	fuse := store.Account{ID: 1, ConfigDir: "/p/acct-01", OverlayKind: string(overlay.KindFuse)}
	sym := store.Account{ID: 2, ConfigDir: "/p/acct-02", OverlayKind: string(overlay.KindSymlink)}
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

// TestCountFuse pins the fuse-row counter both holder checks key on.
func TestCountFuse(t *testing.T) {
	accts := []store.Account{
		{ID: 1, OverlayKind: string(overlay.KindFuse)},
		{ID: 2, OverlayKind: string(overlay.KindSymlink)},
		{ID: 3, OverlayKind: ""}, // legacy rows default to symlink
		{ID: 4, OverlayKind: string(overlay.KindFuse)},
	}
	if got := countFuse(accts); got != 2 {
		t.Errorf("countFuse = %d, want 2", got)
	}
	if got := countFuse(nil); got != 0 {
		t.Errorf("countFuse(nil) = %d, want 0", got)
	}
}

// TestDoctorHealReportsDiscardedDuplicate pins the CLI-side observability of the
// crash-repair conflict resolution: a daemon-less `doctor --fix` drives the
// stranded-private heal itself, so when both roots hold the same private file
// with differing content it must REPORT — not silently discard — the older
// copy. The test installs NO ResolvedConflictLogf seam of its own; it relies on
// checkStrandedPrivate wiring it, so it catches the production silence the
// default no-op would leave (the daemon-only wiring this fix closed). It then
// probes the seam to prove the wiring was restored, so it cannot leak into the
// next command.
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
	if err := (&overlay.SymlinkProvider{}).Setup(base, dir); err != nil {
		t.Fatal(err)
	}

	// The collision an abnormal shutdown leaves: the same private file in both
	// the account dir and the stranded backing root, with differing content.
	// The newer backing copy must survive; the older in-dir copy is discarded
	// and that discard must be reported.
	priv := overlay.FusePrivateRoot(dir)
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

	if !strings.Contains(out.String(), "discarded stale duplicate") {
		t.Errorf("doctor heal output does not report the discarded duplicate: %q", out.String())
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

	// The seam must be restored after the heal, never left pointed at this
	// command's buffer: a later write through the seam must not reach out.
	before := out.Len()
	overlay.ResolvedConflictLogf("leak probe")
	if out.Len() != before {
		t.Errorf("seam leaked past the heal: a write after checkStrandedPrivate still hit the doctor buffer")
	}
}

// TestDoctorSurfacesFuseFallback pins the degraded-overlay surfacing: an account
// demoted to symlink while the pool default is fuse (an automatic fallback) is
// reported with the re-promote guidance — but only when this machine can host
// fuse, and never for a symlink-default pool or a fuse account.
func TestDoctorSurfacesFuseFallback(t *testing.T) {
	cases := map[string]struct {
		acctKind    string
		defaultKind overlay.Kind
		canHostFuse bool
		wantReport  bool
	}{
		"symlink account, fuse default, can host -> surfaced": {"symlink", overlay.KindFuse, true, true},
		"symlink account, symlink default -> quiet":           {"symlink", overlay.KindSymlink, true, false},
		"symlink account, fuse default, cannot host -> quiet": {"symlink", overlay.KindFuse, false, false},
		"fuse account -> quiet":                               {"fuse", overlay.KindFuse, true, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			st, err := store.Open(filepath.Join(home, "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			// Record the default with fuse-hosting enabled (SetDefaultOverlayKind
			// refuses a fuse default otherwise), then set the check-time capability.
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
