package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/version"
)

// The pane/URL come from fusekit's Backend.Enablement, never a cc-pool literal.
var tccHint = " — " + fuseGrantHint(fkoverlay.BackendNFS) + " (cc-pool falls back to symlink automatically if the grant never lands)"

var ansiRE = regexp.MustCompile(`\x1b\[[0-9;]*m`)

func stripANSI(s string) string { return ansiRE.ReplaceAllString(s, "") }

// TestRenderTablePlain pins the plain (non-TTY) status table; columns are
// % used, never % remaining.
func TestRenderTablePlain(t *testing.T) {
	snaps := []pool.Snapshot{
		{
			Account:  store.Account{ID: 1, Label: "best@example.com"},
			Score:    93.9,
			HasUsage: true,
			Util5h:   0,
			Util7d:   13,
		},
		{
			Account:  store.Account{ID: 2, Label: "busy@example.com"},
			Score:    71.5,
			HasUsage: true,
			Util5h:   58,
			Util7d:   61,
			Stale:    true,
			Resets5h: time.Now().Add(2*time.Hour + 3*time.Minute),
		},
	}
	out := stripANSI(renderTable(snaps, dirPin{}))

	if strings.Contains(out, "pinned") {
		t.Errorf("output must not show pin tokens without a pin\n%s", out)
	}

	for _, want := range []string{"5h used", "7d used", "LIVE", "RESETS"} {
		if !strings.Contains(out, want) {
			t.Errorf("header missing %q\n%s", want, out)
		}
	}
	for _, bad := range []string{"SESS", "FLAGS"} {
		if strings.Contains(out, bad) {
			t.Errorf("output still shows retired label %q\n%s", bad, out)
		}
	}

	if !strings.Contains(out, "58%") || !strings.Contains(out, "61%") {
		t.Errorf("rows should show used%% (58/61)\n%s", out)
	}
	if strings.Contains(out, "42%") || strings.Contains(out, "39%") {
		t.Errorf("rows must not show remaining%% (42/39)\n%s", out)
	}

	if !strings.Contains(out, "▸") {
		t.Errorf("missing next-pick marker\n%s", out)
	}
	if !strings.Contains(out, "stale") {
		t.Errorf("stale account should be flagged\n%s", out)
	}

	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if !strings.Contains(lines[1], "best@example.com") || !strings.HasSuffix(strings.TrimRight(lines[1], " "), "-") {
		t.Errorf("best row should end with '-' for an unknown reset\n%q", lines[1])
	}
	if !strings.Contains(lines[2], "AM") && !strings.Contains(lines[2], "PM") {
		t.Errorf("busy row should show an absolute reset clock, got %q", lines[2])
	}

	if !strings.Contains(out, "next pick") || !strings.Contains(out, "% used") {
		t.Errorf("missing legend line\n%s", out)
	}
}

// TestRenderTableNoDataDash pins that an account with no known-good sample renders
// "-" in the used columns (never a fabricated 0%) and a no-data flag, while a
// genuinely-sampled empty account still shows an honest 0%.
func TestRenderTableNoDataDash(t *testing.T) {
	snaps := []pool.Snapshot{
		{Account: store.Account{ID: 1, Label: "sampled@example.com"}, Score: 90, HasUsage: true},
		{Account: store.Account{ID: 2, Label: "nodata@example.com"}, Score: 50, HasUsage: false, RateLimited: true},
	}
	out := stripANSI(renderTable(snaps, dirPin{}))
	row := func(label string) string {
		for _, l := range strings.Split(out, "\n") {
			if strings.Contains(l, label) {
				return l
			}
		}
		t.Fatalf("row %q not found in\n%s", label, out)
		return ""
	}
	if sampled := row("sampled@example.com"); !strings.Contains(sampled, "0%") {
		t.Errorf("genuinely-sampled empty account should show 0%% used\n%q", sampled)
	}
	nodata := row("nodata@example.com")
	if strings.Contains(nodata, "0%") {
		t.Errorf("no-data account must not show a fabricated 0%% used\n%q", nodata)
	}
	if !strings.Contains(nodata, "no-data") {
		t.Errorf("no-data account must carry the no-data flag\n%q", nodata)
	}
}

// TestAbbreviateHome pins the ~-abbreviation used by the pin summary line.
func TestAbbreviateHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	cases := map[string]struct{ in, want string }{
		"inside home":  {home + "/Code/proj", "~/Code/proj"},
		"home itself":  {home, "~"},
		"outside home": {"/tmp/elsewhere", "/tmp/elsewhere"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := abbreviateHome(tc.in); got != tc.want {
				t.Errorf("abbreviateHome(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestHumanizeResetAt pins humanizeResetAt against a fixed now; inputs are in
// time.Local so .Local() is a no-op and the expectations hold in any zone.
func TestHumanizeResetAt(t *testing.T) {
	now := time.Date(2026, 6, 8, 10, 0, 0, 0, time.Local) // Monday
	at := func(mo, d, h, minute int) time.Time {
		return time.Date(2026, time.Month(mo), d, h, minute, 0, 0, time.Local)
	}
	cases := map[string]struct {
		in   time.Time
		want string
	}{
		"zero / no window":      {time.Time{}, "-"},
		"later today":           {at(6, 8, 15, 58), "3:58 PM"},
		"earlier today (past)":  {at(6, 8, 8, 30), "8:30 AM"},
		"yesterday (stale)":     {at(6, 7, 15, 58), "3:58 PM"},
		"tomorrow":              {at(6, 9, 15, 58), "tomorrow 3:58 PM"},
		"two days (weekday)":    {at(6, 10, 15, 58), "Wed 3:58 PM"},
		"six days (edge in)":    {at(6, 14, 9, 5), "Sun 9:05 AM"},
		"seven days (edge out)": {at(6, 15, 15, 58), "Jun 15, 3:58 PM"},
		"far future":            {at(6, 20, 15, 58), "Jun 20, 3:58 PM"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := humanizeResetAt(tc.in, now); got != tc.want {
				t.Errorf("humanizeResetAt(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestRenderTableEmpty keeps the friendly empty-pool message.
func TestRenderTableEmpty(t *testing.T) {
	if got := renderTable(nil, dirPin{}); !strings.Contains(got, "ccp add") {
		t.Errorf("empty pool should suggest `ccp add`, got %q", got)
	}
}

// TestRenderTablePin pins the pinned-row badge and the pin summary line.
func TestRenderTablePin(t *testing.T) {
	snaps := []pool.Snapshot{
		{Account: store.Account{ID: 1, Label: "best@example.com"}, Score: 90, HasUsage: true},
		{
			Account: store.Account{ID: 2, Label: "pinned@example.com"}, Score: 50, HasUsage: true,
			Util5h: 10, Util7d: 10, Remaining5h: 90, Components: score.Components{RawRemaining5h: 90},
		},
	}
	pin := dirPin{cwd: "/proj", ok: true, view: pool.PinView{
		AccountID: 2, Manual: true, Binding: true,
		ExpiresAt: time.Date(2026, 6, 11, 15, 58, 0, 0, time.Local),
	}}
	out := stripANSI(renderTable(snaps, pin))
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if !strings.Contains(lines[2], "pinned@example.com") || !strings.HasSuffix(strings.TrimRight(lines[2], " "), "pinned") {
		t.Errorf("pinned row must carry the badge\n%s", out)
	}
	if strings.Contains(lines[1], "pinned") {
		t.Errorf("unpinned row must not carry the badge\n%s", out)
	}
	for _, want := range []string{"pinned pinned@example.com", "manual", "until", "/proj"} {
		if !strings.Contains(out, want) {
			t.Errorf("pin line missing %q\n%s", want, out)
		}
	}
}

// TestRenderPinLine pins each pin-state phrasing — every arm of the
// UsableForSticky mirror independently.
func TestRenderPinLine(t *testing.T) {
	healthySnap := func(raw5 float64) []pool.Snapshot {
		return []pool.Snapshot{{
			Account:  store.Account{ID: 2, Label: "p@example.com"},
			HasUsage: true, Components: score.Components{RawRemaining5h: raw5},
		}}
	}
	snaps := healthySnap(50)
	view := func(manual, live, binding bool) pool.PinView {
		pv := pool.PinView{AccountID: 2, Manual: manual, Live: live, Binding: binding}
		if !live {
			pv.ExpiresAt = time.Now().Add(30 * time.Minute)
		}
		return pv
	}
	rateLimited := healthySnap(50)
	rateLimited[0].RateLimited = true
	cases := map[string]struct {
		pin   dirPin
		snaps []pool.Snapshot
		want  []string
		none  bool
	}{
		"no pin":           {pin: dirPin{cwd: "/proj"}, snaps: snaps, none: true},
		"manual countdown": {pin: dirPin{cwd: "/proj", ok: true, view: view(true, false, true)}, snaps: snaps, want: []string{"manual", "until"}},
		"manual live":      {pin: dirPin{cwd: "/proj", ok: true, view: view(true, true, true)}, snaps: snaps, want: []string{"manual", "while sessions live"}},
		"auto waiting":     {pin: dirPin{cwd: "/proj", ok: true, view: view(false, true, false)}, snaps: snaps, want: []string{"auto", "waiting for session end"}},
		"auto countdown":   {pin: dirPin{cwd: "/proj", ok: true, view: view(false, false, true)}, snaps: snaps, want: []string{"auto", "until"}},
		"unknown account":  {pin: dirPin{cwd: "/proj", ok: true, view: view(true, false, true)}, snaps: nil, want: []string{"acct-02"}},
		"exhausted account": {
			pin:   dirPin{cwd: "/proj", ok: true, view: view(true, false, true)},
			snaps: []pool.Snapshot{{Account: store.Account{ID: 2, Label: "p@example.com"}, HasUsage: true, Exhausted: true}},
			want:  []string{"can't serve"},
		},
		"rate-limited account": {
			pin:   dirPin{cwd: "/proj", ok: true, view: view(true, false, true)},
			snaps: rateLimited,
			want:  []string{"can't serve"},
		},
		"below the sticky floor": {
			pin:   dirPin{cwd: "/proj", ok: true, view: view(true, false, true)},
			snaps: healthySnap(score.StickyMinRemaining5h - 1),
			want:  []string{"can't serve"},
		},
		"exactly at the floor stays usable": {
			pin:   dirPin{cwd: "/proj", ok: true, view: view(true, false, true)},
			snaps: healthySnap(score.StickyMinRemaining5h),
			want:  []string{"until"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := stripANSI(renderPinLine(tc.pin, tc.snaps))
			if tc.none {
				if got != "" {
					t.Fatalf("want no line, got %q", got)
				}
				return
			}
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Errorf("line %q missing %q", got, want)
				}
			}
		})
	}
}

// TestSortSnapshots pins the display order mirrored from Pick/PickFallback;
// reset credit can leave an exhausted account out-scoring a healthy one
// (observed 31.5 vs 13.3).
func TestSortSnapshots(t *testing.T) {
	snap := func(id int, score float64, exhausted, rateLimited bool) pool.Snapshot {
		s := pool.Snapshot{Score: score, Exhausted: exhausted, RateLimited: rateLimited}
		s.Account.ID = id
		return s
	}
	cases := map[string]struct {
		in   []pool.Snapshot
		want []int
	}{
		"exhausted sinks below available despite higher score": {
			in:   []pool.Snapshot{snap(1, 31.5, true, false), snap(2, 13.3, false, false)},
			want: []int{2, 1},
		},
		"rate-limited sinks below exhausted despite higher score": {
			in:   []pool.Snapshot{snap(1, 50, false, true), snap(2, 5, true, false)},
			want: []int{2, 1},
		},
		"score still rules within a tier": {
			in:   []pool.Snapshot{snap(1, 13.3, false, false), snap(2, 72.3, false, false), snap(3, 25.9, true, false), snap(4, 31.5, true, false)},
			want: []int{2, 1, 4, 3},
		},
		"full tie keeps input order": {
			in:   []pool.Snapshot{snap(1, 40, false, false), snap(2, 40, false, false)},
			want: []int{1, 2},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			sortSnapshots(tc.in)
			got := make([]int, len(tc.in))
			for i, s := range tc.in {
				got[i] = s.Account.ID
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("order = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestRenderTableUnusableSinks pins that the usable account takes row 1 and
// the ▸ marker even when out-scored by an exhausted one.
func TestRenderTableUnusableSinks(t *testing.T) {
	snaps := []pool.Snapshot{
		{
			Account:   store.Account{ID: 1, Label: "pegged@example.com"},
			Score:     31.5,
			HasUsage:  true,
			Util5h:    100,
			Util7d:    20,
			Exhausted: true,
		},
		{
			Account:  store.Account{ID: 2, Label: "healthy@example.com"},
			Score:    13.3,
			HasUsage: true,
			Util5h:   63,
			Util7d:   12,
		},
	}
	out := stripANSI(renderTable(snaps, dirPin{}))
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if !strings.Contains(lines[1], "healthy@example.com") || !strings.HasPrefix(lines[1], "▸") {
		t.Errorf("usable account must be row 1 with the next-pick marker\n%s", out)
	}
	if !strings.Contains(lines[2], "pegged@example.com") || !strings.Contains(lines[2], "exhausted") {
		t.Errorf("exhausted account must render below the usable one\n%s", out)
	}
}

// TestStatusTUISortsTiered pins the TUI refresh to the shared tiered sort; the
// detail pane's "next pick" line keys off row 0.
func TestStatusTUISortsTiered(t *testing.T) {
	exhausted := pool.Snapshot{Score: 31.5, Exhausted: true}
	exhausted.Account.ID = 1
	healthy := pool.Snapshot{Score: 13.3}
	healthy.Account.ID = 2

	model, _ := statusTUI{}.Update(snapsMsg{data: statusData{snaps: []pool.Snapshot{exhausted, healthy}}, at: time.Now()})
	tui, ok := model.(statusTUI)
	if !ok {
		t.Fatalf("Update returned %T, want statusTUI", model)
	}
	if len(tui.snaps) != 2 || tui.snaps[0].Account.ID != 2 || tui.snaps[1].Account.ID != 1 {
		t.Fatalf("TUI must order the usable account first, got %+v", tui.snaps)
	}
}

// TestFromDaemonHasUsageIndependentOfStale pins that "no-data" means
// never-sampled, not stale.
func TestFromDaemonHasUsageIndependentOfStale(t *testing.T) {
	snaps := fromDaemon([]daemon.AccountStatus{
		{ID: 1, Label: "stale-with-data", Stale: true, HasUsage: true, Remaining7d: 87},
		{ID: 2, Label: "never-sampled", Stale: true, HasUsage: false},
	})

	if !snaps[0].HasUsage {
		t.Fatal("a stale account with data must keep HasUsage=true")
	}
	if f := stripANSI(snapshotFlags(snaps[0])); strings.Contains(f, "no-data") || !strings.Contains(f, "stale") {
		t.Fatalf("stale-with-data must be flagged stale but not no-data, got %q", f)
	}
	if snaps[1].HasUsage {
		t.Fatal("a never-sampled account must have HasUsage=false")
	}
	if f := stripANSI(snapshotFlags(snaps[1])); !strings.Contains(f, "no-data") {
		t.Fatalf("never-sampled must be flagged no-data, got %q", f)
	}
}

// TestSnapshotFlagsAwaitingOrigin pins the origin-owned needs-login rendering:
// an awaiting-origin account (a synced peer copy whose token expired) shows the
// softer "origin stale" warning through the daemon wire, while an owned dead
// chain keeps the hard "needs login". The wire's AwaitingOrigin field must
// thread through fromDaemon for the daemon-backed status path.
func TestSnapshotFlagsAwaitingOrigin(t *testing.T) {
	snaps := fromDaemon([]daemon.AccountStatus{
		{ID: 1, Label: "awaiting", NeedsLogin: true, AwaitingOrigin: true},
		{ID: 2, Label: "owned-dead", NeedsLogin: true},
	})
	if f := stripANSI(snapshotFlags(snaps[0])); !strings.Contains(f, "origin stale") || strings.Contains(f, "needs login") {
		t.Fatalf("awaiting-origin must render \"origin stale\", not \"needs login\"; got %q", f)
	}
	if f := stripANSI(snapshotFlags(snaps[1])); !strings.Contains(f, "needs login") || strings.Contains(f, "origin stale") {
		t.Fatalf("owned dead chain must render \"needs login\"; got %q", f)
	}
}

// TestSnapshotFlagsExhaustedAndOverage pins the badges: overage renders in
// dollars (the API reports cents), and enabled-but-$0 overage earns none.
func TestSnapshotFlagsExhaustedAndOverage(t *testing.T) {
	snaps := fromDaemon([]daemon.AccountStatus{
		{ID: 1, Label: "pegged", HasUsage: true, Exhausted: true, ExtraEnabled: true, ExtraUsed: 177, ExtraLimit: 5000},
		{ID: 2, Label: "healthy", HasUsage: true, Remaining5h: 60, Remaining7d: 90},
		{ID: 3, Label: "enabled-unused", HasUsage: true, Remaining5h: 60, Remaining7d: 90, ExtraEnabled: true, ExtraLimit: 5000},
	})
	f := stripANSI(snapshotFlags(snaps[0]))
	if !strings.Contains(f, "exhausted") {
		t.Fatalf("exhausted account must be badged, got %q", f)
	}
	if !strings.Contains(f, "overage $1.77/$50.00") {
		t.Fatalf("overage must render in dollars, got %q", f)
	}
	if f := stripANSI(snapshotFlags(snaps[1])); f != "" {
		t.Fatalf("healthy account must have no flags, got %q", f)
	}
	if f := stripANSI(snapshotFlags(snaps[2])); f != "" {
		t.Fatalf("overage enabled with $0 spent must not be badged, got %q", f)
	}
}

// TestSnapshotFlagsScoped pins the model-scoped weekly badge: present with the
// API label and util when a scoped bucket exists, absent otherwise.
func TestSnapshotFlagsScoped(t *testing.T) {
	snaps := fromDaemon([]daemon.AccountStatus{
		{ID: 1, Label: "fable-pegged", HasUsage: true, Remaining5h: 60, Remaining7d: 40, Scoped7dModel: "Fable", Scoped7dUtil: 100},
		{ID: 2, Label: "no-scope", HasUsage: true, Remaining5h: 60, Remaining7d: 90},
	})
	f := stripANSI(snapshotFlags(snaps[0]))
	if !strings.Contains(f, "Fable 100%") {
		t.Fatalf("scoped bucket must be badged with its API label and util, got %q", f)
	}
	if f := stripANSI(snapshotFlags(snaps[1])); f != "" {
		t.Fatalf("account without a scoped bucket must not carry the badge, got %q", f)
	}
}

// TestDaemonStatusUsable pins the exact-version gate so a pre-upgrade daemon
// never feeds a partial view.
func TestDaemonStatusUsable(t *testing.T) {
	cur := version.String()
	cases := map[string]struct {
		resp *daemon.Response
		err  error
		want bool
	}{
		"current version":  {&daemon.Response{OK: true, Version: cur}, nil, true},
		"transport error":  {nil, errors.New("dial: no socket"), false},
		"not ok":           {&daemon.Response{OK: false, Version: cur}, nil, false},
		"empty version":    {&daemon.Response{OK: true, Version: ""}, nil, false},
		"mismatch version": {&daemon.Response{OK: true, Version: cur + "-old"}, nil, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := daemonStatusUsable(tc.resp, tc.err); got != tc.want {
				t.Errorf("daemonStatusUsable = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestUsageSuffix pins that unknown usage renders nothing, never a fabricated 0%.
func TestUsageSuffix(t *testing.T) {
	cases := map[string]struct {
		hasUsage     bool
		used5, used7 float64
		scopedModel  string
		scopedUsed   float64
		want         string
	}{
		"unknown usage":         {false, 13, 8, "", 0, ""},
		"unknown ignores used":  {false, 0, 0, "", 0, ""},
		"unknown ignores scope": {false, 13, 8, "Fable", 100, ""},
		"rounds to whole":       {true, 13.3, 8.6, "", 0, " · 5h 13% used · 7d 9% used"},
		"drained pick":          {true, 100, 100, "", 0, " · 5h 100% used · 7d 100% used"},
		"scoped present":        {true, 40, 60, "Fable", 100, " · 5h 40% used · 7d 60% used · Fable 100% used"},
		"scoped rounds":         {true, 40, 60, "Fable", 99.6, " · 5h 40% used · 7d 60% used · Fable 100% used"},
		"absent scope omitted":  {true, 40, 60, "", 100, " · 5h 40% used · 7d 60% used"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := stripANSI(usageSuffix(tc.hasUsage, tc.used5, tc.used7, tc.scopedModel, tc.scopedUsed)); got != tc.want {
				t.Errorf("usageSuffix = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestStatusSnapshotJSONLiveFallback pins --json with no daemon: the snapshot
// assembles from cached-fresh live samples.
func TestStatusSnapshotJSONLiveFallback(t *testing.T) {
	// Isolate HOME first: a live daemon at pool.SocketPath() would otherwise hijack the test.
	t.Setenv("HOME", t.TempDir())

	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.UpsertAccount(store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"), Label: "work@example.com",
		KeychainService: "ccp-test-missing", KeychainAccount: "ccp-test",
	}); err != nil {
		t.Fatal(err)
	}
	// A fresh sample keeps Snapshots(live=true) off the network entirely.
	if err := st.InsertUsageSample(store.UsageSample{
		AccountID: 1, TS: time.Now(), Util5h: 40, Util7d: 20,
	}); err != nil {
		t.Fatal(err)
	}

	snap, err := statusSnapshotJSON(t.Context(), &pool.Manager{Store: st}, false)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Proto != daemon.ProtocolVersion || snap.Version != version.String() {
		t.Errorf("proto/version = %d/%q, want %d/%q", snap.Proto, snap.Version, daemon.ProtocolVersion, version.String())
	}
	if len(snap.Accounts) != 1 {
		t.Fatalf("accounts = %+v, want exactly the seeded account", snap.Accounts)
	}
	a := snap.Accounts[0]
	if a.ID != 1 || a.Label != "work@example.com" || a.Remaining5h != 60 || !a.HasUsage {
		t.Errorf("account = %+v, want id 1, label work@example.com, remaining_5h 60, has_usage", a)
	}
	if age := time.Since(snap.GeneratedAt); age < 0 || age > time.Minute {
		t.Errorf("generated_at %v is not recent (age %v)", snap.GeneratedAt, age)
	}
}

// TestStatusSnapshotJSONDaemonBranch pins that a usable daemon's accounts pass
// through verbatim; a fromDaemon round-trip would fabricate sample_age "0s".
func TestStatusSnapshotJSONDaemonBranch(t *testing.T) {
	cases := map[string]struct {
		daemonVersion string
		wantLabel     string
		wantSampleAge string // "" = don't assert (live fallback recomputes it)
	}{
		"usable daemon passes accounts through": {
			daemonVersion: version.String(), wantLabel: "from-daemon", wantSampleAge: "42s",
		},
		"version skew falls back to live sampling": {
			daemonVersion: "0.0.0-old", wantLabel: "from-store",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// Short HOME under /tmp: macOS caps sun_path at 104 bytes.
			home, err := os.MkdirTemp("/tmp", "ccp-home")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(home) })
			t.Setenv("HOME", home)

			if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
				t.Fatal(err)
			}
			ln, err := net.Listen("unix", pool.SocketPath())
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
					var req daemon.Request
					_ = json.NewDecoder(conn).Decode(&req)
					_ = json.NewEncoder(conn).Encode(daemon.Response{
						Proto: daemon.ProtocolVersion, OK: true, Version: tc.daemonVersion,
						Accounts: []daemon.AccountStatus{{
							ID: 1, Label: "from-daemon", SampleAge: "42s",
							HasUsage: true, Remaining5h: 50, Remaining7d: 50,
						}},
					})
					_ = conn.Close()
				}
			}()

			st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			if err := st.UpsertAccount(store.Account{
				ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"), Label: "from-store",
				KeychainService: "ccp-test-missing", KeychainAccount: "ccp-test",
			}); err != nil {
				t.Fatal(err)
			}
			// Fresh sample keeps the fallback's Snapshots(live=true) off the network.
			if err := st.InsertUsageSample(store.UsageSample{
				AccountID: 1, TS: time.Now(), Util5h: 50, Util7d: 50,
			}); err != nil {
				t.Fatal(err)
			}

			snap, err := statusSnapshotJSON(t.Context(), &pool.Manager{Store: st}, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(snap.Accounts) != 1 {
				t.Fatalf("accounts = %+v, want exactly one", snap.Accounts)
			}
			a := snap.Accounts[0]
			if a.Label != tc.wantLabel {
				t.Errorf("label = %q, want %q (wrong branch taken)", a.Label, tc.wantLabel)
			}
			if tc.wantSampleAge != "" && a.SampleAge != tc.wantSampleAge {
				t.Errorf("sample_age = %q, want %q passed through verbatim", a.SampleAge, tc.wantSampleAge)
			}
		})
	}
}

// TestHolderFooter pins the holder alert; holder skew/spawn failures are
// intentionally unsurfaced (the multi-tenant holder isn't cc-pool's to police).
func TestHolderFooter(t *testing.T) {
	cases := map[string]struct {
		h    *daemon.HolderStatus
		want string
	}{
		"nil (live path / pre-holder daemon)": {nil, ""},
		"healthy current holder is silent": {
			&daemon.HolderStatus{Version: version.String(), Mounts: 3}, "",
		},
		"TCC blocked carries the settings deep link": {
			&daemon.HolderStatus{TCCError: "grant Network Volumes access", TCCBlockedBackend: fkoverlay.BackendNFS},
			"mount holder: grant needed — grant Network Volumes access" + tccHint,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := stripANSI(holderFooter(tc.h)); got != tc.want {
				t.Errorf("holderFooter = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestHolderFooterWedged pins the wedged-mirror footer.
func TestHolderFooterWedged(t *testing.T) {
	cases := map[string]struct {
		h    *daemon.HolderStatus
		want string
	}{
		"zero wedged mirrors is silent": {
			&daemon.HolderStatus{Version: version.String(), Mounts: 2}, "",
		},
		"one wedged mirror is singular": {
			&daemon.HolderStatus{Version: version.String(), Mounts: 2, WedgedMounts: 1},
			"mount holder: 1 wedged mirror — run `ccp doctor`",
		},
		"three wedged mirrors is plural": {
			&daemon.HolderStatus{Version: version.String(), Mounts: 3, WedgedMounts: 3},
			"mount holder: 3 wedged mirrors — run `ccp doctor`",
		},
		"wedged prints regardless of holder version": {
			&daemon.HolderStatus{Version: "0.0.1-old", WedgedMounts: 1},
			"mount holder: 1 wedged mirror — run `ccp doctor`",
		},
		"TCC outranks wedged": {
			&daemon.HolderStatus{TCCError: "tcc-msg", WedgedMounts: 1, TCCBlockedBackend: fkoverlay.BackendNFS},
			"mount holder: grant needed — tcc-msg" + tccHint,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := stripANSI(holderFooter(tc.h)); got != tc.want {
				t.Errorf("holderFooter = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestFPWedgedFooter pins the File Provider wedge footer: a breaker-parked
// domain reads "parked (wedged)", one still recovering reads "wedged
// (recovering)", and an empty list is silent.
func TestFPWedgedFooter(t *testing.T) {
	cases := map[string]struct {
		wedged []daemon.FPDomainState
		want   string
	}{
		"none is silent":  {nil, ""},
		"empty is silent": {[]daemon.FPDomainState{}, ""},
		"parked domain": {
			[]daemon.FPDomainState{{ID: 3, BreakerTripped: true}},
			"✗ acct-03 file provider parked (wedged)",
		},
		"recovering domain": {
			[]daemon.FPDomainState{{ID: 5}},
			"✗ acct-05 file provider wedged (recovering)",
		},
		"multiple, one per line": {
			[]daemon.FPDomainState{{ID: 1, BreakerTripped: true}, {ID: 2}},
			"✗ acct-01 file provider parked (wedged)\n✗ acct-02 file provider wedged (recovering)",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := stripANSI(fpWedgedFooter(tc.wedged)); got != tc.want {
				t.Errorf("fpWedgedFooter = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestLedgerFooter pins the compact self-heal rollup: silent unless a row is
// faulted or parked (a parked row counts once, as parked), then counts plus the
// doctor pointer — per-row detail stays `ccp doctor`'s job.
func TestLedgerFooter(t *testing.T) {
	cases := map[string]struct {
		ledgers []daemon.LedgerState
		want    string
	}{
		"none is silent":  {nil, ""},
		"empty is silent": {[]daemon.LedgerState{}, ""},
		"healthy rows are silent": {
			[]daemon.LedgerState{
				{Policy: "auth.streak", Resource: "/p/acct-01", Strikes: 2},
				{Policy: "ratelimit.pool", Resource: "pool", Attempts: 1},
			},
			"",
		},
		"faulted row": {
			[]daemon.LedgerState{{Policy: "fuse.deepwedge", Resource: "/p/acct-01", Faulted: true}},
			"self-heal: 1 faulted — run `ccp doctor` for detail",
		},
		"parked row counts once, as parked": {
			[]daemon.LedgerState{{Policy: "fp.domain", Resource: "/p/acct-02", Faulted: true, Parked: true}},
			"self-heal: 1 parked — run `ccp doctor` for detail",
		},
		"faulted and parked together": {
			[]daemon.LedgerState{
				{Policy: "fuse.deepwedge", Resource: "/p/acct-01", Faulted: true},
				{Policy: "auth.streak", Resource: "/p/acct-03", Faulted: true},
				{Policy: "fp.domain", Resource: "/p/acct-02", Faulted: true, Parked: true},
			},
			"self-heal: 2 faulted, 1 parked — run `ccp doctor` for detail",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := stripANSI(ledgerFooter(tc.ledgers)); got != tc.want {
				t.Errorf("ledgerFooter = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestRunStatusPlainFPWedgedFooter pins the FP-wedge footer end-to-end through
// runStatus against a fake daemon socket.
func TestRunStatusPlainFPWedgedFooter(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "ccp-home")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", pool.SocketPath())
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
			var req daemon.Request
			_ = json.NewDecoder(conn).Decode(&req)
			_ = json.NewEncoder(conn).Encode(daemon.Response{
				Proto: daemon.ProtocolVersion, OK: true, Version: version.String(),
				Accounts: []daemon.AccountStatus{{
					ID: 1, Label: "work@example.com", HasUsage: true, Remaining5h: 50, Remaining7d: 50,
				}},
				FPWedged: []daemon.FPDomainState{{ID: 2, ConfigDir: "/p/acct-02", BreakerTripped: true}},
			})
			_ = conn.Close()
		}
	}()

	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	var buf bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&buf)
	cmd.SetContext(t.Context())
	if err := runStatus(cmd, &pool.Manager{Store: st}, false, false, true); err != nil {
		t.Fatalf("runStatus: %v", err)
	}
	out := stripANSI(buf.String())
	if !strings.Contains(out, "acct-02 file provider parked (wedged)") {
		t.Errorf("plain status missing the FP-wedge footer:\n%s", out)
	}
}

// TestRunStatusPlainHolderFooter pins the holder footer end-to-end through
// runStatus against a fake daemon socket.
func TestRunStatusPlainHolderFooter(t *testing.T) {
	cases := map[string]struct {
		holder *daemon.HolderStatus
		want   string // "" = no holder mention at all
	}{
		"alerting holder prints the footer": {
			holder: &daemon.HolderStatus{Version: version.String(), Mounts: 1, WedgedMounts: 1},
			want:   "mount holder: 1 wedged mirror — run `ccp doctor`",
		},
		"healthy holder prints nothing": {
			holder: &daemon.HolderStatus{Version: version.String(), Mounts: 2},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// Short HOME under /tmp: macOS caps sun_path at 104 bytes.
			home, err := os.MkdirTemp("/tmp", "ccp-home")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(home) })
			t.Setenv("HOME", home)
			if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
				t.Fatal(err)
			}
			ln, err := net.Listen("unix", pool.SocketPath())
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
					var req daemon.Request
					_ = json.NewDecoder(conn).Decode(&req)
					_ = json.NewEncoder(conn).Encode(daemon.Response{
						Proto: daemon.ProtocolVersion, OK: true, Version: version.String(),
						Accounts: []daemon.AccountStatus{{
							ID: 1, Label: "work@example.com",
							HasUsage: true, Remaining5h: 50, Remaining7d: 50,
						}},
						Holder: tc.holder,
					})
					_ = conn.Close()
				}
			}()

			st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })

			var buf bytes.Buffer
			cmd := &cobra.Command{}
			cmd.SetOut(&buf)
			cmd.SetContext(t.Context())
			if err := runStatus(cmd, &pool.Manager{Store: st}, false, false, true); err != nil {
				t.Fatalf("runStatus: %v", err)
			}
			out := stripANSI(buf.String())
			if !strings.Contains(out, "work@example.com") {
				t.Fatalf("table missing the daemon's account:\n%s", out)
			}
			if tc.want == "" {
				if strings.Contains(out, "mount holder") {
					t.Errorf("healthy holder must print nothing:\n%s", out)
				}
				return
			}
			if !strings.Contains(out, tc.want) {
				t.Errorf("output missing footer %q:\n%s", tc.want, out)
			}
		})
	}
}

// TestFPConsentFooter pins the consent footer text and its empty case.
func TestFPConsentFooter(t *testing.T) {
	if got := fpConsentFooter(false); got != "" {
		t.Errorf("fpConsentFooter(false) = %q, want empty", got)
	}
	got := stripANSI(fpConsentFooter(true))
	for _, frag := range []string{"app-group consent prompt", "restart the daemon", "ccp doctor"} {
		if !strings.Contains(got, frag) {
			t.Errorf("fpConsentFooter(true) = %q, missing %q", got, frag)
		}
	}
}

// TestRunStatusPlainFPConsentFooter pins the consent footer end-to-end through
// runStatus against a fake daemon socket: shown when the daemon reports its bridge
// bind parked on the consent prompt, hidden otherwise.
func TestRunStatusPlainFPConsentFooter(t *testing.T) {
	cases := map[string]struct {
		consent   bool
		wantShown bool
	}{
		"pending shows the banner": {true, true},
		"not pending hides it":     {false, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// Short HOME under /tmp: macOS caps sun_path at 104 bytes.
			home, err := os.MkdirTemp("/tmp", "ccp-home")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(home) })
			t.Setenv("HOME", home)
			if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
				t.Fatal(err)
			}
			ln, err := net.Listen("unix", pool.SocketPath())
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
					var req daemon.Request
					_ = json.NewDecoder(conn).Decode(&req)
					_ = json.NewEncoder(conn).Encode(daemon.Response{
						Proto: daemon.ProtocolVersion, OK: true, Version: version.String(),
						Accounts: []daemon.AccountStatus{{
							ID: 1, Label: "work@example.com", HasUsage: true, Remaining5h: 50, Remaining7d: 50,
						}},
						FPConsentPending: tc.consent,
					})
					_ = conn.Close()
				}
			}()

			st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })

			var buf bytes.Buffer
			cmd := &cobra.Command{}
			cmd.SetOut(&buf)
			cmd.SetContext(t.Context())
			if err := runStatus(cmd, &pool.Manager{Store: st}, false, false, true); err != nil {
				t.Fatalf("runStatus: %v", err)
			}
			out := stripANSI(buf.String())
			if !strings.Contains(out, "work@example.com") {
				t.Fatalf("table missing the daemon's account:\n%s", out)
			}
			shown := strings.Contains(out, "app-group consent prompt")
			if shown != tc.wantShown {
				t.Errorf("consent footer shown=%v, want %v:\n%s", shown, tc.wantShown, out)
			}
		})
	}
}
