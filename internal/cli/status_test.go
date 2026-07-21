package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"github.com/yasyf/cc-pool/internal/version"
)

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
	quarantined := pool.Snapshot{Score: 100, CredentialQuarantined: true}
	quarantined.Account.ID = 3

	model, _ := statusTUI{}.Update(snapsMsg{data: statusData{snaps: []pool.Snapshot{quarantined, exhausted, healthy}}, at: time.Now()})
	tui, ok := model.(statusTUI)
	if !ok {
		t.Fatalf("Update returned %T, want statusTUI", model)
	}
	if len(tui.snaps) != 3 || tui.snaps[0].Account.ID != 2 ||
		tui.snaps[1].Account.ID != 1 || tui.snaps[2].Account.ID != 3 {
		t.Fatalf("TUI must order the usable account first, got %+v", tui.snaps)
	}
}

func TestCredentialQuarantineIsUnusableThroughDaemonStatus(t *testing.T) {
	snaps := fromDaemon([]daemon.AccountStatus{{
		ID: 1, Score: 100, CredentialQuarantined: true,
	}})
	if len(snaps) != 1 || !snaps[0].CredentialQuarantined || snapshotTier(snaps[0]) != 3 {
		t.Fatalf("credential quarantine was not preserved as unusable: %+v", snaps)
	}
	if flag := stripANSI(snapshotFlags(snaps[0])); !strings.Contains(flag, "credential recovery") {
		t.Fatalf("credential quarantine flag = %q, want credential recovery", flag)
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

func writeStatusSnapshotTest(t *testing.T, snapshot daemon.StatusSnapshot) {
	t.Helper()
	if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pool.StatusSnapshotPath(), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestStatusSnapshotJSONUsesExactDiskSnapshotWithoutDaemon(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	generatedAt := time.Now().Add(-time.Minute).Truncate(time.Second)
	want := daemon.NewStatusSnapshot([]daemon.AccountStatus{{
		ID: 1, Label: "from-disk", SampleAge: "37s", HasUsage: true, Remaining5h: 60,
	}}, generatedAt)
	writeStatusSnapshotTest(t, want)

	got, err := statusSnapshotJSON(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !got.GeneratedAt.Equal(generatedAt) || len(got.Accounts) != 1 {
		t.Fatalf("disk snapshot = %+v, want generated_at %v and one account", got, generatedAt)
	}
	if account := got.Accounts[0]; account.Label != "from-disk" || account.SampleAge != "37s" {
		t.Fatalf("disk account = %+v", account)
	}
}

// TestStatusSnapshotJSONDaemonBranch pins that a usable daemon's accounts pass
// through verbatim; a fromDaemon round-trip would fabricate sample_age "0s".
func TestStatusSnapshotJSONDaemonBranch(t *testing.T) {
	cases := map[string]struct {
		daemonVersion string
		wantLabel     string
		wantSampleAge string
	}{
		"usable daemon passes accounts through": {
			daemonVersion: version.String(), wantLabel: "from-daemon", wantSampleAge: "42s",
		},
		"version skew reads exact disk snapshot": {
			daemonVersion: "0.0.0-old", wantLabel: "from-disk", wantSampleAge: "17s",
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
			startDaemonTestServer(t, tc.daemonVersion, func(context.Context, daemon.Op, daemon.Request) daemon.Response {
				return daemon.Response{
					OK: true, Version: tc.daemonVersion,
					Accounts: []daemon.AccountStatus{{
						ID: 1, Label: "from-daemon", SampleAge: "42s",
						HasUsage: true, Remaining5h: 50, Remaining7d: 50,
					}},
				}
			})
			writeStatusSnapshotTest(t, daemon.NewStatusSnapshot([]daemon.AccountStatus{{
				ID: 1, Label: "from-disk", SampleAge: "17s",
			}}, time.Now().Add(-time.Minute)))

			snap, err := statusSnapshotJSON(t.Context())
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
			if a.SampleAge != tc.wantSampleAge {
				t.Errorf("sample_age = %q, want %q passed through verbatim", a.SampleAge, tc.wantSampleAge)
			}
		})
	}
}

func TestReadStatusSnapshotRejectsNonExactFormats(t *testing.T) {
	valid := daemon.NewStatusSnapshot([]daemon.AccountStatus{}, time.Now())
	cases := map[string]daemon.StatusSnapshot{
		"wrong protocol":    func() daemon.StatusSnapshot { s := valid; s.Proto++; return s }(),
		"wrong version":     func() daemon.StatusSnapshot { s := valid; s.Version = "old"; return s }(),
		"missing time":      func() daemon.StatusSnapshot { s := valid; s.GeneratedAt = time.Time{}; return s }(),
		"null account list": func() daemon.StatusSnapshot { s := valid; s.Accounts = nil; return s }(),
	}
	for name, snapshot := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "status.json")
			data, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := readStatusSnapshot(path); err == nil {
				t.Fatal("non-exact status snapshot was accepted")
			}
		})
	}
}

func TestReadStatusSnapshotRejectsUnknownAndTrailingJSON(t *testing.T) {
	valid, err := json.Marshal(daemon.NewStatusSnapshot([]daemon.AccountStatus{}, time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(valid, &object); err != nil {
		t.Fatal(err)
	}
	object["unknown"] = true
	unknown, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"unknown field":  unknown,
		"trailing value": append(append([]byte(nil), valid...), []byte(`{}`)...),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "status.json")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := readStatusSnapshot(path); err == nil {
				t.Fatal("non-exact JSON was accepted")
			}
		})
	}
}

func TestGatherStatusUsesDiskSnapshotWithoutManager(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	writeStatusSnapshotTest(t, daemon.NewStatusSnapshot([]daemon.AccountStatus{{
		ID: 7, Label: "disk-only",
	}}, time.Now()))
	snapshots, _, err := gatherStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 || snapshots[0].Account.ID != 7 || snapshots[0].Account.Label != "disk-only" {
		t.Fatalf("gathered disk snapshots = %+v", snapshots)
	}
}

func TestStatusCommandHasNoLiveSamplingFlag(t *testing.T) {
	if flag := newStatusCmd().Flags().Lookup("live"); flag != nil {
		t.Fatalf("retired live sampling flag remains: %+v", flag)
	}
}

// TestLedgerFooter pins the compact auth-health rollup.
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
			[]daemon.LedgerState{{Policy: "auth.streak", Resource: "acct-01", Faulted: true}},
			"auth health: 1 faulted",
		},
		"faulted rows are counted": {
			[]daemon.LedgerState{
				{Policy: "ratelimit.pool", Resource: "pool", Faulted: true},
				{Policy: "auth.streak", Resource: "/p/acct-03", Faulted: true},
				{Policy: "auth.streak", Resource: "acct-02", Faulted: true},
			},
			"auth health: 3 faulted",
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

// TestRunStatusPlainLedgerFooter pins the ledger replacement for the deleted
// FP-wedge status projection end-to-end through runStatus.
func TestRunStatusPlainLedgerFooter(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "ccp-home")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	startDaemonTestServer(t, "", func(context.Context, daemon.Op, daemon.Request) daemon.Response {
		return daemon.Response{
			OK: true, Version: version.String(),
			Accounts: []daemon.AccountStatus{{
				ID: 1, Label: "work@example.com", HasUsage: true, Remaining5h: 50, Remaining7d: 50,
			}},
			Ledgers: []daemon.LedgerState{{Policy: "auth.streak", Resource: "acct-02", Faulted: true}},
		}
	})

	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	var buf bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&buf)
	cmd.SetContext(t.Context())
	if err := runStatus(cmd, &pool.Manager{Store: st}, false, true); err != nil {
		t.Fatalf("runStatus: %v", err)
	}
	out := stripANSI(buf.String())
	if !strings.Contains(out, "auth health: 1 faulted") {
		t.Errorf("plain status missing the ledger footer:\n%s", out)
	}
}

// TestFPConsentFooter pins the consent footer text and its empty case.
