package daemon

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/forecast"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
)

// readSnapshot reads and decodes the snapshot file, failing the test on any error.
func readSnapshot(t *testing.T, path string) StatusSnapshot {
	t.Helper()
	data, err := os.ReadFile(path) //nolint:gosec // G304: path is under the test's own t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	var snap StatusSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatalf("decode snapshot: %v\n%s", err, data)
	}
	return snap
}

func TestWriteStatusSnapshotRoundTrip(t *testing.T) {
	s, _ := newTestServer(t)
	dir := t.TempDir()
	s.snapshot = filepath.Join(dir, "status.json")

	if err := s.writeStatusSnapshot(t.Context()); err != nil {
		t.Fatal(err)
	}

	snap := readSnapshot(t, s.snapshot)
	if snap.Proto != ProtocolVersion {
		t.Errorf("proto = %d, want %d", snap.Proto, ProtocolVersion)
	}
	if snap.Version != version.String() {
		t.Errorf("version = %q, want %q", snap.Version, version.String())
	}
	if !snap.GeneratedAt.Equal(snap.GeneratedAt.Truncate(time.Second)) {
		t.Errorf("generated_at %v carries sub-second precision", snap.GeneratedAt)
	}
	if age := time.Since(snap.GeneratedAt); age < 0 || age > time.Minute {
		t.Errorf("generated_at %v is not recent (age %v)", snap.GeneratedAt, age)
	}

	// The harness seeds acct-1 at util 10 and acct-2 at util 50.
	want5h := map[int]float64{1: 90, 2: 50}
	if len(snap.Accounts) != len(want5h) {
		t.Fatalf("accounts = %d, want %d: %+v", len(snap.Accounts), len(want5h), snap.Accounts)
	}
	for _, a := range snap.Accounts {
		if want, ok := want5h[a.ID]; !ok || a.Remaining5h != want {
			t.Errorf("acct-%02d remaining_5h = %.1f, want %.1f", a.ID, a.Remaining5h, want)
		}
		if !a.HasUsage {
			t.Errorf("acct-%02d has_usage = false on a sampled account", a.ID)
		}
	}

	// Atomic write must leave neither temp files nor anything else behind.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "status.json" {
		t.Errorf("snapshot dir not clean: %v", entries)
	}
	info, err := os.Stat(s.snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("snapshot perms = %o, want 600", perm)
	}
}

func TestWriteStatusSnapshotOverwrites(t *testing.T) {
	s, _ := newTestServer(t)
	s.snapshot = filepath.Join(t.TempDir(), "status.json")

	if err := s.writeStatusSnapshot(t.Context()); err != nil {
		t.Fatal(err)
	}
	// A newer sample must replace the file's view of acct-1 on the next write.
	if err := s.m.Store.InsertUsageSample(store.UsageSample{
		AccountID: 1, TS: time.Now().Add(time.Minute), Util5h: 70, Util7d: 70,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.writeStatusSnapshot(t.Context()); err != nil {
		t.Fatal(err)
	}

	snap := readSnapshot(t, s.snapshot)
	for _, a := range snap.Accounts {
		if a.ID == 1 && a.Remaining5h != 30 {
			t.Errorf("acct-01 remaining_5h = %.1f after newer sample, want 30", a.Remaining5h)
		}
	}
}

func TestWriteStatusSnapshotError(t *testing.T) {
	s, _ := newTestServer(t)
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	s.snapshot = filepath.Join(blocker, "status.json") // parent is a regular file

	err := s.writeStatusSnapshot(t.Context())
	if err == nil {
		t.Fatal("expected an error writing under a regular file")
	}
	if !strings.Contains(err.Error(), "write status snapshot") {
		t.Errorf("error %q lacks the write-layer wrap", err)
	}
}

func TestPollOnceWritesSnapshot(t *testing.T) {
	// Redirect ClaudeDir/StateDir off the real ~/.claude and ~/.cc-pool.
	t.Setenv("HOME", t.TempDir())
	s, _ := newTestServer(t)
	s.snapshot = filepath.Join(t.TempDir(), "status.json")

	s.pollOnce(t.Context())

	snap := readSnapshot(t, s.snapshot)
	if len(snap.Accounts) != 2 {
		t.Fatalf("accounts = %d, want 2: %+v", len(snap.Accounts), snap.Accounts)
	}
}

func TestPollOnceLogsSnapshotFailure(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	s, _ := newTestServer(t)
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	s.snapshot = filepath.Join(blocker, "status.json")

	// Must complete the poll (the snapshot write is its last step) and log the
	// failure rather than aborting.
	s.pollOnce(t.Context())

	if !strings.Contains(buf.String(), "status snapshot:") {
		t.Errorf("log missing snapshot failure:\n%s", buf.String())
	}
}

// TestStatusSnapshotJSONKeys pins the exact wire keys the Swift widget decodes.
// Renaming or re-casing any of these is a breaking change for out-of-process
// readers and must bump ProtocolVersion.
func TestStatusSnapshotJSONKeys(t *testing.T) {
	now := time.Date(2026, 6, 11, 12, 0, 0, 500e6, time.UTC) // sub-second: truncation must strip it

	t.Run("fully populated", func(t *testing.T) {
		full := AccountStatus{
			ID: 1, ConfigDir: "/x/acct-01", Label: "a@b.c", OverlayKind: "symlink",
			Score: 95.5, Remaining5h: 90, Remaining7d: 80, ActiveSessions: 2,
			RateLimited: true, Exhausted: true, HasUsage: true, Stale: true,
			Resets5h: now, Resets7d: now, SampleAge: "30s",
			Burn5hPerHour: 12, Burn7dPerHour: 8, Projected5hAtReset: 62, Depleted5hAt: now,
			ExtraEnabled: true, ExtraUsed: 177, ExtraLimit: 5000,
		}
		// A second usable burning account with no known reset, so the pool
		// rollup emits every PoolOutlook key (burn, dry_at) for the pin below.
		burning := AccountStatus{
			ID: 2, ConfigDir: "/x/acct-02", Label: "b@c.d", OverlayKind: "symlink",
			HasUsage: true, Remaining5h: 50, Remaining7d: 60, SampleAge: "30s",
			Burn5hPerHour: 10,
		}
		data, err := json.Marshal(NewStatusSnapshot([]AccountStatus{full, burning}, now))
		if err != nil {
			t.Fatal(err)
		}

		var top map[string]json.RawMessage
		if err := json.Unmarshal(data, &top); err != nil {
			t.Fatal(err)
		}
		assertKeys(t, "top-level", top, []string{"proto", "version", "generated_at", "accounts", "pool"})
		if got := string(top["generated_at"]); got != `"2026-06-11T12:00:00Z"` {
			t.Errorf("generated_at = %s, want whole-second UTC", got)
		}
		// Absolute pin, not == ProtocolVersion: deployed widgets hard-reject any
		// other value. If a bump is intentional, update supportedProto in
		// widget/Sources/Widget/Provider.swift and re-ship the widget with it.
		if got := string(top["proto"]); got != "1" {
			t.Errorf("snapshot proto = %s; the on-disk format is pinned at 1 for the widget", got)
		}

		var accounts []map[string]json.RawMessage
		if err := json.Unmarshal(top["accounts"], &accounts); err != nil {
			t.Fatal(err)
		}
		assertKeys(t, "account", accounts[0], []string{
			"id", "config_dir", "label", "overlay_kind", "score",
			"remaining_5h", "remaining_7d", "active_sessions", "rate_limited",
			"exhausted", "has_usage", "stale", "resets_5h", "resets_7d",
			"sample_age", "burn_5h_per_hour", "burn_7d_per_hour",
			"projected_5h_at_reset", "depleted_5h_at",
			"extra_enabled", "extra_used", "extra_limit", "components",
		})

		var poolBlock map[string]json.RawMessage
		if err := json.Unmarshal(top["pool"], &poolBlock); err != nil {
			t.Fatal(err)
		}
		assertKeys(t, "pool", poolBlock, []string{
			"remaining_5h_pct", "remaining_7d_pct", "burn_5h_per_hour",
			"net_burn_5h_per_hour", "pace_5h", "pace_7d", "dry_at", "mood",
		})
		// Only acct-2 is usable (acct-1 is rate-limited): mean remaining 50,
		// projected dry with no reset relief bumps easy to uneasy.
		if got := string(poolBlock["mood"]); got != `"uneasy"` {
			t.Errorf("pool mood = %s, want uneasy", got)
		}
		// With one usable account and no reset inside the hour, net equals
		// its burn: 50→40 over the hour.
		if got := string(poolBlock["net_burn_5h_per_hour"]); got != "10" {
			t.Errorf("pool net_burn_5h_per_hour = %s, want 10", got)
		}
		// Pace5h = gross burn / (20%/h × usable). One usable account
		// (acct-1 is rate-limited) burning 10 → 10/(20×1) = 0.5, binary-exact.
		if got := string(poolBlock["pace_5h"]); got != "0.5" {
			t.Errorf("pool pace_5h = %s, want 0.5", got)
		}
		// acct-2 has no 7d burn and acct-1 is excluded, so the 7d pace is a
		// real 0 — pinned because pace is deliberately not omitempty.
		if got := string(poolBlock["pace_7d"]); got != "0" {
			t.Errorf("pool pace_7d = %s, want 0", got)
		}

		// score.Components has no json tags, so its keys are PascalCase amid
		// snake_case siblings — the widget must skip it, never decode it.
		var components map[string]json.RawMessage
		if err := json.Unmarshal(accounts[0]["components"], &components); err != nil {
			t.Fatal(err)
		}
		assertKeys(t, "components", components, []string{
			"Eff5", "Eff7", "RawRemaining5h", "RawRemaining7d",
			"Remaining5h", "Remaining7d", "SessionPenalty", "RateLimitPenalty",
			"NeedsLoginPenalty", "StalePenalty", "Barrier5h", "Barrier7d", "RunwayPenalty",
		})
	})

	t.Run("zero value omits omitempty fields", func(t *testing.T) {
		data, err := json.Marshal(NewStatusSnapshot([]AccountStatus{{}}, now))
		if err != nil {
			t.Fatal(err)
		}
		var top map[string]json.RawMessage
		if err := json.Unmarshal(data, &top); err != nil {
			t.Fatal(err)
		}
		var accounts []map[string]json.RawMessage
		if err := json.Unmarshal(top["accounts"], &accounts); err != nil {
			t.Fatal(err)
		}
		for _, absent := range []string{
			"exhausted", "extra_enabled", "extra_used", "extra_limit",
			"burn_5h_per_hour", "projected_5h_at_reset", "depleted_5h_at",
		} {
			if _, ok := accounts[0][absent]; ok {
				t.Errorf("zero-value account must omit %q (the widget models it as optional)", absent)
			}
		}
		// The zero time is NOT omitted; it serializes as year 1 and the widget
		// must treat it as "no active window".
		if got := string(accounts[0]["resets_5h"]); got != `"0001-01-01T00:00:00Z"` {
			t.Errorf("zero resets_5h = %s, want year-1 sentinel", got)
		}
		// A never-sampled account yields no rollup: the widget falls back to
		// its locally-derived outlook when "pool" is absent.
		if _, ok := top["pool"]; ok {
			t.Error("never-sampled pool must omit the pool block")
		}
	})

	t.Run("idle pool omits gross burn, pins net at 0", func(t *testing.T) {
		// A sampled but idle account: the pool block is present and the gross
		// burn is exactly 0, which omitempty drops — the widget models it as
		// an optional. Net burn is deliberately NOT omitempty: 0 is a real
		// value (a balanced pool), and an absent key reads as "daemon
		// predates the field", flipping the widget to its gross fallback.
		idle := AccountStatus{ID: 1, HasUsage: true, Remaining5h: 50, Remaining7d: 50}
		data, err := json.Marshal(NewStatusSnapshot([]AccountStatus{idle}, now))
		if err != nil {
			t.Fatal(err)
		}
		var top map[string]json.RawMessage
		if err := json.Unmarshal(data, &top); err != nil {
			t.Fatal(err)
		}
		var poolBlock map[string]json.RawMessage
		if err := json.Unmarshal(top["pool"], &poolBlock); err != nil {
			t.Fatal(err)
		}
		assertKeys(t, "idle pool", poolBlock, []string{
			"remaining_5h_pct", "remaining_7d_pct", "net_burn_5h_per_hour",
			"pace_5h", "pace_7d", "mood",
		})
		if got := string(poolBlock["net_burn_5h_per_hour"]); got != "0" {
			t.Errorf("idle pool net_burn_5h_per_hour = %s, want 0", got)
		}
		// Both paces are deliberately NOT omitempty: an idle pool sustains
		// forever (pace 0), and an absent key would read as "old daemon" and
		// flip the widget to its local skew derivation. The keys must appear
		// with a literal 0.
		if got := string(poolBlock["pace_5h"]); got != "0" {
			t.Errorf("idle pool pace_5h = %s, want 0", got)
		}
		if got := string(poolBlock["pace_7d"]); got != "0" {
			t.Errorf("idle pool pace_7d = %s, want 0", got)
		}
	})

	t.Run("recovering pool serializes negative net burn", func(t *testing.T) {
		// An exhausted account refilling inside the horizon: net is negative
		// (recovering) and must reach the wire — the widget's "refilling"
		// caption depends on it.
		drained := AccountStatus{
			ID: 1, HasUsage: true, Remaining5h: 0, Remaining7d: 50,
			Resets5h: now.Add(20 * time.Minute),
		}
		data, err := json.Marshal(NewStatusSnapshot([]AccountStatus{drained}, now))
		if err != nil {
			t.Fatal(err)
		}
		var top map[string]json.RawMessage
		if err := json.Unmarshal(data, &top); err != nil {
			t.Fatal(err)
		}
		var poolBlock map[string]json.RawMessage
		if err := json.Unmarshal(top["pool"], &poolBlock); err != nil {
			t.Fatal(err)
		}
		if got := string(poolBlock["net_burn_5h_per_hour"]); got != "-100" {
			t.Errorf("recovering pool net_burn_5h_per_hour = %s, want -100", got)
		}
	})

	t.Run("empty pool marshals as empty array", func(t *testing.T) {
		// Both empty-pool producers: the daemon write path (ToStatuses) and the
		// --json daemon branch, where Response.Accounts is omitempty so an empty
		// socket reply decodes as a NIL slice that NewStatusSnapshot must
		// normalize — "accounts": null would break the widget's decoder.
		for name, accounts := range map[string][]AccountStatus{
			"via ToStatuses":      ToStatuses(nil),
			"via nil socket pass": nil,
		} {
			data, err := json.Marshal(NewStatusSnapshot(accounts, now))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(data), `"accounts":[]`) {
				t.Errorf("%s: empty pool must serialize accounts as [], got %s", name, data)
			}
			if strings.Contains(string(data), `"pool"`) {
				t.Errorf("%s: empty pool must omit the pool block, got %s", name, data)
			}
		}
	})
}

// TestWriteStatusSnapshotForecast pins the forecast path end to end: seeded
// burn history must surface as wire predictions and the pool rollup.
func TestWriteStatusSnapshotForecast(t *testing.T) {
	s, _ := newTestServer(t)
	s.snapshot = filepath.Join(t.TempDir(), "status.json")

	// The harness seeded acct-1 at util 10 "now"; extend it backward into a
	// steady climb of +2%/3min = 40%/hr. Whole-second timestamps survive the
	// store's integer-second column without skewing the slope.
	latest, ok, err := s.m.Store.LatestUsageSample(1)
	if err != nil || !ok {
		t.Fatalf("latest sample: ok=%v err=%v", ok, err)
	}
	for i := 1; i <= 4; i++ {
		if err := s.m.Store.InsertUsageSample(store.UsageSample{
			AccountID: 1,
			TS:        latest.TS.Add(-time.Duration(i) * 3 * time.Minute),
			Util5h:    latest.Util5h - float64(i)*2,
			Util7d:    latest.Util7d,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// The 7d burn needs a wider lookback (Burn7dWindow=6h, min span 3h), so seed
	// a separate hourly Util7d climb of +2%/hr out to -4h. The recent 5h cluster
	// holds Util7d flat at the top (latest.Util7d), so the 7d secant is the clean
	// endpoint slope (latest.Util7d − 2) / 4h = 2%/hr. Util5h on these older rows
	// is irrelevant: they fall outside the 45-min 5h window.
	for i := 1; i <= 4; i++ {
		if err := s.m.Store.InsertUsageSample(store.UsageSample{
			AccountID: 1,
			TS:        latest.TS.Add(-time.Duration(i) * time.Hour),
			Util5h:    latest.Util5h,
			Util7d:    latest.Util7d - float64(i)*2,
		}); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.writeStatusSnapshot(t.Context()); err != nil {
		t.Fatal(err)
	}
	snap := readSnapshot(t, s.snapshot)

	var acct1 *AccountStatus
	for i := range snap.Accounts {
		if snap.Accounts[i].ID == 1 {
			acct1 = &snap.Accounts[i]
		}
	}
	if acct1 == nil {
		t.Fatalf("acct-1 missing from snapshot: %+v", snap.Accounts)
	}
	if acct1.Burn5hPerHour != 40 {
		t.Errorf("acct-1 burn_5h_per_hour = %v, want 40", acct1.Burn5hPerHour)
	}
	// The seeded +2%/hr Util7d climb surfaces as the gated 7d display drain.
	if acct1.Burn7dPerHour != 2 {
		t.Errorf("acct-1 burn_7d_per_hour = %v, want 2", acct1.Burn7dPerHour)
	}
	// No reset is known, so depletion is projected and at-reset is omitted:
	// 90 points left at 40%/hr = 2h15m from the latest sample.
	wantDepleted := latest.TS.Add(2*time.Hour + 15*time.Minute).Truncate(time.Second)
	if !acct1.Depleted5hAt.Equal(wantDepleted) {
		t.Errorf("acct-1 depleted_5h_at = %v, want %v", acct1.Depleted5hAt, wantDepleted)
	}
	if acct1.Projected5hAtReset != 0 {
		t.Errorf("acct-1 projected_5h_at_reset = %v, want omitted with no known reset", acct1.Projected5hAtReset)
	}

	if snap.Pool == nil {
		t.Fatal("pool block missing from a sampled snapshot")
	}
	// acct-1 90 remaining + acct-2 50 remaining, combined burn 40%/hr:
	// 140 points / 40 = dry in 3.5h with no reset relief — chill bumps to easy.
	if snap.Pool.Remaining5hPct != 70 {
		t.Errorf("pool remaining_5h_pct = %v, want 70", snap.Pool.Remaining5hPct)
	}
	if snap.Pool.Burn5hPerHour != 40 {
		t.Errorf("pool burn_5h_per_hour = %v, want 40", snap.Pool.Burn5hPerHour)
	}
	// No reset lands inside the hour, so net is the mean of burns: (40+0)/2.
	if snap.Pool.NetBurn5hPerHour != 20 {
		t.Errorf("pool net_burn_5h_per_hour = %v, want 20", snap.Pool.NetBurn5hPerHour)
	}
	// Pace5h = Σburn5h / (20%/h × usable) = 40/(20×2) = 1.0, exactly the
	// break-even fixed point.
	if snap.Pool.Pace5h != 1 {
		t.Errorf("pool pace_5h = %v, want 1", snap.Pool.Pace5h)
	}
	// Pace7d = Σburn7d / (regen7dPerHour × usable). Only acct-1 burns the 7d
	// window (2%/hr); acct-2 has a single sample, so its 7d burn is 0. Replicate
	// PoolOf's float ops to pin the value bit-exactly.
	regen7d := 100.0 / (7 * 24)
	wantPace7d := 2.0 / (regen7d * 2)
	if snap.Pool.Pace7d != wantPace7d {
		t.Errorf("pool pace_7d = %v, want %v", snap.Pool.Pace7d, wantPace7d)
	}
	if snap.Pool.DryAt.IsZero() {
		t.Error("pool dry_at missing despite positive burn and no reset relief")
	}
	if snap.Pool.Mood != forecast.MoodEasy {
		t.Errorf("pool mood = %q, want %q", snap.Pool.Mood, forecast.MoodEasy)
	}
}

// assertKeys fails unless m's key set is exactly want.
func assertKeys[V any](t *testing.T, label string, m map[string]V, want []string) {
	t.Helper()
	got := make([]string, 0, len(m))
	for k := range m {
		got = append(got, k)
	}
	slices.Sort(got)
	want = slices.Clone(want)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("%s keys = %v, want %v", label, got, want)
	}
}
