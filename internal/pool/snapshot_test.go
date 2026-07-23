package pool

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
)

// seedClimb seeds a +2%/3min climb ending at util.
func seedClimb(t *testing.T, st *store.Store, accountID int, base time.Time, util float64) {
	t.Helper()
	for i := 0; i < 5; i++ {
		if err := st.InsertUsageSample(store.UsageSample{
			AccountID: accountID,
			TS:        base.Add(-time.Duration(i) * 3 * time.Minute),
			Util5h:    util - float64(i)*2,
			Util7d:    util,
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// seed7dClimb seeds a 7d-window climb; Util5h is set only for realism.
func seed7dClimb(t *testing.T, st *store.Store, accountID int, base time.Time, util float64) {
	t.Helper()
	for i := 0; i < 4; i++ {
		if err := st.InsertUsageSample(store.UsageSample{
			AccountID: accountID,
			TS:        base.Add(-time.Duration(i) * 2 * time.Hour),
			Util5h:    util - float64(i)*2,
			Util7d:    util - float64(i)*2,
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// TestSnapshotScopedFields pins that Snapshot surfaces the binding model-scoped
// weekly bucket from the last known-good sample (like ExtraEnabled) and that a
// pegged scoped bucket flips WeeklyExhausted — the pool-mood signal — even while
// the aggregate 7d window is nowhere near its cap.
func TestSnapshotScopedFields(t *testing.T) {
	cases := map[string]struct {
		scopedModel   string
		scopedUtil    float64
		scopedFuture  time.Duration // scoped reset relative to now; 0 ⇒ no scoped bucket
		wantModel     string
		wantUtil      float64
		wantWeeklyExh bool
	}{
		"pegged scoped bucket surfaces and exhausts the weekly signal": {
			scopedModel: "Fable", scopedUtil: 100, scopedFuture: 48 * time.Hour,
			wantModel: "Fable", wantUtil: 100, wantWeeklyExh: true,
		},
		"no scoped bucket leaves the fields empty and weekly unexhausted": {
			scopedModel: "", scopedUtil: 0, scopedFuture: 0,
			wantModel: "", wantUtil: 0, wantWeeklyExh: false,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			admitPoolTestAccount(t, st, store.Account{
				ID: 1, ConfigDir: t.TempDir(),
				KeychainService: "ccp-test-missing", KeychainAccount: "ccp-test",
			})
			now := time.Now().Truncate(time.Second)
			sample := store.UsageSample{
				AccountID:     1,
				TS:            now,
				Util5h:        10,
				Util7d:        60, // aggregate nowhere near exhausted
				Scoped7dModel: tc.scopedModel,
				Scoped7dUtil:  tc.scopedUtil,
			}
			if tc.scopedFuture > 0 {
				sample.Scoped7dResets = now.Add(tc.scopedFuture)
			}
			if err := st.InsertUsageSample(sample); err != nil {
				t.Fatal(err)
			}

			m := &Manager{Store: st, ScanSessions: noPoolSessions}
			snaps, err := m.Snapshots(t.Context(), false, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(snaps) != 1 {
				t.Fatalf("snapshots = %d, want 1", len(snaps))
			}
			sn := snaps[0]
			if sn.Scoped7dModel != tc.wantModel {
				t.Errorf("Scoped7dModel = %q, want %q", sn.Scoped7dModel, tc.wantModel)
			}
			if sn.Scoped7dUtil != tc.wantUtil {
				t.Errorf("Scoped7dUtil = %v, want %v", sn.Scoped7dUtil, tc.wantUtil)
			}
			if sn.WeeklyExhausted != tc.wantWeeklyExh {
				t.Errorf("WeeklyExhausted = %v, want %v", sn.WeeklyExhausted, tc.wantWeeklyExh)
			}
			if tc.scopedFuture > 0 {
				if !sn.Scoped7dResets.Equal(now.Add(tc.scopedFuture)) {
					t.Errorf("Scoped7dResets = %v, want %v", sn.Scoped7dResets, now.Add(tc.scopedFuture))
				}
			} else if !sn.Scoped7dResets.IsZero() {
				t.Errorf("Scoped7dResets = %v, want zero", sn.Scoped7dResets)
			}
		})
	}
}

// TestSnapshotOnly429IsNoData pins that an account whose only samples are 429
// placeholders serializes as no-data (HasUsage=false) with zeroed utilization —
// never "0% used" — while RateLimited still surfaces from the newest row.
func TestSnapshotOnly429IsNoData(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	admitPoolTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(),
		KeychainService: "ccp-test-missing", KeychainAccount: "ccp-test",
	})
	now := time.Now().Truncate(time.Second)
	// Two 429 placeholders and no clean reading: the newest carries a non-zero
	// util the placeholder path must never expose as real usage.
	for i := 0; i < 2; i++ {
		if err := st.InsertUsageSample(store.UsageSample{
			AccountID: 1, TS: now.Add(-time.Duration(i) * time.Minute),
			Util5h: 100, Util7d: 100, RateLimited: true,
		}); err != nil {
			t.Fatal(err)
		}
	}

	m := &Manager{Store: st, ScanSessions: noPoolSessions}
	snaps, err := m.Snapshots(t.Context(), false, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 1 {
		t.Fatalf("snapshots = %d, want 1", len(snaps))
	}
	sn := snaps[0]
	if sn.HasUsage {
		t.Error("HasUsage = true, want false (only 429 placeholders, no known-good sample)")
	}
	if !sn.RateLimited {
		t.Error("RateLimited = false, want true (newest sample is a 429 marker)")
	}
	if sn.Util5h != 0 || sn.Util7d != 0 {
		t.Errorf("Util5h/Util7d = %v/%v, want 0/0 (placeholder util must not leak)", sn.Util5h, sn.Util7d)
	}
	if sn.Remaining5h != 100 || sn.Remaining7d != 100 {
		t.Errorf("Remaining5h/7d = %v/%v, want 100/100", sn.Remaining5h, sn.Remaining7d)
	}
	if sn.SampleAge != 0 {
		t.Errorf("SampleAge = %v, want 0 (no known-good sample timestamp)", sn.SampleAge)
	}
}

// TestSnapshotBurn7d pins Snapshot's gated 7d drain.
func TestSnapshotBurn7d(t *testing.T) {
	cases := map[string]struct {
		sampleAge time.Duration
		wantBurn  float64
	}{
		"fresh 6h climb yields the 7d burn":      {0, 1}, // 6% over 6h = 1%/hr
		"stale latest gates the 7d burn to zero": {6 * time.Minute, 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			admitPoolTestAccount(t, st, store.Account{
				ID: 1, ConfigDir: t.TempDir(),
				KeychainService: "ccp-test-missing", KeychainAccount: "ccp-test",
			})
			base := time.Now().Truncate(time.Second).Add(-tc.sampleAge)
			seed7dClimb(t, st, 1, base, 6)

			m := &Manager{Store: st, ScanSessions: noPoolSessions}
			snaps, err := m.Snapshots(t.Context(), false, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(snaps) != 1 {
				t.Fatalf("snapshots = %d, want 1", len(snaps))
			}
			if got := snaps[0].Burn7dPerHour; got != tc.wantBurn {
				t.Errorf("Burn7dPerHour = %v, want %v", got, tc.wantBurn)
			}
		})
	}
}

// TestSnapshotsForecast pins the gated/ungated burn split: the scoring burn stays
// live on a stale sample (reservation re-ranks need it) while the display forecast
// zeroes out past DisplayStaleAfter.
func TestSnapshotsForecast(t *testing.T) {
	cases := map[string]struct {
		sampleAge    time.Duration
		wantBurn     float64
		wantForecast bool
	}{
		"fresh history populates both burns": {0, 40, true},
		"stale history keeps the scoring burn but gates the forecast": {
			6 * time.Minute, 40, false,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			admitPoolTestAccount(t, st, store.Account{
				ID: 1, ConfigDir: t.TempDir(),
				KeychainService: "ccp-test-missing", KeychainAccount: "ccp-test",
			})
			base := time.Now().Truncate(time.Second).Add(-tc.sampleAge)
			seedClimb(t, st, 1, base, 10)

			m := &Manager{Store: st, ScanSessions: noPoolSessions}
			snaps, err := m.Snapshots(t.Context(), false, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(snaps) != 1 {
				t.Fatalf("snapshots = %d, want 1", len(snaps))
			}
			sn := snaps[0]
			if sn.Burn5hPerHour != tc.wantBurn {
				t.Errorf("ungated Burn5hPerHour = %v, want %v", sn.Burn5hPerHour, tc.wantBurn)
			}
			if got := sn.Forecast.BurnPerHour > 0; got != tc.wantForecast {
				t.Errorf("Forecast populated = %v, want %v (forecast %+v)", got, tc.wantForecast, sn.Forecast)
			}
			if tc.wantForecast {
				wantDepleted := base.Add(2*time.Hour + 15*time.Minute)
				if !sn.Forecast.DepletedAt.Equal(wantDepleted) {
					t.Errorf("Forecast.DepletedAt = %v, want %v", sn.Forecast.DepletedAt, wantDepleted)
				}
			} else if !sn.Forecast.DepletedAt.IsZero() {
				t.Errorf("stale forecast must be zero, got %+v", sn.Forecast)
			}
		})
	}
}
