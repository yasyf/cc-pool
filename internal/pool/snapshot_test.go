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
			if err := st.UpsertAccount(store.Account{
				ID: 1, ConfigDir: t.TempDir(),
				KeychainService: "ccp-test-missing", KeychainAccount: "ccp-test",
			}); err != nil {
				t.Fatal(err)
			}
			base := time.Now().Truncate(time.Second).Add(-tc.sampleAge)
			seed7dClimb(t, st, 1, base, 6)

			m := &Manager{Store: st, LockDir: t.TempDir()}
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
			if err := st.UpsertAccount(store.Account{
				ID: 1, ConfigDir: t.TempDir(),
				KeychainService: "ccp-test-missing", KeychainAccount: "ccp-test",
			}); err != nil {
				t.Fatal(err)
			}
			base := time.Now().Truncate(time.Second).Add(-tc.sampleAge)
			seedClimb(t, st, 1, base, 10)

			m := &Manager{Store: st, LockDir: t.TempDir()}
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
