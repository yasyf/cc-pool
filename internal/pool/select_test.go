package pool

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
)

// TestScoreInputRateLimitedReadsLastGood pins that scoreInput sources utilization,
// resets, and timestamp from the last known-good sample — never a newer zeroed
// rate_limited placeholder — while RateLimited still tracks the newest row, and
// that HasUsage gates on a known-good sample: an account whose only rows are 429
// placeholders reads as no-data (HasUsage=false), not "0% used".
func TestScoreInputRateLimitedReadsLastGood(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	goodTS := now.Add(-2 * time.Minute)

	type spec struct {
		ts           time.Time
		util5h       float64
		util7d       float64
		rateLimited  bool
		extraEnabled bool
		extraUsed    float64
	}
	cases := map[string]struct {
		samples         []spec
		wantHasUsage    bool
		wantUtil5h      float64
		wantUtil7d      float64
		wantRateLimited bool
		wantBurn        float64
		wantSampleTS    time.Time // zero means "expect SampleTS to be zero"
		wantExtraUsed   float64
		wantGoodNil     bool
	}{
		"good reading then a newer rate_limited marker reads through to the good util": {
			samples: []spec{
				{ts: goodTS, util5h: 41, util7d: 73, rateLimited: false, extraEnabled: true, extraUsed: 177},
				{ts: now, util5h: 0, util7d: 0, rateLimited: true},
			},
			wantHasUsage: true, wantUtil5h: 41, wantUtil7d: 73, wantRateLimited: true,
			wantSampleTS: goodTS, wantExtraUsed: 177,
		},
		"only rate_limited markers reads as no-data but still flags rate-limited": {
			samples: []spec{
				{ts: now.Add(-time.Minute), util5h: 0, util7d: 0, rateLimited: true},
				{ts: now, util5h: 0, util7d: 0, rateLimited: true},
			},
			wantHasUsage: false, wantUtil5h: 0, wantUtil7d: 0, wantRateLimited: true,
			wantBurn: 0, wantSampleTS: time.Time{}, wantGoodNil: true,
		},
		"never sampled reads as no-data and is not rate-limited": {
			samples:      nil,
			wantHasUsage: false, wantUtil5h: 0, wantUtil7d: 0, wantRateLimited: false,
			wantBurn: 0, wantSampleTS: time.Time{}, wantGoodNil: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "s", KeychainAccount: "u"}
			if err := st.UpsertAccount(a); err != nil {
				t.Fatal(err)
			}
			for _, sp := range tc.samples {
				if err := st.InsertUsageSample(store.UsageSample{
					AccountID:    1,
					TS:           sp.ts,
					Util5h:       sp.util5h,
					Util7d:       sp.util7d,
					RateLimited:  sp.rateLimited,
					ExtraEnabled: sp.extraEnabled,
					ExtraUsed:    sp.extraUsed,
				}); err != nil {
					t.Fatal(err)
				}
			}

			m := &Manager{Store: st, LockDir: t.TempDir()}
			in, _, good, err := m.scoreInput(a, nil, now)
			if err != nil {
				t.Fatalf("scoreInput: %v", err)
			}
			if in.HasUsage != tc.wantHasUsage {
				t.Errorf("HasUsage = %v, want %v", in.HasUsage, tc.wantHasUsage)
			}
			if tc.wantGoodNil {
				if good != nil {
					t.Errorf("good sample = %+v, want nil (no clean sample ever)", *good)
				}
			} else if good == nil {
				t.Fatal("good sample = nil, want the last known-good reading")
			} else if good.ExtraUsed != tc.wantExtraUsed {
				t.Errorf("good.ExtraUsed = %v, want %v", good.ExtraUsed, tc.wantExtraUsed)
			}
			if in.RateLimited != tc.wantRateLimited {
				t.Errorf("RateLimited = %v, want %v", in.RateLimited, tc.wantRateLimited)
			}
			if in.Burn5hPerHour != tc.wantBurn {
				t.Errorf("Burn5hPerHour = %v, want %v", in.Burn5hPerHour, tc.wantBurn)
			}
			if in.Util5h != tc.wantUtil5h {
				t.Errorf("Util5h = %v, want %v", in.Util5h, tc.wantUtil5h)
			}
			if in.Util7d != tc.wantUtil7d {
				t.Errorf("Util7d = %v, want %v", in.Util7d, tc.wantUtil7d)
			}
			if tc.wantSampleTS.IsZero() {
				if !in.SampleTS.IsZero() {
					t.Errorf("SampleTS = %v, want zero", in.SampleTS)
				}
			} else if !in.SampleTS.Equal(tc.wantSampleTS) {
				t.Errorf("SampleTS = %v, want %v", in.SampleTS, tc.wantSampleTS)
			}
		})
	}
}
