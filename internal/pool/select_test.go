package pool

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
)

// TestScoreInputRateLimitedReadsLastGood pins the display fix: a 429 records a
// zeroed rate_limited placeholder as the newest sample (load-bearing for the
// daemon backoff), but scoreInput must source utilization, resets, and the
// sample timestamp from the last known-good sample — never that placeholder's
// 0%. RateLimited still tracks the newest row. With no good sample ever, util
// stays 0 and SampleTS zero (honest "rate-limited, utilization unknown").
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
		wantUtil5h      float64
		wantUtil7d      float64
		wantRateLimited bool
		wantSampleTS    time.Time // zero means "expect SampleTS to be zero"
		wantExtraUsed   float64   // overage carried by the returned good sample
		wantGoodNil     bool      // the returned good sample must be nil
	}{
		"good reading then a newer rate_limited marker reads through to the good util": {
			samples: []spec{
				{ts: goodTS, util5h: 41, util7d: 73, rateLimited: false, extraEnabled: true, extraUsed: 177},
				{ts: now, util5h: 0, util7d: 0, rateLimited: true},
			},
			wantUtil5h: 41, wantUtil7d: 73, wantRateLimited: true, wantSampleTS: goodTS, wantExtraUsed: 177,
		},
		"only rate_limited markers yields zero util but still flags rate-limited": {
			samples: []spec{
				{ts: now.Add(-time.Minute), util5h: 0, util7d: 0, rateLimited: true},
				{ts: now, util5h: 0, util7d: 0, rateLimited: true},
			},
			wantUtil5h: 0, wantUtil7d: 0, wantRateLimited: true, wantSampleTS: time.Time{}, wantGoodNil: true,
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
			if !in.HasUsage {
				t.Fatal("HasUsage = false, want true (the account was sampled)")
			}
			// Extra-usage must come from the good sample too, never the zeroed
			// rate_limited placeholder.
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
