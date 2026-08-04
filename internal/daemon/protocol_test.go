package daemon

import (
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/forecast"
	"github.com/yasyf/cc-pool/internal/score"
)

func TestControlPlaneEpochIsHardReset(t *testing.T) {
	const prefix = "com.yasyf.cc-pool.control/"
	const suffix = "/v2"
	schema := string(RuntimeSchema)
	digest := strings.TrimSuffix(strings.TrimPrefix(schema, prefix), suffix)
	if !strings.HasPrefix(schema, prefix) || !strings.HasSuffix(schema, suffix) || len(digest) != 64 {
		t.Fatalf("runtime schema = %q, want generated v2 schema identity", schema)
	}
	for _, character := range digest {
		if !strings.ContainsRune("0123456789abcdef", character) {
			t.Fatalf("runtime schema = %q, want lowercase SHA-256 digest", schema)
		}
	}
	if SnapshotVersion != 1 {
		t.Fatalf("snapshot version = %d", SnapshotVersion)
	}
	if DaemonHealthSchema != 1 {
		t.Fatalf("daemon health schema = %d", DaemonHealthSchema)
	}
}

func TestScoreComponentsWireProjectionIsExact(t *testing.T) {
	want := score.Components{
		Eff5: 1, Eff7: 2, RawRemaining5h: 3, RawRemaining7d: 4,
		Remaining5h: 5, Remaining7d: 6, SessionPenalty: 7,
		RateLimitPenalty: 8, NeedsLoginPenalty: 9, CredentialQuarantinePenalty: 10,
		StalePenalty: 11, Barrier5h: 12, Barrier7d: 13, RunwayPenalty: 14,
	}
	wireComponents := ScoreComponentsFromDomain(want)
	if got := ScoreComponentsToDomain(wireComponents); got != want {
		t.Fatalf("score component round trip = %+v, want %+v", got, want)
	}
}

func TestPoolMoodWireProjectionIsExact(t *testing.T) {
	for domain, want := range map[forecast.Mood]PoolMood{
		forecast.MoodChill: PoolMoodChill, forecast.MoodEasy: PoolMoodEasy,
		forecast.MoodUneasy: PoolMoodUneasy, forecast.MoodWorried: PoolMoodWorried,
		forecast.MoodAlarmed: PoolMoodAlarmed, forecast.MoodPanic: PoolMoodPanic,
	} {
		if got := poolMoodFromForecast(domain); got != want {
			t.Fatalf("pool mood %q = %q, want %q", domain, got, want)
		}
	}
}
