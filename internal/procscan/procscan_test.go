package procscan

import (
	"context"
	"errors"
	"testing"
	"time"
)

const sample = `  501 01:02:03 /Users/me/.local/bin/claude --session-id abc FOO=bar CLAUDE_CONFIG_DIR=/Users/me/.cc-pool/accounts/acct-01 PATH=/usr/bin
  777    05:09 claude --dangerously-skip-permissions PATH=/usr/bin LANG=en_US.UTF-8
  888 02:03:04 fish -c claude CLAUDE_CONFIG_DIR=/Users/me/.cc-pool/accounts/acct-02
  999 00:01 /usr/bin/node /some/script.js CLAUDE_CONFIG_DIR=/Users/me/.cc-pool/accounts/acct-03
 1010 banana /Users/me/.local/bin/claude CLAUDE_CONFIG_DIR=/Users/me/.claude
`

func TestParse(t *testing.T) {
	got := parse(sample, time.Now())
	// Expect: pid 501 (acct-01), pid 777 (default, no env), pid 1010 (~/.claude).
	// pid 888 is `fish` (argv0 != claude). pid 999 is node. Both excluded.
	if len(got) != 3 {
		t.Fatalf("got %d sessions, want 3: %+v", len(got), got)
	}
	byPID := map[int]string{}
	for _, s := range got {
		byPID[s.PID] = s.ConfigDir
	}
	if byPID[501] != "/Users/me/.cc-pool/accounts/acct-01" {
		t.Errorf("pid 501 dir = %q", byPID[501])
	}
	if _, ok := byPID[777]; !ok || byPID[777] != "" {
		t.Errorf("pid 777 should be present with empty config dir, got %q ok=%v", byPID[777], ok)
	}
	if byPID[1010] != "/Users/me/.claude" {
		t.Errorf("pid 1010 dir = %q", byPID[1010])
	}
	if _, ok := byPID[888]; ok {
		t.Error("fish wrapper should be excluded")
	}
	if _, ok := byPID[999]; ok {
		t.Error("node process should be excluded")
	}
}

func TestCountByConfigDir(t *testing.T) {
	sessions := parse(sample, time.Now())
	cases := map[string]struct {
		configDir string
		want      int
	}{
		"exact match":           {"/Users/me/.cc-pool/accounts/acct-01", 1},
		"no sessions for dir":   {"/Users/me/.cc-pool/accounts/acct-99", 0},
		"explicit ~/.claude":    {"/Users/me/.claude", 1}, // pid 1010 only; never a pool account
		"empty matches nothing": {"", 0},                  // plain claude (pid 777) maps to no pool account
	}
	for name, tc := range cases {
		if n := CountByConfigDir(sessions, tc.configDir); n != tc.want {
			t.Errorf("%s: CountByConfigDir(%q) = %d, want %d", name, tc.configDir, n, tc.want)
		}
	}
}

// TestParseStartedAt pins StartedAt = now - etime, and the deliberate
// soft-fail: a malformed etime yields the zero StartedAt but the session is
// still returned — session detection is load-bearing, staleness is advisory.
func TestParseStartedAt(t *testing.T) {
	now := time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC)
	got := parse(sample, now)
	byPID := map[int]Session{}
	for _, s := range got {
		byPID[s.PID] = s
	}
	if want := now.Add(-(1*time.Hour + 2*time.Minute + 3*time.Second)); !byPID[501].StartedAt.Equal(want) {
		t.Errorf("pid 501 StartedAt = %v, want %v", byPID[501].StartedAt, want)
	}
	if want := now.Add(-(5*time.Minute + 9*time.Second)); !byPID[777].StartedAt.Equal(want) {
		t.Errorf("pid 777 StartedAt = %v, want %v", byPID[777].StartedAt, want)
	}
	stale, ok := byPID[1010]
	if !ok {
		t.Fatal("pid 1010 (malformed etime) must still be returned")
	}
	if !stale.StartedAt.IsZero() {
		t.Errorf("pid 1010 StartedAt = %v, want zero for malformed etime", stale.StartedAt)
	}
}

func TestParseEtime(t *testing.T) {
	cases := map[string]struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		"mm:ss":                        {in: "01:23", want: 1*time.Minute + 23*time.Second},
		"zero":                         {in: "00:00", want: 0},
		"hh:mm:ss":                     {in: "13:01:23", want: 13*time.Hour + 1*time.Minute + 23*time.Second},
		"dd-hh:mm:ss":                  {in: "2-03:04:05", want: 51*time.Hour + 4*time.Minute + 5*time.Second},
		"bare seconds":                 {in: "45", wantErr: true},
		"empty":                        {in: "", wantErr: true},
		"garbage":                      {in: "banana", wantErr: true},
		"negative minutes":             {in: "-1:23", wantErr: true},
		"negative seconds":             {in: "1:-23", wantErr: true},
		"days without hours":           {in: "1-2:3", wantErr: true},
		"seconds out of range":         {in: "01:60", wantErr: true},
		"minutes out of range":         {in: "60:00", wantErr: true},
		"hours out of range with days": {in: "1-24:00:00", wantErr: true},
		"four fields":                  {in: "1:2:3:4", wantErr: true},
		"internal spaces":              {in: "01: 23", wantErr: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := parseEtime(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseEtime(%q) = %v, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseEtime(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("parseEtime(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// TestScanPopulatesStartedAt drives Scan through the psOutput seam: parsed
// etimes anchor StartedAt near scan time, a malformed etime keeps its session
// with a zero StartedAt, and a ps failure is Scan's error, not a silent nil.
func TestScanPopulatesStartedAt(t *testing.T) {
	orig := psOutput
	t.Cleanup(func() { psOutput = orig })

	psOutput = func(context.Context) ([]byte, error) { return []byte(sample), nil }
	before := time.Now()
	got, err := Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	if len(got) != 3 {
		t.Fatalf("got %d sessions, want 3: %+v", len(got), got)
	}
	byPID := map[int]Session{}
	for _, s := range got {
		byPID[s.PID] = s
	}
	elapsed := 1*time.Hour + 2*time.Minute + 3*time.Second
	lo, hi := before.Add(-elapsed), after.Add(-elapsed)
	if sa := byPID[501].StartedAt; sa.Before(lo) || sa.After(hi) {
		t.Errorf("pid 501 StartedAt = %v, want within [%v, %v]", sa, lo, hi)
	}
	if sa := byPID[1010].StartedAt; !sa.IsZero() {
		t.Errorf("pid 1010 StartedAt = %v, want zero for malformed etime", sa)
	}

	psOutput = func(context.Context) ([]byte, error) { return nil, errors.New("ps exploded") }
	if _, err := Scan(context.Background()); err == nil {
		t.Fatal("Scan must propagate a ps failure")
	}
}

// TestScanBoundsWedgedPS proves the timeout releases the CALLER even when ps is
// truly unkillable: the stub ignores its ctx entirely — a real ps wedged on a
// D-state read off a hung fuse-t mount does not cooperatively unblock on
// cancellation, which is the whole problem — yet Scan returns DeadlineExceeded
// within a generous bound. Only a goroutine-decoupled Scan passes this; a
// synchronous psOutput call (even with WaitDelay) would hang here, since
// cmd.Wait parks in waitpid and SIGKILL cannot reap a D-state ps.
func TestScanBoundsWedgedPS(t *testing.T) {
	origOut, origTimeout := psOutput, scanTimeout
	release := make(chan struct{})
	t.Cleanup(func() {
		close(release) // free the parked stub goroutine before restoring the seams
		psOutput, scanTimeout = origOut, origTimeout
	})

	scanTimeout = 20 * time.Millisecond
	psOutput = func(ctx context.Context) ([]byte, error) {
		<-release // ignore ctx — mimic a ps that SIGKILL cannot reap
		return nil, ctx.Err()
	}

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := Scan(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Scan err = %v, want context.DeadlineExceeded", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("Scan took %v, want ≲ scanTimeout (%v)", elapsed, scanTimeout)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Scan did not return within the bound — the timeout did not release the caller")
	}
}
