package procscan

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// procargs2 builds a KERN_PROCARGS2 buffer: uint32 argc, the NUL-terminated
// executable path, alignment padding, argc NUL-terminated argv strings, then
// NUL-terminated env strings ended by an empty string — the exact shape the
// kernel hands back and parseProcArgs decodes.
func procargs2(argc uint32, execPath string, argv, env []string) []byte {
	b := make([]byte, 4)
	binary.NativeEndian.PutUint32(b, argc)
	b = append(b, execPath...)
	b = append(b, 0)
	b = append(b, 0, 0) // alignment padding
	for _, a := range argv {
		b = append(b, a...)
		b = append(b, 0)
	}
	for _, e := range env {
		b = append(b, e...)
		b = append(b, 0)
	}
	b = append(b, 0) // empty string terminates the environment
	return b
}

func TestParseProcArgs(t *testing.T) {
	cases := map[string]struct {
		buf        []byte
		wantArgv0  string
		wantCfgDir string
	}{
		"pool session": {
			buf: procargs2(2,
				"/Users/me/.local/bin/claude",
				[]string{"/Users/me/.local/bin/claude", "--session-id=abc"},
				[]string{"FOO=bar", "CLAUDE_CONFIG_DIR=/Users/me/.cc-pool/accounts/acct-01", "PATH=/usr/bin"}),
			wantArgv0:  "/Users/me/.local/bin/claude",
			wantCfgDir: "/Users/me/.cc-pool/accounts/acct-01",
		},
		"plain claude (no CLAUDE_CONFIG_DIR)": {
			buf: procargs2(1,
				"/opt/homebrew/bin/claude",
				[]string{"claude"},
				[]string{"PATH=/usr/bin", "LANG=en_US.UTF-8"}),
			wantArgv0:  "claude",
			wantCfgDir: "",
		},
		"explicit ~/.claude": {
			buf: procargs2(1,
				"/Users/me/.local/bin/claude",
				[]string{"/Users/me/.local/bin/claude"},
				[]string{"CLAUDE_CONFIG_DIR=/Users/me/.claude"}),
			wantArgv0:  "/Users/me/.local/bin/claude",
			wantCfgDir: "/Users/me/.claude",
		},
		"non-claude node child carries CLAUDE_CONFIG_DIR": {
			buf: procargs2(3,
				"/usr/bin/node",
				[]string{"/usr/bin/node", "/some/script.js", "--flag"},
				[]string{"CLAUDE_CONFIG_DIR=/Users/me/.cc-pool/accounts/acct-03"}),
			wantArgv0:  "/usr/bin/node",
			wantCfgDir: "/Users/me/.cc-pool/accounts/acct-03",
		},
		"no env block": {
			buf:        procargs2(1, "/opt/homebrew/bin/claude", []string{"claude"}, nil),
			wantArgv0:  "claude",
			wantCfgDir: "",
		},
		"buffer too short for argc": {
			buf:        []byte{0x01, 0x00},
			wantArgv0:  "",
			wantCfgDir: "",
		},
		"exec path never terminates": {
			buf: append(
				binary.NativeEndian.AppendUint32(nil, 1),
				[]byte("/opt/homebrew/bin/claude")...), // no trailing NUL
			wantArgv0:  "",
			wantCfgDir: "",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			argv0, cfgDir := parseProcArgs(tc.buf)
			if argv0 != tc.wantArgv0 {
				t.Errorf("argv0 = %q, want %q", argv0, tc.wantArgv0)
			}
			if cfgDir != tc.wantCfgDir {
				t.Errorf("configDir = %q, want %q", cfgDir, tc.wantCfgDir)
			}
		})
	}
}

// TestParseProcArgsTruncatedEnv proves a buffer cut mid-environment keeps a
// recoverable argv0 and the CLAUDE_CONFIG_DIR seen before the cut, without
// panicking on the missing terminator.
func TestParseProcArgsTruncatedEnv(t *testing.T) {
	full := procargs2(1,
		"/Users/me/.local/bin/claude",
		[]string{"/Users/me/.local/bin/claude"},
		[]string{"CLAUDE_CONFIG_DIR=/pool/acct-07", "PATH=/usr/bin/very/long/tail"})
	// Drop the trailing bytes so the final env string loses its NUL terminator.
	truncated := full[:len(full)-8]

	argv0, cfgDir := parseProcArgs(truncated)
	if argv0 != "/Users/me/.local/bin/claude" {
		t.Errorf("argv0 = %q, want the claude path", argv0)
	}
	if cfgDir != "/pool/acct-07" {
		t.Errorf("configDir = %q, want /pool/acct-07 (seen before the cut)", cfgDir)
	}

	// Cut before argv[0]'s terminator: argv0 is unrecoverable, no panic.
	early := full[:6]
	if argv0, cfgDir := parseProcArgs(early); argv0 != "" || cfgDir != "" {
		t.Errorf("early cut = (%q, %q), want empty", argv0, cfgDir)
	}
}

// canScan swaps both process-table seams for canned data and restores them.
func canScan(t *testing.T, procs []proc, args map[int][]byte, argErr map[int]error) {
	t.Helper()
	origList, origArgs := listProcs, procArgs
	t.Cleanup(func() { listProcs, procArgs = origList, origArgs })
	listProcs = func(context.Context) ([]proc, error) { return procs, nil }
	procArgs = func(_ context.Context, pid int) ([]byte, error) {
		if err := argErr[pid]; err != nil {
			return nil, err
		}
		return args[pid], nil
	}
}

func TestScan(t *testing.T) {
	start := time.Date(2026, 6, 12, 9, 30, 0, 0, time.UTC)
	procs := []proc{
		{pid: 501, startedAt: start},
		{pid: 777, startedAt: start.Add(-5 * time.Minute)},
		{pid: 888, startedAt: start.Add(-time.Hour)},
		{pid: 999, startedAt: start.Add(-2 * time.Hour)},
		{pid: 1010, startedAt: start.Add(-3 * time.Hour)},
	}
	args := map[int][]byte{
		501: procargs2(1, "/Users/me/.local/bin/claude", []string{"/Users/me/.local/bin/claude"},
			[]string{"CLAUDE_CONFIG_DIR=/Users/me/.cc-pool/accounts/acct-01"}),
		777: procargs2(1, "/opt/homebrew/bin/claude", []string{"claude"}, []string{"PATH=/usr/bin"}),
		888: procargs2(3, "/opt/homebrew/bin/fish", []string{"fish", "-c", "claude"},
			[]string{"CLAUDE_CONFIG_DIR=/Users/me/.cc-pool/accounts/acct-02"}),
		999: procargs2(2, "/usr/bin/node", []string{"/usr/bin/node", "/some/script.js"},
			[]string{"CLAUDE_CONFIG_DIR=/Users/me/.cc-pool/accounts/acct-03"}),
		1010: procargs2(1, "/Users/me/.local/bin/claude", []string{"/Users/me/.local/bin/claude"},
			[]string{"CLAUDE_CONFIG_DIR=/Users/me/.claude"}),
	}
	canScan(t, procs, args, nil)

	got, err := Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d sessions, want 3: %+v", len(got), got)
	}
	byPID := map[int]Session{}
	for _, s := range got {
		byPID[s.PID] = s
	}
	if byPID[501].ConfigDir != "/Users/me/.cc-pool/accounts/acct-01" {
		t.Errorf("pid 501 dir = %q", byPID[501].ConfigDir)
	}
	if !byPID[501].StartedAt.Equal(start) {
		t.Errorf("pid 501 StartedAt = %v, want %v", byPID[501].StartedAt, start)
	}
	if s, ok := byPID[777]; !ok || s.ConfigDir != "" {
		t.Errorf("pid 777 should be present with empty config dir, got %q ok=%v", s.ConfigDir, ok)
	}
	if byPID[1010].ConfigDir != "/Users/me/.claude" {
		t.Errorf("pid 1010 dir = %q", byPID[1010].ConfigDir)
	}
	if _, ok := byPID[888]; ok {
		t.Error("fish wrapper should be excluded")
	}
	if _, ok := byPID[999]; ok {
		t.Error("node child should be excluded even though it carries CLAUDE_CONFIG_DIR")
	}
}

// TestScanFailsClosedOnListError pins the load-bearing fail-closed: a failed
// process enumeration surfaces as Scan's error, never a silent empty slice.
func TestScanFailsClosedOnListError(t *testing.T) {
	origList := listProcs
	t.Cleanup(func() { listProcs = origList })
	listProcs = func(context.Context) ([]proc, error) { return nil, errors.New("sysctl exploded") }

	if _, err := Scan(context.Background()); err == nil {
		t.Fatal("Scan must propagate a process-list failure (fail closed)")
	}
}

// TestScanSkipsGoneProcs proves a process that vanished (ESRCH) or has no
// readable args (EINVAL) is skipped without failing the scan, while a live
// claude alongside it is still reported.
func TestScanSkipsGoneProcs(t *testing.T) {
	procs := []proc{{pid: 501}, {pid: 777}, {pid: 999}}
	args := map[int][]byte{
		501: procargs2(1, "/opt/homebrew/bin/claude", []string{"claude"}, nil),
	}
	argErr := map[int]error{777: unix.ESRCH, 999: unix.EINVAL}
	canScan(t, procs, args, argErr)

	got, err := Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].PID != 501 {
		t.Fatalf("got %+v, want only pid 501", got)
	}
}

// TestScanFailsClosedOnUnexpectedArgError proves an unexpected per-PID read
// error (not ESRCH/EINVAL) fails the whole scan closed rather than silently
// dropping a process we could not classify.
func TestScanFailsClosedOnUnexpectedArgError(t *testing.T) {
	procs := []proc{{pid: 501}}
	argErr := map[int]error{501: unix.EPERM}
	canScan(t, procs, nil, argErr)

	if _, err := Scan(context.Background()); err == nil {
		t.Fatal("an unexpected procargs error must fail the scan closed")
	}
}

func TestCountByConfigDir(t *testing.T) {
	sessions := []Session{
		{PID: 501, ConfigDir: "/Users/me/.cc-pool/accounts/acct-01"},
		{PID: 777, ConfigDir: ""},
		{PID: 1010, ConfigDir: "/Users/me/.claude"},
	}
	cases := map[string]struct {
		configDir string
		want      int
	}{
		"exact match":           {"/Users/me/.cc-pool/accounts/acct-01", 1},
		"no sessions for dir":   {"/Users/me/.cc-pool/accounts/acct-99", 0},
		"explicit ~/.claude":    {"/Users/me/.claude", 1},
		"empty matches nothing": {"", 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if n := CountByConfigDir(sessions, tc.configDir); n != tc.want {
				t.Errorf("CountByConfigDir(%q) = %d, want %d", tc.configDir, n, tc.want)
			}
		})
	}
}

func TestAlivePIDs(t *testing.T) {
	sessions := []Session{{PID: 501}, {PID: 777}}
	alive := AlivePIDs(sessions)
	if !alive[501] || !alive[777] {
		t.Errorf("AlivePIDs = %v, want 501 and 777 present", alive)
	}
	if alive[123] {
		t.Error("AlivePIDs must not report an absent pid as alive")
	}
}

// TestScanBoundsWedgedProcArgs proves scanTimeout releases the caller even when
// a KERN_PROCARGS2 read is unkillable (the stub ignores ctx). Only a
// goroutine-decoupled Scan passes; a synchronous walk would hang.
func TestScanBoundsWedgedProcArgs(t *testing.T) {
	origList, origArgs, origTimeout := listProcs, procArgs, scanTimeout
	release := make(chan struct{})
	t.Cleanup(func() {
		close(release) // free the parked stub goroutine
		listProcs, procArgs, scanTimeout = origList, origArgs, origTimeout
	})

	scanTimeout = 20 * time.Millisecond
	listProcs = func(context.Context) ([]proc, error) { return []proc{{pid: 4242}}, nil }
	procArgs = func(ctx context.Context, _ int) ([]byte, error) {
		<-release // ignore ctx — mimic a procargs2 copyin that SIGKILL cannot reap
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
