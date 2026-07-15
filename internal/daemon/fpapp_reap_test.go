package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/procscan"
)

func fpAppBinary(t *testing.T) (string, time.Time) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "CCPoolStatus")
	if err := os.WriteFile(path, []byte("mach-o"), 0o600); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	ct := fi.Sys().(*syscall.Stat_t).Ctimespec
	return path, time.Unix(ct.Sec, ct.Nsec)
}

type fpAppReapRecorder struct {
	killed    []int
	scanCalls int
	killCalls int
	killErr   error
}

func swapFPAppReapSeams(t *testing.T, binPath string, scans [][]procscan.Proc, killErr error) *fpAppReapRecorder {
	t.Helper()
	origList, origPath, origKill := listFPAppProcs, fpAppBinaryPath, killFPAppPID
	t.Cleanup(func() { listFPAppProcs, fpAppBinaryPath, killFPAppPID = origList, origPath, origKill })
	fpAppBinaryPath = func() string { return binPath }
	r := &fpAppReapRecorder{killErr: killErr}
	listFPAppProcs = func(context.Context, string) ([]procscan.Proc, error) {
		i := r.scanCalls
		if i >= len(scans) {
			i = len(scans) - 1
		}
		r.scanCalls++
		return scans[i], nil
	}
	killFPAppPID = func(pid int) error {
		r.killCalls++
		if r.killErr != nil {
			return r.killErr
		}
		r.killed = append(r.killed, pid)
		return nil
	}
	return r
}

func newFPAppReapServer(out io.Writer) *Server {
	return &Server{fpSynth: alwaysNonEmpty, led: newLedgers(), log: log.New(out, "", 0)}
}

func TestStaleFPAppProcesses(t *testing.T) {
	bin, installedAt := fpAppBinary(t)
	tests := []struct {
		name      string
		startedAt time.Time
		stale     bool
	}{
		{"process predates installed binary", installedAt.Add(-time.Hour), true},
		{"process is newer than installed binary", installedAt.Add(time.Second), false},
		{"process start is within timestamp slack", installedAt.Add(-2 * time.Second), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			swapFPAppReapSeams(t, bin, [][]procscan.Proc{{{PID: 42, StartedAt: tc.startedAt}}}, nil)

			got, err := staleFPAppProcesses(context.Background(), bin)
			if err != nil {
				t.Fatal(err)
			}
			if tc.stale && (len(got) != 1 || got[0].PID != 42 || !got[0].StartedAt.Equal(tc.startedAt)) {
				t.Fatalf("got %+v, want pid 42 @ %s", got, tc.startedAt)
			}
			if !tc.stale && len(got) != 0 {
				t.Fatalf("got %+v, want none", got)
			}
		})
	}
}

func TestStaleFPAppProcessesMissingBinary(t *testing.T) {
	got, err := staleFPAppProcesses(context.Background(), filepath.Join(t.TempDir(), "gone"))
	if err != nil || got != nil {
		t.Fatalf("got %+v, %v; want nil, nil", got, err)
	}
}

func TestStaleFPAppProcessesScanError(t *testing.T) {
	bin, _ := fpAppBinary(t)
	orig := listFPAppProcs
	t.Cleanup(func() { listFPAppProcs = orig })
	listFPAppProcs = func(context.Context, string) ([]procscan.Proc, error) {
		return nil, errors.New("boom")
	}

	if _, err := staleFPAppProcesses(context.Background(), bin); err == nil {
		t.Fatal("a failed process scan must propagate")
	}
}

func TestReconcileStaleFPApp(t *testing.T) {
	bin, installedAt := fpAppBinary(t)
	old := installedAt.Add(-time.Hour)
	tests := []struct {
		name          string
		scans         [][]procscan.Proc
		wired         bool
		consent       bool
		killErr       error
		wantKilled    []int
		wantScanCalls int
		wantKillCalls int
		wantDue       bool
	}{
		{
			name:          "stale app is killed after reconfirm",
			scans:         [][]procscan.Proc{{{PID: 42, StartedAt: old}}, {{PID: 42, StartedAt: old}}},
			wired:         true,
			wantKilled:    []int{42},
			wantScanCalls: 2,
			wantKillCalls: 1,
			wantDue:       true,
		},
		{
			name:          "fresh app is not killed",
			scans:         [][]procscan.Proc{{{PID: 42, StartedAt: installedAt.Add(time.Second)}}},
			wired:         true,
			wantScanCalls: 1,
		},
		{
			name:    "consent pending blocks a stale app reap",
			scans:   [][]procscan.Proc{{{PID: 42, StartedAt: old}}},
			wired:   true,
			consent: true,
		},
		{
			name:  "file provider not wired blocks a stale app reap",
			scans: [][]procscan.Proc{{{PID: 42, StartedAt: old}}},
		},
		{
			name: "pid reuse before kill is spared",
			scans: [][]procscan.Proc{
				{{PID: 42, StartedAt: old}},
				{{PID: 42, StartedAt: installedAt.Add(-30 * time.Minute)}},
			},
			wired:         true,
			wantScanCalls: 2,
		},
		{
			name: "process gone on reconfirm is spared",
			scans: [][]procscan.Proc{
				{{PID: 42, StartedAt: old}},
				{},
			},
			wired:         true,
			wantScanCalls: 2,
		},
		{
			name: "each candidate gets a fresh reconfirm",
			scans: [][]procscan.Proc{
				{{PID: 42, StartedAt: old}, {PID: 43, StartedAt: old}},
				{{PID: 42, StartedAt: old}, {PID: 43, StartedAt: old}},
				{{PID: 42, StartedAt: old}, {PID: 43, StartedAt: installedAt.Add(-30 * time.Minute)}},
			},
			wired:         true,
			wantKilled:    []int{42},
			wantScanCalls: 3,
			wantKillCalls: 1,
			wantDue:       true,
		},
		{
			name: "kill failure keeps launch backoff",
			scans: [][]procscan.Proc{
				{{PID: 42, StartedAt: old}},
				{{PID: 42, StartedAt: old}},
			},
			wired:         true,
			killErr:       errors.New("kill failed"),
			wantScanCalls: 2,
			wantKillCalls: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := swapFPAppReapSeams(t, bin, tc.scans, tc.killErr)
			s := newFPAppReapServer(io.Discard)
			if !tc.wired {
				s.fpSynth = nil
			}
			s.fpConsentPending.Store(tc.consent)
			s.bookFPAppEnsure(time.Now())

			s.reconcileStaleFPApp(context.Background())

			if !equalPIDs(r.killed, tc.wantKilled) {
				t.Fatalf("killed %v, want %v", r.killed, tc.wantKilled)
			}
			if r.scanCalls != tc.wantScanCalls {
				t.Fatalf("scanned %d times, want %d", r.scanCalls, tc.wantScanCalls)
			}
			if r.killCalls != tc.wantKillCalls {
				t.Fatalf("called kill %d times, want %d", r.killCalls, tc.wantKillCalls)
			}
			s.ledMu.Lock()
			gotDue := s.led.due(fpAppPolicy, fpAppResource, time.Now())
			s.ledMu.Unlock()
			if gotDue != tc.wantDue {
				t.Fatalf("fp.app.ensure due = %v, want %v", gotDue, tc.wantDue)
			}
		})
	}
}

func TestReconcileStaleFPAppLogsKill(t *testing.T) {
	bin, installedAt := fpAppBinary(t)
	stale := []procscan.Proc{{PID: 42, StartedAt: installedAt.Add(-time.Hour)}}
	swapFPAppReapSeams(t, bin, [][]procscan.Proc{stale, stale}, nil)
	var buf bytes.Buffer
	s := newFPAppReapServer(&buf)

	s.reconcileStaleFPApp(context.Background())

	if !strings.Contains(buf.String(), "killed stale companion app pid 42") {
		t.Fatalf("log %q missing the kill line", buf.String())
	}
}

func equalPIDs(got, want []int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
