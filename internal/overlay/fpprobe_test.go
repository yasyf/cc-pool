package overlay

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/yasyf/fusekit/fileproviderd"
)

// fakeProber scripts one control-op verdict for FPDomainProbe: a byte count (via
// bytes, nil-safe through hasBytes) or an error.
type fakeProber struct {
	bytes    *int64
	err      error
	gotDir   string
	gotCalls int
}

func (f *fakeProber) ProbeDomain(_ context.Context, accountDir string) (*int64, error) {
	f.gotCalls++
	f.gotDir = accountDir
	return f.bytes, f.err
}

func ptr(n int64) *int64 { return &n }

func TestFPDomainProbeClassifies(t *testing.T) {
	cases := []struct {
		name    string
		bytes   *int64
		err     error
		want    error   // nil means a healthy (non-empty) read
		notWant []error // forbidden verdicts a case must never be confused with
	}{
		{
			name:    "bytes read is healthy",
			bytes:   ptr(7),
			want:    nil,
			notWant: []error{ErrFPProbeMissing, ErrFPProbeEmpty, ErrFPProbeWedged, ErrFPProbeNoVerdict},
		},
		{
			name:    "absent .claude.json is missing, never wedged or no-verdict",
			bytes:   nil,
			want:    ErrFPProbeMissing,
			notWant: []error{ErrFPProbeWedged, ErrFPProbeEmpty, ErrFPProbeNoVerdict},
		},
		{
			name:    "zero-byte served file is empty, never missing or wedged",
			bytes:   ptr(0),
			want:    ErrFPProbeEmpty,
			notWant: []error{ErrFPProbeMissing, ErrFPProbeWedged, ErrFPProbeNoVerdict},
		},
		{
			name:    "domain-not-serving is wedged, never missing or no-verdict",
			err:     fmt.Errorf("state domain acct-01: %w", fileproviderd.ErrDomainNotServing),
			want:    ErrFPProbeWedged,
			notWant: []error{ErrFPProbeMissing, ErrFPProbeEmpty, ErrFPProbeNoVerdict},
		},
		{
			name:    "no-domain is missing (control-plane repair), never wedged",
			err:     fmt.Errorf("state domain acct-01: %w", fileproviderd.ErrNoDomain),
			want:    ErrFPProbeMissing,
			notWant: []error{ErrFPProbeWedged, ErrFPProbeEmpty, ErrFPProbeNoVerdict},
		},
		{
			name:    "busy is no-verdict, never wedged or missing",
			err:     fmt.Errorf("probe: %w", fileproviderd.ErrBusy),
			want:    ErrFPProbeNoVerdict,
			notWant: []error{ErrFPProbeWedged, ErrFPProbeMissing, ErrFPProbeEmpty},
		},
		{
			name:    "app-unavailable is no-verdict, never wedged",
			err:     fmt.Errorf("probe: %w", fileproviderd.ErrAppUnavailable),
			want:    ErrFPProbeNoVerdict,
			notWant: []error{ErrFPProbeWedged, ErrFPProbeMissing, ErrFPProbeEmpty},
		},
		{
			name:    "op-unsupported (old app) is no-verdict, never wedged",
			err:     fmt.Errorf("probe: %w", fileproviderd.ErrOpUnsupported),
			want:    ErrFPProbeNoVerdict,
			notWant: []error{ErrFPProbeWedged, ErrFPProbeMissing, ErrFPProbeEmpty},
		},
		{
			name:    "unrecognized error is no-verdict, never a strike",
			err:     errors.New("some transport hiccup"),
			want:    ErrFPProbeNoVerdict,
			notWant: []error{ErrFPProbeWedged, ErrFPProbeMissing, ErrFPProbeEmpty},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &fakeProber{bytes: tc.bytes, err: tc.err}
			err := FPDomainProbe(context.Background(), p, "/p/acct-01")
			if p.gotCalls != 1 {
				t.Fatalf("ProbeDomain calls = %d, want 1", p.gotCalls)
			}
			if p.gotDir != "/p/acct-01" {
				t.Fatalf("ProbeDomain dir = %q, want /p/acct-01", p.gotDir)
			}
			if tc.want == nil {
				if err != nil {
					t.Fatalf("FPDomainProbe = %v, want nil (healthy)", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("errors.Is(err, %v) = false; err = %v", tc.want, err)
			}
			for _, bad := range tc.notWant {
				if errors.Is(err, bad) {
					t.Errorf("err must not be %v; err = %v", bad, err)
				}
			}
		})
	}
}
