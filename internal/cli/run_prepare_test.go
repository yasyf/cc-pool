package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/fileproviderd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

type fakePreparer struct {
	err         error
	calls       int
	gotDeadline time.Duration
}

func (f *fakePreparer) PrepareDomain(_ context.Context, _ string, d time.Duration) error {
	f.calls++
	f.gotDeadline = d
	return f.err
}

// TestPrepareFPForLaunch pins the P7 launch gate mapping: non-FP is a no-op;
// ErrOpUnsupported surfaces the loud cask-upgrade error; a definitive not-serving
// names the account (forced) or points at a re-run (auto); a busy/unreachable app
// fails THIS launch; an unknown error fails closed. run.go calls this BEFORE the
// session lease, so a failure aborts with no lease, no banner, and no exec.
func TestPrepareFPForLaunch(t *testing.T) {
	fpAcct := store.Account{ID: 1, ConfigDir: "/x/acct-01", OverlayKind: string(fkoverlay.BackendFileProvider)}
	symAcct := store.Account{ID: 2, ConfigDir: "/x/acct-02", OverlayKind: string(fkoverlay.BackendSymlink)}

	cases := []struct {
		name      string
		acct      store.Account
		forced    bool
		prepErr   error
		wantErr   bool
		wantCall  bool
		errSubstr string
		wantIs    error
	}{
		{name: "non-FP account is a no-op", acct: symAcct, wantErr: false, wantCall: false},
		{name: "success proceeds", acct: fpAcct, prepErr: nil, wantErr: false, wantCall: true},
		{
			name: "op-unsupported is a loud upgrade error", acct: fpAcct,
			prepErr: fmt.Errorf("upgrade the cc-pool-status cask: %w", fileproviderd.ErrOpUnsupported),
			wantErr: true, wantCall: true, errSubstr: "upgrade", wantIs: fileproviderd.ErrOpUnsupported,
		},
		{
			name: "not-serving forced names the account", acct: fpAcct, forced: true,
			prepErr: fmt.Errorf("prep: %w", fileproviderd.ErrDomainNotServing),
			wantErr: true, wantCall: true, errSubstr: "acct-01", wantIs: fileproviderd.ErrDomainNotServing,
		},
		{
			name: "not-serving auto points at a re-run", acct: fpAcct, forced: false,
			prepErr: fmt.Errorf("prep: %w", fileproviderd.ErrDomainNotServing),
			wantErr: true, wantCall: true, errSubstr: "another account", wantIs: fileproviderd.ErrDomainNotServing,
		},
		{
			name: "busy fails this launch, no wedge", acct: fpAcct,
			prepErr: fmt.Errorf("prep: %w", fileproviderd.ErrBusy),
			wantErr: true, wantCall: true, errSubstr: "busy", wantIs: fileproviderd.ErrBusy,
		},
		{
			name: "app-unavailable fails this launch", acct: fpAcct,
			prepErr: fmt.Errorf("prep: %w", fileproviderd.ErrAppUnavailable),
			wantErr: true, wantCall: true, wantIs: fileproviderd.ErrAppUnavailable,
		},
		{
			name: "unknown error fails closed", acct: fpAcct,
			prepErr: errors.New("weird transport hiccup"),
			wantErr: true, wantCall: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fp := &fakePreparer{err: tc.prepErr}
			prev := fpLaunchPreparer
			fpLaunchPreparer = func() (fpDomainPreparer, error) { return fp, nil }
			t.Cleanup(func() { fpLaunchPreparer = prev })

			err := prepareFPForLaunch(context.Background(), tc.acct, tc.forced)

			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if (fp.calls > 0) != tc.wantCall {
				t.Fatalf("PrepareDomain calls = %d, wantCall = %v", fp.calls, tc.wantCall)
			}
			if tc.wantCall && fp.gotDeadline != fpLaunchPrepareDeadline {
				t.Fatalf("PrepareDomain deadline = %v, want %v", fp.gotDeadline, fpLaunchPrepareDeadline)
			}
			if tc.errSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.errSubstr)) {
				t.Fatalf("err %v must contain %q", err, tc.errSubstr)
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Fatalf("errors.Is(err, %v) = false; err = %v", tc.wantIs, err)
			}
		})
	}
}
