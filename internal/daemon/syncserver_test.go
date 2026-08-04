package daemon

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/workerexec"
)

func TestIsSyncHelperInvocationRequiresExactArgv(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "exact", args: []string{syncHelperArgument, "/opt/homebrew/bin/synckitd"}, want: true},
		{name: "missing executable", args: []string{syncHelperArgument}, want: false},
		{name: "extra argument", args: []string{syncHelperArgument, "/x", "/y"}, want: false},
		{name: "other role", args: []string{"__other", "/x"}, want: false},
		{name: "empty", args: nil, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsSyncHelperInvocation(tt.args); got != tt.want {
				t.Fatalf("IsSyncHelperInvocation(%v) = %t, want %t", tt.args, got, tt.want)
			}
		})
	}
}

func TestScopeRunnerRequiresPositiveTotalTimeout(t *testing.T) {
	runner := scopeRunner{scope: daemonTestScope(t)}
	_, err := runner.Run(t.Context(), workerexec.CommandRequest{Path: "/usr/bin/true"})
	if err == nil || !strings.Contains(err.Error(), "positive total timeout") {
		t.Fatalf("zero-timeout run = %v, want a timeout refusal", err)
	}
}

func TestScopeRunnerRunsOneBoundedCommand(t *testing.T) {
	runner := scopeRunner{scope: daemonTestScope(t)}
	result, err := runner.Run(t.Context(), workerexec.CommandRequest{
		Path: "/bin/echo", Args: []string{"bounded"}, TotalTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(result.Stdout)) != "bounded" || result.ExitCode != 0 {
		t.Fatalf("run result = %+v", result)
	}
}

func TestSyncEnabledMeta(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	server := &Server{cl: newClaims(), m: &pool.Manager{Store: state}}

	if on, err := server.syncEnabled(); err != nil || on {
		t.Fatalf("syncEnabled with no meta = %v (err %v), want false", on, err)
	}
	if err := state.SetMeta(metaSyncEnabled, "1"); err != nil {
		t.Fatal(err)
	}
	if on, err := server.syncEnabled(); err != nil || !on {
		t.Fatalf("syncEnabled after set 1 = %v (err %v), want true", on, err)
	}
}
