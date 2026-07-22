package pool

import (
	"errors"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/proc"
)

func TestValidateHostSyncWorkerOwnerRequiresExactCurrentSessionLeader(t *testing.T) {
	identity := proc.Identity{PID: 42, StartTime: "1.0", Boot: "boot", Comm: "worker"}
	owner := proc.Record{
		RecoveryClass: proc.RecoverySourceOwner,
		PID:           42, StartTime: "1.0", Boot: "boot", Comm: "worker",
		Generation: "generation", ProcessGroup: true, SessionID: 42,
	}
	if err := validateHostSyncWorkerOwner(owner, identity, 42); err != nil {
		t.Fatal(err)
	}

	wrongIdentity := identity
	wrongIdentity.StartTime = "2.0"
	if err := validateHostSyncWorkerOwner(owner, wrongIdentity, 42); !errors.Is(err, proc.ErrIdentityChanged) {
		t.Fatalf("identity mismatch = %v, want ErrIdentityChanged", err)
	}
	if err := validateHostSyncWorkerOwner(owner, identity, 41); err == nil {
		t.Fatal("non-session-leader host-sync worker was admitted")
	}
	owner.ProcessGroup = false
	owner.SessionID = 0
	if err := validateHostSyncWorkerOwner(owner, identity, 42); err == nil {
		t.Fatal("non-process-group owner was admitted")
	}
}

func TestHostSyncWorkersUseDistinctV1ProcessLedger(t *testing.T) {
	if HostSyncWorkerStorePath() == DisposableWorkerStorePath() {
		t.Fatal("host-sync and daemon workers share a process ledger")
	}
	if got := HostSyncWorkerStorePath(); !strings.HasSuffix(got, "hostsync-workers-v1.json") {
		t.Fatalf("host-sync worker ledger = %q, want v1 path", got)
	}
}
