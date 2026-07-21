package pool

import (
	"context"
	"errors"
	"testing"

	"github.com/yasyf/cc-pool/internal/procscan"
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

func TestNewManagerAcceptsExactInlineSourceOwner(t *testing.T) {
	st := openTestStore(t)
	dummy := &Manager{}
	owner := bindTestWorkerAuthority(t, dummy, "inline-source-owner")
	owner.RecoveryClass = proc.RecoverySourceOwner
	owner.ProcessGroup = true
	owner.SessionID = owner.PID
	authority := newInlineWorkerAuthority(
		rejectingTestTaskRunner{}, owner.Executable, owner,
	)
	manager, err := NewManager(
		st,
		&fakeOAuth{},
		func(context.Context) ([]procscan.Session, error) { return nil, nil },
		authority,
	)
	if err != nil {
		t.Fatal(err)
	}
	if manager.workerAuthority == nil || !manager.workerAuthority.inline {
		t.Fatal("inline source owner was not retained")
	}
}
